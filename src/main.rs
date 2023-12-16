//use std::slice::Iter;
use std::thread;
use std::time::Duration;
use std::env::var;
use lazy_static::lazy_static;
use std::string::ToString;
use crate::error::GraphManipulationError;
use crate::graph::{GraphSingleton, NodeId, GRAPH};
use crate::lib_graph::{MeritRank, MyDiGraph, MyGraph, Weight};
use nng::{Aio, AioResult, Context, Message, Protocol, Socket};

use itertools::Itertools;
use std::collections::HashMap;
use std::ops::Index;
use petgraph::graph::{EdgeIndex, NodeIndex};
use petgraph::visit::EdgeRef;


mod graph; // This module is for graph related operations
// #[cfg(feature = "shared")]
// mod shared; // This module contains shared data structures

mod error;
mod lib_graph; // This module contains graph related operations and data structures

lazy_static! {
    static ref SERVICE_URL: String =
        var("RUST_SERVICE_URL")
            .unwrap_or("tcp://127.0.0.1:10234".to_string());

    static ref EMPTY_RESULT: Vec<u8> = {
        const EMPTY_ROWS_VEC: Vec<(&str, &str, f64)> = Vec::new();
        rmp_serde::to_vec(&EMPTY_ROWS_VEC).unwrap()
    };
}

// const PARALLEL: usize = 128;
fn main() -> Result<(), Box<dyn std::error::Error + 'static>> {
    match var("RUST_SERVICE_PARALLEL") {
        Ok(s) => {
            let parallel =
                s.parse()
                    .expect("Error: RUST_SERVICE_PARALLEL env. isn't a number!");
            main_async(parallel)
        },
        _ => main_sync()
    }
}

fn main_sync() -> Result<(), Box<dyn std::error::Error + 'static>> {
    println!("Starting server at {}", *SERVICE_URL);

    let s = Socket::new(Protocol::Rep0)?;
    s.listen(&SERVICE_URL)?;

    loop {
        let request: Message = s.recv()?;

        let reply: Vec<u8> =
            process(request)
                .map(|msg| msg)
                .unwrap_or_else(|e| {
                    println!("{}", e);
                    let s: String = e.to_string();
                    rmp_serde::to_vec(&s).unwrap()
                });

        let _ = s.send(reply.as_slice()).map_err(|(_, e)| e)?;
    }
    // Ok(())
}

fn main_async(parallel: usize) -> Result<(), Box<dyn std::error::Error + 'static>> {
    println!("Starting server at {}. PARALLEL={parallel}", *SERVICE_URL);

    let s = Socket::new(Protocol::Rep0)?;

    // Create all of the worker contexts
    let workers: Vec<_> = (0..parallel)
        .map(|_| {
            let ctx = Context::new(&s)?;
            let ctx_clone = ctx.clone();
            let aio = Aio::new(move |aio, res| worker_callback(aio, &ctx_clone, res))?;
            Ok((aio, ctx))
        })
        .collect::<Result<_, nng::Error>>()?;

    // Only after we have the workers do we start listening.
    s.listen(&SERVICE_URL)?;

    // Now start all of the workers listening.
    for (a, c) in &workers {
        c.recv(a)?;
    }

    thread::sleep(Duration::from_secs(60 * 60 * 24 * 365)); // 1 year

    Ok(())
}

/// Callback function for workers.
fn worker_callback(aio: Aio, ctx: &Context, res: AioResult) {
    match res {
        // We successfully sent the message, wait for a new one.
        AioResult::Send(Ok(_)) => ctx.recv(&aio).unwrap(),

        // We successfully received a message.
        AioResult::Recv(Ok(req)) => {
            let msg: Vec<u8> = process(req).unwrap_or_else(|e| {
                println!("{}", e);
                let s: String = e.to_string();
                rmp_serde::to_vec(&s).unwrap()
            });
            ctx.send(&aio, msg.as_slice()).unwrap();
        }

        AioResult::Sleep(_) =>
            println!("Slept!"),

        // Anything else is an error and we will just panic.
        AioResult::Send(Err(e)) =>
            panic!("Error: {}", e.1),

        AioResult::Recv(Err(e)) =>
            panic!("Error: {}", e)
    }
}

fn process(req: Message) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let slice = req.as_slice();

    if let Ok(((("src", "=", ego), ("dest", "=", target)), ())) =
        rmp_serde::from_slice(slice)
    {
        mr_node_score(ego, target)
    } else if let Ok(((("src", "=", ego), ), ())) = rmp_serde::from_slice(slice) {
        mr_scores(ego)
    } else if let Ok((((subject, object, amount), ), ())) = rmp_serde::from_slice(slice) {
        mr_edge(subject, object, amount)
    } else if let Ok(((("src", "delete", ego), ("dest", "delete", target)), ())) = rmp_serde::from_slice(slice) {
        Ok(mr_delete_edge(ego, target).map(|_| EMPTY_RESULT.to_vec())?)
    } else if let Ok(((("src", "delete", ego), ), ())) = rmp_serde::from_slice(slice) {
        Ok(mr_delete_node(ego).map(|_| EMPTY_RESULT.to_vec())?)
    } else if let Ok((((ego, "gravity", focus), ), ())) = rmp_serde::from_slice(slice) {
        mr_gravity_graph(ego, focus)
    } else if let Ok((((ego, "gravity_nodes", focus), ), ())) = rmp_serde::from_slice(slice) {
        mr_gravity_nodes(ego, focus)
    } else {
        let err: String = format!("Error: Cannot understand request {:?}", &req[..]);
        eprintln!("{}", err);
        Err(err.into())
    }
}

fn mr_node_score(ego: &str, target: &str) -> Result<Vec<u8>, Box<dyn std::error::Error + 'static>> {
    let mut rank = GraphSingleton::get_rank()?;
    let ego_id: NodeId = GraphSingleton::node_name_to_id(ego)?;
    let target_id: NodeId = GraphSingleton::node_name_to_id(target)?;
    let _ = rank.calculate(ego_id, 10)?;
    let w: Weight = rank.get_node_score(ego_id, target_id)?;
    let result: Vec<(&str, &str, f64)> = [(ego, target, w)].to_vec();
    let v: Vec<u8> = rmp_serde::to_vec(&result)?;
    Ok(v)
}

fn mr_scores(ego: &str) -> Result<Vec<u8>, Box<dyn std::error::Error + 'static>> {
    let mut rank = GraphSingleton::get_rank()?;
    let node_id: NodeId = GraphSingleton::node_name_to_id(ego)?;
    let _ = rank.calculate(node_id, 10)?;
    let result: Vec<(&str, String, Weight)> = rank
        .get_ranks(node_id, None)?
        .into_iter()
        .map(|(n, w)| {
            (
                ego,
                GraphSingleton::node_id_to_name(n).unwrap_or(n.to_string()),
                w,
            )
        })
        .collect();
    let v: Vec<u8> = rmp_serde::to_vec(&result)?;
    Ok(v)
}

fn mr_edge(
    subject: &str,
    object: &str,
    amount: f64,
) -> Result<Vec<u8>, Box<dyn std::error::Error + 'static>> {
    meritrank_add(subject, object, amount)?;
    let result: Vec<(&str, &str, f64)> = [(subject, object, amount)].to_vec();
    let v: Vec<u8> = rmp_serde::to_vec(&result)?;
    Ok(v)
}

// todo: move into graph.rs
fn meritrank_add(
    subject: &str,
    object: &str,
    amount: f64,
) -> Result<(), GraphManipulationError> {
    match GRAPH.lock() {
        Ok(mut graph) => {
            let subject_id = graph.get_node_id(subject);
            let object_id = graph.get_node_id(object);

            graph
                .borrow_graph_mut()
                .add_edge((&subject_id).into(), (&object_id).into(), amount)?;
            Ok(())
        }
        Err(e) => Err(GraphManipulationError::MutexLockFailure(format!(
            "Mutex lock error: {}",
            e
        ))),
    }
}

// todo: move into graph.rs
fn mr_delete_edge(
    subject: &str,
    object: &str,
) -> Result<(), GraphManipulationError> {
    // meritrank_add(ego, target, 0)
    match GRAPH.lock() {
        Ok(mut graph) => {
            let subject_id = graph.get_node_id(subject);
            let object_id = graph.get_node_id(object);

            graph
                .borrow_graph_mut()
                .remove_edge((&subject_id).into(), (&object_id).into());
            Ok(())
        }
        Err(e) => Err(GraphManipulationError::MutexLockFailure(format!(
            "Mutex lock error: {}",
            e
        ))),
    }
}

// todo: move into graph.rs
fn mr_delete_node(
    ego: &str,
) -> core::result::Result<(), GraphManipulationError> {
    match GRAPH.lock() {
        Ok(mut graph) => {
            let ego_id = graph.get_node_id(ego);
            let graph: &mut MyGraph = graph.borrow_graph_mut();
            graph
                .neighbors(&ego_id)
                .iter()
                .for_each(|n| graph.remove_edge((&ego_id).into(), n));
            Ok(())
        }
        Err(e) => Err(GraphManipulationError::MutexLockFailure(format!(
            "Mutex lock error: {}",
            e
        ))),
    }
}

fn mr_gravity_graph(
    //&self,
    ego: &str,
    focus: &str
) -> core::result::Result<Vec<u8>, Box<dyn std::error::Error + 'static>> {
    let (result, _) = gravity_graph(ego, focus, false, 3)?;
    let v: Vec<u8> = rmp_serde::to_vec(&result)?;
    Ok(v)
}

fn mr_gravity_nodes(
    //&self,
    ego: &str,
    focus: &str
) -> core::result::Result<Vec<u8>, Box<dyn std::error::Error + 'static>> {
    // TODO: change HashMap to string pairs here!?
    let (_, hash_map) = gravity_graph(ego, focus, false, 3)?;
    let result: Vec<_> = hash_map.iter().collect();
    let v: Vec<u8> = rmp_serde::to_vec(&result)?;
    Ok(v)
}

fn gravity_graph(
    ego: &str,
    focus: &str,
    positive_only: bool,
    limit: usize /* | None */
) -> Result<(Vec<(String, String, Weight)>, HashMap<String, Weight>), GraphManipulationError> {
    match GRAPH.lock() {
        Ok(mut graph) => {
            let mut rank: MeritRank = MeritRank::new(graph.borrow_graph().clone())?;

            let mut copy: MyGraph = MyGraph::new();
            let source_graph: &MyGraph = graph.borrow_graph();

            // focus_id in graph
            let focus_id: NodeId = graph.node_name_to_id_unsafe(focus)?;
            let focus_vector: Vec<(NodeId, NodeId, Weight)> =
                source_graph.edges(focus_id).into_iter().flatten().collect();
            for (a_id, b_id, w_ab) in focus_vector {

                //let a: String = graph.node_id_to_name_unsafe(a_id)?;
                let b: String = graph.node_id_to_name_unsafe(b_id)?;

                if b.starts_with("U") {
                    if positive_only && rank.get_node_score(a_id, b_id)?<=0f64 {
                        continue
                    }
                    // assert!( get_edge(a, b) != None);

                    copy.add_edge(&a_id, &b_id, w_ab);

                } else if b.starts_with("C") || b.starts_with("B"){
                    // ? # For connections user-> comment | beacon -> user,
                    // ? # convolve those into user->user

                    let v_b : Vec<(NodeId, NodeId, Weight)> =
                        source_graph.edges(b_id).into_iter().flatten().collect();

                    for (_, c_id, w_bc) in v_b {
                        if positive_only && w_bc<=0.0f64 {
                            continue
                        }
                        if c_id==a_id || c_id==b_id { // note: c_id==b_id not in Python version !?
                            continue
                        }

                        let c: String = graph.node_id_to_name_unsafe(c_id)?;

                        if !c.starts_with("U") {
                            continue
                        }
                        // let w_ac = self.get_transitive_edge_weight(a, b, c);
                        // TODO: proper handling of negative edges
                        // Note that enemy of my enemy is not my friend.
                        // Though, this is pretty irrelevant for our current case
                        // where comments can't have outgoing negative edges.
                        // return w_ab * w_bc * (-1 if w_ab < 0 and w_bc < 0 else 1)
                        let w_ac: f64 =
                            w_ab * w_bc * (if w_ab < 0.0f64 && w_bc < 0.0f64 { -1.0f64 } else { 1.0f64 });

                        copy.add_edge(&a_id, &c_id, w_ac);
                    }
                }
            }

            // self.remove_outgoing_edges_upto_limit(G, ego, focus, limit or 3):
            // neighbours = list(dest for src, dest in G.out_edges(focus))
            let neighbours: Vec<(EdgeIndex, NodeIndex, NodeId)> = copy.outgoing(&focus_id);

            // ego_id in graph
            let ego_id: NodeId = graph.node_name_to_id_unsafe(ego)?;

            let mut sorted: Vec<(Weight, (&EdgeIndex, &NodeIndex))> =
                neighbours
                    .iter()
                    .map(|(edge_index, node_index, node_id)| {
                        let w: f64 = rank.get_node_score(ego_id, *node_id).unwrap_or(0f64);
                        (w, (edge_index, node_index))
                    })
                    .collect::<Vec<_>>();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
            //sort by weight

            // for dest in sorted(neighbours, key=lambda x: self.get_node_score(ego, x))[limit:]:
            let limited: Vec<&(&EdgeIndex, &NodeIndex)> =
                sorted.iter().map(|(_, tuple)| tuple).take(limit).collect();

            for (edge_index, node_index) in limited {
                let node_id = copy.index2node(**node_index);
                copy.remove_edge(&ego_id, &node_id);
                //G.remove_node(dest) // ???
            }

            // add_path_to_graph(G, ego, focus)
            let path: Vec<NodeId> =
                copy
                    .shortest_path(&ego_id, &focus_id)
                    .unwrap_or(Vec::new());
            // Note: no loops or "self edges" are expected in the path
            let ok: Result<(), GraphManipulationError> = {
                let v3: Vec<&NodeId> = path.iter().take(3).collect::<Vec<&NodeId>>();
                if let Some((a, b, c)) = v3.clone().into_iter().collect_tuple() {
                    // # merge transitive edges going through comments and beacons

                    // ???
                    /*
                    if c is None and not (a.startswith("C") or a.startswith("B")):
                        new_edge = (a, b, self.get_edge(a, b))
                    elif ... */

                    let a_name = graph.node_id_to_name_unsafe(*a)?;
                    let b_name = graph.node_id_to_name_unsafe(*b)?;
                    let c_name = graph.node_id_to_name_unsafe(*c)?;
                    if b_name.starts_with("C") || b_name.starts_with("B") {
                        let w_ab =
                            copy.edge_weight(a, b)
                                .ok_or(GraphManipulationError::WeightExtractionFailure(
                                    format!("Cannot extrac tweight from {} to {}",
                                        a_name, b_name
                                    )
                                ))?;
                        let w_bc =
                            copy.edge_weight(b, c)
                                .ok_or(GraphManipulationError::WeightExtractionFailure(
                                    format!("Cannot extrac tweight from {} to {}",
                                            a_name, c_name
                                    )
                                ))?;
                        // get_transitive_edge_weight
                        let w_ac: f64 =
                            w_ab * w_bc * (if w_ab < 0.0f64 && w_bc < 0.0f64 { -1.0f64 } else { 1.0f64 });
                        copy.add_edge(a, c, w_ac)?;
                        Ok(())
                    } else if a_name.starts_with("U") {
                        let weight =
                            copy.edge_weight(a, b)
                                .ok_or(GraphManipulationError::WeightExtractionFailure(
                                    format!("Cannot extrac tweight from {} to {}",
                                            a_name, b_name
                                    )
                                ))?;
                        copy.add_edge(a, b, weight)?;
                        Ok(())
                    } else {
                        Ok(())
                    }
                } else if let Some((a, b)) = v3.clone().into_iter().collect_tuple()
                {
                    /*
                    # Add the final (and only)
                    final_nodes = ego_to_focus_path[-2:]
                    final_edge = (*final_nodes, self.get_edge(*final_nodes))
                    edges.append(final_edge)
                    */
                    // ???
                    let a_name = graph.node_id_to_name_unsafe(*a)?;
                    let b_name = graph.node_id_to_name_unsafe(*b)?;
                    let weight =
                        copy.edge_weight(a, b)
                            .ok_or(GraphManipulationError::WeightExtractionFailure(
                                format!("Cannot extrac tweight from {} to {}",
                                        a_name, b_name
                                )
                            ))?;
                    copy.add_edge(a, b, weight)?;
                    Ok(())
                } else if v3.len()==1 {
                    // ego == focus ?
                    // do nothing
                    Ok(())
                } else if v3.is_empty() {
                    // No path found, so add just the focus node to show at least something
                    copy.add_node(lib_graph::node::Node::new(focus_id));
                    Ok(())
                } else {
                    Err(GraphManipulationError::DataExtractionFailure(
                        "Should never be here (v3)".to_string()
                    ))
                }
            };
            let _ = ok?;

            // self.remove_self_edges(copy);
            // todo: just not let them pass into the graph

            let (nodes, edges) = copy.all();

            let table: Vec<(String, String, f64)> =
                edges
                    .iter()
                    .map(|(n1, n2, weight)| {
                        let name1 = graph.node_id_to_name_unsafe(*n1)?;
                        let name2 = graph.node_id_to_name_unsafe(*n2)?;
                        Ok::<(String, String, f64), GraphManipulationError>( (name1, name2, *weight) )
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>();

            let nodes_dict: HashMap<String, Weight> =
                nodes
                    .iter()
                    .map(|node_id| {
                        let name = graph.node_id_to_name_unsafe(*node_id)?;
                        let score = rank.get_node_score(ego_id, *node_id)?;
                        Ok::<(String, Weight), GraphManipulationError>( (name, score) )
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
                    .flatten()
                    .collect::<HashMap<String, Weight>>();

            Ok( (table, nodes_dict) )
        }
        Err(e) => Err(GraphManipulationError::MutexLockFailure(format!(
            "Mutex lock error: {}",
            e
        ))),
    }
}
