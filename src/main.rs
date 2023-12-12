use std::slice::Iter;
use std::thread;
use std::time::Duration;
use crate::error::GraphManipulationError;
use crate::graph::{GraphSingleton, NodeId, GRAPH};
use crate::lib_graph::{MeritRank, MyDiGraph, MyGraph, Weight};
use nng::{Aio, AioResult, Context, Message, Protocol, Socket};

mod graph; // This module is for graph related operations
// #[cfg(feature = "shared")]
// mod shared; // This module contains shared data structures

mod error;
mod lib_graph; // This module contains graph related operations and data structures

const SERVICE_URL: &str = "tcp://127.0.0.1:10234";

fn EMPTY_RESULT() -> Vec<u8> {
    const EMPTY_ROWS_VEC: Vec<(&str, &str, f64)> = Vec::new();
    rmp_serde::to_vec(&EMPTY_ROWS_VEC).unwrap()
}

fn main() -> Result<(), Box<dyn std::error::Error + 'static>> {
    main_async()
}

fn main_sync() -> Result<(), Box<dyn std::error::Error + 'static>> {
    println!("Starting server at {SERVICE_URL}");

    let s = Socket::new(Protocol::Rep0)?;
    s.listen(SERVICE_URL)?;

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

const PARALLEL: usize = 128;

fn main_async() -> Result<(), Box<dyn std::error::Error + 'static>> {
    println!("Starting server at {SERVICE_URL}");

    let s = Socket::new(Protocol::Rep0)?;

    // Create all of the worker contexts
    let workers: Vec<_> = (0..PARALLEL)
        .map(|_| {
            let ctx = Context::new(&s)?;
            let ctx_clone = ctx.clone();
            let aio = Aio::new(move |aio, res| worker_callback(aio, &ctx_clone, res))?;
            Ok((aio, ctx))
        })
        .collect::<Result<_, nng::Error>>()?;

    // Only after we have the workers do we start listening.
    s.listen(SERVICE_URL)?;

    // Now start all of the workers listening.
    for (a, c) in &workers {
        c.recv(a)?;
    }

    thread::sleep(Duration::from_secs(60 * 60 * 24 * 365));

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
        Ok(mr_delete_edge(ego, target).map(|_| EMPTY_RESULT())?)
    } else if let Ok(((("src", "delete", ego), ), ())) = rmp_serde::from_slice(slice) {
        Ok(mr_delete_node(ego).map(|_| EMPTY_RESULT())?)
    } else if let Ok((ego, focus)) = rmp_serde::from_slice(slice) {
        Ok(mr_gravity_graph(ego, focus).map(|_| EMPTY_RESULT())?)
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
            let subject_id = graph.get_node_id(subject)?;
            let object_id = graph.get_node_id(object)?;

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
            let subject_id = graph.get_node_id(subject)?;
            let object_id = graph.get_node_id(object)?;

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
            let ego_id = graph.get_node_id(ego)?;
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


// =====
use std::collections::HashMap;
use petgraph::graph::{DiGraph, node_index, EdgeIndex, NodeIndex, edge_index};
use petgraph::graph::Edge;
use crate::lib_graph::node::Node; // , NodeId, Weight}
//use petgraph::graph::EdgeReference;
use petgraph::visit::EdgeRef;

/*
fn remove_outgoing_edges_upto_limit(
    //self,
    G: DiGraph<_, _>,
    ego,
    focus,
    limit
) = {
    neighbours = list(dest for src, dest in G.out_edges(focus))

    for dest in sorted(neighbours, key=lambda x: self.get_node_score(ego, x))[limit:]:
    G.remove_edge(focus, dest)
    G.remove_node(dest)
}
*/

/*
fn remove_self_edges(
    //self,
    G: DiGraph<_, _>
) = {
    for src, dest in list(G.edges()):
    if src == dest:
    G.remove_edge(src, dest)
}
*/

/*
def weight_fun(u, v, edge):
    w = edge['weight']
    if w > 0:
        return 1.0 / w
    return None

   def add_path_to_graph(self, G, ego, focus):
        if ego == focus:
            return
        ego_to_focus_path = nx.dijkstra_path(self._IncrementalMeritRank__graph, ego, focus, weight=weight_fun)
        ego_to_focus_path.append(None)

        edges = []
        for a, b, c in zip(ego_to_focus_path, ego_to_focus_path[1:], ego_to_focus_path[2:]):
            # merge transitive edges going through comments and beacons
            if c is None and not (a.startswith("C") or a.startswith("B")):
                new_edge = (a, b, self.get_edge(a, b))
            elif b.startswith("C") or b.startswith("B"):
                new_edge = (a, c, self.get_transitive_edge_weight(a, b, c))
            elif a.startswith("U"):
                new_edge = (a, b, self.get_edge(a, b))

            edges.append(new_edge)
        if len(ego_to_focus_path) == 2:
            # Add the final (and only)
            final_nodes = ego_to_focus_path[-2:]
            final_edge = (*final_nodes, self.get_edge(*final_nodes))
            edges.append(final_edge)
        G.add_weighted_edges_from(edges)

 */

fn mr_gravity_graph(
    //&self,
    ego: &str,
    focus: &str
) -> core::result::Result<String, GraphManipulationError> {
    let _ = gravity_graph(ego, focus, false, 3)?;
    Ok("ok".to_string())
}


fn gravity_graph(
    //&self,
    ego: &str,
    focus: &str,
    positive_only: bool,
    limit: usize /* | None */
) -> core::result::Result<(Vec<Edge<Weight>>, HashMap<String, Weight>), GraphManipulationError> {
    match GRAPH.lock() {
        Ok(mut graph) => {
            // GRAPH.lock() inside ! dead lock?
            // see:
            // https://github.com/rust-lang/rust/issues/32260
            // parking_lot::ReentrantMutex
            let mut rank: MeritRank = GraphSingleton::get_rank()?;

            let mut G: DiGraph<NodeId, Weight> = DiGraph::new();
            let mut GM: MyGraph = MyGraph::new();
            let source_graph: &MyGraph = graph.borrow_graph();

            /*
            for e in source_graph.all_edges() {
                let source: NodeIndex = e.source();
                let target: NodeIndex = e.target();
                if source==target {
                    continue
                }
                let w_ab = e.weight;
                let a_id: NodeId = source_graph.index2node(source);
                let b_id: NodeId = source_graph.index2node(target);
             */
            // TODO !!!
            let focus_id: NodeId = GraphSingleton::node_name_to_id(focus)?; // G.get_node_id(focus)?;
            let v_focus: Vec<(NodeId, NodeId, Weight)> =
                source_graph.edges(focus_id).into_iter().flatten().collect();
            for (a_id, b_id, w_ab) in v_focus {

                let b: String = GraphSingleton::node_id_to_name(b_id)?;

                if (b.starts_with("U")) {
                    if positive_only && rank.get_node_score(a_id, b_id)?<=0f64 {
                        continue
                    }
                    // assert!( self.get_edge(a, b) != None);
                    // G.add_edge(a, b, w_ab);
                    let a_idx = G.add_node(a_id);
                    let b_idx = G.add_node(b_id);
                    G.add_edge(a_idx, b_idx, w_ab);
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

                        let c: String = GraphSingleton::node_id_to_name(c_id)?;
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

                        // G.add_edge(a, c, w_ac);
                        let a_idx = G.add_node(a_id);
                        let c_idx = G.add_node(c_id);
                        G.add_edge(a_idx, c_idx, w_ac);
                    }
                }
            }

            let ego_id: NodeId = GraphSingleton::node_name_to_id(ego)?;

            // self.remove_outgoing_edges_upto_limit(G, ego, focus, limit or 3):
            // neighbours = list(dest for src, dest in G.out_edges(focus))
            let index: NodeIndex = GM.get_node_index(&focus_id).unwrap(); // TODO !!!
            let dir = petgraph::Direction::Outgoing;
            let out = G.edges_directed(index, dir).into_iter();
            let neighbours: Vec<(EdgeIndex, NodeIndex, NodeId)> =
                out // .out_edges(&focus_id)
                    .map(|e| (e.id(), e.target()))
                    .map(|(edge_idx, node_idx)|
                        (edge_idx, node_idx, source_graph.index2node(node_idx))
                    )
                    .collect();

            let mut sorted =
                neighbours
                    .iter()
                    .map(|(edge_index, node_index, node_id)| {
                        let w: f64 = rank.get_node_score(ego_id, *node_id).unwrap_or(0f64);
                        (w, (edge_index, node_index))
                    })
                    .collect::<Vec<_>>();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

            let limited: Vec<&(&EdgeIndex, &NodeIndex)> =
                sorted.iter().map(|(_, tuple)| tuple).take(limit).collect();

            for (edge_index, node_index) in limited {
                G.remove_edge(**edge_index);
                G.remove_node(**node_index);
            }

            /*
            for dest in sorted(neighbours, key=lambda x: self.get_node_score(ego, x))[limit:]:
            G.remove_edge(focus, dest)
            G.remove_node(dest)

            try:
                self.add_path_to_graph(G, ego, focus)
            except nx.exception.NetworkXNoPath:
            # No path found, so add just the focus node to show at least something
            G.add_node(focus)
            */

            // self.remove_self_edges(G);

            let (nodes, edges) = G.into_nodes_edges();
            let vec: Vec<(String, Weight)> = nodes.iter().map(|n| {
                let node_id: NodeId = n.weight;
                let node: String = GraphSingleton::node_id_to_name(node_id)?;
                let score: Weight = rank.get_node_score(ego_id, node_id)?;
                Ok::<(String, f64), GraphManipulationError>((node, score))
            }).collect::<Vec<_>>().into_iter().collect::<Result<Vec<_>,_>>()?;
            let nodes_dict: HashMap<String, Weight> = vec.into_iter().collect::<HashMap<_,_>>();

            Ok((edges, nodes_dict))
        }
        Err(e) => Err(GraphManipulationError::MutexLockFailure(format!(
            "Mutex lock error: {}",
            e
        ))),
    }
}
