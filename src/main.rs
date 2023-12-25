use std::slice::Iter;
use std::thread;
use std::time::Duration;
use std::env::var;
use lazy_static::lazy_static;
use std::string::ToString;
use crate::error::GraphManipulationError;
use crate::graph::{GraphSingleton, NodeId, GRAPH};
use crate::lib_graph::{MyGraph, Weight};
use nng::{Aio, AioResult, Context, Message, Protocol, Socket};

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
                .map(|msg| msg )
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
                .add_edge(subject_id.into(), object_id.into(), amount)?;
            Ok(())
        }
        Err(e) => Err(GraphManipulationError::MutexLockFailure(format!(
            "Mutex lock error: {}",
            e
        ))),
    }
}

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
                .remove_edge(subject_id.into(), object_id.into());
            Ok(())
        }
        Err(e) => Err(GraphManipulationError::MutexLockFailure(format!(
            "Mutex lock error: {}",
            e
        ))),
    }
}

fn mr_delete_node(
    ego: &str,
) -> core::result::Result<(), GraphManipulationError> {
    match GRAPH.lock() {
        Ok(mut graph) => {
            let ego_id = graph.get_node_id(ego)?;
            let graph: &mut MyGraph = graph.borrow_graph_mut();
            graph
                .neighbors(ego_id)
                .iter()
                .for_each(|n| graph.remove_edge(ego_id.into(), *n));
            Ok(())
        }
        Err(e) => Err(GraphManipulationError::MutexLockFailure(format!(
            "Mutex lock error: {}",
            e
        ))),
    }
}

