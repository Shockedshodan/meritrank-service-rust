use crate::error::GraphManipulationError;
use crate::graph::{GraphSingleton, NodeId, GRAPH};
use crate::lib_graph::Weight;
use nng::{Message, Protocol, Socket};

mod graph; // This module is for graph related operations
// #[cfg(feature = "shared")]
// mod shared; // This module contains shared data structures

mod error;
mod lib_graph; // This module contains graph related operations and data structures

const SERVICE_URL: &str = "tcp://127.0.0.1:10234";

fn main() -> Result<(), Box<dyn std::error::Error + 'static>> {
    println!("Starting server at {SERVICE_URL}");

    let s = Socket::new(Protocol::Rep0)?;
    s.listen(SERVICE_URL)?;

    loop {
        let req: Message = s.recv()?;
        let slice = req.as_slice();

        let result: Result<Vec<u8>, Box<dyn std::error::Error>> = if let Ok(((("src", "=", ego), ("dest", "=", target)), ())) =
            rmp_serde::from_slice(slice)
        {
            mr_node_score(ego, target)
        } else if let Ok(((("src", "=", ego), ), ())) = rmp_serde::from_slice(slice) {
            mr_scores(ego)
        } else if let Ok((((subject, object, amount), ), ())) = rmp_serde::from_slice(slice) {
            mr_edge(subject, object, amount)
        } else {
            let err: String = format!("Error: Cannot understand request {:?}", &req[..]);
            eprintln!("{}", err);
            Err(err.into())
        };

        let reply: Vec<u8> =
            result
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
