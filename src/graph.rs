// Standard library imports
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::rc::Rc;

// External crate imports
use lazy_static::lazy_static;
use petgraph::IntoWeightedEdge;


// Current crate (`crate::`) imports
pub use crate::error::GraphManipulationError;
// #[allow(unused_imports)]
// use crate::logger::Logger;

// Current crate (`crate::`) imports
pub use crate::lib_graph::NodeId;
use crate::lib_graph::{MeritRank, MyGraph};

use std::borrow::BorrowMut;

// Singleton instance
lazy_static! {
    pub static ref GRAPH: Arc<parking_lot::ReentrantMutex<GraphSingleton>> =
        Arc::new(parking_lot::ReentrantMutex::new(GraphSingleton::new()));
}
// see:
// https://github.com/rust-lang/rust/issues/32260
// parking_lot::ReentrantMutex


#[allow(dead_code)]
// GraphSingleton structure
pub struct GraphSingleton {
    graph: MyGraph,
    node_names: HashMap<String, NodeId>,
}

#[allow(dead_code)]
impl GraphSingleton {
    /// Constructor
    pub fn new() -> GraphSingleton {
        GraphSingleton {
            graph: MyGraph::new(),
            node_names: HashMap::new(),
        }
    }

    /// Get MeritRank object
    pub fn get_rank() -> Result<MeritRank, GraphManipulationError> {
        let lock = GRAPH.lock();
        let graph =  lock.borrow_graph();
        let merit_rank = MeritRank::new(graph.clone())?;
        Ok(merit_rank)
    }

    /// Borrow Node Names
    pub fn borrow_node_names(&self) -> &HashMap<String, NodeId> {
        &self.node_names
    }

    /// ???
    pub fn borrow_me_mut(&mut self) -> &mut GraphSingleton {
        self
    }


    /// Borrow Graph
    pub fn borrow_graph(&self) -> &MyGraph {
        &self.graph
    }

    /// Borrow Graph Mut
    pub fn borrow_graph_mut(&mut self) -> &mut MyGraph {
        &mut self.graph
    }

    // Node-related methods

    /// Creates a new node with the given name and returns its ID.
    /// If the node already exists, it returns the ID of the existing node.
    ///
    /// # Arguments
    ///
    /// * `node_name` - The name of the node to create or retrieve.
    ///
    /// # Errors
    ///
    /// Returns a `GraphManipulationError::MutexLockFailure()` if the mutex lock fails.
    pub fn add_node(node_name: &str) -> Result<NodeId, GraphManipulationError> {
        /*
-        match GRAPH.lock() {
-            Ok(mut graph) => graph.get_node_id(node_name),
        */
        let mut lock = GRAPH.lock();
        lock.borrow_me_mut().get_node_id(node_name)
        // graph.get_node_id(node_name)
    }

    // This method remains largely the same, it's already well structured
    pub fn get_node_id(&mut self, node_name: &str) -> Result<NodeId, GraphManipulationError> {
        if let Some(&node_id) = self.node_names.get(node_name) {
            Ok(node_id)
        } else {
            let new_node_id = self.graph.node_count() + 1;
            let node_id = NodeId::UInt(new_node_id);
            self.node_names.insert(node_name.to_string(), node_id);
            self.graph.add_node(node_id.into());
            Ok(node_id)
        }
    }

    /// Returns the name of the node with the given ID.
    pub fn node_name_to_id(node_name: &str) -> Result<NodeId, GraphManipulationError> {
        if let Some(&node_id) = GRAPH.lock().node_names.get(node_name) {
            Ok(node_id)
        } else {
            Err(GraphManipulationError::NodeNotFound(format!(
                "Node not found: {}",
                node_name
            )))
        }
    }

    /// Returns the ID of the node with the given name.
    pub fn node_id_to_name(node_id: NodeId) -> Result<String, GraphManipulationError> {
        for (name, id) in GRAPH.lock().node_names.iter() {
            if *id == node_id {
                return Ok(name.to_string());
            }
        }
        Err(GraphManipulationError::NodeNotFound(format!(
            "Node not found: {}",
            node_id
        )))
    }

    pub fn clear_graph() -> Result<(), GraphManipulationError> {
        let graph = GRAPH.lock();
        graph.graph.clear();
        graph.node_names.clear();
        Ok(())
    }
}
