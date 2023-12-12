use std::collections::HashMap;

use petgraph::algo::has_path_connecting;
use petgraph::Directed;
use petgraph::graph::{DiGraph, Edge};
use petgraph::prelude::NodeIndex;

#[allow(unused_imports)]
use petgraph::visit::EdgeRef;

// use crate::lib_graph::{MeritRankError, NodeId, Weight, Node};
use crate::lib_graph::errors::MeritRankError;
use crate::lib_graph::node::{Node, NodeId, Weight};

pub type MyDiGraph = DiGraph<Node, Weight>;

#[derive(Debug, Clone)]
pub struct MyGraph {
    graph: MyDiGraph,
    nodes: HashMap<NodeId, NodeIndex>,
}

#[allow(dead_code)]
impl MyGraph {
    /// Creates a new empty `MyGraph`.
    pub fn new() -> Self {
        // Initialize a new MyGraph with an empty directed graph and an empty mapping of NodeId to NodeIndex
        MyGraph {
            graph: MyDiGraph::new(),
            nodes: HashMap::new(),
        }
    }

    /// Checks if the graph is empty.
    pub fn is_empty(&self) -> bool {
        self.graph.node_count() == 0
    }

    /// Adds a node to the graph and returns its `NodeIndex`.
    pub fn add_node(&mut self, node: Node) -> NodeIndex {
        // Add a node to the graph and store its NodeIndex in the nodes mapping
        let index = self.graph.add_node(node.clone());
        self.nodes.insert(node.get_id(), index);
        index
    }

    /// Retrieves the `NodeIndex` of a node in the graph based on its `NodeId`.
    pub fn get_node_index(&self, node_id: &NodeId) -> Option<NodeIndex> {
        // Get the NodeIndex from the nodes mapping based on the given NodeId
        self.nodes.get(&node_id).cloned().or_else(|| {
            // If the NodeIndex is not found in the mapping, iterate over all node indices in the graph
            // and find the first node with a matching NodeId
            self.graph
                .node_indices()
                .find(|&index| self.graph[index].get_id() == *node_id)
        })
    }

    /// Updates the index of nodes in the graph.
    pub fn update_index(&mut self) {
        // Update the nodes mapping by iterating over all node indices in the graph
        // and mapping their NodeId to the corresponding NodeIndex
        self.nodes = self
            .graph
            .node_indices()
            .map(|index| (self.graph[index].get_id(), index))
            .collect();
    }

    /// Checks if a node with the given `NodeId` exists in the graph.
    pub fn contains_node(&self, node_id: &NodeId) -> bool {
        // Check if the given NodeId exists in the nodes mapping
        self.get_node_index(node_id).is_some()
    }

    /// Checks if an edge between the two given nodes exists in the graph.
    pub fn contains_edge(&self, source: &NodeId, target: &NodeId) -> bool {
        // Check if the source and target nodes have valid NodeIndices in the graph
        if let (Some(source_index), Some(target_index)) =
            (self.get_node_index(source), self.get_node_index(target))
        {
            // Check if there is an edge between the source and target NodeIndices
            self.graph.contains_edge(source_index, target_index)
        } else {
            false
        }
    }

    /// Adds an edge between the two given nodes in the graph.
    pub fn add_edge(
        &mut self,
        source: &NodeId,
        target: &NodeId,
        weight: Weight,
    ) -> Result<(), MeritRankError> {
        // Check if the source and target nodes have valid NodeIndices in the graph
        if let (Some(source_index), Some(target_index)) =
            (self.get_node_index(source), self.get_node_index(target))
        {
            // Add an edge between the source and target NodeIndices with the given weight
            self.graph.add_edge(source_index, target_index, weight);
            Ok(())
        } else {
            Err(MeritRankError::InvalidNode)
        }
    }

    /// Removes the edge between the two given nodes from the graph.
    pub fn remove_edge(&mut self, source: &NodeId, target: &NodeId) {
        // Check if the source and target nodes have valid NodeIndices in the graph
        if let (Some(source_index), Some(target_index)) =
            (self.get_node_index(source), self.get_node_index(target))
        {
            // Find the edge index between the source and target NodeIndices and remove it from the graph
            if let Some(edge_index) = self.graph.find_edge(source_index, target_index) {
                self.graph.remove_edge(edge_index);
            }
        }
    }

    /// Retrieves the neighboring nodes of a given node.
    pub fn neighbors(&self, ego: &NodeId) -> Vec<NodeId> {
        // Get the NodeIndex of the ego node from the nodes mapping
        self.get_node_index(ego)
            .map(|ego_index| {
                // Get the neighboring NodeIndices of the ego node in the graph
                // and retrieve their corresponding NodeIds
                self.graph
                    .neighbors(ego_index)
                    .map(|neighbor_index| self.graph[neighbor_index].get_id())
                    .collect()
            })
            .unwrap_or_else(Vec::new)
    }

    /// Retrieves the edges of the graph.
    ///
    /// This method returns a vector of tuples representing the edges connected to the specified `ego` node.
    /// Each tuple contains the source node, destination node, and weight of the edge.
    /// If the `ego` node does not exist in the graph or there are no edges connected to it, `None` is returned.
    ///
    /// # Arguments
    ///
    /// * `ego` - The node for which to retrieve the edges.
    ///
    /// # Returns
    ///
    /// A vector of tuples representing the edges connected to the specified `ego` node,
    /// or `None` if the `ego` node does not exist in the graph or there are no edges connected to it.
    pub fn edges(&self, ego: NodeId) -> Option<Vec<(NodeId, NodeId, Weight)>> {
        // Return None if the ego node does not exist in the graph
        // or if there are no edges connected to it
        // Get the NodeIndex of the ego node from the nodes mapping
        self.get_node_index(&ego).and_then(|ego_index| {
            // Get the edges of the graph and filter out the edges that do not have the ego node as source
            let ego_edges = self.graph.edges(ego_index);
            let filtered_edges = ego_edges.filter(|edge| edge.source() == ego_index);

            // Collect the filtered edges into a vector of tuples
            let collected_edges: Vec<_> = filtered_edges
                .map(|edge| {
                    // Get the target NodeIndex and weight of the edge
                    let target_index = edge.target();
                    let weight = edge.weight().clone();

                    // Get the NodeId of the target node from the nodes mapping
                    let target = self.graph[target_index].get_id();

                    (ego, target, weight)
                })
                .collect();

            if collected_edges.is_empty() {
                None
            } else {
                Some(collected_edges)
            }
        })
    }

    /// Checks if there is a path between the two given nodes.
    pub fn is_connecting(&self, source: &NodeId, target: &NodeId) -> bool {
        // Check if the source and target nodes have valid NodeIndices in the graph
        if let (Some(source_index), Some(target_index)) =
            (self.get_node_index(source), self.get_node_index(target))
        {
            // Use the `has_path_connecting` function from the petgraph library to check if there is a path
            // between the source and target NodeIndices in the graph
            has_path_connecting(&self.graph, source_index, target_index, None)
        } else {
            false
        }
    }

    /// Checks if the graph contains any self-reference and returns an error if found.
    pub fn check_self_reference(&self) -> Result<(), MeritRankError> {
        // Iterate over all node indices in the graph and check if any node has a self-reference
        for node in self.graph.node_indices() {
            if self.graph.contains_edge(node, node) {
                return Err(MeritRankError::SelfReferenceNotAllowed);
            }
        }
        Ok(())
    }

    /// Retrieves the weight of the edge between the two given nodes.
    pub fn edge_weight(&self, source: &NodeId, target: &NodeId) -> Option<Weight> {
        // Check if the source and target nodes have valid NodeIndices in the graph
        if let (Some(source_index), Some(target_index)) =
            (self.get_node_index(source), self.get_node_index(target))
        {
            // Find the edge index between the source and target NodeIndices
            // and retrieve the corresponding weight if it exists
            self.graph
                .find_edge(source_index, target_index)
                .and_then(|edge_index| self.graph.edge_weight(edge_index).copied())
        } else {
            None
        }
    }

    /// Returns the number of nodes in the graph
    pub fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    /// Clears the graph.
    pub fn clear(&mut self) {
        self.graph.clear();
        self.nodes.clear();
    }

    /*
    /// Returns all edges (with weights and NodeIndex/ices)
    pub fn all_edges(&self) -> Vec<Edge<Weight>> {
        self.graph.clone().into_nodes_edges().1
    }

    /// out_edges
    pub fn out_edges(&self, focus: &NodeId) -> petgraph::graph::Edges<Weight, Directed> {
        let index: NodeIndex = self.nodes[focus];
        let dir = petgraph::Direction::Outgoing;
        self.graph.edges_directed(index, dir).into_iter()
    }
    */

    /// NodeIndex --> NodeId
    pub fn index2node(&self, index: NodeIndex) -> NodeId {
        self.graph[index].get_id() // "syntax index"
    }

    /// ego_to_focus_path - find the shortst path from ego to focus
    /// using 1/(edge weight) cost
    pub fn ego_to_focus_path(&self, ego: &NodeId, focus: &NodeId) -> HashMap<NodeIndex, Weight> {
        petgraph::algo::dijkstra::dijkstra(
            &self.graph,
            self.nodes[ego],
            Some(self.nodes[focus]),
            |edge| {
                if *edge.weight()==0.0f64 {
                    Weight::INFINITY // f64::INFINITY
                } else {
                    1.0 / edge.weight()
                }
            }
        )
    }
}

impl PartialEq for MyGraph {
    fn eq(&self, other: &Self) -> bool {
        // Check if the number of nodes and edges are equal
        if self.graph.node_count() != other.graph.node_count()
            || self.graph.edge_count() != other.graph.edge_count()
        {
            return false;
        }

        // Compare nodes
        for node in self.graph.node_indices() {
            let node1 = &self.graph[node];
            let node2 = &other.graph[node];

            if node1 != node2 {
                return false;
            }
        }

        // Compare edges
        for edge in self.graph.edge_references() {
            let source = edge.source();
            let target = edge.target();
            let &weight1 = edge.weight();
            let weight2 = other
                .graph
                .edge_weight(other.graph.find_edge(source, target).unwrap());

            if let Some(w2) = weight2 {
                if weight1 != *w2 {
                    return false;
                }
            } else {
                return false;
            }
        }

        true
    }
}