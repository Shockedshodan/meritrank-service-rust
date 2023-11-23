use std::fmt;

#[allow(unused_imports)]
use crate::lib_graph::{node::NodeId, storage::WalkStorage, walk::PosWalk, walk::RandomWalk};

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            NodeId::Int(value) => write!(f, "{}", value),
            NodeId::UInt(value) => write!(f, "{}", value),
            NodeId::None => write!(f, "None"),
        }
    }
}
