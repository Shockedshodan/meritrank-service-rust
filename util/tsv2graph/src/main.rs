use std::env::var;
use lazy_static::lazy_static;
use nng::{Message, Protocol, Socket};
use std::fs::read_to_string;
use reflection::Reflection;
use reflection_derive::Reflection;
use tsv::Env;
use itertools::Itertools;
use rayon::prelude::*;

lazy_static! {
    static ref SERVICE_URL: String =
        var("RUST_SERVICE_URL")
            .unwrap_or("tcp://127.0.0.1:10234".to_string());
}

const FILES: &'static [&str] = &[
    "edges.tsv"
    /*
    "/tmp/vote_user.tsv",
    "/tmp/vote_comment.tsv",
    "/tmp/vote_beacon.tsv"
    */
];
/*
full_dump.sql.gz (30 Dec, 2023):
postgres=# COPY (select * from vote_user) TO '/tmp/vote_user.tsv'  CSV  HEADER DELIMITER E'\t';
COPY 242
postgres=# COPY (select * from vote_comment) TO '/tmp/vote_comment.tsv'  CSV  HEADER DELIMITER E'\t';
COPY 112
postgres=# COPY (select * from vote_beacon) TO '/tmp/vote_beacon.tsv'  CSV  HEADER DELIMITER E'\t';
COPY 184

postgres=# COPY (select * from edges) TO '/tmp/edges.tsv'  CSV  HEADER DELIMITER E'\t';
COPY 860

 */

fn request<T: for<'a> serde::Deserialize<'a>>(
    req: &Vec<u8>,
) -> Result<Vec<T>, Box<dyn std::error::Error + 'static>> {
    let client = Socket::new(Protocol::Req0)?;
    client.dial(&SERVICE_URL)?;
    client
        .send(Message::from(req.as_slice()))
        .map_err(|(_, err)| err)?;
    let msg: Message = client.recv()?;
    let slice: &[u8] = msg.as_slice();
    rmp_serde::from_slice(slice).or_else(|_| {
        let err: String = rmp_serde::from_slice(slice)?;
        Err(Box::from(format!("Server error: {}", err)))
    })
}

fn mr_edge(
    src: &str,
    dest: &str,
    weight: f64,
) -> Result<(), Box<dyn std::error::Error>,
> {
    let rq = (((src, dest, weight), ), ());
    let req = rmp_serde::to_vec(&rq)?;
    let _: Vec<(String, String, f64)> = request(&req)?;
    Ok(())
}

//#[derive(Deserialize,Reflection,Debug,PartialEq)]
#[derive(serde_derive::Deserialize,Reflection,Debug)]
struct Rec {
    subject: String,
    object: String,
    amount: f64,
    created_at: String,
    updated_at: String
}
fn add_tsv(file_name: &str) {
    println!("{file_name}");

    let mut env = // Env::default();
        Env::make_env(
            false,
            "-".to_string(),
            "O".to_string(),
            "X".to_string()
        ).unwrap();

    read_to_string(file_name)
        .unwrap()   // panic on possible file-reading errors
        .lines()    // split the string into an iterator of string slices
        .skip(1) // header
        .flat_map(|s|
            tsv::de::from_str::<Rec>(s, Rec::schemata(), &mut env)
        )
        //.par_chunks(8);
        .chunks(8)
        .into_iter()
        .for_each(|chunk|
            chunk.collect::<Vec<_>>().into_par_iter().for_each(|r|
                mr_edge(r.subject.as_str(), r.object.as_str(), r.amount)
                    .unwrap_or_else(|e| {
                        println!("Error adding {:?}, {:?}", r, e)
                    })
            )
        )
}

fn main() {
    println!("tsv2graph");
    for &file in FILES {
        add_tsv(file);
    }
}
