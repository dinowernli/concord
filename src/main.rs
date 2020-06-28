mod cluster;

use cluster::raft;
use cluster::raft_grpc;
use cluster::RaftImpl;
use futures::executor;
use grpc::ClientStubExt;
use raft::AppendRequest;
use raft::Server;
use raft::VoteRequest;
use raft_grpc::RaftClient;
use raft_grpc::RaftServer;

use env_logger::Env;
use log::info;
use std::thread;

fn server(host: &str, port: i32) -> Server {
    let mut result = Server::new();
    result.set_host(host.to_string());
    result.set_port(port);
    return result;
}

fn start_node(address: &Server, all: &Vec<Server>) -> grpc::Server {
    let mut server_builder = grpc::ServerBuilder::new_plain();
    let raft = RaftImpl::new(address, all);

    raft.start();

    server_builder.add_service(RaftServer::new_service_def(raft));
    server_builder.http.set_port(address.get_port() as u16);
    server_builder.build().expect("server")
}

fn make_append(address: &Server, term: i64) {
    let client_conf = Default::default();
    let port = address.get_port() as u16;
    info!("Making append rpc to [{:?}]", &address);
    let client = RaftClient::new_plain(address.get_host(), port, client_conf).unwrap();
    let mut request = AppendRequest::new();
    request.set_term(term);
    let response = client
        .append(grpc::RequestOptions::new(), request)
        .join_metadata_result();
    info!(
        "Made append rpc, got response {:?}",
        executor::block_on(response)
    );
}

fn make_vote(address: &Server, term: i64) {
    let client_conf = Default::default();
    let port = address.get_port() as u16;
    info!("Making vote rpc to [{:?}]", &address);
    let client = RaftClient::new_plain(address.get_host(), port, client_conf).unwrap();
    let mut request = VoteRequest::new();
    request.set_term(term);
    let response = client
        .vote(grpc::RequestOptions::new(), request)
        .join_metadata_result();
    info!(
        "Made vote rpc, got response {:?}",
        executor::block_on(response)
    );
}

fn main() {
    //env_logger::from_env(Env::default().default_filter_or("concord=info")).init();
    env_logger::from_env(Env::default().default_filter_or("info")).init();

    let addresses = vec![
        server("::1", 12345),
        server("::1", 12346),
        server("::1", 12347),
    ];
    info!("Starting {} servers", addresses.len());

    let mut servers = Vec::<grpc::Server>::new();
    for address in &addresses {
        servers.push(start_node(&address, &addresses));
    }

    make_append(&addresses[0], 2);
    make_append(&addresses[1], 5);
    make_append(&addresses[2], 17);
    make_vote(&addresses[0], 2);
    make_vote(&addresses[1], 5);
    make_vote(&addresses[2], 17);

    loop {
        thread::park();
    }
}
