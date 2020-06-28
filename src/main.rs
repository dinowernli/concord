mod cluster;

use cluster::raft;
use cluster::raft_grpc;
use cluster::RaftImpl;
use raft::Server;
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

fn main() {
    env_logger::from_env(Env::default().default_filter_or("concord=info")).init();

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

    loop {
        thread::park();
    }
}
