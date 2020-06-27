mod cluster;

use cluster::raft;
use cluster::raft_grpc;
use cluster::RaftImpl;
use raft::AppendRequest;
use raft::Server;
use raft_grpc::RaftClient;
use raft_grpc::RaftServer;

use env_logger::Env;
use futures::executor;
use grpc::ClientStubExt;
use log::info;
use std::convert::TryInto;
use std::thread;

fn server(host: &str, port: i32) -> Server {
    let mut result = Server::new();
    result.set_host(host.to_string());
    result.set_port(port);
    return result;
}

fn start_node(address: &Server, all: &Vec<Server>) -> grpc::Server {
    let mut server_builder = grpc::ServerBuilder::new_plain();
    server_builder.add_service(RaftServer::new_service_def(RaftImpl::new(address, all)));
    server_builder
        .http
        .set_port(address.get_port().try_into().unwrap());
    server_builder.build().expect("server")
}

fn make_append(address: &Server, term: i64) {
    let client_conf = Default::default();
    let client =
        RaftClient::new_plain("::1", address.get_port().try_into().unwrap(), client_conf).unwrap();
    let mut request = AppendRequest::new();
    request.set_term(term);
    let response = client
        .append(grpc::RequestOptions::new(), request)
        .join_metadata_result();
    info!("Made rpc to server");
    info!("{:?}", executor::block_on(response));
}

fn main() {
    env_logger::from_env(Env::default().default_filter_or("concord=info")).init();

    let addresses = vec![
        server("::1", 12345),
        server("::1", 12346),
        server("::1", 12347),
    ];

    let mut servers = Vec::<grpc::Server>::new();
    for address in &addresses {
        servers.push(start_node(&address, &addresses));
        info!("Started server on port {}", address.get_port());
    }

    make_append(&addresses[0], 2);
    make_append(&addresses[0], 17);

    loop {
        thread::park();
    }
}
