fn main() {
    std::fs::create_dir_all("src/raft/generated").expect("create-dir");
    protoc_rust_grpc::Codegen::new()
        .out_dir("src/raft/generated")
        .input("src/raft/raft_proto.proto")
        .rust_protobuf(true)
        .run()
        .expect("protoc-rust-grpc");
}
