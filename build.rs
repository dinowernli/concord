fn main() {
    std::fs::create_dir_all("src/generated").expect("create-dir");
    protoc_rust_grpc::Codegen::new()
        .out_dir("src/generated")
        .input("src/raft.proto")
        .rust_protobuf(true)
        .run()
        .expect("protoc-rust-grpc");
}
