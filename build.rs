fn main() {
    protoc_rust_grpc::Codegen::new()
        .out_dir("src/generated")
        .input("src/raft.proto")
        .rust_protobuf(true)
        .run()
        .expect("protoc-rust-grpc");
}
