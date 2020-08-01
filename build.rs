fn main() {
    // This makes sure that this build script is always re-run, even if
    // none of the files on disk have changed. We want this in order to
    // deal with cases where the output has been modified, e.g., if the
    // "generated" directory has been manually deleted.
    println!("cargo:rerun-if-changed=\"someBogusFile\"");

    let raft_gen_dir = "src/raft/generated";
    std::fs::create_dir_all(raft_gen_dir).expect("create-dir");
    protoc_rust_grpc::Codegen::new()
        .out_dir(raft_gen_dir)
        .input("src/raft/raft_proto.proto")
        .rust_protobuf(true)
        .run()
        .expect("protoc-rust-grpc");

    let keyvalue_gen_dir = "src/keyvalue/generated";
    std::fs::create_dir_all(keyvalue_gen_dir).expect("create-dir");
    protoc_rust_grpc::Codegen::new()
        .out_dir(keyvalue_gen_dir)
        .input("src/keyvalue/keyvalue_proto.proto")
        .rust_protobuf(true)
        .run()
        .expect("protoc-rust-grpc");
}
