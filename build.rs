
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // This makes sure that this build script is always re-run, even if
    // none of the files on disk have changed. We want this in order to
    // deal with cases where the output has been modified, e.g., if the
    // "generated" directory has been manually deleted.
    println!("cargo:rerun-if-changed=\"someBogusFile\"");

    let raft_gen_dir = "src/raft/generated";
    std::fs::create_dir_all(raft_gen_dir).expect("create-dir");

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/raft/generated")  // you can change the generated code's location
        .compile(
            &["src/raft/raft_proto.proto"],
            &["src/raft"],
        )?;


    let keyvalue_gen_dir = "src/keyvalue/generated";
    std::fs::create_dir_all(keyvalue_gen_dir).expect("create-dir");
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/keyvalue/generated")  // you can change the generated code's location
        .compile(
            &["src/keyvalue/keyvalue_proto.proto"],
            &["src/keyvalue"],
        )?;

    Ok(())
}
