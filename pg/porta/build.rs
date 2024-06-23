fn main() {
    let protos = ["src/porta_api.proto"];
    let includes = ["proto"];

    tonic_build::configure()
        .emit_rerun_if_changed(true)
        .compile(&protos, &includes)
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}