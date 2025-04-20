fn main() {
    let protos = ["src/vera_api.proto"];
    let includes = ["proto"];

    tonic_build::configure()
        .emit_rerun_if_changed(true)
        .protoc_arg("--experimental_allow_proto3_optional")
        .file_descriptor_set_path("vera_api.bin")
        .compile_protos(&protos, &includes)
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}