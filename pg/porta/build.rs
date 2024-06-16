use std::io::Result;
fn main() {
    tonic_build::compile_protos("src/porta_api.proto")
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}