#[cfg(host_family = "windows")]
macro_rules! PATH_SEPARATOR {() => (
    r"\"
)}
#[cfg(not(host_family = "windows"))]
macro_rules! PATH_SEPARATOR {() => (
    r"/"
)}

pub mod protocol {
    include!(concat!(env!("OUT_DIR"), PATH_SEPARATOR!(), "indigo.protocol.rs"));
}
