/*
 * Ceph - scalable distributed file system
 *
 * See file COPYING for licensing information.
 */

//! Build script for rgw-lancedb

fn main() {
    // rustc sets no soname on a cdylib, so the ceph build passes in a
    // versioned one for the binaries to record and the packages to track.
    println!("cargo:rerun-if-env-changed=RGW_LANCEDB_SONAME");
    if let Ok(soname) = std::env::var("RGW_LANCEDB_SONAME") {
        println!("cargo:rustc-link-arg-cdylib=-Wl,-soname,{soname}");
    }
}
