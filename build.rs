extern crate bindgen;

use bindgen::callbacks::{ParseCallbacks, IntKind};

extern crate gcc;

use std::env;
use std::path::PathBuf;
use std::path::Path;

fn main() {



    gcc::Config::new()
        .file("src/CDeps/Redis/redismodule.c")
        .include("src/CDeps/Redis/include")
        .out_dir(&Path::new("target/"))
        .compile("libredismodule.a");

    gcc::Config::new()
        .file("src/CDeps/SQLite/sqlite3.c")
        .include("src/CDeps/SQLite/include")
        .out_dir(&Path::new("target/"))
        .compile("libsqlite3.a");

    // Tell cargo to tell rustc to link the system bzip2
    // shared library.
    // println!("cargo:rustc-link-lib=bz2");

    #[derive(Debug)]
    struct SqliteTypeChooser;

    impl ParseCallbacks for SqliteTypeChooser {
        fn int_macro(&self, _name: &str, value: i64) -> Option<IntKind> {
            if value >= i32::min_value() as i64 && value <= i32::max_value() as i64 {
                Some(IntKind::I32)
            } else {
                None
            }
        }
    }


    // println!("cargo:rustc-link-lib=static=libsqlite3.a");
    // println!("cargo:rustc-link-search=native=/home/simo/rediSQL-rst/target/");

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        // Do not generate unstable Rust code that
        // requires a nightly rustc and enabling
        // unstable features.
        .no_unstable_rust()
   
        .parse_callbacks(Box::new(SqliteTypeChooser))
        // The input header we would like to generate
        // bindings for.
        .header("wrapper.h")
     //   .link_static("sqlite3")
     //   .link("sqlite3")
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings.write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
