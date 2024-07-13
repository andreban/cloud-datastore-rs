fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(false)
        .out_dir("src/google")
        .compile(
            &["proto/googleapis/google/datastore/v1/datastore.proto"],
            &["proto/googleapis"],
        )?;
    Ok(())
}
