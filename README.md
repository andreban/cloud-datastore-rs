# cloud-datastore-rs

## Cloning the project

This project uses git submodules. To clone the project, use the following command:

```sh
git clone --recurse-submodules https://github.com/andreban/cloud-datastore-rs.git
```

## Updating protobuf generated sources

To update the generated sources, run the following commands:
```sh
git submodule update --remote
cargo run --features protobuild --bin protobuild
```
