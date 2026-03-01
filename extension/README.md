# pgmqtt Extension (Rust)

This directory contains the core Rust implementation of the `pgmqtt` extension, built with `pgrx`.

For general project information, usage, and quickstart, please refer to the [Root README](../README.md).

## Development

The extension is designed to be built and tested within the provided Docker environment to ensure compatibility with `pgrx` and PostgreSQL.

### Distributable Build

The package is built using `cargo pgrx package` within our `pg-docker` image. See `docker/Dockerfile` and `scripts/build_package.sh` for details on how we package the .so and .control files for distribution.