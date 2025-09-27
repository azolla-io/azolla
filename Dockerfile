# Stage 1: Build the application
FROM rust:1.82-slim as builder

# Create a new empty shell project
WORKDIR /usr/src/app
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev protobuf-compiler

# Copy over your manifests
COPY Cargo.toml Cargo.lock ./
COPY build.rs ./
COPY proto ./proto
COPY src ./src
COPY migrations ./migrations

# Build the application
RUN cargo build --release

# Stage 2: Create the final, smaller image
FROM debian:stable-slim

# Copy the built binary from the builder stage
COPY --from=builder /usr/src/app/target/release/azolla /usr/local/bin/
# Copy runtime configuration and migrations
COPY config ./config
COPY migrations ./migrations

# Expose the gRPC port
EXPOSE 52710

# Set the entrypoint
CMD ["/usr/local/bin/azolla"] 