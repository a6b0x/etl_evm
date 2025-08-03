# Stage 1: Build the application
FROM rust:1.87 as builder

# Set the working directory
WORKDIR /usr/src/app

# Copy the dependency manifests
COPY Cargo.toml Cargo.lock ./

# Build only the dependencies to leverage Docker layer caching.
# We create a dummy main.rs to allow cargo to build dependencies without the full source code.
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release

# Now, copy the actual source code, overwriting the dummy file
COPY src ./src

# Build the application. This step will be fast if only the source code has changed.
RUN cargo build --release

# Stage 2: Create the final, minimal image
FROM debian:bullseye-slim

# Install runtime dependencies required by the application (e.g., OpenSSL)
RUN apt-get update && apt-get install -y libssl1.1 ca-certificates && rm -rf /var/lib/apt/lists/*

# Set the working directory in the final image
WORKDIR /app

# Copy the compiled binary from the builder stage and the data directory
COPY --from=builder /usr/src/app/target/release/etl_evm .
COPY data ./data

# Set the command to run when container starts
ENTRYPOINT ["./etl_evm"]
