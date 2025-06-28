FROM clux/muslrust:stable as chef

RUN cargo install cargo-chef
WORKDIR /market-streamer

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /market-streamer/recipe.json recipe.json
RUN cargo chef cook --release --target x86_64-unknown-linux-musl --recipe-path recipe.json
COPY . .
RUN cargo build --release --target x86_64-unknown-linux-musl

# Use distroless image instead of scratch - includes CA certificates
FROM gcr.io/distroless/static-debian11
COPY --from=builder /market-streamer/target/x86_64-unknown-linux-musl/release/market-streamer /market-streamer
ENTRYPOINT ["/market-streamer"]
EXPOSE 8888