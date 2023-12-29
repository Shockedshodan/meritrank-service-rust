FROM rust:1.74.1-bookworm
WORKDIR /usr/src/service
# COPY . .
COPY Cargo.toml .
COPY src src
RUN apt-get update && apt-get -y install cmake
RUN cargo build
ENV RUST_SERVICE_PARALLEL=128
ENV RUST_SERVICE_URL=tcp://0.0.0.0:10234
EXPOSE 10234
ENTRYPOINT ["cargo", "run"]

