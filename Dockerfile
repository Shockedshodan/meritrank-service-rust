FROM rust:1.74.1-bookworm as compile
WORKDIR /usr/project
COPY . .
RUN apt-get update && apt-get -y install cmake
RUN cargo build # debug
# RUN cargo build --release

FROM debian:bookworm-slim as debug
WORKDIR /home
COPY --from=compile target/debug/mertirank-service mertirank-service
ENV RUST_SERVICE_PARALLEL=128
ENV RUST_SERVICE_URL=tcp://0.0.0.0:10234
EXPOSE 10234
ENTRYPOINT ["mertirank-service"]

FROM debian:bookworm-slim as release
WORKDIR /home
COPY --from=compile /usr/project/target/release/mertirank-service /home/mertirank-service
ENV RUST_SERVICE_PARALLEL=128
ENV RUST_SERVICE_URL=tcp://0.0.0.0:10234
EXPOSE 10234
ENTRYPOINT ["mertirank-service"]
