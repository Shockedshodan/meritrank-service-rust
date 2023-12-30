FROM rust:1.74.1-bookworm as compile
WORKDIR /usr/project
COPY . .
RUN apt-get update && apt-get -y install cmake
RUN cargo build # debug
RUN cargo build --release
RUN cd util/zerorec && cargo build && cargo build --release

FROM scratch as debug
COPY --from=compile /usr/project/target/debug/meritrank-rust-service /meritrank-rust-service
COPY --from=compile /usr/project/util/zerorec/target/debug/zerorec /zerorec
#COPY --from=compile /usr/project/util/crontab /etc/cron.d/zerorec
# ENV RUST_SERVICE_PARALLEL=128
ENV RUST_SERVICE_URL=tcp://0.0.0.0:10234
EXPOSE 10234
#RUN apt-get update && apt-get -y install cron
ENTRYPOINT ["/meritrank-rust-service"]

FROM scratch as release
COPY --from=compile /usr/project/target/release/meritrank-rust-service /meritrank-rust-service
COPY --from=compile /usr/project/util/zerorec/target/release/zerorec /zerorec
#COPY --from=compile /usr/project/util/crontab /etc/cron.d/zerorec
ENV RUST_SERVICE_PARALLEL=128
ENV RUST_SERVICE_URL=tcp://0.0.0.0:10234
EXPOSE 10234
#RUN apt-get update && apt-get -y install cron
ENTRYPOINT ["/meritrank-rust-service"]
