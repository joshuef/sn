# sn_node

The Safe Network Node Implementation.

## Building

### OpenTelemetry Protocol (OTLP)

OTLP allows for inspecting and visualizing log spans.

By specifying the `otlp` feature for the `sn_node` binary, logs will be sent to an OTLP endpoint. This endpoint can be configured by environment variables. (See [opentelemetry.io/docs/...](https://opentelemetry.io/docs/reference/specification/protocol/exporter/) for more information.)

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4317" # Already the default
export RUST_LOG=sn_node=info # This filters the output for stdout/files, not OTLP
export RUST_LOG_OTLP=sn_node=trace # This filters what is sent to OTLP endpoint 
cargo run --release --bin sn_node --features otlp -- --first 127.0.0.1:0 --local-addr=127.0.0.1:0
```

Before running the node, an OTLP endpoint should be available. An example of an OTLP-supporting endpoint is Jaeger, which can be launched with Docker like this (see [documentation](https://www.jaegertracing.io/docs/1.42/getting-started/#all-in-one)):
```
docker run -d --name jaeger \
  -e COLLECTOR_ZIPKIN_HOST_PORT=:9411 \
  -e COLLECTOR_OTLP_ENABLED=true \
  -p 6831:6831/udp \
  -p 6832:6832/udp \
  -p 5778:5778 \
  -p 16686:16686 \
  -p 4317:4317 \
  -p 4318:4318 \
  -p 14250:14250 \
  -p 14268:14268 \
  -p 14269:14269 \
  -p 9411:9411 \
  jaegertracing/all-in-one:1.41
```

In the web interface of Jaeger (http://localhost:16686) one can filter several things, e.g. the tag `service.instance.id=<PID>`, where PID is the process ID of the node. The service name is `sn_node`.

## License

This Safe Network repository is licensed under the General Public License (GPL), version 3 ([LICENSE](LICENSE) http://www.gnu.org/licenses/gpl-3.0.en.html).

### Linking exception

safe_network is licensed under GPLv3 with linking exception. This means you can link to and use the library from any program, proprietary or open source; paid or gratis. However, if you modify safe_network, you must distribute the source to your modified version under the terms of the GPLv3.

See the [LICENSE](LICENSE) file for more details.

## Contributing

Want to contribute? Great :tada:

There are many ways to give back to the project, whether it be writing new code, fixing bugs, or just reporting errors. All forms of contributions are encouraged!

For instructions on how to contribute, see our [Guide to contributing](https://github.com/maidsafe/QA/blob/master/CONTRIBUTING.md).
