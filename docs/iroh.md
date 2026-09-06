# Iroh workers and clients

Java VGI workers use the language-neutral `vgi-iroh-bridge`. Both integration
styles are one worker call:

```java
worker.runIrohTcpUpstream(
        "127.0.0.1", 9400, 0, new IrohBridgeOptions("production"));

worker.runHttp(
        "127.0.0.1", 9401, new IrohBridgeOptions("production"));
```

The ordinary `runFromArgs` entry point accepts the same portable flags as the
other VGI frameworks:

```console
worker --iroh-raw-upstream 127.0.0.1:9400 --iroh-issuer production
worker --http --port 9401 --iroh-issuer production
```

Repeat `--iroh-trusted-proxy <exact-IP>` to replace the loopback trust list.
Use `--iroh-observe` to expose verified EndpointId evidence without promoting
it to the application principal. The worker upstream must remain unreachable
except through that trusted bridge.

For clients, add the optional `farm.query:vgirpc-iroh` module and use
`HttpRpcConnection.irohBuilder("httpi://<endpoint-id>", options)` for HTTP
semantics, or `IrohRpcConnection.connect(...)` for stateful Arrow-mux. Both use
the official JVM Iroh bindings and accept stable identity, custom/private relay,
direct-address, cancellation, and timeout configuration.
