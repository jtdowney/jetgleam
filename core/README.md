# jetgleam

[![Package Version](https://img.shields.io/hexpm/v/jetgleam)](https://hex.pm/packages/jetgleam)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://jetgleam.hexdocs.pm/)

The target-independent core of a NATS and JetStream client for
Gleam. This package does no I/O. It holds the message type and every
JetStream and key/value operation as a value that `jetgleam_erlang` runs.

On the BEAM, install [`jetgleam_erlang`](https://hex.pm/packages/jetgleam_erlang)
alongside this package. It connects to the server and runs the values built
here.

```sh
gleam add jetgleam@1 jetgleam_erlang@1
```

```gleam
import gleam/time/duration
import jetgleam
import jetgleam/stream
import jetgleam_erlang/nats

pub fn main() {
  let assert Ok(config) = nats.config("nats://localhost:4222")
  let assert Ok(conn) = nats.connect(config)

  // Describe the stream here, run it with jetgleam_erlang.
  let assert Ok(_) =
    stream.config("ORDERS", ["orders.>"])
    |> stream.with_max_age(duration.hours(24))
    |> stream.create
    |> nats.execute(on: conn)

  let assert Ok(ack) =
    jetgleam.message("orders.new", <<"hello":utf8>>)
    |> jetgleam.set_header("Nats-Msg-Id", "order-42")
    |> jetgleam.publish
    |> nats.execute(on: conn)

  echo ack.sequence
}
```
