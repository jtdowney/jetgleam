# jetgleam_erlang

[![Package Version](https://img.shields.io/hexpm/v/jetgleam_erlang)](https://hex.pm/packages/jetgleam_erlang)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://jetgleam-erlang.hexdocs.pm/)

A NATS and JetStream client for Gleam on the BEAM. It connects, publishes,
subscribes and makes requests, and it runs the JetStream and key/value
descriptions built with [`jetgleam`](https://hex.pm/packages/jetgleam).

```sh
gleam add jetgleam_erlang@1 jetgleam@1
```

## Core NATS

```gleam
import gleam/erlang/process
import gleam/io
import gleam/string
import jetgleam_erlang/nats

pub fn main() {
  let assert Ok(config) = nats.config("nats://localhost:4222")
  let assert Ok(conn) = config |> nats.with_name("worker") |> nats.connect

  let assert Ok(sub) = nats.subscribe(conn, "orders.*")
  let assert Ok(Nil) = nats.publish(conn, "orders.new", <<"hello":utf8>>)
  let assert Ok(msg) = process.receive(nats.messages(sub), 1000)
  io.println(msg.subject)

  let outcome = case nats.request(conn, "time.service", <<>>, timeout: 1000) {
    Ok(reply) -> string.inspect(reply.payload)
    Error(nats.NoResponders) -> "nobody is listening"
    Error(nats.Timeout) -> "no reply in time"
    Error(other) -> string.inspect(other)
  }
  io.println(outcome)
}
```

## JetStream

The snippets from here on assume a `conn` like the one above.

Build the operation with a `jetgleam` module and pipe it into
`nats.execute`. Stream and consumer setup usually runs once, at deploy or
startup:

```gleam
import gleam/list
import gleam/time/duration
import jetgleam/consumer
import jetgleam/stream
import jetgleam_erlang/nats

let assert Ok(_) =
  stream.config("ORDERS", ["orders.>"])
  |> stream.with_max_age(duration.hours(24))
  |> stream.create
  |> nats.execute(on: conn)

let assert Ok(_) =
  consumer.durable("worker")
  |> consumer.with_max_deliver(5)
  |> consumer.create(stream: "ORDERS")
  |> nats.execute(on: conn)
```

A producer imports `jetgleam` and `nats`:

```gleam
let assert Ok(ack) =
  jetgleam.message("orders.new", <<"hello":utf8>>)
  |> jetgleam.set_header("Nats-Msg-Id", "order-42")
  |> jetgleam.publish
  |> nats.execute(on: conn)
```

A worker uses `consumer` and `nats`:

```gleam
let assert Ok(batch) =
  consumer.fetch(
    stream: "ORDERS",
    consumer: "worker",
    max: 10,
    wait: duration.seconds(2),
  )
  |> nats.fetch(on: conn)

use msg <- list.each(batch)
let assert Ok(Nil) = consumer.ack(msg, consumer.Ack) |> nats.execute(on: conn)
```

## Key/value

```gleam
import gleam/erlang/process
import gleam/io
import gleam/string
import jetgleam
import jetgleam/kv
import jetgleam_erlang/nats

let assert Ok(Nil) =
  kv.config("settings")
  |> kv.with_history(5)
  |> kv.create_bucket
  |> nats.execute(on: conn)

let assert Ok(revision) =
  kv.put(bucket: "settings", key: "theme", value: <<"dark":utf8>>)
  |> nats.execute(on: conn)

// A conditional write that lost comes back as WrongLastSequence.
case
  kv.update(bucket: "settings", key: "theme", value: <<"light":utf8>>, revision:)
  |> nats.execute(on: conn)
{
  Ok(_) -> Nil
  Error(nats.JetStream(jetgleam.WrongLastSequence(_))) ->
    io.println("another writer got there first")
  Error(other) -> panic as string.inspect(other)
}
```

`nats.collect` reads a finite set of entries. `nats.watch` delivers the
current entries followed by live changes:

```gleam
let assert Ok(entries) =
  kv.keys("settings") |> nats.collect(on: conn, timeout: 5000)

let assert Ok(watch) = kv.watch("settings", filter: ">") |> nats.watch(on: conn)
let assert Ok(nats.Changed(entry)) = process.receive(nats.messages(watch), 5000)
let assert Ok(nats.CaughtUp) = process.receive(nats.messages(watch), 5000)
nats.unsubscribe(watch)
```

## Supervision

```gleam
let name = process.new_name("nats")

let assert Ok(config) = nats.config(url)
let assert Ok(_) =
  static_supervisor.new(static_supervisor.OneForOne)
  |> static_supervisor.add(nats.supervised(config, name:))
  |> static_supervisor.start

let conn = nats.named_connection(name)
```
