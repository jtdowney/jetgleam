import gleam/erlang/charlist.{type Charlist}
import gleam/erlang/process
import gleam/string
import jetgleam/stream
import jetgleam_erlang/nats
import nats_server

@external(erlang, "filelib", "is_dir")
fn is_dir(path: String) -> Bool

@external(erlang, "os", "getenv")
fn getenv(name: Charlist) -> Charlist

@external(erlang, "os", "putenv")
fn putenv(name: Charlist, value: Charlist) -> Bool

fn streams(server: nats_server.Server) -> List(String) {
  let assert Ok(conn) = nats_server.config(server) |> nats.connect
  let assert Ok(names) = stream.names() |> nats.execute(on: conn)
  names
}

pub fn fixtures_own_their_store_test() {
  let first = nats_server.start([])
  let assert Ok(conn) = nats_server.config(first) |> nats.connect
  let assert Ok(_) =
    stream.config("KEEP", ["keep"])
    |> stream.create
    |> nats.execute(on: conn)

  let first = nats_server.restart(first)
  assert streams(first) == ["KEEP"]

  let second = nats_server.start([])
  assert second.store != first.store
  assert streams(second) == []
  nats_server.stop(second)

  assert is_dir(first.store)
  nats_server.stop(first)
  assert !is_dir(first.store)
}

pub fn missing_executable_is_reported_test() {
  let name = charlist.from_string("PATH")
  let path = getenv(name)
  putenv(name, charlist.from_string("/nonexistent"))
  let pid = process.spawn_unlinked(fn() { nats_server.start([]) })
  let down =
    process.new_selector()
    |> process.select_specific_monitor(process.monitor(pid), string.inspect)
  let reason = process.selector_receive(down, 1000)
  putenv(name, path)
  let assert Ok(reason) = reason
  assert string.contains(reason, "NatsServerNotFound")
}
