import gleam/erlang/atom
import gleam/erlang/port.{type Port}
import gleam/erlang/process
import gleam/int
import jetgleam_erlang/nats

pub type Server {
  Server(port: Int, store: String, args: List(String), handle: Port)
}

@external(erlang, "nats_server_ffi", "free_port")
pub fn free_port() -> Int

@external(erlang, "nats_server_ffi", "new_store")
fn new_store() -> String

@external(erlang, "nats_server_ffi", "remove_store")
fn remove_store(store: String) -> Nil

@external(erlang, "nats_server_ffi", "start")
fn ffi_start(port: Int, store: String, args: List(String)) -> Port

@external(erlang, "nats_server_ffi", "stop")
fn ffi_stop(handle: Port, port: Int) -> Nil

pub fn start(args: List(String)) -> Server {
  launch(free_port(), new_store(), args)
}

fn launch(port: Int, store: String, args: List(String)) -> Server {
  Server(port:, store:, args:, handle: ffi_start(port, store, args))
}

pub fn with(args: List(String), f: fn(Server) -> a) -> a {
  let server = start(args)
  let result = f(server)
  stop(server)
  result
}

pub fn signal(server: Server, signal: String) -> Nil {
  ffi_signal(server.port, signal)
}

@external(erlang, "nats_server_ffi", "signal")
fn ffi_signal(port: Int, signal: String) -> Nil

pub fn stop(server: Server) -> Nil {
  halt(server)
  remove_store(server.store)
}

pub fn halt(server: Server) -> Nil {
  ffi_stop(server.handle, server.port)
}

pub fn restart(server: Server) -> Server {
  halt(server)
  launch(server.port, server.store, server.args)
}

pub fn url(server: Server) -> String {
  "nats://127.0.0.1:" <> int.to_string(server.port)
}

pub fn leftover_messages(operation: fn() -> a) -> Int {
  let result = process.new_subject()
  process.spawn(fn() {
    operation()
    process.sleep(200)
    let #(_, count) =
      process_info(process.self(), atom.create("message_queue_len"))
    process.send(result, count)
  })
  let assert Ok(count) = process.receive(result, 10_000)
  count
}

@external(erlang, "erlang", "process_info")
fn process_info(pid: process.Pid, item: atom.Atom) -> #(atom.Atom, Int)

pub fn config(server: Server) -> nats.Config {
  let assert Ok(config) = nats.config(url(server))
  config
}
