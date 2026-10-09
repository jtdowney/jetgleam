//// Connection lifecycle, core NATS, and the executors for core
//// descriptions. JetStream and KV operations are built in `jetgleam` and
//// run here with `execute`, `fetch`, `watch` or `collect`.
////
//// ```gleam
//// import gleam/erlang/process
//// import jetgleam_erlang/nats
////
//// let assert Ok(config) = nats.config("nats://localhost:4222")
//// let assert Ok(config) = nats.add_server(config, "nats://backup:4222")
//// let assert Ok(conn) = config |> nats.with_name("worker") |> nats.connect
////
//// let assert Ok(sub) = nats.subscribe(conn, "orders.*")
//// let assert Ok(Nil) = nats.publish(conn, "orders.new", <<"hello":utf8>>)
//// let assert Ok(msg) = process.receive(nats.messages(sub), 1000)
//// msg.subject
//// // -> "orders.new"
//// ```
////
//// `config`, `add_server`, `with_nkey` and `with_credentials` check their
//// input and return `InvalidUrl` or `InvalidCredentials` straight away; the
//// other builders cannot fail. An out-of-range timing setting is caught
//// before anything starts: `connect` returns `InvalidConfig`, and `start`
//// and `supervised` fail with `actor.InitFailed`. Otherwise starting fails
//// only on the network or the server.
////
//// The connection runs in its own process, linked to the one that started
//// it, and reconnects on its own. While it is disconnected, publishes are
//// buffered (see `with_buffer_size`) and subscriptions are restored when it
//// comes back. Pass `with_events` to be told about status changes. It stops
//// when its owner does; run it under a supervisor with `supervised`. Calls to
//// a connection whose process is gone crash the caller.
////
//// ## Running JetStream and KV
////
//// The description comes first and the connection is labelled `on:`, so
//// operations pipe:
////
//// ```gleam
//// let assert Ok(ack) =
////   jetgleam.message("orders.new", <<"hello":utf8>>)
////   |> jetgleam.publish
////   |> jetgleam.with_timeout(2000)
////   |> nats.execute(on: conn)
//// ```
////
//// API failures arrive as `JetStream(jetgleam.Error)`, so a missing KV key
//// matches `Error(nats.JetStream(jetgleam.NotFound))`.

import gleam/bool
import gleam/crypto
import gleam/dict.{type Dict}
import gleam/erlang/atom
import gleam/erlang/process
import gleam/erlang/reference.{type Reference}
import gleam/int
import gleam/list
import gleam/option.{type Option}
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result
import gleam/string
import jetgleam
import jetgleam/consumer
import jetgleam/internal/nkey
import jetgleam/internal/protocol
import jetgleam/kv
import mug

/// The servers, credentials and timing settings for a connection.
pub opaque type Config {
  Config(protocol: protocol.Config, events: Option(process.Subject(Event)))
}

/// One required server, or `InvalidUrl`. A `nats://user:pass@host:port` URL
/// supplies credentials unless a credential builder is used.
pub fn config(url: String) -> Result(Config, Error) {
  use protocol <- result.map(
    protocol.config(url) |> result.map_error(from_protocol),
  )
  Config(protocol:, events: option.None)
}

/// Appends a failover server, tried in the order added, or `InvalidUrl`.
pub fn add_server(config: Config, url: String) -> Result(Config, Error) {
  use protocol <- result.map(
    protocol.add_server(config.protocol, url) |> result.map_error(from_protocol),
  )
  Config(..config, protocol:)
}

/// A name the server shows for this connection in monitoring.
pub fn with_name(config: Config, name: String) -> Config {
  Config(
    ..config,
    protocol: protocol.Config(..config.protocol, name: option.Some(name)),
  )
}

/// Authenticates with a token.
pub fn with_token(config: Config, token: String) -> Config {
  Config(
    ..config,
    protocol: protocol.Config(
      ..config.protocol,
      auth: option.Some(protocol.Token(token)),
    ),
  )
}

/// Authenticates with a user name and password.
pub fn with_user_password(
  config: Config,
  user user: String,
  password password: String,
) -> Config {
  Config(
    ..config,
    protocol: protocol.Config(
      ..config.protocol,
      auth: option.Some(protocol.UserPassword(user:, password:)),
    ),
  )
}

/// The contents of an NKey seed, or `InvalidCredentials`.
pub fn with_nkey(config: Config, seed: String) -> Result(Config, Error) {
  use seed <- result.map(
    nkey.from_seed(seed)
    |> result.replace_error(InvalidCredentials("invalid nkey seed")),
  )
  Config(
    ..config,
    protocol: protocol.Config(
      ..config.protocol,
      auth: option.Some(protocol.Nkey(signer(seed))),
    ),
  )
}

/// The contents of a `.creds` file, or `InvalidCredentials`.
pub fn with_credentials(
  config: Config,
  contents: String,
) -> Result(Config, Error) {
  use #(jwt, seed) <- result.map(
    nkey.from_credentials(contents)
    |> result.replace_error(InvalidCredentials("invalid credentials file")),
  )
  let signer = signer(seed)
  Config(
    ..config,
    protocol: protocol.Config(
      ..config.protocol,
      auth: option.Some(protocol.Jwt(jwt:, signer:)),
    ),
  )
}

fn signer(seed: nkey.Seed) -> nkey.Signer {
  nkey.Signer(
    public_key: nkey.public_key(seed, ed25519_public_key(seed.raw)),
    sign: ed25519_sign(seed.raw, _),
  )
}

@external(erlang, "jetgleam_erlang_ffi", "ed25519_public_key")
fn ed25519_public_key(seed: BitArray) -> BitArray

@external(erlang, "jetgleam_erlang_ffi", "ed25519_sign")
fn ed25519_sign(seed: BitArray, data: BitArray) -> BitArray

/// Whether subscriptions on this connection receive its own publishes.
/// Default True.
pub fn with_echo(config: Config, enabled: Bool) -> Config {
  Config(
    ..config,
    protocol: protocol.Config(..config.protocol, echo_enabled: enabled),
  )
}

/// How long each connection attempt may take to open the socket and finish
/// the handshake. Default 2000 ms. Must be between 1 ms and 2^32 - 1001 ms,
/// checked when the connection starts.
pub fn with_connect_timeout(config: Config, milliseconds: Int) -> Config {
  Config(
    ..config,
    protocol: protocol.Config(..config.protocol, connect_timeout: milliseconds),
  )
}

/// How often the client pings the server to detect a stale connection.
/// Default 120000 ms. Must be between 1 ms and 2^32 - 1 ms, checked when
/// the connection starts.
pub fn with_ping_interval(config: Config, milliseconds: Int) -> Config {
  Config(
    ..config,
    protocol: protocol.Config(..config.protocol, ping_interval: milliseconds),
  )
}

/// The delay doubles per failed attempt, from `initial` up to `max`
/// milliseconds. Default 100 to 2000 ms. Checked when the connection starts:
/// `initial` must be at least 1 ms and at most `max`, and `max` plus the
/// connect timeout at most 2^32 - 1 ms.
pub fn with_reconnect_delay(
  config: Config,
  initial initial: Int,
  max max: Int,
) -> Config {
  Config(
    ..config,
    protocol: protocol.Config(
      ..config.protocol,
      reconnect_initial: initial,
      reconnect_max: max,
    ),
  )
}

/// The most bytes of publishes held while disconnected; past it `publish`
/// returns `BufferFull`. Default 8 MiB.
pub fn with_buffer_size(config: Config, bytes: Int) -> Config {
  Config(
    ..config,
    protocol: protocol.Config(..config.protocol, buffer_size: bytes),
  )
}

/// Status changes and server warnings are sent here.
///
/// ```gleam
/// let events = process.new_subject()
/// let assert Ok(config) = nats.config(url)
/// let assert Ok(conn) = config |> nats.with_events(events) |> nats.connect
///
/// // Elsewhere:
/// case process.receive(events, 1000) {
///   Ok(nats.StatusChanged(nats.Reconnecting)) -> log("lost the server")
///   _ -> Nil
/// }
/// ```
pub fn with_events(config: Config, subject: process.Subject(Event)) -> Config {
  Config(..config, events: option.Some(subject))
}

/// A handle to a connection process.
pub opaque type Connection {
  Connection(subject: process.Subject(Command))
}

/// The connection process's message type, for `process.Name(nats.Command)`.
pub opaque type Command {
  Connect(generation: protocol.Generation, server: protocol.Server)
  Attempted(generation: protocol.Generation, result: Result(mug.Socket, String))
  Tcp(mug.TcpMessage)
  TimerFired(timer: protocol.Timer, token: Reference)
  AwaitReady(reply: process.Subject(Result(Nil, Error)))
  ConnectDeadline
  GetStatus(reply: process.Subject(protocol.Status))
  Publish(
    message: jetgleam.Message,
    reply: process.Subject(Result(Nil, protocol.Error)),
  )
  Subscribe(
    subject: String,
    queue: Option(String),
    owner: process.Pid,
    deliver_to: process.Subject(jetgleam.Message),
    reply: process.Subject(
      Result(#(Int, process.Subject(Command)), protocol.Error),
    ),
  )
  Unsubscribe(sid: Int)
  StopSubscription(sid: Int, reply: process.Subject(Nil))
  OwnerDown(process.Down)
  Flush(reply: process.Subject(Result(Nil, Error)))
  CancelFlush(
    reply: process.Subject(Result(Nil, Error)),
    ack: process.Subject(Nil),
  )
  NewInbox(reply: process.Subject(String))
}

/// A live subscription or KV watch. Items arrive on `messages`, and only
/// the process that created it may receive them.
pub opaque type Subscription(a) {
  Subscription(
    messages: process.Subject(a),
    stop: fn() -> Nil,
    close: fn() -> Nil,
  )
}

/// Where the connection is in its lifecycle.
pub type Status {
  /// Opening the first socket or completing the first handshake.
  Connecting
  /// The handshake has completed and the connection is usable.
  Connected
  /// The connection was established before and is being restored.
  Reconnecting
}

/// Sent to the subject passed to `with_events`.
pub type Event {
  /// The connection's status changed.
  StatusChanged(
    /// The new status.
    status: Status,
  )
  /// The server reported an error that did not end the connection.
  Warning(
    /// The server's error text.
    message: String,
  )
  /// A publish buffered while disconnected was too large for, or had headers
  /// unsupported by, the server reached on reconnect. It was not sent.
  PublishDropped(
    /// `PayloadTooLarge` or `HeadersNotSupported`.
    error: Error,
  )
}

/// An item from a KV watch.
pub type WatchEvent {
  /// A key changed, or the initial replay reached it.
  Changed(
    /// The new revision.
    entry: kv.Entry,
  )
  /// The initial replay is complete. Sent exactly once, even when empty.
  CaughtUp
  /// The watch cannot continue. Nothing follows.
  Failed(
    /// Why it stopped.
    error: Error,
  )
}

/// Every way a `nats` call can fail.
pub type Error {
  /// A server URL could not be parsed.
  InvalidUrl(
    /// The URL as given.
    url: String,
    /// What is wrong with it.
    reason: String,
  )
  /// An NKey seed or credentials file could not be used.
  InvalidCredentials(
    /// What is wrong with it.
    reason: String,
  )
  /// A timing setting is out of range. Nothing was started.
  InvalidConfig(
    /// The setting, such as `ping_interval`.
    field: String,
    /// The allowed range.
    reason: String,
  )
  /// No server could be reached within the connect timeout.
  Unreachable(
    /// The last connection error.
    reason: String,
  )
  /// The server refused the handshake, including bad credentials.
  Rejected(
    /// The server's error text.
    message: String,
  )
  /// No reply arrived in time.
  Timeout
  /// The connection dropped before the server confirmed a flush. It
  /// reconnects on its own.
  Disconnected
  /// A request went to a subject with no subscribers.
  NoResponders
  /// A publish subject or subscription pattern is malformed.
  InvalidSubject(
    /// The subject as given.
    subject: String,
  )
  /// A reply subject is malformed.
  InvalidReplySubject(
    /// The subject as given.
    subject: String,
  )
  /// A header name is empty or contains a colon or anything other than
  /// printable ASCII.
  InvalidHeaderName(
    /// The name as given.
    name: String,
  )
  /// A header value contains a line break.
  InvalidHeaderValue(
    /// The name of the header whose value is invalid.
    name: String,
  )
  /// A queue group name is empty or contains whitespace.
  InvalidQueueGroup(
    /// The group as given.
    group: String,
  )
  /// Payload plus headers exceed the server's limit.
  PayloadTooLarge(
    /// The server's limit in bytes.
    max: Int,
  )
  /// The reconnect buffer is full; nothing was queued.
  BufferFull
  /// The server does not support headers, so a message with headers cannot
  /// be sent. The connection is unaffected.
  HeadersNotSupported
  /// A JetStream or KV operation failed.
  JetStream(
    /// The JetStream error.
    error: jetgleam.Error,
  )
}

/// A fetch that ended early, with whatever arrived first.
pub type FetchError {
  FetchError(
    /// Messages that arrived before the failure.
    received: List(jetgleam.Message),
    /// Why the fetch ended.
    error: Error,
  )
}

/// Starts a connection linked to the calling process and waits for the
/// first handshake. Returns `Rejected` or `Unreachable` rather than a
/// connection that will never work.
///
/// ```gleam
/// case config |> nats.with_token(token) |> nats.connect {
///   Ok(conn) -> run(conn)
///   Error(nats.Rejected(message)) -> panic as message
///   Error(error) -> panic as string.inspect(error)
/// }
/// ```
pub fn connect(config: Config) -> Result(Connection, Error) {
  use Nil <- result.try(validate(config.protocol))
  case start_process(config, option.None) {
    Error(error) -> Error(Unreachable(string.inspect(error)))
    Ok(started) -> {
      let timeout = config.protocol.connect_timeout + 1000
      case process.call(started.data, timeout, AwaitReady) {
        Ok(Nil) -> Ok(Connection(started.data))
        Error(error) -> {
          process.unlink(started.pid)
          process.kill(started.pid)
          Error(error)
        }
      }
    }
  }
}

/// Starts an unnamed connection without waiting for the handshake, for
/// supervisors that pass children's data by value.
pub fn start(config: Config) -> actor.StartResult(Connection) {
  start_connection(config, option.None)
}

/// A child specification that registers the connection under `name`. Look
/// the connection up with `named_connection`.
///
/// ```gleam
/// let name = process.new_name("nats")
///
/// let assert Ok(config) = nats.config(url)
/// let assert Ok(_) =
///   static_supervisor.new(static_supervisor.OneForOne)
///   |> static_supervisor.add(nats.supervised(config, name:))
///   |> static_supervisor.start
///
/// let conn = nats.named_connection(name)
/// ```
pub fn supervised(
  config: Config,
  name name: process.Name(Command),
) -> supervision.ChildSpecification(Connection) {
  supervision.worker(fn() { start_connection(config, option.Some(name)) })
}

/// A pure lookup that always succeeds. Calls through it crash while nothing
/// holds the name, as calls through any `Connection` do once its process
/// has stopped.
pub fn named_connection(name: process.Name(Command)) -> Connection {
  Connection(process.named_subject(name))
}

/// Where the connection is now. Crashes if the connection process is gone.
pub fn status(connection: Connection) -> Status {
  from_status(process.call(connection.subject, 5000, GetStatus))
}

fn from_status(status: protocol.Status) -> Status {
  case status {
    protocol.Connecting -> Connecting
    protocol.Connected -> Connected
    protocol.Reconnecting -> Reconnecting
  }
}

const max_timer = 4_294_967_295

// Timing settings reach Erlang timers, some of them summed: `connect` waits a
// second past the connect timeout, and a reconnect's handshake deadline is
// its delay plus the connect timeout.
fn validate(config: protocol.Config) -> Result(Nil, Error) {
  let in_range = fn(field, value, low, high) {
    case value >= low && value <= high {
      True -> Ok(Nil)
      False ->
        Error(InvalidConfig(
          field:,
          reason: "must be between "
            <> int.to_string(low)
            <> " and "
            <> int.to_string(high)
            <> " ms",
        ))
    }
  }
  let timeout = config.connect_timeout
  use Nil <- result.try(in_range(
    "ping_interval",
    config.ping_interval,
    1,
    max_timer,
  ))
  use Nil <- result.try(in_range(
    "connect_timeout",
    timeout,
    1,
    max_timer - 1000,
  ))
  use Nil <- result.try(in_range(
    "reconnect_initial",
    config.reconnect_initial,
    1,
    config.reconnect_max,
  ))
  in_range("reconnect_max", config.reconnect_max, 1, max_timer - timeout)
}

const write_timeout = 1000

@external(erlang, "jetgleam_erlang_ffi", "set_socket_options")
fn set_socket_options(
  socket: mug.Socket,
  write_timeout: Int,
) -> Result(Nil, mug.Error)

@external(erlang, "gen_tcp", "controlling_process")
fn controlling_process(socket: mug.Socket, pid: process.Pid) -> atom.Atom

@external(erlang, "gen_tcp", "close")
fn close_socket(socket: mug.Socket) -> atom.Atom

/// Test hook: delivers a finished connection attempt for `generation`, as a
/// slow attempt that a newer one superseded would.
@internal
pub fn deliver_attempt(
  connection: Connection,
  generation: protocol.Generation,
  socket: mug.Socket,
) -> Nil {
  process.send(connection.subject, Attempted(generation, Ok(socket)))
}

/// Test hook: delivers a connection attempt for `generation`, as a callback
/// left over from an earlier attempt would.
@internal
pub fn deliver_connect(
  connection: Connection,
  generation: protocol.Generation,
  server: protocol.Server,
) -> Nil {
  process.send(connection.subject, Connect(generation, server))
}

fn start_connection(
  config: Config,
  name: Option(process.Name(Command)),
) -> actor.StartResult(Connection) {
  use Nil <- result.try(
    validate(config.protocol)
    |> result.map_error(fn(error) { actor.InitFailed(string.inspect(error)) }),
  )
  use started <- result.map(start_process(config, name))
  actor.Started(pid: started.pid, data: Connection(started.data))
}

type State {
  State(
    // Bound to this process, unlike a named connection's public subject, so
    // callbacks scheduled by a previous instance cannot reach this one.
    self: process.Subject(Command),
    // The process that started the connection; it stops when that one does,
    // even on a normal exit that the link ignores.
    owner: process.Monitor,
    connection: protocol.Connection,
    current: Option(#(protocol.Generation, mug.Socket)),
    timers: Timers(protocol.Timer),
    waiters: List(process.Subject(Result(Nil, Error))),
    last_loss: Option(String),
    subscribers: Dict(Int, Subscriber),
    flushes: Dict(Int, process.Subject(Result(Nil, Error))),
    events: Option(process.Subject(Event)),
  )
}

type Subscriber {
  Subscriber(
    deliver_to: process.Subject(jetgleam.Message),
    monitor: process.Monitor,
  )
}

// Running timers by key. Each start gets a fresh token, so a callback that
// was already in the mailbox when its timer was cancelled or replaced is
// recognised as stale.
type Timers(key) =
  Dict(key, #(Reference, process.Timer))

@internal
pub fn start_timer(
  timers: Timers(key),
  key: key,
  after: Int,
  to subject: process.Subject(message),
  message message: fn(Reference) -> message,
) -> Timers(key) {
  let token = reference.new()
  let timer = process.send_after(subject, after, message(token))
  cancel_timer(timers, key) |> dict.insert(key, #(token, timer))
}

fn cancel_timer(timers: Timers(key), key: key) -> Timers(key) {
  case dict.get(timers, key) {
    Ok(#(_, timer)) -> {
      process.cancel_timer(timer)
      dict.delete(timers, key)
    }
    Error(Nil) -> timers
  }
}

@internal
pub fn claim_timer(
  timers: Timers(key),
  key: key,
  token: Reference,
) -> Result(Timers(key), Nil) {
  case dict.get(timers, key) {
    Ok(#(current, _)) if current == token -> Ok(dict.delete(timers, key))
    _ -> Error(Nil)
  }
}

fn start_process(
  config: Config,
  name: Option(process.Name(Command)),
) -> actor.StartResult(process.Subject(Command)) {
  let owner = process.self()
  let builder =
    actor.new_with_initialiser(5000, fn(public) {
      let self = process.new_subject()
      let selector =
        process.new_selector()
        |> process.select(public)
        |> process.select(self)
        |> mug.select_tcp_messages(Tcp)
        |> process.select_monitors(OwnerDown)
      let connection =
        protocol.new(config.protocol, crypto.strong_random_bytes(16))
      let state =
        State(
          self:,
          owner: process.monitor(owner),
          connection:,
          current: option.None,
          timers: dict.new(),
          waiters: [],
          last_loss: option.None,
          subscribers: dict.new(),
          flushes: dict.new(),
          events: config.events,
        )
      apply(state, protocol.open(connection))
      |> actor.initialised
      |> actor.selecting(selector)
      |> actor.returning(public)
      |> Ok
    })
    |> actor.on_message(handle)
  case name {
    option.Some(name) -> actor.named(builder, name)
    option.None -> builder
  }
  |> actor.start
}

fn handle(state: State, message: Command) -> actor.Next(State, Command) {
  case message {
    OwnerDown(process.ProcessDown(monitor:, ..)) if monitor == state.owner ->
      actor.stop()
    _ -> actor.continue(step(state, message))
  }
}

fn step(state: State, message: Command) -> State {
  case message {
    Connect(generation, server) -> {
      use <- bool.guard(
        when: !protocol.is_current(state.connection, generation),
        return: state,
      )
      let timeout = state.connection.config.connect_timeout
      let owner = process.self()
      let self = state.self
      process.spawn(fn() {
        let result =
          mug.new(server.host, port: server.port)
          |> mug.timeout(timeout)
          |> mug.connect
          |> result.map_error(describe_connect_error)
          |> result.try(fn(socket) {
            use Nil <- result.try(
              set_socket_options(socket, write_timeout)
              |> result.map_error(mug.describe_error),
            )
            case controlling_process(socket, owner) == atom.create("ok") {
              True -> Ok(socket)
              False -> Error("could not hand over the socket")
            }
          })
        process.send(self, Attempted(generation, result))
      })
      state
    }
    Attempted(generation, result) ->
      case protocol.is_current(state.connection, generation), result {
        True, Ok(socket) -> {
          mug.receive_next_packet_as_message(socket)
          State(..state, current: option.Some(#(generation, socket)))
        }
        True, Error(reason) ->
          apply(
            state,
            protocol.transport_failed(state.connection, generation, reason),
          )
        False, Ok(socket) -> {
          close_socket(socket)
          state
        }
        False, Error(_) -> state
      }
    Tcp(message) -> handle_tcp(state, message)
    TimerFired(timer, token) ->
      case claim_timer(state.timers, timer, token) {
        Ok(running) ->
          apply(
            State(..state, timers: running),
            protocol.timer_fired(state.connection, timer),
          )
        Error(Nil) -> state
      }
    AwaitReady(reply) ->
      case state.connection.status {
        protocol.Connected -> {
          process.send(reply, Ok(Nil))
          state
        }
        protocol.Connecting | protocol.Reconnecting -> {
          let timeout = state.connection.config.connect_timeout
          process.send_after(state.self, timeout, ConnectDeadline)
          State(..state, waiters: [reply, ..state.waiters])
        }
      }
    ConnectDeadline -> {
      let reason = option.unwrap(state.last_loss, "timed out")
      reply_waiters(state, Error(Unreachable(reason)))
    }
    GetStatus(reply) -> {
      process.send(reply, state.connection.status)
      state
    }
    Publish(message, reply) ->
      case protocol.publish(state.connection, message) {
        Ok(step) -> {
          process.send(reply, Ok(Nil))
          apply(state, step)
        }
        Error(error) -> {
          process.send(reply, Error(error))
          state
        }
      }
    Subscribe(subject, queue, owner, deliver_to, reply) ->
      case protocol.subscribe(state.connection, subject, queue) {
        Ok(#(connection, sid, effects)) -> {
          process.send(reply, Ok(#(sid, state.self)))
          let subscriber = Subscriber(deliver_to, process.monitor(owner))
          apply(
            State(
              ..state,
              subscribers: dict.insert(state.subscribers, sid, subscriber),
            ),
            #(connection, effects),
          )
        }
        Error(error) -> {
          process.send(reply, Error(error))
          state
        }
      }
    Unsubscribe(sid) -> remove_subscriber(state, sid)
    StopSubscription(sid, reply) -> {
      let state = remove_subscriber(state, sid)
      process.send(reply, Nil)
      state
    }
    OwnerDown(process.ProcessDown(monitor:, ..)) ->
      dict.fold(state.subscribers, state, fn(state, sid, subscriber) {
        case subscriber.monitor == monitor {
          True -> remove_subscriber(state, sid)
          False -> state
        }
      })
    OwnerDown(process.PortDown(..)) -> state
    Flush(reply) -> {
      let #(connection, token, effects) = protocol.flush(state.connection)
      apply(
        State(..state, flushes: dict.insert(state.flushes, token, reply)),
        #(connection, effects),
      )
    }
    CancelFlush(reply, ack) -> {
      let state =
        dict.fold(state.flushes, state, fn(state, token, waiter) {
          case waiter == reply {
            True ->
              State(
                ..state,
                connection: protocol.cancel_flush(state.connection, token),
                flushes: dict.delete(state.flushes, token),
              )
            False -> state
          }
        })
      process.send(ack, Nil)
      state
    }
    NewInbox(reply) -> {
      let #(connection, inbox) = protocol.new_inbox(state.connection)
      process.send(reply, inbox)
      State(..state, connection:)
    }
  }
}

fn describe_connect_error(error: mug.ConnectError) -> String {
  case error {
    mug.ConnectFailedIpv4(error)
    | mug.ConnectFailedIpv6(error)
    | mug.ConnectFailedBoth(error, _) -> mug.describe_error(error)
  }
}

fn handle_tcp(state: State, message: mug.TcpMessage) -> State {
  case state.current {
    option.Some(#(generation, socket)) ->
      case message {
        mug.Packet(from, bytes) if from == socket -> {
          mug.receive_next_packet_as_message(socket)
          apply(state, protocol.received(state.connection, generation, bytes))
        }
        mug.SocketClosed(from) if from == socket ->
          apply(state, protocol.transport_closed(state.connection, generation))
        mug.TcpError(from, error) if from == socket ->
          apply(
            state,
            protocol.transport_failed(
              state.connection,
              generation,
              mug.describe_error(error),
            ),
          )
        _ -> state
      }
    option.None -> state
  }
}

/// Stores the new connection, performs its effects in order and reports a
/// status change.
fn apply(
  state: State,
  step: #(protocol.Connection, List(protocol.Effect)),
) -> State {
  let before = state.connection.status
  let #(connection, effects) = step
  let state = perform(State(..state, connection:), effects)
  let after = state.connection.status
  use <- bool.guard(when: before == after, return: state)
  notify(state, StatusChanged(from_status(after)))
  state
}

fn notify(state: State, event: Event) -> Nil {
  let _ = option.map(state.events, process.send(_, event))
  Nil
}

fn perform(state: State, effects: List(protocol.Effect)) -> State {
  case effects {
    [] -> state
    [protocol.Transmit(bytes), ..rest] ->
      case state.current {
        option.Some(#(generation, socket)) ->
          case mug.send_builder(socket, bytes) {
            Ok(Nil) -> perform(state, rest)
            Error(error) ->
              case protocol.is_current(state.connection, generation) {
                // A loss already scheduled later in this batch must still run.
                False -> perform(state, rest)
                True -> {
                  let #(connection, effects) =
                    protocol.transport_failed(
                      state.connection,
                      generation,
                      mug.describe_error(error),
                    )
                  // Discard the abandoned transport's remaining effects,
                  // including any successful-handshake announcement.
                  perform(State(..state, connection:), effects)
                }
              }
          }
        option.None -> perform(state, rest)
      }
    [protocol.Open(generation:, server:, after:), ..rest] -> {
      process.send_after(state.self, after, Connect(generation, server))
      perform(state, rest)
    }
    [protocol.CloseTransport(generation), ..rest] -> {
      let state = case state.current {
        option.Some(#(current, socket)) if current == generation -> {
          let _ = mug.shutdown(socket)
          State(..state, current: option.None)
        }
        _ -> state
      }
      perform(state, rest)
    }
    [protocol.StartTimer(timer, after), ..rest] ->
      perform(
        State(
          ..state,
          timers: start_timer(
            state.timers,
            timer,
            after,
            to: state.self,
            message: TimerFired(timer, _),
          ),
        ),
        rest,
      )
    [protocol.CancelTimer(timer), ..rest] ->
      perform(State(..state, timers: cancel_timer(state.timers, timer)), rest)
    [protocol.Emit(event), ..rest] -> perform(emit(state, event), rest)
  }
}

fn remove_subscriber(state: State, sid: Int) -> State {
  case dict.get(state.subscribers, sid) {
    Ok(subscriber) -> process.demonitor_process(subscriber.monitor)
    Error(Nil) -> Nil
  }
  apply(
    State(..state, subscribers: dict.delete(state.subscribers, sid)),
    protocol.unsubscribe(state.connection, sid),
  )
}

fn emit(state: State, event: protocol.Event) -> State {
  case event {
    protocol.Ready -> reply_waiters(state, Ok(Nil))
    protocol.Lost(protocol.Rejected(message)) if state.waiters != [] ->
      reply_waiters(state, Error(Rejected(message)))
    protocol.Lost(reason) ->
      State(..state, last_loss: option.Some(describe(reason)))
    protocol.Warned(message) -> {
      notify(state, Warning(message))
      state
    }
    protocol.PublishDropped(limit) -> {
      notify(state, PublishDropped(from_protocol(protocol.limit_error(limit))))
      state
    }
    protocol.Delivered(sid, message) -> {
      let _ =
        dict.get(state.subscribers, sid)
        |> result.map(fn(subscriber) {
          process.send(subscriber.deliver_to, message)
        })
      state
    }
    protocol.Flushed(token) -> reply_flush(state, token, Ok(Nil))
    protocol.FlushFailed(token) ->
      reply_flush(state, token, Error(Disconnected))
  }
}

fn reply_flush(state: State, token: Int, result: Result(Nil, Error)) -> State {
  let _ = dict.get(state.flushes, token) |> result.map(process.send(_, result))
  State(..state, flushes: dict.delete(state.flushes, token))
}

fn reply_waiters(state: State, result: Result(Nil, Error)) -> State {
  list.each(state.waiters, process.send(_, result))
  State(..state, waiters: [])
}

fn describe(reason: protocol.Disconnect) -> String {
  case reason {
    protocol.TransportClosed -> "connection closed"
    protocol.TransportFailed(reason) | protocol.ProtocolViolation(reason) ->
      reason
    protocol.Stale -> "stale connection"
    protocol.Rejected(message) -> message
  }
}

/// Sends `payload` to `subject` without waiting for the server. While
/// disconnected the publish is buffered, and `BufferFull` means it was not.
/// Returns `InvalidSubject` or `PayloadTooLarge` without sending.
pub fn publish(
  connection: Connection,
  subject: String,
  payload: BitArray,
) -> Result(Nil, Error) {
  publish_message(connection, jetgleam.message(subject, payload))
}

/// Publishes a message built with `jetgleam`, for when you need
/// headers or a reply subject.
///
/// ```gleam
/// jetgleam.message("orders.new", payload)
/// |> jetgleam.set_header("Trace", trace_id)
/// |> nats.publish_message(conn, _)
/// ```
pub fn publish_message(
  connection: Connection,
  message: jetgleam.Message,
) -> Result(Nil, Error) {
  call(connection, Publish(message, _))
}

/// Delivers messages on `subject` (wildcards allowed) to `messages`. The
/// calling process owns the subscription: only it can receive, and the
/// subscription ends when it exits. Survives reconnects.
pub fn subscribe(
  connection: Connection,
  subject: String,
) -> Result(Subscription(jetgleam.Message), Error) {
  start_subscription(connection, subject, option.None)
}

/// Like `subscribe`, but each message goes to only one member of `group`.
///
/// ```gleam
/// // Run this in every worker; each order is handled once.
/// let assert Ok(sub) = nats.queue_subscribe(conn, "orders.*", group: "workers")
/// ```
pub fn queue_subscribe(
  connection: Connection,
  subject: String,
  group group: String,
) -> Result(Subscription(jetgleam.Message), Error) {
  start_subscription(connection, subject, option.Some(group))
}

fn start_subscription(
  connection: Connection,
  subject: String,
  queue: Option(String),
) -> Result(Subscription(jetgleam.Message), Error) {
  let messages = process.new_subject()
  use #(sid, actor) <- result.map(
    call(connection, Subscribe(subject, queue, process.self(), messages, _)),
  )
  Subscription(
    messages:,
    stop: fn() { process.send(actor, Unsubscribe(sid)) },
    close: fn() { process.call(actor, 5000, StopSubscription(sid, _)) },
  )
}

fn close(subscription: Subscription(a)) -> Nil {
  subscription.close()
  drain(subscription.messages)
}

/// Where items arrive. Only the subscribing process may receive.
pub fn messages(subscription: Subscription(a)) -> process.Subject(a) {
  subscription.messages
}

/// Stops delivery. A plain subscription returns without waiting; a KV watch
/// waits up to 5 seconds for its watch process to exit. Messages already
/// delivered stay in the mailbox.
pub fn unsubscribe(subscription: Subscription(a)) -> Nil {
  subscription.stop()
}

/// Publishes to `subject` and waits for the first reply. A 503 status reply
/// becomes `NoResponders` at once rather than waiting out the timeout.
///
/// `timeout` covers only the wait for the reply, which starts once the
/// connection has accepted the publish; while disconnected the publish is
/// buffered and the wait runs anyway. After a timeout a late reply is
/// dropped, not left in the caller's mailbox.
///
/// ```gleam
/// case nats.request(conn, "time.service", <<>>, timeout: 1000) {
///   Ok(reply) -> Ok(reply.payload)
///   Error(nats.NoResponders) -> Error("nobody is listening")
///   Error(_) -> Error("no reply in time")
/// }
/// ```
///
/// To answer requests, subscribe and publish to each message's `reply_to`.
pub fn request(
  connection: Connection,
  subject: String,
  payload: BitArray,
  timeout milliseconds: Int,
) -> Result(jetgleam.Message, Error) {
  request_message(
    connection,
    jetgleam.message(subject, payload),
    timeout: milliseconds,
  )
}

/// Like `request`, for a message with headers. Any `reply_to` is replaced
/// by a fresh inbox.
pub fn request_message(
  connection: Connection,
  message: jetgleam.Message,
  timeout milliseconds: Int,
) -> Result(jetgleam.Message, Error) {
  let inbox = new_inbox(connection)
  use sub <- result.try(subscribe(connection, inbox))
  let reply =
    publish_message(connection, jetgleam.set_reply_to(message, inbox))
    |> result.try(fn(_) {
      process.receive(messages(sub), milliseconds)
      |> result.replace_error(Timeout)
    })
  close(sub)
  use reply <- result.try(reply)
  case reply.status {
    option.Some(jetgleam.Status(code: 503, ..)) -> Error(NoResponders)
    _ -> Ok(reply)
  }
}

/// Waits until the server has processed everything sent so far. Returns
/// `Disconnected` if the connection drops before the server answers, and
/// `Timeout` if it does not answer in time. A flush made while disconnected
/// waits for the reconnect. Timing out removes its PING if it has not yet
/// been sent; already-sent PINGs retain their position in the PONG queue.
pub fn flush(
  connection: Connection,
  timeout milliseconds: Int,
) -> Result(Nil, Error) {
  let reply = process.new_subject()
  let assert Ok(owner) = process.subject_owner(connection.subject)
    as "connection subject had no owner"
  let monitor = process.monitor(owner)
  process.send(connection.subject, Flush(reply))
  let outcome =
    process.new_selector()
    |> process.select(reply)
    |> process.select_specific_monitor(monitor, fn(down) {
      panic as { "connection exited: " <> string.inspect(down) }
    })
    |> process.selector_receive(milliseconds)
  process.demonitor_process(monitor)
  case outcome {
    Ok(result) -> result
    Error(Nil) -> {
      // Withdraw the waiter and any unsent PING. Once acknowledged, any
      // answer sent first is already here, so nothing arrives later.
      process.call(connection.subject, 5000, CancelFlush(reply, _))
      process.receive(reply, 0) |> result.unwrap(Error(Timeout))
    }
  }
}

/// A subject unique to this connection, starting `_INBOX.`, for replies.
pub fn new_inbox(connection: Connection) -> String {
  process.call(connection.subject, 5000, NewInbox)
}

fn call(
  connection: Connection,
  make: fn(process.Subject(Result(a, protocol.Error))) -> Command,
) -> Result(a, Error) {
  process.call(connection.subject, 5000, make)
  |> result.map_error(from_protocol)
}

fn from_protocol(error: protocol.Error) -> Error {
  case error {
    protocol.InvalidUrl(url:, reason:) -> InvalidUrl(url:, reason:)
    protocol.InvalidSubject(subject) -> InvalidSubject(subject)
    protocol.InvalidReplySubject(subject) -> InvalidReplySubject(subject)
    protocol.InvalidHeaderName(name) -> InvalidHeaderName(name)
    protocol.InvalidHeaderValue(name) -> InvalidHeaderValue(name)
    protocol.InvalidQueueGroup(group) -> InvalidQueueGroup(group)
    protocol.PayloadTooLarge(max) -> PayloadTooLarge(max)
    protocol.BufferFull -> BufferFull
    protocol.HeadersNotSupported -> HeadersNotSupported
  }
}

/// Runs a JetStream or KV operation. API errors arrive as `JetStream(_)`;
/// transport errors as the matching `nats` variant. Each request in the
/// operation gets its own timeout (`jetgleam.with_timeout`), so one that
/// pages through a listing can take several timeouts in total.
///
/// ```gleam
/// case kv.get(bucket: "settings", key: "theme") |> nats.execute(on: conn) {
///   Ok(entry) -> entry.value
///   Error(nats.JetStream(jetgleam.NotFound)) -> <<"light":utf8>>
///   Error(error) -> panic as string.inspect(error)
/// }
/// ```
pub fn execute(
  operation: jetgleam.Operation(a),
  on connection: Connection,
) -> Result(a, Error) {
  jetgleam.run(
    operation,
    publish: publish_message(connection, _),
    request: fn(message, timeout) {
      request_message(connection, message, timeout:)
    },
    error: JetStream,
  )
}

/// Runs a `consumer.Fetch` pull. A full batch or server completion returns
/// `Ok`, including `Ok([])`. Reaching the deadline or a server error returns
/// `FetchError` with what arrived first, so those messages can still be
/// handled. A lost connection is not reported separately; the fetch waits
/// for the deadline.
///
/// ```gleam
/// let batch = case
///   consumer.fetch(
///     stream: "ORDERS",
///     consumer: "worker",
///     max: 10,
///     wait: duration.seconds(2),
///   )
///   |> nats.fetch(on: conn)
/// {
///   Ok(batch) -> batch
///   Error(nats.FetchError(received:, error: _)) -> received
/// }
/// ```
pub fn fetch(
  fetch: consumer.Fetch,
  on connection: Connection,
) -> Result(List(jetgleam.Message), FetchError) {
  let started = now()
  let inbox = new_inbox(connection)
  let failed = fn(error) { FetchError([], error) }
  use #(fetching, message, wait) <- result.try(
    consumer.start_fetch(fetch, inbox)
    |> result.map_error(fn(error) { failed(JetStream(error)) }),
  )
  use sub <- result.try(
    subscribe(connection, inbox) |> result.map_error(failed),
  )
  let result = case publish_message(connection, message) {
    Ok(Nil) -> receive_fetch(sub, fetching, started + wait)
    Error(error) -> Error(failed(error))
  }
  close(sub)
  result
}

fn receive_fetch(
  sub: Subscription(jetgleam.Message),
  fetching: consumer.Fetching,
  deadline: Int,
) -> Result(List(jetgleam.Message), FetchError) {
  case process.receive(messages(sub), int.max(deadline - now(), 0)) {
    Error(Nil) -> Error(FetchError(consumer.fetch_messages(fetching), Timeout))
    Ok(message) ->
      case consumer.fetch_received(fetching, message) {
        consumer.Continue(fetching) -> receive_fetch(sub, fetching, deadline)
        consumer.Complete(messages) -> Ok(messages)
        consumer.Stop(messages, error) ->
          Error(FetchError(messages, JetStream(error)))
      }
  }
}

/// Monotonic milliseconds.
@external(erlang, "os", "perf_counter")
fn now_in(per_second: Int) -> Int

fn now() -> Int {
  now_in(1000)
}

/// Starts a live KV watch in a linked process. Survives reconnects and
/// server restarts; reports `Failed` only when it cannot recover. Items
/// arrive on `messages(subscription)`; stop with `unsubscribe`.
///
/// ```gleam
/// let assert Ok(watch) = kv.watch("settings", filter: ">") |> nats.watch(on: conn)
/// let assert Ok(nats.Changed(entry)) = process.receive(nats.messages(watch), 5000)
/// nats.unsubscribe(watch)
/// ```
pub fn watch(
  watch: kv.Watch,
  on connection: Connection,
) -> Result(Subscription(WatchEvent), Error) {
  let events = process.new_subject()
  let io =
    WatchIo(
      subscribe: fn(inbox) {
        use sub <- result.map(subscribe(connection, inbox))
        #(messages(sub), sub.close)
      },
      publish: publish_message(connection, _),
      execute: execute(_, on: connection),
      new_inbox: fn() { new_inbox(connection) },
      fatal: fn(error) {
        case error {
          JetStream(jetgleam.NotFound) -> True
          _ -> False
        }
      },
    )
  use stop <- result.map(
    start_watch(
      watch,
      io,
      fn(entry) { process.send(events, Changed(entry)) },
      fn() { process.send(events, CaughtUp) },
      fn(error) { process.send(events, Failed(error)) },
    ),
  )
  Subscription(messages: events, stop:, close: stop)
}

/// Runs a watch until `CaughtUp` and returns the entries. Use with
/// `kv.keys` (map `entry.key`) or `kv.history`.
///
/// `timeout` starts once the watch's consumer exists and covers the whole
/// replay. On `Timeout` the watch is stopped and the entries received so
/// far are discarded.
///
/// ```gleam
/// let assert Ok(entries) =
///   kv.keys("settings") |> nats.collect(on: conn, timeout: 5000)
/// list.map(entries, fn(entry) { entry.key })
/// ```
pub fn collect(
  target: kv.Watch,
  on connection: Connection,
  timeout milliseconds: Int,
) -> Result(List(kv.Entry), Error) {
  use subscription <- result.try(watch(target, on: connection))
  let result = collect_events(subscription, now() + milliseconds, [])
  close(subscription)
  result
}

fn collect_events(
  subscription: Subscription(WatchEvent),
  deadline: Int,
  entries: List(kv.Entry),
) -> Result(List(kv.Entry), Error) {
  case process.receive(messages(subscription), int.max(deadline - now(), 0)) {
    Error(Nil) -> Error(Timeout)
    Ok(Changed(entry)) ->
      collect_events(subscription, deadline, [entry, ..entries])
    Ok(CaughtUp) -> Ok(list.reverse(entries))
    Ok(Failed(error)) -> Error(error)
  }
}

// The `subscribe` callback returns a synchronous close barrier: after it
// returns, no more messages may be sent to the returned subject. Messages
// already queued there are drained by the watch before it changes inboxes.
@internal
pub type WatchIo(e) {
  WatchIo(
    subscribe: fn(String) ->
      Result(#(process.Subject(jetgleam.Message), fn() -> Nil), e),
    publish: fn(jetgleam.Message) -> Result(Nil, e),
    execute: fn(jetgleam.Operation(consumer.Info)) -> Result(consumer.Info, e),
    new_inbox: fn() -> String,
    fatal: fn(e) -> Bool,
  )
}

type WatchInput {
  Received(jetgleam.Message)
  Stop
}

// The inbox of the current consumer. Each recreation gets a fresh one, so
// the replaced consumer's deliveries and control frames are never read.
type ConsumerSubscription {
  ConsumerSubscription(
    messages: process.Subject(jetgleam.Message),
    close: fn() -> Nil,
  )
}

type Runner(e) {
  Runner(
    io: WatchIo(e),
    stop: process.Subject(Nil),
    owner: process.Monitor,
    deliver: fn(kv.Entry) -> Nil,
    caught_up: fn() -> Nil,
    failed: fn(e) -> Nil,
  )
}

@internal
pub fn start_watch(
  watch: kv.Watch,
  io: WatchIo(e),
  deliver: fn(kv.Entry) -> Nil,
  caught_up: fn() -> Nil,
  failed: fn(e) -> Nil,
) -> Result(fn() -> Nil, e) {
  let ready = process.new_subject()
  let owner = process.self()
  let pid =
    process.spawn(fn() {
      let owner = process.monitor(owner)
      let inbox = io.new_inbox()
      let #(tracker, operation) = kv.track(watch, inbox)
      case create(io, inbox, operation) {
        Error(error) -> process.send(ready, Error(error))
        Ok(#(subscription, info)) -> {
          let stop = process.new_subject()
          process.send(ready, Ok(stop))
          let runner = Runner(io:, stop:, owner:, deliver:, caught_up:, failed:)
          let #(tracker, steps) = kv.consumer_created(tracker, info)
          let #(tracker, subscription, outcome) =
            apply_steps(runner, tracker, subscription, steps)
          let subscription = loop(runner, tracker, subscription, outcome)
          retire(subscription)
        }
      }
    })
  use stop <- result.map(process.receive_forever(ready))
  fn() { halt(pid, stop) }
}

fn halt(pid: process.Pid, stop: process.Subject(Nil)) -> Nil {
  let monitor = process.monitor(pid)
  process.send(stop, Nil)
  let _ =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(_) { Nil })
    |> process.selector_receive(5000)
  Nil
}

fn create(
  io: WatchIo(e),
  inbox: String,
  operation: jetgleam.Operation(consumer.Info),
) -> Result(#(ConsumerSubscription, consumer.Info), e) {
  use #(messages, close) <- result.try(io.subscribe(inbox))
  case io.execute(operation) {
    Ok(info) -> Ok(#(ConsumerSubscription(messages:, close:), info))
    Error(error) -> {
      retire(ConsumerSubscription(messages:, close:))
      Error(error)
    }
  }
}

fn retire(subscription: ConsumerSubscription) -> Nil {
  subscription.close()
  drain(subscription.messages)
}

fn drain(subject: process.Subject(a)) -> Nil {
  case process.receive(subject, 0) {
    Ok(_) -> drain(subject)
    Error(Nil) -> Nil
  }
}

fn loop(
  runner: Runner(e),
  tracker: kv.Tracker,
  subscription: ConsumerSubscription,
  outcome: Result(Nil, e),
) -> ConsumerSubscription {
  let fatal = case outcome {
    Error(error) -> runner.io.fatal(error)
    Ok(Nil) -> False
  }
  case outcome {
    Error(error) if fatal -> {
      runner.failed(error)
      subscription
    }
    _ -> {
      let selector =
        process.new_selector()
        |> process.select_map(subscription.messages, Received)
        |> process.select_map(runner.stop, fn(_) { Stop })
        |> process.select_specific_monitor(runner.owner, fn(_) { Stop })
      let next = case
        process.selector_receive(selector, kv.heartbeat_interval)
      {
        Error(Nil) -> Ok(#(tracker, [kv.Recreate]))
        Ok(Received(message)) -> Ok(kv.observe(tracker, message))
        Ok(Stop) -> Error(Nil)
      }
      case next {
        Error(Nil) -> subscription
        Ok(#(tracker, steps)) -> {
          let #(tracker, subscription, outcome) =
            apply_steps(runner, tracker, subscription, steps)
          loop(runner, tracker, subscription, outcome)
        }
      }
    }
  }
}

fn apply_steps(
  runner: Runner(e),
  tracker: kv.Tracker,
  subscription: ConsumerSubscription,
  steps: List(kv.Step),
) -> #(kv.Tracker, ConsumerSubscription, Result(Nil, e)) {
  case steps {
    [] -> #(tracker, subscription, Ok(Nil))
    [kv.Deliver(entry), ..rest] -> {
      runner.deliver(entry)
      apply_steps(runner, tracker, subscription, rest)
    }
    [kv.CaughtUp, ..rest] -> {
      runner.caught_up()
      apply_steps(runner, tracker, subscription, rest)
    }
    [kv.Respond(message), ..rest] ->
      case runner.io.publish(message) {
        Ok(Nil) -> apply_steps(runner, tracker, subscription, rest)
        Error(error) -> #(tracker, subscription, Error(error))
      }
    [kv.Recreate, ..rest] -> {
      let inbox = runner.io.new_inbox()
      let #(next, operation) = kv.recreate(tracker, inbox)
      case create(runner.io, inbox, operation) {
        Error(error) -> #(tracker, subscription, Error(error))
        Ok(#(replacement, info)) -> {
          retire(subscription)
          let #(next, more) = kv.consumer_created(next, info)
          apply_steps(runner, next, replacement, list.append(more, rest))
        }
      }
    }
  }
}
