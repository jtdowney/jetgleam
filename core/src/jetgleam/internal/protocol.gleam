import gleam/bit_array
import gleam/bool
import gleam/bytes_tree.{type BytesTree}
import gleam/dict.{type Dict}
import gleam/dynamic/decode
import gleam/function
import gleam/int
import gleam/json.{type Json}
import gleam/list
import gleam/option.{type Option}
import gleam/result
import gleam/string
import gleam/uri
import jetgleam
import jetgleam/internal/nkey

pub type Config {
  Config(
    server: Server,
    standby: List(Server),
    name: Option(String),
    auth: Option(Auth),
    echo_enabled: Bool,
    ping_interval: Int,
    connect_timeout: Int,
    reconnect_initial: Int,
    reconnect_max: Int,
    buffer_size: Int,
  )
}

pub type Auth {
  Token(String)
  UserPassword(user: String, password: String)
  Nkey(nkey.Signer)
  Jwt(jwt: String, signer: nkey.Signer)
}

const max_pings_outstanding = 2

pub fn config(url: String) -> Result(Config, Error) {
  use #(server, auth) <- result.map(parse_url(url))
  Config(
    server:,
    standby: [],
    name: option.None,
    auth:,
    echo_enabled: True,
    ping_interval: 120_000,
    connect_timeout: 2000,
    reconnect_initial: 100,
    reconnect_max: 2000,
    buffer_size: 8_388_608,
  )
}

pub fn add_server(config: Config, url: String) -> Result(Config, Error) {
  use #(server, _) <- result.map(parse_url(url))
  Config(..config, standby: list.append(config.standby, [server]))
}

pub type Server {
  Server(host: String, port: Int)
}

pub type ServerInfo {
  ServerInfo(
    max_payload: Int,
    headers: Bool,
    tls_required: Bool,
    nonce: Option(String),
    connect_urls: List(String),
  )
}

pub type Connection {
  Connection(
    config: Config,
    status: Status,
    server: Server,
    standby: List(Server),
    next_generation: Int,
    info: Option(ServerInfo),
    inbox_prefix: String,
    inbox_counter: Int,
    leftover: BitArray,
    pongs: List(PendingPong),
    failures: Int,
    outstanding_pings: Int,
    subscriptions: Dict(Int, ValidSubscription),
    next_sid: Int,
    next_token: Int,
    buffer: List(Buffered),
    buffer_bytes: Int,
  )
}

pub type PendingPong {
  HandshakePong
  FlushPong(token: Int)
  KeepalivePong
}

pub type Buffered {
  BufferedPublish(publish: ValidPublish, bytes: BytesTree)
  BufferedFlush(token: Int)
}

pub type Generation {
  Generation(Int)
}

pub type Status {
  Connecting
  Connected
  Reconnecting
}

pub type Timer {
  Keepalive
  Handshake
}

pub type Disconnect {
  TransportClosed
  TransportFailed(reason: String)
  ProtocolViolation(reason: String)
  Stale
  Rejected(message: String)
}

pub type Event {
  Ready
  Lost(reason: Disconnect)
  Delivered(sid: Int, message: jetgleam.Message)
  Flushed(token: Int)
  FlushFailed(token: Int)
  Warned(message: String)
  PublishDropped(limit: Limit)
}

pub type Limit {
  MaxPayload(max: Int)
  NoHeaders
}

pub type Effect {
  Transmit(bytes: BytesTree)
  Open(generation: Generation, server: Server, after: Int)
  CloseTransport(generation: Generation)
  StartTimer(timer: Timer, after: Int)
  CancelTimer(timer: Timer)
  Emit(event: Event)
}

pub type Error {
  InvalidUrl(url: String, reason: String)
  InvalidSubject(subject: String)
  InvalidReplySubject(subject: String)
  InvalidHeaderName(name: String)
  InvalidHeaderValue(name: String)
  InvalidQueueGroup(group: String)
  PayloadTooLarge(max: Int)
  BufferFull
  HeadersNotSupported
}

pub fn limit_error(limit: Limit) -> Error {
  case limit {
    MaxPayload(max) -> PayloadTooLarge(max)
    NoHeaders -> HeadersNotSupported
  }
}

pub fn new(config: Config, entropy: BitArray) -> Connection {
  Connection(
    config:,
    status: Connecting,
    server: config.server,
    standby: config.standby,
    next_generation: 0,
    info: option.None,
    inbox_prefix: bit_array.base16_encode(entropy),
    inbox_counter: 0,
    leftover: <<>>,
    pongs: [],
    failures: 0,
    outstanding_pings: 0,
    subscriptions: dict.new(),
    next_sid: 1,
    next_token: 0,
    buffer: [],
    buffer_bytes: 0,
  )
}

fn parse_url(url: String) -> Result(#(Server, Option(Auth)), Error) {
  let with_scheme = case string.contains(url, "://") {
    True -> url
    False -> "nats://" <> url
  }
  use parsed <- result.try(
    uri.parse(with_scheme)
    |> result.replace_error(InvalidUrl(url:, reason: "malformed")),
  )
  use <- bool.guard(
    when: parsed.scheme != option.Some("nats"),
    return: invalid_url(url, "scheme must be nats"),
  )
  use host <- result.try(case parsed.host {
    option.Some(host) if host != "" -> Ok(host)
    _ -> invalid_url(url, "missing host")
  })
  let port = option.unwrap(parsed.port, 4222)
  use <- bool.guard(
    when: port < 1 || port > 65_535,
    return: invalid_url(url, "port out of range"),
  )
  let decode = fn(text) {
    case well_escaped(text) {
      True -> uri.percent_decode(text)
      False -> Error(Nil)
    }
    |> result.replace_error(InvalidUrl(url:, reason: "bad percent-encoding"))
  }
  use auth <- result.map(case parsed.userinfo {
    option.None -> Ok(option.None)
    option.Some(userinfo) ->
      case string.split_once(userinfo, ":") {
        Ok(#(user, password)) -> {
          use user <- result.try(decode(user))
          use password <- result.map(decode(password))
          option.Some(UserPassword(user, password))
        }
        Error(Nil) ->
          decode(userinfo)
          |> result.map(fn(token) { option.Some(Token(token)) })
      }
  })
  #(Server(host, port), auth)
}

// Every `%` starts two hex digits. Checked here because Erlang's
// `uri.percent_decode` passes a malformed escape through unchanged.
fn well_escaped(text: String) -> Bool {
  string.split(text, "%")
  |> list.drop(1)
  |> list.all(fn(escape) {
    case string.to_graphemes(escape) {
      [a, b, ..] -> string.contains(hex, a) && string.contains(hex, b)
      _ -> False
    }
  })
}

const hex = "0123456789abcdefABCDEF"

fn invalid_url(url: String, reason: String) -> Result(a, Error) {
  Error(InvalidUrl(url:, reason:))
}

pub fn open(connection: Connection) -> #(Connection, List(Effect)) {
  let effects = [
    Open(Generation(connection.next_generation), connection.server, after: 0),
    StartTimer(Handshake, connection.config.connect_timeout),
  ]
  #(
    Connection(..connection, next_generation: connection.next_generation + 1),
    effects,
  )
}

pub fn transport_closed(
  connection: Connection,
  generation: Generation,
) -> #(Connection, List(Effect)) {
  transport_lost(connection, generation, TransportClosed)
}

pub fn transport_failed(
  connection: Connection,
  generation: Generation,
  reason: String,
) -> #(Connection, List(Effect)) {
  transport_lost(connection, generation, TransportFailed(reason))
}

pub fn is_current(connection: Connection, generation: Generation) -> Bool {
  generation == Generation(connection.next_generation - 1)
}

fn transport_lost(
  connection: Connection,
  generation: Generation,
  reason: Disconnect,
) -> #(Connection, List(Effect)) {
  use <- bool.guard(
    when: !is_current(connection, generation),
    return: #(connection, []),
  )
  lose(connection, reason)
}

pub fn received(
  connection: Connection,
  generation: Generation,
  bytes: BitArray,
) -> #(Connection, List(Effect)) {
  use <- bool.guard(
    when: !is_current(connection, generation),
    return: #(connection, []),
  )
  let #(ops, rest) = parse(bit_array.append(connection.leftover, bytes))
  case rest {
    Ok(leftover) -> handle_ops(Connection(..connection, leftover:), ops, [])
    // The ops before the error count; a loss among them already moved on to
    // a new generation, so the error is not a second loss.
    Error(reason) -> {
      let #(connection, effects) = handle_ops(connection, ops, [])
      case is_current(connection, generation) {
        True -> {
          let #(connection, lost) = lose(connection, ProtocolViolation(reason))
          #(connection, list.append(effects, lost))
        }
        False -> #(connection, effects)
      }
    }
  }
}

fn handle_ops(
  connection: Connection,
  ops: List(ServerOp),
  chunks: List(List(Effect)),
) -> #(Connection, List(Effect)) {
  let done = fn(connection, chunks) {
    #(connection, list.flatten(list.reverse(chunks)))
  }
  case ops {
    [] -> done(connection, chunks)
    [op, ..rest] -> {
      let #(connection, effects, lost) = handle_op(connection, op)
      let chunks = [effects, ..chunks]
      case lost {
        True -> done(connection, chunks)
        False -> handle_ops(connection, rest, chunks)
      }
    }
  }
}

fn handle_op(
  connection: Connection,
  op: ServerOp,
) -> #(Connection, List(Effect), Bool) {
  let lost = fn(reason) {
    let #(connection, effects) = lose(connection, reason)
    #(connection, effects, True)
  }
  let handshaking = connection.status != Connected
  // CONNECT goes out once per attempt; a queued handshake PONG means it has.
  // A later INFO only updates the server details. Loss clears `pongs`.
  let awaiting_info =
    handshaking && !list.contains(connection.pongs, HandshakePong)
  case op {
    Info(raw) ->
      case json.parse(raw, info_decoder()) {
        Error(_) -> lost(ProtocolViolation("invalid INFO"))
        Ok(info) if awaiting_info && info.tls_required ->
          lost(Rejected("server requires TLS"))
        Ok(info) if awaiting_info -> {
          let connect = encode_connect(connect_body(connection, info))
          #(
            Connection(
              ..connection,
              info: option.Some(info),
              standby: advertised(connection, info.connect_urls),
              pongs: list.append(connection.pongs, [HandshakePong]),
            ),
            [Transmit(bytes_tree.append_tree(connect, ping()))],
            False,
          )
        }
        Ok(info) -> #(
          Connection(
            ..connection,
            info: option.Some(info),
            standby: advertised(connection, info.connect_urls),
          ),
          [],
          False,
        )
      }
    Ping -> #(connection, [Transmit(pong())], False)
    Pong -> {
      let connection = Connection(..connection, outstanding_pings: 0)
      case connection.pongs {
        [HandshakePong, ..pongs] -> {
          let #(connection, replay) = replay(Connection(..connection, pongs:))
          #(
            Connection(..connection, status: Connected, failures: 0),
            list.flatten([
              [CancelTimer(Handshake)],
              replay,
              [
                StartTimer(Keepalive, connection.config.ping_interval),
                Emit(Ready),
              ],
            ]),
            False,
          )
        }
        [FlushPong(token), ..pongs] -> #(
          Connection(..connection, pongs:),
          [Emit(Flushed(token))],
          False,
        )
        [KeepalivePong, ..pongs] -> #(
          Connection(..connection, pongs:),
          [],
          False,
        )
        [] -> #(connection, [], False)
      }
    }
    Ack -> #(connection, [], False)
    ServerError(text) if handshaking -> lost(Rejected(text))
    ServerError(text) -> #(connection, [Emit(Warned(text))], False)
    Msg(sid, message) -> #(
      connection,
      case dict.has_key(connection.subscriptions, sid) {
        True -> [Emit(Delivered(sid, message))]
        False -> []
      },
      False,
    )
  }
}

fn replay(connection: Connection) -> #(Connection, List(Effect)) {
  let subs =
    dict.to_list(connection.subscriptions)
    |> list.sort(fn(a, b) { int.compare(a.0, b.0) })
    |> list.map(fn(entry) { encode_subscribe(entry.1, entry.0) })
  let #(frames, tokens, dropped) =
    list.fold(connection.buffer, #([], [], []), fn(acc, item) {
      let #(frames, tokens, dropped) = acc
      case item {
        BufferedFlush(token) -> #(
          [ping(), ..frames],
          [FlushPong(token), ..tokens],
          dropped,
        )
        BufferedPublish(publish, bytes) ->
          case accepts(connection, publish) {
            Ok(Nil) -> #([bytes, ..frames], tokens, dropped)
            Error(limit) -> #(frames, tokens, [
              Emit(PublishDropped(limit)),
              ..dropped
            ])
          }
      }
    })
  let connection =
    Connection(
      ..connection,
      pongs: list.append(connection.pongs, tokens),
      buffer: [],
      buffer_bytes: 0,
    )
  let transmit = case list.append(subs, frames) {
    [] -> []
    all -> [Transmit(bytes_tree.concat(all))]
  }
  #(connection, list.append(transmit, dropped))
}

fn accepts(
  connection: Connection,
  publish: ValidPublish,
) -> Result(Nil, Limit) {
  case connection.info {
    option.Some(info) if publish.size > info.max_payload ->
      Error(MaxPayload(info.max_payload))
    option.Some(info) if !info.headers && publish.message.headers != [] ->
      Error(NoHeaders)
    _ -> Ok(Nil)
  }
}

fn info_decoder() -> decode.Decoder(ServerInfo) {
  let int = fn(key, next) { decode.optional_field(key, 0, decode.int, next) }
  let bool = fn(key, next) {
    decode.optional_field(key, False, decode.bool, next)
  }
  use max_payload <- int("max_payload")
  use headers <- bool("headers")
  use tls_required <- bool("tls_required")
  use nonce <- decode.optional_field(
    "nonce",
    option.None,
    decode.optional(decode.string),
  )
  use connect_urls <- decode.optional_field(
    "connect_urls",
    [],
    decode.list(decode.string),
  )
  decode.success(ServerInfo(
    max_payload:,
    headers:,
    tls_required:,
    nonce:,
    connect_urls:,
  ))
}

fn connect_body(connection: Connection, info: ServerInfo) -> Json {
  let name = case connection.config.name {
    option.Some(name) -> [#("name", json.string(name))]
    option.None -> []
  }
  let sign = fn(signer: nkey.Signer) {
    case info.nonce {
      option.Some(nonce) -> {
        let sig = signer.sign(bit_array.from_string(nonce))
        [#("sig", json.string(bit_array.base64_url_encode(sig, False)))]
      }
      option.None -> []
    }
  }
  let auth = case connection.config.auth {
    option.None -> []
    option.Some(Token(token)) -> [#("auth_token", json.string(token))]
    option.Some(UserPassword(user, password)) -> [
      #("user", json.string(user)),
      #("pass", json.string(password)),
    ]
    option.Some(Nkey(signer)) -> [
      #("nkey", json.string(signer.public_key)),
      ..sign(signer)
    ]
    option.Some(Jwt(jwt, signer)) -> [
      #("jwt", json.string(jwt)),
      ..sign(signer)
    ]
  }
  json.object(
    list.flatten([
      [
        #("verbose", json.bool(False)),
        #("pedantic", json.bool(False)),
        #("tls_required", json.bool(False)),
      ],
      name,
      [
        #("lang", json.string("gleam")),
        #("version", json.string("1.0.0")),
        #("protocol", json.int(1)),
        #("echo", json.bool(connection.config.echo_enabled)),
        #("headers", json.bool(info.headers)),
        #("no_responders", json.bool(info.headers)),
      ],
      auth,
    ]),
  )
}

fn advertised(connection: Connection, urls: List(String)) -> List(Server) {
  let new =
    list.filter_map(urls, parse_url)
    |> list.map(fn(entry) { entry.0 })
  list.fold(new, connection.standby, fn(standby, server) {
    case server == connection.server || list.contains(standby, server) {
      True -> standby
      False -> list.append(standby, [server])
    }
  })
}

fn lose(
  connection: Connection,
  reason: Disconnect,
) -> #(Connection, List(Effect)) {
  let failures = connection.failures + 1
  let config = connection.config
  let after =
    int.min(
      config.reconnect_initial
        * int.bitwise_shift_left(1, int.min(failures - 1, 30)),
      config.reconnect_max,
    )
  let #(server, standby) = case connection.standby {
    [] -> #(connection.server, [])
    [next, ..rest] -> #(next, list.append(rest, [connection.server]))
  }
  let status = case connection.status {
    Connected -> Reconnecting
    status -> status
  }
  let effects =
    list.flatten([
      [
        CancelTimer(Keepalive),
        CancelTimer(Handshake),
        CloseTransport(Generation(connection.next_generation - 1)),
        Emit(Lost(reason)),
      ],
      failed_flushes(connection.pongs),
      [
        Open(Generation(connection.next_generation), server, after),
        StartTimer(Handshake, after + config.connect_timeout),
      ],
    ])
  #(
    Connection(
      ..connection,
      status:,
      server:,
      standby:,
      next_generation: connection.next_generation + 1,
      failures:,
      pongs: [],
      leftover: <<>>,
    ),
    effects,
  )
}

pub fn timer_fired(
  connection: Connection,
  timer: Timer,
) -> #(Connection, List(Effect)) {
  let connected = connection.status == Connected
  case timer {
    Keepalive if connected ->
      case connection.outstanding_pings >= max_pings_outstanding {
        True -> lose(connection, Stale)
        False -> #(
          Connection(
            ..connection,
            outstanding_pings: connection.outstanding_pings + 1,
            pongs: list.append(connection.pongs, [KeepalivePong]),
          ),
          [
            Transmit(ping()),
            StartTimer(Keepalive, connection.config.ping_interval),
          ],
        )
      }
    Handshake if !connected ->
      lose(connection, TransportFailed("connect timed out"))
    Keepalive | Handshake -> #(connection, [])
  }
}

pub fn publish(
  connection: Connection,
  message: jetgleam.Message,
) -> Result(#(Connection, List(Effect)), Error) {
  use valid <- result.try(validate_publish(message))
  use Nil <- result.try(
    accepts(connection, valid) |> result.map_error(limit_error),
  )
  let frame = encode_publish(valid)
  let size = bytes_tree.byte_size(frame)
  case connection.status {
    Connected -> Ok(#(connection, [Transmit(frame)]))
    Connecting | Reconnecting -> {
      use <- bool.guard(
        when: connection.buffer_bytes + size > connection.config.buffer_size,
        return: Error(BufferFull),
      )
      Ok(
        #(
          Connection(
            ..connection,
            buffer: [BufferedPublish(valid, frame), ..connection.buffer],
            buffer_bytes: connection.buffer_bytes + size,
          ),
          [],
        ),
      )
    }
  }
}

pub fn subscribe(
  connection: Connection,
  subject: String,
  queue_group: Option(String),
) -> Result(#(Connection, Int, List(Effect)), Error) {
  use valid <- result.map(validate_subscription(subject, queue_group))
  let sid = connection.next_sid
  let effects = case connection.status {
    Connected -> [Transmit(encode_subscribe(valid, sid))]
    Connecting | Reconnecting -> []
  }
  #(
    Connection(
      ..connection,
      next_sid: sid + 1,
      subscriptions: dict.insert(connection.subscriptions, sid, valid),
    ),
    sid,
    effects,
  )
}

pub fn unsubscribe(
  connection: Connection,
  sid: Int,
) -> #(Connection, List(Effect)) {
  use <- bool.guard(
    when: !dict.has_key(connection.subscriptions, sid),
    return: #(connection, []),
  )
  let connection =
    Connection(
      ..connection,
      subscriptions: dict.delete(connection.subscriptions, sid),
    )
  case connection.status {
    Connected -> #(connection, [Transmit(encode_unsubscribe(sid))])
    Connecting | Reconnecting -> #(connection, [])
  }
}

pub fn flush(connection: Connection) -> #(Connection, Int, List(Effect)) {
  let token = connection.next_token
  let connection = Connection(..connection, next_token: token + 1)
  case connection.status {
    Connected -> #(
      Connection(
        ..connection,
        pongs: list.append(connection.pongs, [FlushPong(token)]),
      ),
      token,
      [Transmit(ping())],
    )
    Connecting | Reconnecting -> #(
      Connection(..connection, buffer: [
        BufferedFlush(token),
        ..connection.buffer
      ]),
      token,
      [],
    )
  }
}

// Removes an unsent flush. Already-sent PINGs keep their place in the PONG
// queue so cancelling one cannot acknowledge a later flush prematurely.
pub fn cancel_flush(connection: Connection, token: Int) -> Connection {
  Connection(
    ..connection,
    buffer: remove_buffered_flush(connection.buffer, token),
  )
}

fn remove_buffered_flush(buffer: List(Buffered), token: Int) -> List(Buffered) {
  case buffer {
    [] -> []
    [BufferedFlush(current), ..rest] if current == token -> rest
    [first, ..rest] -> [first, ..remove_buffered_flush(rest, token)]
  }
}

pub fn new_inbox(connection: Connection) -> #(Connection, String) {
  let subject =
    "_INBOX."
    <> connection.inbox_prefix
    <> "."
    <> int.to_string(connection.inbox_counter)
  #(
    Connection(..connection, inbox_counter: connection.inbox_counter + 1),
    subject,
  )
}

fn failed_flushes(pongs: List(PendingPong)) -> List(Effect) {
  list.filter_map(pongs, fn(pong) {
    case pong {
      FlushPong(token) -> Ok(Emit(FlushFailed(token)))
      _ -> Error(Nil)
    }
  })
}

pub type ValidPublish {
  ValidPublish(message: jetgleam.Message, size: Int)
}

pub type ValidSubscription {
  ValidSubscription(subject: String, queue_group: Option(String))
}

pub fn validate_publish(
  message: jetgleam.Message,
) -> Result(ValidPublish, Error) {
  use _ <- result.try(valid_subject(message.subject, False, InvalidSubject))
  use _ <- result.try(case message.reply_to {
    option.Some(reply) -> valid_subject(reply, False, InvalidReplySubject)
    option.None -> Ok(Nil)
  })
  use _ <- result.try(list.try_each(message.headers, valid_header))
  let size =
    bit_array.byte_size(header_block(message.headers))
    + bit_array.byte_size(message.payload)
  Ok(ValidPublish(message:, size:))
}

pub fn validate_subscription(
  subject: String,
  queue_group: Option(String),
) -> Result(ValidSubscription, Error) {
  use _ <- result.try(valid_subject(subject, True, InvalidSubject))
  use _ <- result.try(case queue_group {
    option.Some(group) ->
      bool.guard(
        when: group == "" || has_whitespace(group),
        return: Error(InvalidQueueGroup(group)),
        otherwise: fn() { Ok(Nil) },
      )
    option.None -> Ok(Nil)
  })
  Ok(ValidSubscription(subject:, queue_group:))
}

fn has_whitespace(s: String) -> Bool {
  list.any([" ", "\t", "\r", "\n"], string.contains(s, _))
}

fn valid_subject(
  subject: String,
  pattern: Bool,
  invalid: fn(String) -> Error,
) -> Result(Nil, Error) {
  let tokens = string.split(subject, ".")
  let last = list.length(tokens) - 1
  let tokens_ok =
    list.index_map(tokens, fn(token, index) {
      case token {
        "" -> False
        "*" -> pattern
        ">" -> pattern && index == last
        _ -> True
      }
    })
    |> list.all(function.identity)
  case subject != "" && !has_whitespace(subject) && tokens_ok {
    True -> Ok(Nil)
    False -> Error(invalid(subject))
  }
}

fn valid_header(header: #(String, String)) -> Result(Nil, Error) {
  let #(name, value) = header
  let name_ok =
    name != ""
    && list.all(string.to_utf_codepoints(name), fn(cp) {
      let c = string.utf_codepoint_to_int(cp)
      c >= 33 && c <= 126 && c != 58
    })
  use <- bool.guard(when: !name_ok, return: Error(InvalidHeaderName(name)))
  bool.guard(
    when: string.contains(value, "\r") || string.contains(value, "\n"),
    return: Error(InvalidHeaderValue(name)),
    otherwise: fn() { Ok(Nil) },
  )
}

fn header_block(headers: List(#(String, String))) -> BitArray {
  use <- bool.guard(when: headers == [], return: <<>>)
  let lines =
    list.map(headers, fn(h) { h.0 <> ": " <> h.1 <> "\r\n" })
    |> string.concat
  <<"NATS/1.0\r\n":utf8, lines:utf8, "\r\n":utf8>>
}

pub fn encode_publish(publish: ValidPublish) -> BytesTree {
  let ValidPublish(message:, size:) = publish
  let reply = case message.reply_to {
    option.Some(r) -> [r]
    option.None -> []
  }
  let block = header_block(message.headers)
  let control = case block {
    <<>> -> [
      "PUB",
      message.subject,
      ..list.append(reply, [int.to_string(size)])
    ]
    _ -> [
      "HPUB",
      message.subject,
      ..list.append(reply, [
        int.to_string(bit_array.byte_size(block)),
        int.to_string(size),
      ])
    ]
  }
  bytes_tree.from_string(string.join(control, " ") <> "\r\n")
  |> bytes_tree.append(block)
  |> bytes_tree.append(message.payload)
  |> bytes_tree.append_string("\r\n")
}

pub fn encode_subscribe(sub: ValidSubscription, sid: Int) -> BytesTree {
  let queue = option.map(sub.queue_group, fn(q) { [q] }) |> option.unwrap([])
  let parts = ["SUB", sub.subject, ..list.append(queue, [int.to_string(sid)])]
  bytes_tree.from_string(string.join(parts, " ") <> "\r\n")
}

pub fn encode_unsubscribe(sid: Int) -> BytesTree {
  bytes_tree.from_string("UNSUB " <> int.to_string(sid) <> "\r\n")
}

pub fn ping() -> BytesTree {
  bytes_tree.from_string("PING\r\n")
}

pub fn pong() -> BytesTree {
  bytes_tree.from_string("PONG\r\n")
}

pub fn encode_connect(body: Json) -> BytesTree {
  bytes_tree.from_string("CONNECT " <> json.to_string(body) <> "\r\n")
}

pub type ServerOp {
  Info(json: String)
  Msg(sid: Int, message: jetgleam.Message)
  Ping
  Pong
  Ack
  ServerError(message: String)
}

pub fn parse(buffer: BitArray) -> #(List(ServerOp), Result(BitArray, String)) {
  parse_loop(buffer, buffer, [])
}

fn parse_loop(
  start: BitArray,
  buffer: BitArray,
  ops: List(ServerOp),
) -> #(List(ServerOp), Result(BitArray, String)) {
  case take_line(buffer, <<>>) {
    Error(Nil) -> #(list.reverse(ops), Ok(start))
    Ok(#(line, rest)) ->
      case
        bit_array.to_string(line)
        |> result.replace_error("invalid utf8")
        |> result.try(parse_op(_, rest))
      {
        Error(reason) -> #(list.reverse(ops), Error(reason))
        Ok(option.None) -> #(list.reverse(ops), Ok(start))
        Ok(option.Some(#(op, rest))) -> parse_loop(rest, rest, [op, ..ops])
      }
  }
}

fn take_line(
  buffer: BitArray,
  acc: BitArray,
) -> Result(#(BitArray, BitArray), Nil) {
  case buffer {
    <<"\r\n":utf8, rest:bits>> -> Ok(#(acc, rest))
    <<byte, rest:bits>> -> take_line(rest, <<acc:bits, byte>>)
    _ -> Error(Nil)
  }
}

fn parse_op(
  line: String,
  rest: BitArray,
) -> Result(Option(#(ServerOp, BitArray)), String) {
  let line = string.replace(line, "\t", " ") |> string.trim
  let #(op, args) = case string.split_once(line, " ") {
    Ok(#(op, args)) -> #(op, string.trim(args))
    Error(Nil) -> #(line, "")
  }
  let args_list = string.split(args, " ") |> list.filter(fn(a) { a != "" })
  let done = fn(op) { Ok(option.Some(#(op, rest))) }
  case string.uppercase(op) {
    "INFO" -> done(Info(args))
    "PING" -> done(Ping)
    "PONG" -> done(Pong)
    "+OK" -> done(Ack)
    "-ERR" -> done(ServerError(unquote(args)))
    "MSG" ->
      case args_list {
        [subject, sid, size] ->
          parse_msg(subject, sid, option.None, "0", size, rest)
        [subject, sid, reply, size] ->
          parse_msg(subject, sid, option.Some(reply), "0", size, rest)
        _ -> Error("malformed MSG")
      }
    "HMSG" ->
      case args_list {
        [subject, sid, hdr, total] ->
          parse_msg(subject, sid, option.None, hdr, total, rest)
        [subject, sid, reply, hdr, total] ->
          parse_msg(subject, sid, option.Some(reply), hdr, total, rest)
        _ -> Error("malformed HMSG")
      }
    _ -> Error("unknown op: " <> op)
  }
}

fn unquote(text: String) -> String {
  let text = string.trim(text)
  let text = case string.starts_with(text, "'") {
    True -> string.drop_start(text, 1)
    False -> text
  }
  case string.ends_with(text, "'") {
    True -> string.drop_end(text, 1)
    False -> text
  }
}

fn parse_msg(
  subject: String,
  sid: String,
  reply_to: Option(String),
  hdr: String,
  total: String,
  rest: BitArray,
) -> Result(Option(#(ServerOp, BitArray)), String) {
  use sid <- result.try(int.parse(sid) |> result.replace_error("bad sid"))
  use hdr <- result.try(int.parse(hdr) |> result.replace_error("bad size"))
  use total <- result.try(int.parse(total) |> result.replace_error("bad size"))
  use <- bool.guard(when: hdr < 0 || hdr > total, return: Error("bad size"))
  use <- bool.guard(
    when: bit_array.byte_size(rest) < total + 2,
    return: Ok(option.None),
  )
  case rest {
    <<
      block:bytes-size(hdr),
      payload:bytes-size(total - hdr),
      "\r\n":utf8,
      after:bits,
    >> -> {
      use #(status, headers) <- result.try(case hdr {
        0 -> Ok(#(option.None, []))
        _ -> decode_headers(block) |> result.replace_error("bad headers")
      })
      let message =
        jetgleam.Message(subject:, reply_to:, headers:, status:, payload:)
      Ok(option.Some(#(Msg(sid, message), after)))
    }
    _ -> Error("missing CRLF after payload")
  }
}

pub fn decode_headers(
  block: BitArray,
) -> Result(#(Option(jetgleam.Status), List(#(String, String))), Nil) {
  use text <- result.try(bit_array.to_string(block))
  use head <- result.try(case string.split(text, "\r\n\r\n") {
    [head, ""] -> Ok(head)
    _ -> Error(Nil)
  })
  use #(first, lines) <- result.try(case string.split(head, "\r\n") {
    [first, ..lines] -> Ok(#(first, lines))
    [] -> Error(Nil)
  })
  use status <- result.try(case first {
    "NATS/1.0" -> Ok(option.None)
    "NATS/1.0 " <> status -> decode_status(string.trim(status))
    _ -> Error(Nil)
  })
  use headers <- result.map(
    list.try_map(lines, fn(line) {
      case string.split_once(line, ":") {
        Ok(#(name, value)) if name != "" -> Ok(#(name, string.trim(value)))
        _ -> Error(Nil)
      }
    }),
  )
  #(status, headers)
}

fn decode_status(text: String) -> Result(Option(jetgleam.Status), Nil) {
  use <- bool.guard(when: text == "", return: Ok(option.None))
  let #(code, description) = case string.split_once(text, " ") {
    Ok(#(code, description)) -> #(code, string.trim(description))
    Error(Nil) -> #(text, "")
  }
  use code <- result.map(int.parse(code))
  let description = case description {
    "" -> option.None
    d -> option.Some(d)
  }
  option.Some(jetgleam.Status(code:, description:))
}
