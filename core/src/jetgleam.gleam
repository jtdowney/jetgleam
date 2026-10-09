//// The vocabulary every jetgleam module shares: the `Message` type and its
//// headers, `Operation` (JetStream or key/value work described as a
//// value), the JetStream `Error` type, and publishing to JetStream.
////
//// ```gleam
//// import jetgleam
//// import jetgleam_erlang/nats
////
//// let msg =
////   jetgleam.message("orders.new", <<"hello":utf8>>)
////   |> jetgleam.set_header("Nats-Msg-Id", "order-42")
////   |> jetgleam.add_header("Trace", "a")
////   |> jetgleam.add_header("Trace", "b")
////
//// jetgleam.get_headers(msg, "Trace")
//// // -> ["a", "b"]
////
//// let assert Ok(ack) = jetgleam.publish(msg) |> nats.execute(on: conn)
//// ack.sequence
//// // -> 1
//// ```
////
//// Received messages are records, so read them by field (`msg.payload`,
//// `msg.reply_to`) or pattern match on them.
////
//// Every operation in `jetgleam/stream`, `jetgleam/consumer` and
//// `jetgleam/kv` returns an `Operation`. Building one does nothing; run it with
//// `nats.execute`. API failures come back as `nats.JetStream(error)`, where
//// `error` is this module's `Error`.

import gleam/bit_array
import gleam/bool
import gleam/dict
import gleam/dynamic/decode
import gleam/function
import gleam/int
import gleam/json.{type Json}
import gleam/list
import gleam/option.{type Option}
import gleam/result
import gleam/string
import gleam/time/duration.{type Duration}
import gleam/time/timestamp.{type Timestamp}

/// A NATS message, either received or about to be published.
pub type Message {
  Message(
    /// The subject it was published to.
    subject: String,
    /// Where replies should go. Requests and JetStream deliveries set it.
    reply_to: Option(String),
    /// Header name and value pairs in wire order. A name can appear more than
    /// once; read them with `get_header` and `get_headers`.
    headers: List(#(String, String)),
    /// The `NATS/1.0 <code> <description>` status line, if the server sent one.
    status: Option(Status),
    /// The message body.
    payload: BitArray,
  )
}

/// The status line the server puts on control messages, such as `503` for
/// no responders or `408` when a pull request expires.
pub type Status {
  Status(
    /// The numeric status, such as `503`.
    code: Int,
    /// The text after the code, if any.
    description: Option(String),
  )
}

/// A message with no reply subject, headers or status.
///
/// ```gleam
/// jetgleam.message("greet.joe", <<"hi":utf8>>)
/// // -> Message("greet.joe", None, [], None, <<"hi":utf8>>)
/// ```
pub fn message(subject: String, payload: BitArray) -> Message {
  Message(subject, option.None, [], option.None, payload)
}

/// Sets the subject a responder should reply to, replacing any earlier one.
pub fn set_reply_to(message: Message, subject: String) -> Message {
  Message(..message, reply_to: option.Some(subject))
}

/// Replaces every header called `name` with one value. Names match exactly,
/// case included.
pub fn set_header(
  message: Message,
  name name: String,
  value value: String,
) -> Message {
  message |> remove_header(name) |> add_header(name, value)
}

/// Appends a header, keeping any existing values for `name`.
pub fn add_header(
  message: Message,
  name name: String,
  value value: String,
) -> Message {
  Message(..message, headers: list.append(message.headers, [#(name, value)]))
}

/// The first value for `name`. Names match exactly, case included.
pub fn get_header(message: Message, name: String) -> Result(String, Nil) {
  list.key_find(message.headers, name)
}

/// Every value for `name`, in order. Names match exactly, case included.
pub fn get_headers(message: Message, name: String) -> List(String) {
  list.key_filter(message.headers, name)
}

/// Drops every header called `name`. Names match exactly, case included.
pub fn remove_header(message: Message, name: String) -> Message {
  Message(
    ..message,
    headers: list.filter(message.headers, fn(h) { h.0 != name }),
  )
}

/// A JetStream or key/value operation. Building one does no I/O; run it
/// with `nats.execute`.
pub opaque type Operation(a) {
  Request(message: Message, timeout: Int, then: fn(Message) -> Operation(a))
  Publish(message: Message, then: Operation(a))
  Done(result: Result(a, Error))
}

/// Default request timeout: 5000 ms.
pub const default_timeout = 5000

/// Sets the per-request timeout in milliseconds for every request in `operation`.
/// The default is `default_timeout`.
///
/// ```gleam
/// stream.info("ORDERS")
/// |> jetgleam.with_timeout(1000)
/// |> nats.execute(on: conn)
/// ```
pub fn with_timeout(
  operation: Operation(a),
  milliseconds: Int,
) -> Operation(a) {
  rewrite(operation, function.identity, fn(_) { milliseconds })
}

/// Routes API requests to a JetStream domain (`$JS.<domain>.API.`) instead of
/// the local one. Subjects outside `$JS.API.` are untouched, so a publish
/// still goes to its own subject.
///
/// ```gleam
/// stream.names()
/// |> jetgleam.with_domain("hub")
/// |> nats.execute(on: conn)
/// ```
pub fn with_domain(operation: Operation(a), domain: String) -> Operation(a) {
  rewrite(operation, route(_, domain), function.identity)
}

fn rewrite(
  operation: Operation(a),
  message: fn(Message) -> Message,
  timeout: fn(Int) -> Int,
) -> Operation(a) {
  case operation {
    Request(request, after, then) ->
      Request(message(request), timeout(after), fn(reply) {
        rewrite(then(reply), message, timeout)
      })
    Publish(publish, next) ->
      Publish(message(publish), rewrite(next, message, timeout))
    Done(_) -> operation
  }
}

fn route(message: Message, domain: String) -> Message {
  case message.subject {
    "$JS.API." <> rest ->
      Message(..message, subject: "$JS." <> domain <> ".API." <> rest)
    _ -> message
  }
}

/// Send `message` as a request with the default timeout and continue with
/// the reply.
@internal
pub fn request(
  message: Message,
  then: fn(Message) -> Operation(a),
) -> Operation(a) {
  Request(message, default_timeout, then)
}

/// Publish `message` without waiting for a reply, then continue.
@internal
pub fn send(message: Message, then: Operation(a)) -> Operation(a) {
  Publish(message, then)
}

/// A finished operation. Validation failures use this to return without I/O.
@internal
pub fn done(result: Result(a, Error)) -> Operation(a) {
  Done(result)
}

/// Continues with `f` once `operation` finishes, whether it succeeded or not.
@internal
pub fn bind(
  operation: Operation(a),
  f: fn(Result(a, Error)) -> Operation(b),
) -> Operation(b) {
  case operation {
    Request(message, timeout, then) ->
      Request(message, timeout, fn(reply) { bind(then(reply), f) })
    Publish(message, next) -> Publish(message, bind(next, f))
    Done(result) -> f(result)
  }
}

/// Performs `operation` with the runtime's I/O, mapping JetStream errors into
/// the runtime's error type with `error`.
@internal
pub fn run(
  operation: Operation(a),
  publish publish: fn(Message) -> Result(Nil, e),
  request request: fn(Message, Int) -> Result(Message, e),
  error error: fn(Error) -> e,
) -> Result(a, e) {
  case operation {
    Request(message, timeout, then) ->
      case request(message, timeout) {
        Ok(reply) -> run(then(reply), publish:, request:, error:)
        Error(e) -> Error(e)
      }
    Publish(message, next) ->
      case publish(message) {
        Ok(Nil) -> run(next, publish:, request:, error:)
        Error(e) -> Error(e)
      }
    Done(result) -> result.map_error(result, error)
  }
}

/// A JetStream or key/value failure.
pub type Error {
  /// The stream, consumer, message or key does not exist (a deleted KV key
  /// counts as not found).
  NotFound
  /// It exists with a different configuration.
  AlreadyExists
  /// A conditional write lost.
  WrongLastSequence(
    /// The current sequence or revision, when the server reported one this
    /// client can hold exactly.
    actual: Option(Int),
  )
  /// A configuration setting was rejected before sending.
  InvalidConfig(
    /// The setting that was rejected, such as `"name"`.
    field: String,
    /// What is wrong with it.
    reason: String,
  )
  /// A function argument was rejected before sending.
  InvalidArgument(
    /// The argument that was rejected, such as `"sequence"`.
    field: String,
    /// What is wrong with it.
    reason: String,
  )
  /// A KV key or watch filter has invalid subject syntax.
  InvalidKey(
    /// The key or filter as given.
    key: String,
  )
  /// The message has no JetStream reply subject, so it cannot be acknowledged.
  NotAcknowledgeable
  /// Any other server error.
  Api(
    /// The HTTP-style status, such as `400`.
    code: Int,
    /// The JetStream error code, such as `10058`. Use it to tell errors
    /// apart.
    error_code: Int,
    /// The server's description.
    description: String,
  )
  /// The reply could not be decoded.
  BadResponse(
    /// What could not be decoded.
    reason: String,
  )
}

/// The stream's confirmation that it stored a published message.
pub type PubAck {
  PubAck(
    /// The stream that stored it.
    stream: String,
    /// Its sequence in that stream.
    sequence: Int,
    /// `True` when the stream already held a message with this
    /// `Nats-Msg-Id`, so nothing new was stored.
    duplicate: Bool,
  )
}

/// Publish and wait for the stream to store it.
///
/// Set `Nats-Msg-Id` on the message and the stream drops repeats within its
/// duplicate window; the second `PubAck` has `duplicate: True`. Set
/// `Nats-Expected-Last-Sequence` for a conditional write, which fails with
/// `WrongLastSequence` if the stream has moved on.
///
/// ```gleam
/// let assert Ok(ack) =
///   jetgleam.message("orders.new", <<"hello":utf8>>)
///   |> jetgleam.set_header("Nats-Msg-Id", "order-42")
///   |> jetgleam.publish
///   |> nats.execute(on: conn)
///
/// ack.stream
/// // -> "ORDERS"
/// ```
pub fn publish(message: Message) -> Operation(PubAck) {
  request(message, fn(reply) { done(decode_reply(reply, pub_ack_decoder())) })
}

/// Whether `n` is exactly representable in a JavaScript number.
@internal
pub fn is_safe_int(n: Int) -> Bool {
  let max = 9_007_199_254_740_991
  n >= 0 - max && n <= max
}

/// An Int that passes `is_safe_int`.
@internal
pub fn safe_int_decoder() -> decode.Decoder(Int) {
  use n <- decode.then(decode.int)
  case is_safe_int(n) {
    True -> decode.success(n)
    False -> decode.failure(0, "Sequence")
  }
}

fn pub_ack_decoder() -> decode.Decoder(PubAck) {
  use stream <- decode.field("stream", decode.string)
  use sequence <- decode.field("seq", safe_int_decoder())
  use duplicate <- decode.optional_field("duplicate", False, decode.bool)
  decode.success(PubAck(stream, sequence, duplicate))
}

/// Decodes a JetStream API reply: a server `error` becomes the matching
/// `Error`, anything else goes through `decoder`.
@internal
pub fn decode_reply(
  reply: Message,
  decoder: decode.Decoder(a),
) -> Result(a, Error) {
  case reply.payload, reply.status {
    <<>>, option.Some(Status(code, description)) ->
      Error(Api(code, 0, option.unwrap(description, "")))
    _, _ -> {
      let error_decoder = {
        use code <- decode.field("code", decode.int)
        use err_code <- decode.optional_field("err_code", 0, decode.int)
        use description <- decode.optional_field(
          "description",
          "",
          decode.string,
        )
        decode.success(map_error(code, err_code, description))
      }
      let parsed =
        json.parse_bits(reply.payload, {
          use error <- decode.optional_field(
            "error",
            option.None,
            decode.map(error_decoder, option.Some),
          )
          case error {
            option.Some(error) -> decode.success(Error(error))
            option.None -> decode.map(decoder, Ok)
          }
        })
      case parsed {
        Ok(result) -> result
        Error(reason) -> Error(BadResponse(string.inspect(reason)))
      }
    }
  }
}

fn map_error(code: Int, err_code: Int, description: String) -> Error {
  case err_code {
    10_059 | 10_014 | 10_037 -> NotFound
    10_058 | 10_013 | 10_148 -> AlreadyExists
    10_071 -> WrongLastSequence(last_int(description))
    _ if code == 404 -> NotFound
    _ -> Api(code, err_code, description)
  }
}

// The integer after the last space, if it is exactly representable in a
// JavaScript number.
fn last_int(description: String) -> Option(Int) {
  case string.split(description, " ") |> list.last |> result.try(int.parse) {
    Ok(n) ->
      bool.guard(when: !is_safe_int(n), return: option.None, otherwise: fn() {
        option.Some(n)
      })
    _ -> option.None
  }
}

@internal
pub type Name {
  Argument(field: String, value: String)
  Setting(field: String, value: String)
}

@internal
pub fn api_request(
  action: String,
  names: List(Name),
  payload: BitArray,
  decoder: decode.Decoder(a),
) -> Operation(a) {
  case api_subject(action, names) {
    Error(error) -> done(Error(error))
    Ok(subject) ->
      message("$JS.API." <> subject, payload)
      |> request(fn(reply) { done(decode_reply(reply, decoder)) })
  }
}

@internal
pub fn api_subject(action: String, names: List(Name)) -> Result(String, Error) {
  use subject, name <- list.try_fold(names, action)
  let invalid =
    name.value == ""
    || list.any(string.to_graphemes(name.value), fn(c) {
      string.contains(". *>\t\r\n", c)
    })
  let reason = "must not be empty or contain . * > or whitespace"
  case invalid, name {
    False, _ -> Ok(subject <> "." <> name.value)
    True, Argument(field:, ..) -> Error(InvalidArgument(field, reason))
    True, Setting(field:, ..) -> Error(InvalidConfig(field, reason))
  }
}

@internal
pub const unsafe_int = "must be between -(2^53 - 1) and 2^53 - 1"

@internal
pub fn invalid_config(field: String, reason: String) -> Operation(a) {
  done(Error(InvalidConfig(field, reason)))
}

@internal
pub fn invalid_argument(field: String, reason: String) -> Operation(a) {
  done(Error(InvalidArgument(field, reason)))
}

@internal
pub fn json_payload(value: Json) -> BitArray {
  value |> json.to_string |> bit_array.from_string
}

@internal
pub fn json_decoder() -> decode.Decoder(Json) {
  let value = decode.recursive(json_decoder)
  decode.one_of(decode.string |> decode.map(json.string), [
    decode.int |> decode.map(json.int),
    decode.float |> decode.map(json.float),
    decode.bool |> decode.map(json.bool),
    decode.list(value) |> decode.map(json.preprocessed_array),
    decode.dict(decode.string, value)
      |> decode.map(fn(fields) { json.object(dict.to_list(fields)) }),
    decode.success(json.null()),
  ])
}

@internal
pub fn nanoseconds(duration: Duration) -> Int {
  let #(seconds, nanos) = duration.to_seconds_and_nanoseconds(duration)
  seconds * 1_000_000_000 + nanos
}

@internal
pub fn duration_decoder() -> decode.Decoder(Duration) {
  decode.map(decode.int, duration.nanoseconds)
}

@internal
pub fn enum_decoder(
  name: String,
  default: a,
  cases: List(#(String, a)),
  next: fn(a) -> decode.Decoder(b),
) -> decode.Decoder(b) {
  let value = {
    use text <- decode.then(decode.string)
    case list.key_find(cases, text) {
      Ok(value) -> decode.success(value)
      Error(Nil) -> decode.failure(default, name)
    }
  }
  decode.optional_field(name, default, value, next)
}

@internal
pub fn positive_decoder(
  name: String,
  next: fn(option.Option(Int)) -> decode.Decoder(a),
) -> decode.Decoder(a) {
  use value <- decode.optional_field(name, 0, decode.int)
  next(case value > 0 {
    True -> option.Some(value)
    False -> option.None
  })
}

@internal
pub fn timestamp_decoder() -> decode.Decoder(Timestamp) {
  use text <- decode.then(decode.string)
  case timestamp.parse_rfc3339(text) {
    Ok(timestamp) -> decode.success(timestamp)
    Error(Nil) -> decode.failure(timestamp.unix_epoch, "Timestamp")
  }
}

@internal
pub fn rfc3339(timestamp: Timestamp) -> String {
  timestamp.to_rfc3339(timestamp, duration.seconds(0))
}

@internal
pub fn success_decoder() -> decode.Decoder(Nil) {
  use success <- decode.field("success", decode.bool)
  case success {
    True -> decode.success(Nil)
    False -> decode.failure(Nil, "success")
  }
}

@internal
pub fn paged(
  action: String,
  names: List(Name),
  field: String,
  item: decode.Decoder(a),
) -> Operation(List(a)) {
  case api_subject(action, names) {
    Error(error) -> done(Error(error))
    Ok(subject) -> pages_from(subject, field, item, 0, [])
  }
}

fn pages_from(
  subject: String,
  field: String,
  item: decode.Decoder(a),
  offset: Int,
  pages: List(List(a)),
) -> Operation(List(a)) {
  let decoder = {
    use total <- decode.field("total", decode.int)
    use page <- decode.optional_field(
      field,
      [],
      decode.optional(decode.list(item))
        |> decode.map(fn(page) { option.unwrap(page, []) }),
    )
    decode.success(#(total, page))
  }
  let payload = json_payload(json.object([#("offset", json.int(offset))]))
  message("$JS.API." <> subject, payload)
  |> request(fn(reply) {
    case decode_reply(reply, decoder) {
      Error(error) -> done(Error(error))
      Ok(#(total, page)) -> {
        let offset = offset + list.length(page)
        let pages = [page, ..pages]
        case offset < total && page != [] {
          True -> pages_from(subject, field, item, offset, pages)
          False -> done(Ok(list.flatten(list.reverse(pages))))
        }
      }
    }
  })
}

@internal
pub fn optional(
  name: String,
  value: Option(a),
  encode: fn(a) -> Json,
) -> List(#(String, Json)) {
  case value {
    option.Some(value) -> [#(name, encode(value))]
    option.None -> []
  }
}
