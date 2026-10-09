//// Stream configuration, state, stored messages, and every stream operation.
////
//// A stream stores messages published to its subjects. Configure one with
//// `config` and the `with_*` builders, then create it:
////
//// ```gleam
//// import gleam/time/duration
//// import jetgleam/stream
//// import jetgleam_erlang/nats
////
//// let assert Ok(info) =
////   stream.config("ORDERS", ["orders.>"])
////   |> stream.with_max_age(duration.hours(24))
////   |> stream.with_storage(stream.Memory)
////   |> stream.create
////   |> nats.execute(on: conn)
////
//// info.state.messages
//// // -> 0
//// ```
////
//// Every operation returns a `jetgleam.Operation` and does nothing until a
//// runtime executes it. To publish into a stream, use `jetgleam.publish`;
//// it routes by subject, not by stream name.
////
//// To change a stream, adjust the configuration `info` returns and pass it
//// to `update`:
////
//// ```gleam
//// let assert Ok(info) = stream.info("ORDERS") |> nats.execute(on: conn)
//// info.config
//// |> stream.with_max_age(duration.hours(48))
//// |> stream.update
//// |> nats.execute(on: conn)
//// ```

import gleam/bit_array
import gleam/dict
import gleam/dynamic/decode
import gleam/json.{type Json}
import gleam/list
import gleam/option.{type Option}
import gleam/time/duration.{type Duration}
import gleam/time/timestamp.{type Timestamp}
import jetgleam
import jetgleam/internal/protocol

/// When the stream removes messages.
pub type Retention {
  /// Keep messages until a limit (age, count or size) removes them.
  Limits
  /// Keep each message until every consumer whose filter matches it has
  /// acknowledged it. A message no consumer matches is removed as soon as
  /// it is stored. Limits still apply.
  Interest
  /// Remove each message once a consumer acknowledges it. Limits still
  /// apply.
  WorkQueue
}

/// Where the server keeps messages.
pub type Storage {
  /// On disk. Survives restarts.
  File
  /// In memory. Lost on restart.
  Memory
}

/// What happens when the stream reaches a limit.
pub type Discard {
  /// Delete the oldest messages to make room.
  Old
  /// Reject new messages.
  New
}

/// A subject pattern and its server-side replacement template.
pub type SubjectTransform {
  SubjectTransform(source: String, destination: String)
}

/// The settings for a stream.
///
/// Server settings this library does not model are kept from `info` and
/// sent back unchanged on `update`.
pub opaque type Config {
  Config(
    name: String,
    subjects: List(String),
    description: Option(String),
    retention: Retention,
    storage: Storage,
    discard: Discard,
    max_consumers: Option(Int),
    max_messages: Option(Int),
    max_bytes: Option(Int),
    max_age: Option(Duration),
    max_messages_per_subject: Option(Int),
    max_message_size: Option(Int),
    replicas: Int,
    duplicate_window: Option(Duration),
    allow_direct: Bool,
    allow_rollup: Bool,
    deny_delete: Bool,
    deny_purge: Bool,
    subject_transform: Option(SubjectTransform),
    unmodeled: List(#(String, Json)),
  )
}

/// How much the stream holds right now.
pub type State {
  State(
    /// Messages stored.
    messages: Int,
    /// Total size of the stored messages.
    bytes: Int,
    /// Sequence of the oldest stored message.
    first_sequence: Int,
    /// Sequence of the newest stored message.
    last_sequence: Int,
    /// Consumers on the stream.
    consumers: Int,
  )
}

/// A stream's configuration and state, as the server reports them.
pub type Info {
  Info(
    /// The configuration in effect as the server reports it, with its
    /// defaults filled in. Adjust it with the `with_*` builders and pass it
    /// to `update`.
    config: Config,
    /// What the stream holds.
    state: State,
    /// When the stream was created.
    created: Timestamp,
  )
}

/// A message read back from a stream.
pub type StoredMessage {
  StoredMessage(
    /// The subject it was published to.
    subject: String,
    /// Its sequence in the stream.
    sequence: Int,
    /// Header name and value pairs, as in `jetgleam.Message`.
    headers: List(#(String, String)),
    /// The message body.
    payload: BitArray,
    /// When the stream stored it.
    time: Timestamp,
  )
}

/// A stream named `name` that stores messages published to `subjects`.
/// Limits retention, file storage, discard old, one replica, no limits.
///
/// ```gleam
/// stream.config("ORDERS", ["orders.>"])
/// |> stream.with_retention(stream.WorkQueue)
/// |> stream.with_max_messages(1_000_000)
/// ```
pub fn config(name: String, subjects: List(String)) -> Config {
  Config(
    name:,
    subjects:,
    description: option.None,
    retention: Limits,
    storage: File,
    discard: Old,
    max_consumers: option.None,
    max_messages: option.None,
    max_bytes: option.None,
    max_age: option.None,
    max_messages_per_subject: option.None,
    max_message_size: option.None,
    replicas: 1,
    duplicate_window: option.None,
    allow_direct: False,
    allow_rollup: False,
    deny_delete: False,
    deny_purge: False,
    subject_transform: option.None,
    unmodeled: [],
  )
}

/// Replaces the subjects the stream stores.
pub fn with_subjects(config: Config, subjects: List(String)) -> Config {
  Config(..config, subjects:)
}

/// Transforms matching subjects before the stream stores messages.
/// Default: none. Requires NATS server 2.10 or later.
pub fn with_subject_transform(
  config: Config,
  transform: SubjectTransform,
) -> Config {
  Config(..config, subject_transform: option.Some(transform))
}

/// A note for operators; the server does not interpret it.
/// Default: none.
pub fn with_description(config: Config, description: String) -> Config {
  Config(..config, description: option.Some(description))
}

/// When stored messages may be removed, besides limits such as
/// `with_max_messages`, `with_max_bytes` and `with_max_age`.
/// Default: `Limits`.
pub fn with_retention(config: Config, retention: Retention) -> Config {
  Config(..config, retention:)
}

/// File storage survives a server restart; memory storage does not.
/// Default: `File`.
pub fn with_storage(config: Config, storage: Storage) -> Config {
  Config(..config, storage:)
}

/// What happens to a publish once a limit is reached: `Old` removes the
/// oldest messages, `New` rejects the publish.
/// Default: `Old`.
pub fn with_discard(config: Config, discard: Discard) -> Config {
  Config(..config, discard:)
}

/// The most consumers the stream allows.
/// Default: no limit.
pub fn with_max_consumers(config: Config, max: Int) -> Config {
  Config(..config, max_consumers: option.Some(max))
}

/// The most messages the stream keeps. Past it, `discard` decides whether
/// the oldest message goes or the publish is rejected.
/// Default: no limit.
pub fn with_max_messages(config: Config, max: Int) -> Config {
  Config(..config, max_messages: option.Some(max))
}

/// The most bytes the stream keeps, payloads and headers included. Past
/// it, `discard` decides whether old messages go or the publish is
/// rejected.
/// Default: no limit.
pub fn with_max_bytes(config: Config, max: Int) -> Config {
  Config(..config, max_bytes: option.Some(max))
}

/// Messages older than `max` are removed.
/// Default: forever.
pub fn with_max_age(config: Config, max: Duration) -> Config {
  Config(..config, max_age: option.Some(max))
}

/// The most messages kept for each subject. Older ones are removed first.
/// Default: no limit.
pub fn with_max_messages_per_subject(config: Config, max: Int) -> Config {
  Config(..config, max_messages_per_subject: option.Some(max))
}

/// The largest message, in bytes, the stream accepts.
/// Default: no limit.
pub fn with_max_message_size(config: Config, max: Int) -> Config {
  Config(..config, max_message_size: option.Some(max))
}

/// Copies kept across a cluster, 1 to 5. A single server accepts only 1.
/// Default: 1.
pub fn with_replicas(config: Config, replicas: Int) -> Config {
  Config(..config, replicas:)
}

/// How long the server remembers `Nats-Msg-Id` headers to drop repeated
/// publishes.
/// Default: the server's (2 minutes).
pub fn with_duplicate_window(config: Config, window: Duration) -> Config {
  Config(..config, duplicate_window: option.Some(window))
}

/// Enables the server's direct-get API, which lets any replica answer
/// message reads. This client reads through the leader, but other NATS
/// clients use direct get when it is on, so KV buckets enable it.
/// Default: `False`.
pub fn with_allow_direct(config: Config, allow: Bool) -> Config {
  Config(..config, allow_direct: allow)
}

/// Lets a publish with a `Nats-Rollup` header replace earlier messages.
/// Default: `False`.
pub fn with_allow_rollup(config: Config, allow: Bool) -> Config {
  Config(..config, allow_rollup: allow)
}

/// Refuses requests to delete individual messages.
/// Default: `False`.
pub fn with_deny_delete(config: Config, deny: Bool) -> Config {
  Config(..config, deny_delete: deny)
}

/// Refuses requests to purge the stream.
/// Default: `False`.
pub fn with_deny_purge(config: Config, deny: Bool) -> Config {
  Config(..config, deny_purge: deny)
}

/// Creates the stream. Creating one that already exists with the same
/// configuration succeeds; a different configuration is `AlreadyExists`.
pub fn create(config: Config) -> jetgleam.Operation(Info) {
  jetgleam.api_request(
    "STREAM.CREATE",
    [jetgleam.Setting("name", config.name)],
    jetgleam.json_payload(encode_config(config)),
    info_decoder(),
  )
}

/// Replaces the configuration of an existing stream. The server rejects
/// changes it cannot apply in place, such as switching storage.
pub fn update(config: Config) -> jetgleam.Operation(Info) {
  jetgleam.api_request(
    "STREAM.UPDATE",
    [jetgleam.Setting("name", config.name)],
    jetgleam.json_payload(encode_config(config)),
    info_decoder(),
  )
}

/// The stream's configuration and state. `NotFound` if it does not exist.
pub fn info(name: String) -> jetgleam.Operation(Info) {
  jetgleam.api_request(
    "STREAM.INFO",
    [jetgleam.Argument("name", name)],
    <<>>,
    info_decoder(),
  )
}

/// Deletes the stream, its messages and its consumers. `NotFound` if it does
/// not exist.
pub fn delete(name: String) -> jetgleam.Operation(Nil) {
  jetgleam.api_request(
    "STREAM.DELETE",
    [jetgleam.Argument("name", name)],
    <<>>,
    jetgleam.success_decoder(),
  )
}

/// Removes every message from the stream and keeps the stream itself.
pub fn purge(name: String) -> jetgleam.Operation(Nil) {
  jetgleam.api_request(
    "STREAM.PURGE",
    [jetgleam.Argument("name", name)],
    <<>>,
    jetgleam.success_decoder(),
  )
}

/// Every stream name; pages through the server's listing.
pub fn names() -> jetgleam.Operation(List(String)) {
  jetgleam.paged("STREAM.NAMES", [], "streams", decode.string)
}

/// Every stream's configuration and state; pages through the server's
/// listing.
pub fn list() -> jetgleam.Operation(List(Info)) {
  jetgleam.paged("STREAM.LIST", [], "streams", info_decoder())
}

/// Reads one stored message by its stream sequence.
///
/// ```gleam
/// let assert Ok(stored) =
///   stream.get_message(stream: "ORDERS", sequence: 1)
///   |> nats.execute(on: conn)
/// stored.subject
/// // -> "orders.new"
/// ```
pub fn get_message(
  stream stream: String,
  sequence sequence: Int,
) -> jetgleam.Operation(StoredMessage) {
  case jetgleam.is_safe_int(sequence) {
    True -> get_stored(stream, "seq", json.int(sequence))
    False -> jetgleam.invalid_argument("sequence", jetgleam.unsafe_int)
  }
}

fn get_stored(
  stream: String,
  key: String,
  value: Json,
) -> jetgleam.Operation(StoredMessage) {
  jetgleam.api_request(
    "STREAM.MSG.GET",
    [jetgleam.Argument("stream", stream)],
    jetgleam.json_payload(json.object([#(key, value)])),
    decode.at(["message"], stored_message_decoder()),
  )
}

/// Reads the newest stored message on `subject`. Fails with `NotFound` if the
/// stream holds none.
pub fn get_last_message(
  stream stream: String,
  subject subject: String,
) -> jetgleam.Operation(StoredMessage) {
  get_stored(stream, "last_by_subj", json.string(subject))
}

fn limit(value: Option(Int)) -> Json {
  json.int(option.unwrap(value, -1))
}

fn encode_config(config: Config) -> Json {
  json.object(
    list.flatten([
      [
        #("name", json.string(config.name)),
        #("subjects", json.array(config.subjects, json.string)),
      ],
      jetgleam.optional("description", config.description, json.string),
      jetgleam.optional(
        "subject_transform",
        config.subject_transform,
        encode_subject_transform,
      ),
      [
        #(
          "retention",
          json.string(case config.retention {
            Limits -> "limits"
            Interest -> "interest"
            WorkQueue -> "workqueue"
          }),
        ),
        #(
          "storage",
          json.string(case config.storage {
            File -> "file"
            Memory -> "memory"
          }),
        ),
        #(
          "discard",
          json.string(case config.discard {
            Old -> "old"
            New -> "new"
          }),
        ),
        #("max_consumers", limit(config.max_consumers)),
        #("max_msgs", limit(config.max_messages)),
        #("max_bytes", limit(config.max_bytes)),
        #("max_msgs_per_subject", limit(config.max_messages_per_subject)),
        #("max_msg_size", limit(config.max_message_size)),
        #(
          "max_age",
          json.int(
            option.map(config.max_age, jetgleam.nanoseconds) |> option.unwrap(0),
          ),
        ),
        #("num_replicas", json.int(config.replicas)),
      ],
      jetgleam.optional("duplicate_window", config.duplicate_window, fn(window) {
        json.int(jetgleam.nanoseconds(window))
      }),
      [
        #("allow_direct", json.bool(config.allow_direct)),
        #("allow_rollup_hdrs", json.bool(config.allow_rollup)),
        #("deny_delete", json.bool(config.deny_delete)),
        #("deny_purge", json.bool(config.deny_purge)),
      ],
      config.unmodeled,
    ]),
  )
}

fn encode_subject_transform(transform: SubjectTransform) -> Json {
  json.object([
    #("src", json.string(transform.source)),
    #("dest", json.string(transform.destination)),
  ])
}

const modeled_fields = [
  "name", "subjects", "description", "subject_transform", "retention", "storage",
  "discard", "max_consumers", "max_msgs", "max_bytes", "max_msgs_per_subject",
  "max_msg_size", "max_age", "num_replicas", "duplicate_window", "allow_direct",
  "allow_rollup_hdrs", "deny_delete", "deny_purge",
]

fn unmodeled_decoder() -> decode.Decoder(List(#(String, Json))) {
  use fields <- decode.map(decode.dict(decode.string, jetgleam.json_decoder()))
  dict.to_list(fields)
  |> list.filter(fn(field) { !list.contains(modeled_fields, field.0) })
}

fn subject_transform_decoder() -> decode.Decoder(SubjectTransform) {
  use source <- decode.field("src", decode.string)
  use destination <- decode.field("dest", decode.string)
  decode.success(SubjectTransform(source:, destination:))
}

fn config_decoder() -> decode.Decoder(Config) {
  use name <- decode.field("name", decode.string)
  use subjects <- decode.optional_field(
    "subjects",
    [],
    decode.list(decode.string),
  )
  use description <- decode.optional_field(
    "description",
    option.None,
    decode.optional(decode.string),
  )
  use retention <- jetgleam.enum_decoder("retention", Limits, [
    #("limits", Limits),
    #("interest", Interest),
    #("workqueue", WorkQueue),
  ])
  use storage <- jetgleam.enum_decoder("storage", File, [
    #("file", File),
    #("memory", Memory),
  ])
  use discard <- jetgleam.enum_decoder("discard", Old, [
    #("old", Old),
    #("new", New),
  ])
  use max_consumers <- jetgleam.positive_decoder("max_consumers")
  use max_messages <- jetgleam.positive_decoder("max_msgs")
  use max_bytes <- jetgleam.positive_decoder("max_bytes")
  use max_age <- jetgleam.positive_decoder("max_age")
  use max_messages_per_subject <- jetgleam.positive_decoder(
    "max_msgs_per_subject",
  )
  use max_message_size <- jetgleam.positive_decoder("max_msg_size")
  use replicas <- decode.optional_field("num_replicas", 1, decode.int)
  use duplicate_window <- jetgleam.positive_decoder("duplicate_window")
  use allow_direct <- decode.optional_field("allow_direct", False, decode.bool)
  use allow_rollup <- decode.optional_field(
    "allow_rollup_hdrs",
    False,
    decode.bool,
  )
  use deny_delete <- decode.optional_field("deny_delete", False, decode.bool)
  use deny_purge <- decode.optional_field("deny_purge", False, decode.bool)
  use subject_transform <- decode.optional_field(
    "subject_transform",
    option.None,
    decode.optional(subject_transform_decoder()),
  )
  use unmodeled <- decode.then(unmodeled_decoder())
  decode.success(Config(
    name:,
    subjects:,
    description:,
    retention:,
    storage:,
    discard:,
    max_consumers:,
    max_messages:,
    max_bytes:,
    max_age: option.map(max_age, duration.nanoseconds),
    max_messages_per_subject:,
    max_message_size:,
    replicas:,
    duplicate_window: option.map(duplicate_window, duration.nanoseconds),
    allow_direct:,
    allow_rollup:,
    deny_delete:,
    deny_purge:,
    subject_transform:,
    unmodeled:,
  ))
}

fn base64_decoder() -> decode.Decoder(BitArray) {
  use text <- decode.then(decode.string)
  case bit_array.base64_decode(text) {
    Ok(bits) -> decode.success(bits)
    Error(Nil) -> decode.failure(<<>>, "Base64")
  }
}

fn stored_message_decoder() -> decode.Decoder(StoredMessage) {
  use subject <- decode.field("subject", decode.string)
  use sequence <- decode.field("seq", jetgleam.safe_int_decoder())
  use headers <- decode.optional_field("hdrs", [], {
    use block <- decode.then(base64_decoder())
    case protocol.decode_headers(block) {
      Ok(#(_, headers)) -> decode.success(headers)
      Error(Nil) -> decode.failure([], "Headers")
    }
  })
  use payload <- decode.optional_field("data", <<>>, base64_decoder())
  use time <- decode.field("time", jetgleam.timestamp_decoder())
  decode.success(StoredMessage(subject:, sequence:, headers:, payload:, time:))
}

fn info_decoder() -> decode.Decoder(Info) {
  use config <- decode.field("config", config_decoder())
  use state <- decode.field("state", {
    use messages <- decode.field("messages", decode.int)
    use bytes <- decode.field("bytes", decode.int)
    use first_sequence <- decode.field("first_seq", jetgleam.safe_int_decoder())
    use last_sequence <- decode.field("last_seq", jetgleam.safe_int_decoder())
    use consumers <- decode.field("consumer_count", decode.int)
    decode.success(State(
      messages:,
      bytes:,
      first_sequence:,
      last_sequence:,
      consumers:,
    ))
  })
  use created <- decode.field("created", jetgleam.timestamp_decoder())
  decode.success(Info(config:, state:, created:))
}
