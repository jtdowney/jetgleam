//// Consumer configuration, info, fetching, acknowledgement and delivery
//// metadata. A worker process needs this module and `nats`, and no other
//// part of `jetgleam`.
////
//// A consumer is the server's record of how far a reader has got through a
//// stream. Create a durable pull consumer once, at deploy or startup:
////
//// ```gleam
//// import jetgleam/consumer
//// import jetgleam_erlang/nats
////
//// let assert Ok(_) =
////   consumer.durable("worker")
////   |> consumer.with_max_deliver(5)
////   |> consumer.with_filter_subjects(["orders.new"])
////   |> consumer.create(stream: "ORDERS")
////   |> nats.execute(on: conn)
//// ```
////
//// Then each worker pulls a batch, handles it, and acknowledges every
//// message:
////
//// ```gleam
//// let assert Ok(batch) =
////   consumer.fetch(
////     stream: "ORDERS",
////     consumer: "worker",
////     max: 10,
////     wait: duration.seconds(2),
////   )
////   |> nats.fetch(on: conn)
////
//// use msg <- list.each(batch)
//// let kind = case handle(msg) {
////   Ok(Nil) -> consumer.Ack
////   Error(_) -> consumer.NakAfter(duration.seconds(30))
//// }
//// let assert Ok(Nil) = consumer.ack(msg, kind) |> nats.execute(on: conn)
//// ```
////
//// An unacknowledged message is redelivered after the consumer's
//// `ack_wait`, up to `max_deliver` times. `metadata` tells you which
//// delivery you are looking at.

import gleam/bit_array
import gleam/dynamic/decode
import gleam/int
import gleam/json.{type Json}
import gleam/list
import gleam/option.{type Option}
import gleam/result
import gleam/string
import gleam/time/duration.{type Duration}
import gleam/time/timestamp.{type Timestamp}
import jetgleam

/// How messages reach the consumer.
pub type Delivery {
  /// Clients ask for batches with `fetch`.
  Pull
  /// The server sends messages to a subject as they arrive.
  Push(
    /// Where to send them.
    subject: String,
    /// How often the server sends a heartbeat when there is nothing to
    /// deliver.
    heartbeat: Option(Duration),
    /// Whether the server waits for flow-control responses.
    flow_control: Bool,
  )
}

/// Where in the stream a new consumer starts.
pub type DeliverPolicy {
  /// From the oldest stored message.
  DeliverAll
  /// From the newest stored message.
  DeliverLast
  /// Only messages stored after the consumer is created.
  DeliverNew
  /// From a stream sequence.
  DeliverFromSequence(
    /// The first stream sequence to deliver.
    sequence: Int,
  )
  /// From a point in time.
  DeliverFromTime(
    /// Deliver messages stored at or after this time.
    time: Timestamp,
  )
  /// The newest message on each subject, then new messages.
  DeliverLastPerSubject
}

/// Which messages need acknowledging.
pub type AckPolicy {
  /// None. Messages count as acknowledged once sent.
  AckNone
  /// Acknowledging one message acknowledges every earlier one.
  AckAll
  /// Each message is acknowledged on its own.
  AckExplicit
}

/// How fast stored messages are delivered.
pub type ReplayPolicy {
  /// As fast as the consumer accepts them.
  ReplayInstant
  /// At the rate they were originally published.
  ReplayOriginal
}

type Naming {
  Durable(name: String)
  Ephemeral(name: Option(String))
}

/// The settings for a consumer.
pub opaque type Config {
  Config(
    naming: Naming,
    delivery: Delivery,
    description: Option(String),
    deliver_policy: DeliverPolicy,
    ack_policy: AckPolicy,
    ack_wait: Duration,
    max_deliver: Option(Int),
    filter_subjects: List(String),
    replay_policy: ReplayPolicy,
    max_ack_pending: Int,
    max_waiting: Int,
    inactive_threshold: Option(Duration),
    replicas: Int,
    headers_only: Bool,
  )
}

/// A position counted two ways.
pub type SequencePair {
  SequencePair(
    /// The consumer sequence, which counts deliveries, redeliveries
    /// included.
    consumer: Int,
    /// The stream sequence of the message.
    stream: Int,
  )
}

/// A consumer's configuration and progress, as the server reports them.
pub type Info {
  Info(
    /// The stream it reads from.
    stream: String,
    /// The consumer's name, assigned by the server for an unnamed
    /// ephemeral consumer.
    name: String,
    /// The configuration in effect as the server reports it. Settings the
    /// server leaves out read back as this module's defaults and do not
    /// apply: `max_waiting` for a push consumer, and `ack_wait` and
    /// `max_ack_pending` for `AckNone`. `replicas` is 0 when inherited from
    /// the stream.
    config: Config,
    /// The last message delivered.
    delivered: SequencePair,
    /// Every message up to here has been acknowledged.
    ack_floor: SequencePair,
    /// Messages not yet delivered.
    pending: Int,
    /// Messages delivered and waiting for an acknowledgement.
    ack_pending: Int,
    /// Messages delivered more than once and still unacknowledged.
    redelivered: Int,
    /// Pull requests waiting at the server.
    waiting: Int,
  )
}

/// A durable pull consumer named `name`: deliver all, explicit ack, 30 s ack
/// wait, 1000 max ack pending, 512 max waiting. It keeps its position until
/// deleted.
///
/// ```gleam
/// consumer.durable("worker")
/// |> consumer.with_deliver_policy(consumer.DeliverNew)
/// |> consumer.create(stream: "ORDERS")
/// ```
pub fn durable(name: String) -> Config {
  Config(
    naming: Durable(name),
    delivery: Pull,
    description: option.None,
    deliver_policy: DeliverAll,
    ack_policy: AckExplicit,
    ack_wait: duration.seconds(30),
    max_deliver: option.None,
    filter_subjects: [],
    replay_policy: ReplayInstant,
    max_ack_pending: 1000,
    max_waiting: 512,
    inactive_threshold: option.None,
    replicas: 0,
    headers_only: False,
  )
}

/// An ephemeral pull consumer with the same defaults and a 5 minute inactive
/// threshold. The server deletes it once it has been idle that long, and
/// names it unless `with_name` is used.
///
/// ```gleam
/// let assert Ok(info) =
///   consumer.ephemeral()
///   |> consumer.with_deliver_policy(consumer.DeliverLastPerSubject)
///   |> consumer.create(stream: "ORDERS")
///   |> nats.execute(on: conn)
///
/// info.name
/// // -> a server-assigned name
/// ```
pub fn ephemeral() -> Config {
  Config(
    ..durable(""),
    naming: Ephemeral(option.None),
    inactive_threshold: option.Some(duration.minutes(5)),
  )
}

/// Names an ephemeral consumer. Ignored on a durable config, whose name is
/// fixed by `durable`.
pub fn with_name(config: Config, name: String) -> Config {
  case config.naming {
    Durable(_) -> config
    Ephemeral(_) -> Config(..config, naming: Ephemeral(option.Some(name)))
  }
}

/// Pull (the client asks with `fetch`) or push (the server sends to a
/// subject).
/// Default: `Pull`.
pub fn with_delivery(config: Config, delivery: Delivery) -> Config {
  Config(..config, delivery: delivery)
}

/// A note for operators; the server does not interpret it.
/// Default: none.
pub fn with_description(config: Config, description: String) -> Config {
  Config(..config, description: option.Some(description))
}

/// Where in the stream delivery starts.
/// Default: `DeliverAll`.
pub fn with_deliver_policy(config: Config, policy: DeliverPolicy) -> Config {
  Config(..config, deliver_policy: policy)
}

/// Which messages must be acknowledged.
/// Default: `AckExplicit`.
pub fn with_ack_policy(config: Config, policy: AckPolicy) -> Config {
  Config(..config, ack_policy: policy)
}

/// How long the server waits for an acknowledgement before redelivering.
/// Default: 30 seconds.
pub fn with_ack_wait(config: Config, wait: Duration) -> Config {
  Config(..config, ack_wait: wait)
}

/// The most times one message is delivered, first delivery included.
/// Default: no limit.
pub fn with_max_deliver(config: Config, max: Int) -> Config {
  Config(..config, max_deliver: option.Some(max))
}

/// Delivers only messages on these subjects. Empty, the default, means all.
pub fn with_filter_subjects(config: Config, subjects: List(String)) -> Config {
  Config(..config, filter_subjects: subjects)
}

/// `ReplayInstant` delivers as fast as possible; `ReplayOriginal` keeps the
/// original publishing pace.
/// Default: `ReplayInstant`.
pub fn with_replay_policy(config: Config, policy: ReplayPolicy) -> Config {
  Config(..config, replay_policy: policy)
}

/// The most delivered but unacknowledged messages. Delivery pauses at the
/// limit.
/// Default: 1000.
pub fn with_max_ack_pending(config: Config, max: Int) -> Config {
  Config(..config, max_ack_pending: max)
}

/// The most pull requests the server queues at once.
/// Default: 512.
pub fn with_max_waiting(config: Config, max: Int) -> Config {
  Config(..config, max_waiting: max)
}

/// The server deletes the consumer after this long with no activity.
/// Default: none for durable, 5 minutes for ephemeral.
pub fn with_inactive_threshold(config: Config, threshold: Duration) -> Config {
  Config(..config, inactive_threshold: option.Some(threshold))
}

/// Copies kept across a cluster. A single server accepts only 1.
/// Default: 0, the stream's count.
pub fn with_replicas(config: Config, replicas: Int) -> Config {
  Config(..config, replicas: replicas)
}

/// Delivers headers without payloads. Each message carries a
/// `Nats-Msg-Size` header with the original size.
/// Default: `False`.
pub fn with_headers_only(config: Config, headers_only: Bool) -> Config {
  Config(..config, headers_only: headers_only)
}

fn name(config: Config) -> Option(String) {
  case config.naming {
    Durable(name) -> option.Some(name)
    Ephemeral(name) -> name
  }
}

/// Creates the consumer on `stream` and returns its info. Creating one that
/// already exists with the same configuration succeeds; a different
/// configuration is `AlreadyExists`. A `DeliverFromSequence` outside
/// ±(2^53 - 1) fails with `InvalidConfig` without sending anything.
///
/// ```gleam
/// consumer.durable("worker") |> consumer.create(stream: "ORDERS")
/// ```
pub fn create(
  config: Config,
  stream stream: String,
) -> jetgleam.Operation(Info) {
  let names = case name(config) {
    option.Some(name) -> [
      jetgleam.Argument("stream", stream),
      jetgleam.Setting("name", name),
    ]
    option.None -> [jetgleam.Argument("stream", stream)]
  }
  let payload =
    json.object([
      #("stream_name", json.string(stream)),
      #("config", encode_config(config)),
      #("action", json.string("create")),
    ])
  let safe = case config.deliver_policy {
    DeliverFromSequence(sequence) -> jetgleam.is_safe_int(sequence)
    _ -> True
  }
  case safe {
    True ->
      jetgleam.api_request(
        "CONSUMER.CREATE",
        names,
        jetgleam.json_payload(payload),
        info_decoder(),
      )
    False -> jetgleam.invalid_config("sequence", jetgleam.unsafe_int)
  }
}

/// The consumer's configuration and progress. `NotFound` if the stream or
/// consumer does not exist.
pub fn info(
  stream stream: String,
  consumer consumer: String,
) -> jetgleam.Operation(Info) {
  jetgleam.api_request(
    "CONSUMER.INFO",
    [
      jetgleam.Argument("stream", stream),
      jetgleam.Argument("consumer", consumer),
    ],
    <<>>,
    info_decoder(),
  )
}

/// Deletes the consumer. `NotFound` if the stream or consumer does not exist.
pub fn delete(
  stream stream: String,
  consumer consumer: String,
) -> jetgleam.Operation(Nil) {
  jetgleam.api_request(
    "CONSUMER.DELETE",
    [
      jetgleam.Argument("stream", stream),
      jetgleam.Argument("consumer", consumer),
    ],
    <<>>,
    jetgleam.success_decoder(),
  )
}

/// Every consumer name on `stream`; pages through the server's listing.
pub fn names(stream: String) -> jetgleam.Operation(List(String)) {
  jetgleam.paged(
    "CONSUMER.NAMES",
    [jetgleam.Argument("stream", stream)],
    "consumers",
    decode.string,
  )
}

/// Every consumer's configuration and progress on `stream`; pages through
/// the server's listing.
pub fn list(stream: String) -> jetgleam.Operation(List(Info)) {
  jetgleam.paged(
    "CONSUMER.LIST",
    [jetgleam.Argument("stream", stream)],
    "consumers",
    info_decoder(),
  )
}

fn nanos(duration: Duration) -> Json {
  json.int(jetgleam.nanoseconds(duration))
}

fn encode_config(config: Config) -> Json {
  let #(policy, extra) = case config.deliver_policy {
    DeliverAll -> #("all", [])
    DeliverLast -> #("last", [])
    DeliverNew -> #("new", [])
    DeliverLastPerSubject -> #("last_per_subject", [])
    DeliverFromSequence(sequence) -> #("by_start_sequence", [
      #("opt_start_seq", json.int(sequence)),
    ])
    DeliverFromTime(time) -> #("by_start_time", [
      #("opt_start_time", json.string(jetgleam.rfc3339(time))),
    ])
  }
  let ack = case config.ack_policy {
    AckNone -> "none"
    AckAll -> "all"
    AckExplicit -> "explicit"
  }
  json.object(
    list.flatten([
      jetgleam.optional("name", name(config), json.string),
      case config.naming {
        Durable(name) -> [#("durable_name", json.string(name))]
        Ephemeral(_) -> []
      },
      jetgleam.optional("description", config.description, json.string),
      [#("deliver_policy", json.string(policy))],
      extra,
      [
        #("ack_policy", json.string(ack)),
        #("ack_wait", nanos(config.ack_wait)),
        #("max_deliver", json.int(option.unwrap(config.max_deliver, -1))),
      ],
      case config.filter_subjects {
        [] -> []
        subjects -> [#("filter_subjects", json.array(subjects, json.string))]
      },
      [
        #(
          "replay_policy",
          json.string(case config.replay_policy {
            ReplayInstant -> "instant"
            ReplayOriginal -> "original"
          }),
        ),
      ],
      // The server rejects max_ack_pending with AckNone and max_waiting on push.
      case config.ack_policy {
        AckNone -> []
        AckAll | AckExplicit -> [
          #("max_ack_pending", json.int(config.max_ack_pending)),
        ]
      },
      case config.delivery {
        Pull -> [#("max_waiting", json.int(config.max_waiting))]
        Push(..) -> []
      },
      jetgleam.optional("inactive_threshold", config.inactive_threshold, nanos),
      [
        #("num_replicas", json.int(config.replicas)),
        #("headers_only", json.bool(config.headers_only)),
      ],
      case config.delivery {
        Pull -> []
        Push(subject, heartbeat, flow_control) ->
          list.flatten([
            [#("deliver_subject", json.string(subject))],
            jetgleam.optional("idle_heartbeat", heartbeat, nanos),
            [#("flow_control", json.bool(flow_control))],
          ])
      },
    ]),
  )
}

fn optional_string(
  name: String,
  next: fn(Option(String)) -> decode.Decoder(a),
) -> decode.Decoder(a) {
  decode.optional_field(name, option.None, decode.optional(decode.string), next)
}

fn config_decoder() -> decode.Decoder(Config) {
  use durable_name <- optional_string("durable_name")
  use name <- optional_string("name")
  use description <- optional_string("description")
  use filter_subject <- optional_string("filter_subject")
  use filter_subjects <- decode.optional_field(
    "filter_subjects",
    [],
    decode.optional(decode.list(decode.string))
      |> decode.map(option.unwrap(_, [])),
  )
  use deliver_subject <- optional_string("deliver_subject")
  use start_sequence <- decode.optional_field(
    "opt_start_seq",
    option.None,
    decode.optional(jetgleam.safe_int_decoder()),
  )
  use start_time <- decode.optional_field(
    "opt_start_time",
    option.None,
    decode.optional(jetgleam.timestamp_decoder()),
  )
  use deliver_policy <- jetgleam.enum_decoder("deliver_policy", DeliverAll, [
    #("all", DeliverAll),
    #("last", DeliverLast),
    #("new", DeliverNew),
    #("last_per_subject", DeliverLastPerSubject),
    ..option.values([
      option.map(start_sequence, fn(sequence) {
        #("by_start_sequence", DeliverFromSequence(sequence))
      }),
      option.map(start_time, fn(time) {
        #("by_start_time", DeliverFromTime(time))
      }),
    ])
  ])
  use ack_policy <- jetgleam.enum_decoder("ack_policy", AckExplicit, [
    #("none", AckNone),
    #("all", AckAll),
    #("explicit", AckExplicit),
  ])
  use ack_wait <- decode.optional_field(
    "ack_wait",
    duration.seconds(30),
    jetgleam.duration_decoder(),
  )
  use max_deliver <- decode.optional_field("max_deliver", -1, decode.int)
  use replay_policy <- jetgleam.enum_decoder("replay_policy", ReplayInstant, [
    #("instant", ReplayInstant),
    #("original", ReplayOriginal),
  ])
  use max_ack_pending <- decode.optional_field(
    "max_ack_pending",
    1000,
    decode.int,
  )
  use max_waiting <- decode.optional_field("max_waiting", 512, decode.int)
  use inactive_threshold <- jetgleam.positive_decoder("inactive_threshold")
  use replicas <- decode.optional_field("num_replicas", 0, decode.int)
  use headers_only <- decode.optional_field("headers_only", False, decode.bool)
  use heartbeat <- jetgleam.positive_decoder("idle_heartbeat")
  use flow_control <- decode.optional_field("flow_control", False, decode.bool)
  let filter_subjects = case filter_subject, filter_subjects {
    option.Some(subject), [] if subject != "" -> [subject]
    _, subjects -> subjects
  }
  decode.success(Config(
    naming: case durable_name {
      option.Some(durable_name) -> Durable(option.unwrap(name, durable_name))
      option.None -> Ephemeral(name)
    },
    delivery: case deliver_subject {
      option.Some(subject) ->
        Push(
          subject:,
          heartbeat: option.map(heartbeat, duration.nanoseconds),
          flow_control:,
        )
      option.None -> Pull
    },
    description:,
    deliver_policy:,
    ack_policy:,
    ack_wait:,
    max_deliver: case max_deliver {
      -1 -> option.None
      max -> option.Some(max)
    },
    filter_subjects:,
    replay_policy:,
    max_ack_pending:,
    max_waiting:,
    inactive_threshold: option.map(inactive_threshold, duration.nanoseconds),
    replicas:,
    headers_only:,
  ))
}

fn sequence_pair_decoder() -> decode.Decoder(SequencePair) {
  use consumer <- decode.field("consumer_seq", jetgleam.safe_int_decoder())
  use stream <- decode.field("stream_seq", jetgleam.safe_int_decoder())
  decode.success(SequencePair(consumer:, stream:))
}

fn info_decoder() -> decode.Decoder(Info) {
  use stream <- decode.field("stream_name", decode.string)
  use name <- decode.field("name", decode.string)
  use config <- decode.field("config", config_decoder())
  use delivered <- decode.field("delivered", sequence_pair_decoder())
  use ack_floor <- decode.field("ack_floor", sequence_pair_decoder())
  use pending <- decode.optional_field("num_pending", 0, decode.int)
  use ack_pending <- decode.optional_field("num_ack_pending", 0, decode.int)
  use redelivered <- decode.optional_field("num_redelivered", 0, decode.int)
  use waiting <- decode.optional_field("num_waiting", 0, decode.int)
  decode.success(Info(
    stream:,
    name:,
    config:,
    delivered:,
    ack_floor:,
    pending:,
    ack_pending:,
    redelivered:,
    waiting:,
  ))
}

/// A one-shot pull of messages from a consumer.
pub opaque type Fetch {
  Fetch(
    stream: String,
    consumer: String,
    max: Int,
    wait: Duration,
    heartbeat: Option(Duration),
    domain: Option(String),
  )
}

/// A one-shot pull of up to `max` messages. `wait` is how long the server
/// may hold the request open, and the local deadline for the whole fetch,
/// counted in whole milliseconds. Run it with `nats.fetch`, which fails
/// without sending anything for an invalid stream or consumer name, a `max`
/// outside 1 to 2^53 - 1, or a `wait` outside 1 to 2^31 - 1 ms; the error is
/// a `FetchError` holding `JetStream(InvalidArgument)`.
///
/// ```gleam
/// let assert Ok(batch) =
///   consumer.fetch(
///     stream: "ORDERS",
///     consumer: "worker",
///     max: 10,
///     wait: duration.seconds(2),
///   )
///   |> nats.fetch(on: conn)
/// ```
///
/// A fetch can return fewer than `max` messages, including none, when the
/// wait runs out first.
pub fn fetch(
  stream stream: String,
  consumer consumer: String,
  max max: Int,
  wait wait: Duration,
) -> Fetch {
  Fetch(
    stream:,
    consumer:,
    max:,
    wait:,
    heartbeat: option.None,
    domain: option.None,
  )
}

/// Asks the server for a heartbeat every `interval` while it has nothing to
/// send. The server expires the request at 90% of `wait` and needs the
/// heartbeat to be at most half of that, so `interval` must be between 1 ms
/// and `wait * 9 / 20`; otherwise `nats.fetch` fails with a `FetchError`
/// holding `JetStream(InvalidConfig)` without sending anything.
pub fn with_heartbeat(fetch: Fetch, interval: Duration) -> Fetch {
  Fetch(..fetch, heartbeat: option.Some(interval))
}

/// Fetches from the JetStream domain `domain` instead of the local one; see
/// `jetgleam.with_domain`.
pub fn with_domain(fetch: Fetch, domain: String) -> Fetch {
  Fetch(..fetch, domain: option.Some(domain))
}

/// A fetch in progress: messages collected so far and how many remain.
@internal
pub opaque type Fetching {
  Fetching(max: Int, collected: List(jetgleam.Message), count: Int)
}

/// What `fetch_received` made of one message.
@internal
pub type FetchStep {
  /// Keep receiving.
  Continue(
    /// The updated fetch.
    fetching: Fetching,
  )
  /// A full batch or a server completion status. May be empty.
  Complete(
    /// The messages, in delivery order.
    messages: List(jetgleam.Message),
  )
  /// The server rejected the pull; `messages` arrived first.
  Stop(
    /// The messages that arrived before the rejection.
    messages: List(jetgleam.Message),
    /// Why the server rejected it.
    error: jetgleam.Error,
  )
}

/// The pull request to publish with replies routed to `inbox`, and the
/// local deadline in milliseconds. Fails with `InvalidArgument` for an invalid
/// stream or consumer name, batch size or wait, and `InvalidConfig` for an
/// invalid heartbeat.
@internal
pub fn start_fetch(
  fetch: Fetch,
  inbox: String,
) -> Result(#(Fetching, jetgleam.Message, Int), jetgleam.Error) {
  use subject <- result.try(
    jetgleam.api_subject("CONSUMER.MSG.NEXT", [
      jetgleam.Argument("stream", fetch.stream),
      jetgleam.Argument("consumer", fetch.consumer),
    ]),
  )
  let wait = duration.to_milliseconds(fetch.wait)
  // The server expires first so a short batch ends with a 408.
  let expires = wait * 900_000
  use Nil <- result.try(require(
    fetch.max >= 1 && jetgleam.is_safe_int(fetch.max),
    jetgleam.InvalidArgument("max", "must be between 1 and 2^53 - 1"),
  ))
  use Nil <- result.try(require(
    wait >= 1 && wait <= 2_147_483_647,
    jetgleam.InvalidArgument("wait", "must be between 1 and 2^31 - 1 ms"),
  ))
  use Nil <- result.try(case fetch.heartbeat {
    option.Some(heartbeat) -> {
      let interval = jetgleam.nanoseconds(heartbeat)
      require(
        interval >= 1_000_000 && interval * 2 <= expires,
        jetgleam.InvalidConfig(
          "heartbeat",
          "must be between 1 ms and half the server expiry, 90% of wait",
        ),
      )
    }
    option.None -> Ok(Nil)
  })
  let prefix = case fetch.domain {
    option.Some(domain) -> "$JS." <> domain <> ".API."
    option.None -> "$JS.API."
  }
  let payload =
    json.object([
      #("batch", json.int(fetch.max)),
      #("expires", json.int(expires)),
      ..jetgleam.optional("idle_heartbeat", fetch.heartbeat, nanos)
    ])
  let message =
    jetgleam.message(prefix <> subject, jetgleam.json_payload(payload))
    |> jetgleam.set_reply_to(inbox)
  Ok(#(Fetching(fetch.max, [], 0), message, wait))
}

fn require(valid: Bool, error: jetgleam.Error) -> Result(Nil, jetgleam.Error) {
  case valid {
    True -> Ok(Nil)
    False -> Error(error)
  }
}

/// Classifies one message from the inbox: delivery, heartbeat, or status.
@internal
pub fn fetch_received(
  fetching: Fetching,
  message: jetgleam.Message,
) -> FetchStep {
  case message.status {
    option.None -> {
      let collected = [message, ..fetching.collected]
      let count = fetching.count + 1
      case count == fetching.max {
        True -> Complete(list.reverse(collected))
        False -> Continue(Fetching(..fetching, collected:, count:))
      }
    }
    option.Some(jetgleam.Status(100, _)) -> Continue(fetching)
    option.Some(jetgleam.Status(404, _))
    | option.Some(jetgleam.Status(408, _)) -> Complete(fetch_messages(fetching))
    option.Some(jetgleam.Status(code, description)) ->
      Stop(
        fetch_messages(fetching),
        jetgleam.Api(code, 0, option.unwrap(description, "")),
      )
  }
}

/// The deadline passed; returns what arrived.
@internal
pub fn fetch_messages(fetching: Fetching) -> List(jetgleam.Message) {
  list.reverse(fetching.collected)
}

/// How to acknowledge a message.
pub type AckKind {
  /// Processed. Do not redeliver.
  Ack
  /// Not processed. Redeliver now.
  Nak
  /// Not processed. Redeliver later.
  NakAfter(
    /// How long to wait before redelivering.
    delay: Duration,
  )
  /// Never redeliver, whatever `max_deliver` says.
  Term
  /// Still working. Restarts the ack wait.
  InProgress
}

/// Fire-and-forget acknowledgement (a single publish). Fails with
/// `NotAcknowledgeable`, without I/O, when `message` did not come from a
/// consumer.
///
/// ```gleam
/// consumer.ack(msg, consumer.Ack) |> nats.execute(on: conn)
///
/// // Ask for redelivery in a minute.
/// consumer.ack(msg, consumer.NakAfter(duration.minutes(1)))
/// |> nats.execute(on: conn)
///
/// // Still working; reset the ack wait.
/// consumer.ack(msg, consumer.InProgress) |> nats.execute(on: conn)
/// ```
pub fn ack(
  message: jetgleam.Message,
  kind: AckKind,
) -> jetgleam.Operation(Nil) {
  use reply_to, body <- ack_body(message, kind)
  jetgleam.send(jetgleam.message(reply_to, body), jetgleam.done(Ok(Nil)))
}

fn ack_body(
  message: jetgleam.Message,
  kind: AckKind,
  next: fn(String, BitArray) -> jetgleam.Operation(Nil),
) -> jetgleam.Operation(Nil) {
  case message.reply_to {
    option.Some("$JS.ACK." <> _ as reply_to) -> {
      let text = case kind {
        Ack -> "+ACK"
        Nak -> "-NAK"
        NakAfter(delay) ->
          "-NAK "
          <> json.to_string(
            json.object([#("delay", json.int(jetgleam.nanoseconds(delay)))]),
          )
        Term -> "+TERM"
        InProgress -> "+WPI"
      }
      next(reply_to, bit_array.from_string(text))
    }
    _ -> jetgleam.done(Error(jetgleam.NotAcknowledgeable))
  }
}

/// Acknowledgement the server confirms (a request and reply). Use when a lost
/// ack would cause unacceptable redelivery.
pub fn ack_sync(
  message: jetgleam.Message,
  kind: AckKind,
) -> jetgleam.Operation(Nil) {
  use reply_to, body <- ack_body(message, kind)
  jetgleam.message(reply_to, body)
  |> jetgleam.request(fn(_) { jetgleam.done(Ok(Nil)) })
}

/// Delivery details read from a consumer message's reply subject.
pub type Metadata {
  Metadata(
    /// The JetStream domain, if the subject carried one.
    domain: Option(String),
    /// The stream the message is stored in.
    stream: String,
    /// The consumer that delivered it.
    consumer: String,
    /// How many times it has been delivered, starting at 1.
    delivered: Int,
    /// Its sequence in the stream.
    stream_sequence: Int,
    /// Its sequence among the consumer's deliveries.
    consumer_sequence: Int,
    /// When the stream stored it.
    time: Timestamp,
    /// Messages left for this consumer after this one.
    pending: Int,
  )
}

/// Parses the `$JS.ACK` reply subject (v1 layout, or v2 with 11+ tokens).
/// Fails for messages that did not come from a consumer.
///
/// ```gleam
/// let assert Ok(meta) = consumer.metadata(msg)
/// case meta.delivered > 1 {
///   True -> log("redelivery " <> int.to_string(meta.delivered))
///   False -> Nil
/// }
/// ```
pub fn metadata(message: jetgleam.Message) -> Result(Metadata, Nil) {
  use reply_to <- result.try(option.to_result(message.reply_to, Nil))
  case string.split(reply_to, ".") {
    ["$JS", "ACK", stream, consumer, d, s, c, t, p] ->
      parse_metadata(option.None, stream, consumer, d, s, c, t, p)
    ["$JS", "ACK", domain, _hash, stream, consumer, d, s, c, t, p, ..] ->
      parse_metadata(
        case domain {
          "_" -> option.None
          _ -> option.Some(domain)
        },
        stream,
        consumer,
        d,
        s,
        c,
        t,
        p,
      )
    _ -> Error(Nil)
  }
}

fn parse_metadata(
  domain: Option(String),
  stream: String,
  consumer: String,
  delivered: String,
  stream_sequence: String,
  consumer_sequence: String,
  time: String,
  pending: String,
) -> Result(Metadata, Nil) {
  use delivered <- result.try(counter(delivered))
  use stream_sequence <- result.try(counter(stream_sequence))
  use consumer_sequence <- result.try(counter(consumer_sequence))
  use time <- result.try(unix_nanoseconds(time))
  use pending <- result.try(counter(pending))
  Ok(Metadata(
    domain:,
    stream:,
    consumer:,
    delivered:,
    stream_sequence:,
    consumer_sequence:,
    time:,
    pending:,
  ))
}

// A count within the cross-target safe range. JavaScript rounds a larger
// number to at least 2^53, which this still rejects.
fn counter(text: String) -> Result(Int, Nil) {
  use n <- result.try(int.parse(text))
  case n >= 0 && jetgleam.is_safe_int(n) {
    True -> Ok(n)
    False -> Error(Nil)
  }
}

// Nanoseconds since the epoch, split as text: the whole count is beyond
// JavaScript's safe integers.
fn unix_nanoseconds(text: String) -> Result(Timestamp, Nil) {
  let split = string.length(text) - 9
  let #(seconds, nanoseconds) = case split > 0 {
    True -> #(string.slice(text, 0, split), string.drop_start(text, split))
    False -> #("0", text)
  }
  use seconds <- result.try(counter(seconds))
  use nanoseconds <- result.try(counter(nanoseconds))
  Ok(timestamp.from_unix_seconds_and_nanoseconds(seconds, nanoseconds))
}
