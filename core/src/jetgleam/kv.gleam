//// Key/value buckets stored in JetStream. Keys and bucket names are both
//// `String`, so every function that takes both labels them. An invalid key
//// fails with `jetgleam.InvalidKey(key)` without sending anything.
////
//// ```gleam
//// import jetgleam
//// import jetgleam/kv
//// import jetgleam_erlang/nats
////
//// let assert Ok(Nil) =
////   kv.config("settings")
////   |> kv.with_history(5)
////   |> kv.create_bucket
////   |> nats.execute(on: conn)
////
//// let assert Ok(revision) =
////   kv.put(bucket: "settings", key: "theme", value: <<"dark":utf8>>)
////   |> nats.execute(on: conn)
////
//// let assert Ok(entry) =
////   kv.get(bucket: "settings", key: "theme") |> nats.execute(on: conn)
//// entry.value
//// // -> <<"dark":utf8>>
//// ```
////
//// ## Conditional writes
////
//// `create` writes only if the key has no live value, and `update` writes
//// only if the key is still at a given revision. Either one fails with
//// `jetgleam.WrongLastSequence` when another writer got there first:
////
//// ```gleam
//// case
////   kv.update(bucket: "settings", key: "theme", value: <<"light":utf8>>, revision:)
////   |> nats.execute(on: conn)
//// {
////   Ok(new_revision) -> Ok(new_revision)
////   // Someone else wrote first. Read the key again and retry.
////   Error(nats.JetStream(jetgleam.WrongLastSequence(actual: _))) -> retry()
////   Error(error) -> Error(error)
//// }
//// ```
////
//// ## Watching
////
//// `watch`, `keys` and `history` describe what to read. `nats.collect`
//// reads it once and returns the entries; `nats.watch` keeps going with
//// live changes:
////
//// ```gleam
//// let assert Ok(entries) =
////   kv.keys("settings") |> nats.collect(on: conn, timeout: 5000)
//// list.map(entries, fn(entry) { entry.key })
//// // -> ["theme"]
//// ```

import gleam/bool
import gleam/function
import gleam/int
import gleam/list
import gleam/option.{type Option}
import gleam/order
import gleam/result
import gleam/string
import gleam/time/duration.{type Duration}
import gleam/time/timestamp.{type Timestamp}
import jetgleam
import jetgleam/consumer
import jetgleam/stream

/// The settings for a bucket.
pub opaque type Config {
  Config(
    name: String,
    description: Option(String),
    history: Int,
    ttl: Option(Duration),
    max_value_size: Option(Int),
    max_bytes: Option(Int),
    storage: stream.Storage,
    replicas: Int,
  )
}

/// What a revision did to its key.
pub type Change {
  /// Set a value.
  Put
  /// Marked the key deleted. Earlier revisions remain, within the
  /// bucket's history and TTL limits.
  Delete
  /// Marked the key purged and removed earlier revisions.
  Purge
}

/// One revision of a key.
pub type Entry {
  Entry(
    /// The bucket it belongs to.
    bucket: String,
    /// The key.
    key: String,
    /// The value. Empty for delete and purge markers, and for entries from
    /// `keys`.
    value: BitArray,
    /// The revision number. It counts writes across the whole bucket, not
    /// per key.
    revision: Int,
    /// When this revision was written.
    created: Timestamp,
    /// What this revision did.
    change: Change,
  )
}

/// A bucket named `name`. History 1, file storage, one replica, no limits.
///
/// ```gleam
/// kv.config("sessions")
/// |> kv.with_ttl(duration.hours(1))
/// |> kv.with_storage(stream.Memory)
/// |> kv.create_bucket
/// ```
pub fn config(name: String) -> Config {
  Config(
    name:,
    description: option.None,
    history: 1,
    ttl: option.None,
    max_value_size: option.None,
    max_bytes: option.None,
    storage: stream.File,
    replicas: 1,
  )
}

/// A note for operators; the server does not interpret it.
/// Default: none.
pub fn with_description(config: Config, description: String) -> Config {
  Config(..config, description: option.Some(description))
}

/// Revisions kept per key, 1 to 64.
/// Default: 1.
pub fn with_history(config: Config, history: Int) -> Config {
  Config(..config, history:)
}

/// Revisions older than `ttl` are removed, including the latest one, so a
/// key whose last write is older than `ttl` is gone.
/// Default: none.
pub fn with_ttl(config: Config, ttl: Duration) -> Config {
  Config(..config, ttl: option.Some(ttl))
}

/// The largest value, in bytes, a write accepts.
/// Default: no limit.
pub fn with_max_value_size(config: Config, bytes: Int) -> Config {
  Config(..config, max_value_size: option.Some(bytes))
}

/// The most bytes the bucket keeps across all keys and revisions. Writes
/// fail once it is full.
/// Default: no limit.
pub fn with_max_bytes(config: Config, bytes: Int) -> Config {
  Config(..config, max_bytes: option.Some(bytes))
}

/// File storage survives a server restart; memory storage does not.
/// Default: `stream.File`.
pub fn with_storage(config: Config, storage: stream.Storage) -> Config {
  Config(..config, storage:)
}

/// Copies kept across a cluster, 1 to 5. A single server accepts only 1.
/// Default: 1.
pub fn with_replicas(config: Config, replicas: Int) -> Config {
  Config(..config, replicas:)
}

/// Creates the bucket. Creating one that already exists with the same
/// configuration succeeds; a different configuration is `AlreadyExists`.
/// A name that does not match `[A-Za-z0-9_-]+` or a history outside 1 to 64
/// fails with `InvalidConfig` without sending anything.
pub fn create_bucket(config: Config) -> jetgleam.Operation(Nil) {
  case
    valid_bucket_name(config.name),
    config.history >= 1 && config.history <= 64
  {
    False, _ -> jetgleam.invalid_config("name", "must match [A-Za-z0-9_-]+")
    _, False -> jetgleam.invalid_config("history", "must be between 1 and 64")
    True, True ->
      stream.config("KV_" <> config.name, ["$KV." <> config.name <> ".>"])
      |> stream.with_max_messages_per_subject(config.history)
      |> stream.with_storage(config.storage)
      |> stream.with_replicas(config.replicas)
      |> stream.with_allow_rollup(True)
      |> stream.with_deny_delete(True)
      |> stream.with_allow_direct(True)
      |> stream.with_discard(stream.New)
      |> set(config.ttl, stream.with_max_age)
      |> set(config.ttl, fn(stream_config, ttl) {
        case duration.compare(ttl, duration.minutes(2)) {
          order.Lt -> stream.with_duplicate_window(stream_config, ttl)
          _ -> stream_config
        }
      })
      |> set(config.max_value_size, stream.with_max_message_size)
      |> set(config.max_bytes, stream.with_max_bytes)
      |> set(config.description, stream.with_description)
      |> stream.create
      |> map(fn(_) { Nil })
  }
}

/// Deletes the bucket and every key in it. `NotFound` if it does not exist.
pub fn delete_bucket(name: String) -> jetgleam.Operation(Nil) {
  use <- valid_bucket(name)
  stream.delete("KV_" <> name)
}

/// The latest revision. A deleted or purged key is `NotFound`.
pub fn get(
  bucket bucket: String,
  key key: String,
) -> jetgleam.Operation(Entry) {
  use <- valid_key(key)
  last(bucket, key)
  |> jetgleam.bind(fn(result) {
    jetgleam.done(case result {
      Ok(entry) if entry.change == Put -> Ok(entry)
      Ok(_) -> Error(jetgleam.NotFound)
      Error(error) -> Error(error)
    })
  })
}

/// Writes `value` to `key`, whatever the key's current state, and returns
/// the new revision.
pub fn put(
  bucket bucket: String,
  key key: String,
  value value: BitArray,
) -> jetgleam.Operation(Int) {
  write(bucket, key, value, [])
}

/// Like `put`, but fails with `WrongLastSequence` if the key has a live
/// value.
pub fn create(
  bucket bucket: String,
  key key: String,
  value value: BitArray,
) -> jetgleam.Operation(Int) {
  write(bucket, key, value, [#(expected_header, "0")])
  |> jetgleam.bind(fn(result) {
    case result {
      Error(jetgleam.WrongLastSequence(_) as error) ->
        last(bucket, key)
        |> jetgleam.bind(fn(last) {
          case last {
            Ok(Entry(change: Delete, revision:, ..))
            | Ok(Entry(change: Purge, revision:, ..)) ->
              update(bucket:, key:, value:, revision:)
            _ -> jetgleam.done(Error(error))
          }
        })
      _ -> jetgleam.done(result)
    }
  })
}

/// Like `put`, but only if the key is still at `revision`.
pub fn update(
  bucket bucket: String,
  key key: String,
  value value: BitArray,
  revision revision: Int,
) -> jetgleam.Operation(Int) {
  case jetgleam.is_safe_int(revision) {
    True ->
      write(bucket, key, value, [
        #(expected_header, int.to_string(revision)),
      ])
    False -> jetgleam.invalid_argument("revision", jetgleam.unsafe_int)
  }
}

/// Writes a delete marker. The marker takes one of the key's `history`
/// slots, so with history 1 it replaces the value. Earlier revisions remain
/// only while history and TTL allow.
pub fn delete(
  bucket bucket: String,
  key key: String,
) -> jetgleam.Operation(Nil) {
  write(bucket, key, <<>>, [#("KV-Operation", "DEL")])
  |> map(fn(_) { Nil })
}

/// Writes a purge marker and removes earlier revisions.
pub fn purge(
  bucket bucket: String,
  key key: String,
) -> jetgleam.Operation(Nil) {
  write(bucket, key, <<>>, [#("KV-Operation", "PURGE"), #("Nats-Rollup", "sub")])
  |> map(fn(_) { Nil })
}

const alphanumeric =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

const expected_header = "Nats-Expected-Last-Subject-Sequence"

fn all_in(text: String, allowed: String) -> Bool {
  text != "" && list.all(string.to_graphemes(text), string.contains(allowed, _))
}

fn set(
  config: stream.Config,
  value: Option(a),
  with: fn(stream.Config, a) -> stream.Config,
) -> stream.Config {
  case value {
    option.Some(value) -> with(config, value)
    option.None -> config
  }
}

fn map(
  operation: jetgleam.Operation(a),
  f: fn(a) -> b,
) -> jetgleam.Operation(b) {
  jetgleam.bind(operation, fn(result) { jetgleam.done(result.map(result, f)) })
}

fn valid_key(
  key: String,
  then: fn() -> jetgleam.Operation(a),
) -> jetgleam.Operation(a) {
  let valid = valid_tokens(key, False)
  bool.guard(
    when: !valid,
    return: jetgleam.done(Error(jetgleam.InvalidKey(key))),
    otherwise: then,
  )
}

fn valid_bucket(
  bucket: String,
  then: fn() -> jetgleam.Operation(a),
) -> jetgleam.Operation(a) {
  bool.guard(
    when: !valid_bucket_name(bucket),
    return: jetgleam.invalid_argument("bucket", "must match [A-Za-z0-9_-]+"),
    otherwise: then,
  )
}

fn valid_bucket_name(bucket: String) -> Bool {
  all_in(bucket, alphanumeric <> "-_")
}

fn valid_tokens(text: String, wildcards: Bool) -> Bool {
  let tokens = string.split(text, ".")
  let count = list.length(tokens)
  list.index_map(tokens, fn(token, i) {
    all_in(token, alphanumeric <> "-/_=")
    || { wildcards && token == "*" }
    || { wildcards && token == ">" && i == count - 1 }
  })
  |> list.all(function.identity)
}

fn subject(bucket: String, key: String) -> String {
  "$KV." <> bucket <> "." <> key
}

fn write(
  bucket: String,
  key: String,
  value: BitArray,
  headers: List(#(String, String)),
) -> jetgleam.Operation(Int) {
  use <- valid_bucket(bucket)
  use <- valid_key(key)
  list.fold(headers, jetgleam.message(subject(bucket, key), value), fn(m, h) {
    jetgleam.set_header(m, h.0, h.1)
  })
  |> jetgleam.publish
  |> map(fn(ack) { ack.sequence })
}

fn last(bucket: String, key: String) -> jetgleam.Operation(Entry) {
  use <- valid_bucket(bucket)
  stream.get_last_message(
    stream: "KV_" <> bucket,
    subject: subject(bucket, key),
  )
  |> map(fn(stored) {
    Entry(
      bucket:,
      key:,
      value: stored.payload,
      revision: stored.sequence,
      created: stored.time,
      change: change_from(stored.headers),
    )
  })
}

fn change_from(headers: List(#(String, String))) -> Change {
  let operation = list.key_find(headers, "KV-Operation")
  let marker = list.key_find(headers, "Nats-Marker-Reason")
  case operation, marker {
    Ok("DEL"), _ -> Delete
    Ok("PURGE"), _ -> Purge
    Error(Nil), Ok("MaxAge") | Error(Nil), Ok("Purge") -> Purge
    Error(Nil), Ok("Remove") -> Delete
    _, _ -> Put
  }
}

/// What to read from a bucket: a live watch, its keys, or one key's history.
pub opaque type Watch {
  Watch(
    bucket: String,
    filter: String,
    wildcards: Bool,
    policy: consumer.DeliverPolicy,
    headers_only: Bool,
    include_deletes: Bool,
    domain: Option(String),
  )
}

/// Latest value per matching key, then live changes, deletes included.
/// `filter` is a key or subject-style pattern (`*` per token, `>` last).
/// Run it with `nats.watch`.
///
/// ```gleam
/// let assert Ok(watch) =
///   kv.watch("settings", filter: "ui.>") |> nats.watch(on: conn)
///
/// case process.receive(nats.messages(watch), 5000) {
///   Ok(nats.Changed(entry)) -> io.println("changed: " <> entry.key)
///   Ok(nats.CaughtUp) -> io.println("replay done, now live")
///   Ok(nats.Failed(error)) -> io.println(string.inspect(error))
///   Error(Nil) -> io.println("quiet")
/// }
/// ```
pub fn watch(bucket: String, filter filter: String) -> Watch {
  Watch(
    bucket:,
    filter:,
    wildcards: True,
    policy: consumer.DeliverLastPerSubject,
    headers_only: False,
    include_deletes: True,
    domain: option.None,
  )
}

/// Latest live key names, values omitted, deletes excluded. Meant for
/// `nats.collect`.
pub fn keys(bucket: String) -> Watch {
  Watch(
    ..watch(bucket, filter: ">"),
    headers_only: True,
    include_deletes: False,
  )
}

/// Every retained revision of one key, oldest first. Meant for
/// `nats.collect`.
///
/// ```gleam
/// let assert Ok(revisions) =
///   kv.history(bucket: "settings", key: "theme")
///   |> nats.collect(on: conn, timeout: 5000)
/// ```
pub fn history(bucket bucket: String, key key: String) -> Watch {
  Watch(
    ..watch(bucket, filter: key),
    wildcards: False,
    policy: consumer.DeliverAll,
  )
}

/// Reads from the JetStream domain `domain` instead of the local one; see
/// `jetgleam.with_domain`.
pub fn with_domain(watch: Watch, domain: String) -> Watch {
  Watch(..watch, domain: option.Some(domain))
}

/// The state of a watch's ordered consumer: consumer and stream sequences
/// for spotting gaps, and progress through the initial replay.
@internal
pub opaque type Tracker {
  Tracker(
    watch: Watch,
    inbox: String,
    last_stream: Int,
    last_consumer: Int,
    replay: Replay,
  )
}

// Counting a consumer's initial backlog keeps live writes from postponing
// `CaughtUp`. An interrupted snapshot restarts with a fresh backlog.
type Replay {
  Starting
  Replaying(remaining: Int)
  Live
}

/// Something the tracker asks the runtime to do.
@internal
pub type Step {
  /// Pass this entry to the watcher.
  Deliver(
    /// The entry.
    entry: Entry,
  )
  /// The initial replay is complete. Emitted exactly once.
  CaughtUp
  /// Publish this flow-control or heartbeat response.
  Respond(
    /// The response to publish.
    message: jetgleam.Message,
  )
  /// The consumer must be replaced from the last delivered sequence. Pass a
  /// fresh inbox to `recreate`, so the old consumer's traffic stays on the
  /// old subscription.
  Recreate
}

/// Milliseconds without traffic after which the consumer is recreated.
@internal
pub const heartbeat_interval = 5000

/// Starts tracking. Execute the returned operation to create the ordered push
/// consumer delivering to `inbox`.
@internal
pub fn track(
  watch: Watch,
  inbox: String,
) -> #(Tracker, jetgleam.Operation(consumer.Info)) {
  let tracker =
    Tracker(watch:, inbox:, last_stream: 0, last_consumer: 0, replay: Starting)
  let operation = {
    use <- valid_bucket(watch.bucket)
    case valid_tokens(watch.filter, watch.wildcards) {
      True -> create_consumer(tracker, watch.policy)
      False -> jetgleam.done(Error(jetgleam.InvalidKey(watch.filter)))
    }
  }
  #(tracker, operation)
}

fn create_consumer(
  tracker: Tracker,
  policy: consumer.DeliverPolicy,
) -> jetgleam.Operation(consumer.Info) {
  let watch = tracker.watch
  let operation =
    consumer.ephemeral()
    |> consumer.with_delivery(consumer.Push(
      subject: tracker.inbox,
      heartbeat: option.Some(duration.milliseconds(heartbeat_interval / 2)),
      flow_control: True,
    ))
    |> consumer.with_ack_policy(consumer.AckNone)
    |> consumer.with_max_deliver(1)
    |> consumer.with_deliver_policy(policy)
    |> consumer.with_filter_subjects([subject(watch.bucket, watch.filter)])
    |> consumer.with_headers_only(watch.headers_only)
    |> consumer.with_replicas(1)
    |> consumer.with_inactive_threshold(duration.seconds(30))
    |> consumer.create(stream: "KV_" <> watch.bucket)
  case watch.domain {
    option.Some(domain) -> jetgleam.with_domain(operation, domain)
    option.None -> operation
  }
}

/// Replaces the consumer with one delivering to `inbox`. An unfinished
/// latest-per-key snapshot restarts with a fresh backlog; history and live
/// watches resume after the last stream sequence. Execute the operation,
/// then call `consumer_created`.
@internal
pub fn recreate(
  tracker: Tracker,
  inbox: String,
) -> #(Tracker, jetgleam.Operation(consumer.Info)) {
  let #(policy, replay) = case tracker.replay, tracker.watch.policy {
    Live, _ -> #(consumer.DeliverFromSequence(tracker.last_stream + 1), Live)
    Starting, consumer.DeliverLastPerSubject
    | Replaying(_), consumer.DeliverLastPerSubject
    -> #(consumer.DeliverLastPerSubject, Starting)
    Starting, _ | Replaying(_), _ -> #(
      consumer.DeliverFromSequence(tracker.last_stream + 1),
      Starting,
    )
  }
  let tracker = Tracker(..tracker, inbox:, replay:)
  #(tracker, create_consumer(tracker, policy))
}

/// Call once the consumer exists; emits `CaughtUp` at once for an empty
/// snapshot. A replacement's backlog defines the restarted snapshot, while
/// an already caught-up watch stays live.
@internal
pub fn consumer_created(
  tracker: Tracker,
  info: consumer.Info,
) -> #(Tracker, List(Step)) {
  let replay = case tracker.replay, info.pending {
    Starting, 0 | Replaying(_), 0 -> Live
    Starting, pending -> Replaying(pending)
    replay, _ -> replay
  }
  #(
    Tracker(..tracker, last_consumer: 0, replay:),
    finished(tracker.replay, replay),
  )
}

fn finished(before: Replay, after: Replay) -> List(Step) {
  case before, after {
    Live, _ -> []
    _, Live -> [CaughtUp]
    _, _ -> []
  }
}

@internal
pub fn observe(
  tracker: Tracker,
  message: jetgleam.Message,
) -> #(Tracker, List(Step)) {
  case message.status {
    option.Some(jetgleam.Status(code: 100, ..)) -> #(
      tracker,
      control(tracker, message),
    )
    _ ->
      case consumer.metadata(message) {
        Error(_) -> #(tracker, [])
        Ok(meta) if meta.consumer_sequence != tracker.last_consumer + 1 -> #(
          tracker,
          [Recreate],
        )
        Ok(meta) -> {
          let watch = tracker.watch
          let change = change_from(message.headers)
          let deliver =
            bool.guard(
              when: meta.stream_sequence <= tracker.last_stream
                || { !watch.include_deletes && change != Put },
              return: [],
              otherwise: fn() {
                [
                  Deliver(Entry(
                    bucket: watch.bucket,
                    key: string.drop_start(
                      message.subject,
                      string.length(subject(watch.bucket, "")),
                    ),
                    value: message.payload,
                    revision: meta.stream_sequence,
                    created: meta.time,
                    change:,
                  )),
                ]
              },
            )
          // Replayed revisions and suppressed markers still count toward
          // this consumer's snapshot, even though they are not delivered.
          let replay = case tracker.replay {
            Replaying(remaining) if remaining <= 1 || meta.pending == 0 -> Live
            Replaying(remaining) -> Replaying(remaining - 1)
            replay -> replay
          }
          #(
            Tracker(
              ..tracker,
              last_stream: int.max(tracker.last_stream, meta.stream_sequence),
              last_consumer: meta.consumer_sequence,
              replay:,
            ),
            list.append(deliver, finished(tracker.replay, replay)),
          )
        }
      }
  }
}

fn control(tracker: Tracker, message: jetgleam.Message) -> List(Step) {
  let stalled = jetgleam.get_header(message, "Nats-Consumer-Stalled")
  let last = jetgleam.get_header(message, "Nats-Last-Consumer")
  case message.reply_to, stalled, last {
    option.Some(reply_to), _, _ -> [Respond(jetgleam.message(reply_to, <<>>))]
    _, Ok(subject), _ -> [Respond(jetgleam.message(subject, <<>>))]
    _, _, Ok(last) ->
      case int.parse(last) == Ok(tracker.last_consumer) {
        True -> []
        False -> [Recreate]
      }
    _, _, _ -> []
  }
}
