import fake
import gleam/bit_array
import gleam/dynamic/decode
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option}
import gleam/string
import gleam/time/duration
import gleam/time/timestamp
import jetgleam
import jetgleam/consumer
import jetgleam/kv
import jetgleam/stream
import qcheck

fn key_gen() -> qcheck.Generator(String) {
  let segment =
    qcheck.non_empty_string_from(
      qcheck.codepoint_from_strings("a", ["Z", "0", "-", "/", "_", "="]),
    )
  qcheck.map2(segment, qcheck.list_from(segment), fn(head, rest) {
    string.join([head, ..rest], ".")
  })
}

fn count_gen() -> qcheck.Generator(Int) {
  qcheck.bounded_int(1, 8)
}

fn seq(from: Int, count: Int) -> List(Int) {
  list.repeat(Nil, count) |> list.index_map(fn(_, i) { from + i })
}

fn delivery(
  bucket: String,
  key: String,
  stream_seq: Int,
  consumer_seq: Int,
  pending: Int,
  operation: Option(String),
) -> jetgleam.Message {
  let reply_to =
    string.join(
      [
        "$JS.ACK.KV_b.c.1",
        int.to_string(stream_seq),
        int.to_string(consumer_seq),
        "0",
        int.to_string(pending),
      ],
      ".",
    )
  let message =
    jetgleam.message("$KV." <> bucket <> "." <> key, <<"v":utf8>>)
    |> jetgleam.set_reply_to(reply_to)
  case operation {
    option.Some(operation) ->
      jetgleam.set_header(message, "KV-Operation", operation)
    option.None -> message
  }
}

fn status_100(reply_to: Option(String)) -> jetgleam.Message {
  jetgleam.Message(
    ..jetgleam.message("_INBOX.w", <<>>),
    reply_to:,
    status: option.Some(jetgleam.Status(100, option.None)),
  )
}

fn tracker(
  watch: kv.Watch,
) -> #(kv.Tracker, jetgleam.Operation(consumer.Info)) {
  kv.track(watch, "_INBOX.w")
}

fn ack_reply(sequence: Int) -> jetgleam.Message {
  fake.reply(
    json.object([
      #("stream", json.string("KV_b")),
      #("seq", json.int(sequence)),
    ]),
  )
}

fn stored_reply(
  sequence: Int,
  headers: String,
  data: String,
) -> jetgleam.Message {
  let encode = fn(text) { bit_array.base64_encode(<<text:utf8>>, True) }
  fake.reply(
    json.object([
      #(
        "message",
        json.object([
          #("subject", json.string("$KV.b.k")),
          #("seq", json.int(sequence)),
          #("hdrs", json.string(encode(headers))),
          #("data", json.string(encode(data))),
          #("time", json.string("2024-01-02T03:04:05Z")),
        ]),
      ),
    ]),
  )
}

fn marker(operation: String) -> String {
  "NATS/1.0\r\nKV-Operation: " <> operation <> "\r\n\r\n"
}

pub fn create_bucket_test() {
  let expected =
    stream.config("KV_b", ["$KV.b.>"])
    |> stream.with_max_messages_per_subject(5)
    |> stream.with_storage(stream.Memory)
    |> stream.with_replicas(3)
    |> stream.with_allow_rollup(True)
    |> stream.with_deny_delete(True)
    |> stream.with_allow_direct(True)
    |> stream.with_discard(stream.New)
    |> stream.with_max_age(duration.seconds(30))
    |> stream.with_duplicate_window(duration.seconds(30))
    |> stream.with_max_message_size(1024)
    |> stream.with_max_bytes(1_000_000)
    |> stream.with_description("d")
    |> stream.create
  let operation =
    kv.config("b")
    |> kv.with_description("d")
    |> kv.with_history(5)
    |> kv.with_ttl(duration.seconds(30))
    |> kv.with_max_value_size(1024)
    |> kv.with_max_bytes(1_000_000)
    |> kv.with_storage(stream.Memory)
    |> kv.with_replicas(3)
    |> kv.create_bucket
  let sent = fake.sent(operation)
  let want = fake.sent(expected)
  assert sent.subject == want.subject
  assert sent.payload == want.payload

  let info =
    json.object([
      #("config", json.object([#("name", json.string("KV_b"))])),
      #(
        "state",
        json.object([
          #("messages", json.int(0)),
          #("bytes", json.int(0)),
          #("first_seq", json.int(0)),
          #("last_seq", json.int(0)),
          #("consumer_count", json.int(0)),
        ]),
      ),
      #("created", json.string("2024-01-02T03:04:05Z")),
    ])
  assert fake.run(operation, fn(_) { fake.reply(info) }) == Ok(Nil)
}

pub fn delete_bucket_test() {
  assert fake.sent(kv.delete_bucket("b")).subject
    == "$JS.API.STREAM.DELETE.KV_b"
}

pub fn validation_without_io_test() {
  let assert Error(jetgleam.InvalidConfig("name", _)) =
    kv.config("a.b") |> kv.create_bucket |> fake.run(fake.no_io)
  let assert Error(jetgleam.InvalidConfig("history", _)) =
    kv.config("b")
    |> kv.with_history(0)
    |> kv.create_bucket
    |> fake.run(fake.no_io)
  let assert Error(jetgleam.InvalidConfig("history", _)) =
    kv.config("b")
    |> kv.with_history(65)
    |> kv.create_bucket
    |> fake.run(fake.no_io)

  use key <- list.each([".a", "a.", "a..b", "a b", ""])
  assert kv.put(bucket: "b", key:, value: <<>>) |> fake.run(fake.no_io)
    == Error(jetgleam.InvalidKey(key))
  assert kv.get(bucket: "b", key:) |> fake.run(fake.no_io)
    == Error(jetgleam.InvalidKey(key))
}

pub fn invalid_bucket_without_io_test() {
  use bucket <- list.each(["b.ui", "", "a b", "*", ">"])
  let watch_op = fn(watch) { kv.track(watch, "_INBOX.w").1 }
  assert kv.get(bucket:, key: "k") |> fake.run(fake.no_io) == invalid_bucket()
  assert kv.put(bucket:, key: "k", value: <<>>) |> fake.run(fake.no_io)
    == invalid_bucket()
  assert kv.create(bucket:, key: "k", value: <<>>) |> fake.run(fake.no_io)
    == invalid_bucket()
  assert kv.update(bucket:, key: "k", value: <<>>, revision: 1)
    |> fake.run(fake.no_io)
    == invalid_bucket()
  assert kv.delete(bucket:, key: "k") |> fake.run(fake.no_io)
    == invalid_bucket()
  assert kv.purge(bucket:, key: "k") |> fake.run(fake.no_io) == invalid_bucket()
  assert kv.delete_bucket(bucket) |> fake.run(fake.no_io) == invalid_bucket()
  assert kv.watch(bucket, filter: ">") |> watch_op |> fake.run(fake.no_io)
    == invalid_bucket()
  assert kv.keys(bucket) |> watch_op |> fake.run(fake.no_io) == invalid_bucket()
  assert kv.history(bucket:, key: "k") |> watch_op |> fake.run(fake.no_io)
    == invalid_bucket()
}

fn invalid_bucket() -> Result(a, jetgleam.Error) {
  Error(jetgleam.InvalidArgument("bucket", "must match [A-Za-z0-9_-]+"))
}

pub fn put_accepts_valid_keys_test() {
  use key <- qcheck.given(key_gen())
  let operation = kv.put(bucket: "b", key:, value: <<"v":utf8>>)
  let sent = fake.sent(operation)
  assert sent.subject == "$KV.b." <> key
  assert sent.payload == <<"v":utf8>>
  assert fake.run(operation, fn(_) { ack_reply(9) }) == Ok(9)
}

pub fn get_test() {
  let get = kv.get(bucket: "b", key: "k")
  assert fake.sent(get).subject == "$JS.API.STREAM.MSG.GET.KV_b"
  assert fake.run(get, fn(_) { stored_reply(4, marker("DEL"), "") })
    == Error(jetgleam.NotFound)
  assert fake.run(get, fn(_) { stored_reply(4, marker("PURGE"), "") })
    == Error(jetgleam.NotFound)
  let assert Ok(created) = timestamp.parse_rfc3339("2024-01-02T03:04:05Z")
  let assert Error(jetgleam.BadResponse(..)) =
    fake.run(get, fn(_) {
      stored_reply(5, "NATS/1.0\r\nno colon\r\n\r\n", "hi")
    })
  assert fake.run(get, fn(_) { stored_reply(5, "NATS/1.0\r\n\r\n", "hi") })
    == Ok(kv.Entry("b", "k", <<"hi":utf8>>, 5, created, kv.Put))
}

pub fn get_treats_server_markers_as_missing_test() {
  let get = kv.get(bucket: "b", key: "k")
  use reason <- list.each(["MaxAge", "Purge", "Remove"])
  let headers = "NATS/1.0\r\nNats-Marker-Reason: " <> reason <> "\r\n\r\n"
  assert fake.run(get, fn(_) { stored_reply(4, headers, "") })
    == Error(jetgleam.NotFound)
}

pub fn create_retries_after_delete_marker_test() {
  let respond = fn(message: jetgleam.Message) {
    case message.subject, jetgleam.get_header(message, expected) {
      "$KV.b.k", Ok("0") ->
        fake.reply(fake.error_reply(400, 10_071, "wrong last sequence: 4"))
      "$KV.b.k", Ok("4") -> ack_reply(5)
      "$JS.API.STREAM.MSG.GET.KV_b", _ -> stored_reply(4, marker("DEL"), "")
      _, _ -> panic as "unexpected request"
    }
  }
  assert kv.create(bucket: "b", key: "k", value: <<"v":utf8>>)
    |> fake.run(respond)
    == Ok(5)
}

pub fn create_keeps_error_for_live_value_test() {
  let respond = fn(message: jetgleam.Message) {
    case message.subject {
      "$KV.b.k" ->
        fake.reply(fake.error_reply(400, 10_071, "wrong last sequence: 4"))
      _ -> stored_reply(4, "NATS/1.0\r\n\r\n", "x")
    }
  }
  assert kv.create(bucket: "b", key: "k", value: <<"v":utf8>>)
    |> fake.run(respond)
    == Error(jetgleam.WrongLastSequence(actual: option.Some(4)))
}

const expected = "Nats-Expected-Last-Subject-Sequence"

pub fn update_delete_purge_headers_test() {
  let update =
    fake.sent(kv.update(bucket: "b", key: "k", value: <<>>, revision: 7))
  assert jetgleam.get_header(update, expected) == Ok("7")
  let assert Error(jetgleam.InvalidArgument("revision", _)) =
    kv.update(
      bucket: "b",
      key: "k",
      value: <<>>,
      revision: 9_007_199_254_740_991 + 1,
    )
    |> fake.run(fake.no_io)

  let delete = fake.sent(kv.delete(bucket: "b", key: "k"))
  assert delete.subject == "$KV.b.k"
  assert delete.payload == <<>>
  assert jetgleam.get_header(delete, "KV-Operation") == Ok("DEL")

  let purge = fake.sent(kv.purge(bucket: "b", key: "k"))
  assert jetgleam.get_header(purge, "KV-Operation") == Ok("PURGE")
  assert jetgleam.get_header(purge, "Nats-Rollup") == Ok("sub")
  assert kv.purge(bucket: "b", key: "k") |> fake.run(fn(_) { ack_reply(3) })
    == Ok(Nil)
}

pub fn track_builds_ordered_consumer_test() {
  let pipeline = fn(policy, headers_only, filter) {
    consumer.ephemeral()
    |> consumer.with_delivery(consumer.Push(
      subject: "_INBOX.w",
      heartbeat: option.Some(duration.milliseconds(kv.heartbeat_interval / 2)),
      flow_control: True,
    ))
    |> consumer.with_ack_policy(consumer.AckNone)
    |> consumer.with_max_deliver(1)
    |> consumer.with_deliver_policy(policy)
    |> consumer.with_filter_subjects([filter])
    |> consumer.with_headers_only(headers_only)
    |> consumer.with_replicas(1)
    |> consumer.with_inactive_threshold(duration.seconds(30))
    |> consumer.create(stream: "KV_b")
    |> fake.sent
  }
  let same = fn(operation, expected: jetgleam.Message) {
    let sent = fake.sent(operation)
    assert sent.subject == expected.subject
    assert sent.payload == expected.payload
  }
  same(
    tracker(kv.watch("b", filter: "a.>")).1,
    pipeline(consumer.DeliverLastPerSubject, False, "$KV.b.a.>"),
  )
  same(
    tracker(kv.keys("b")).1,
    pipeline(consumer.DeliverLastPerSubject, True, "$KV.b.>"),
  )
  same(
    tracker(kv.history(bucket: "b", key: "k")).1,
    pipeline(consumer.DeliverAll, False, "$KV.b.k"),
  )
  let domain = tracker(kv.watch("b", filter: ">") |> kv.with_domain("hub")).1
  assert fake.sent(domain).subject == "$JS.hub.API.CONSUMER.CREATE.KV_b"

  assert tracker(kv.watch("b", filter: "a.>.b")).1 |> fake.run(fake.no_io)
    == Error(jetgleam.InvalidKey("a.>.b"))
  assert tracker(kv.history(bucket: "b", key: "a.*")).1 |> fake.run(fake.no_io)
    == Error(jetgleam.InvalidKey("a.*"))
}

pub fn in_order_deliveries_catch_up_once_test() {
  use n <- qcheck.given(count_gen())
  let observe = fn(state: #(kv.Tracker, List(kv.Step)), i) {
    let #(tracker, steps) = state
    let #(tracker, new) =
      kv.observe(tracker, delivery("b", "k", 10 + i, i, n - i, option.None))
    #(tracker, list.append(steps, new))
  }
  let #(tracker, _) = kv.track(kv.watch("b", filter: ">"), "_INBOX.w")
  let #(tracker, _) = created(tracker, n)
  let #(tracker, steps) = seq(1, n) |> list.fold(#(tracker, []), observe)
  let revisions =
    list.filter_map(steps, fn(step) {
      case step {
        kv.Deliver(entry) -> Ok(entry.revision)
        _ -> Error(Nil)
      }
    })
  assert revisions == seq(11, n)
  assert list.last(steps) == Ok(kv.CaughtUp)
  assert list.count(steps, fn(step) { step == kv.CaughtUp }) == 1
  let #(_, later) =
    kv.observe(tracker, delivery("b", "k", 11 + n, n + 1, 0, option.None))
  assert list.length(later) == 1
  assert !list.contains(later, kv.CaughtUp)
}

pub fn watch_delivers_entry_contents_test() {
  let #(tracker, _) = tracker(kv.watch("b", filter: ">"))
  let #(tracker, _) = created(tracker, 0)
  let put =
    delivery("b", "a.b.c", 10, 1, 0, option.None)
    |> jetgleam.set_reply_to("$JS.ACK.KV_b.c.1.10.1.1700000000123456789.0")
  let assert #(tracker, [kv.Deliver(entry)]) = kv.observe(tracker, put)
  assert entry
    == kv.Entry(
      bucket: "b",
      key: "a.b.c",
      value: <<"v":utf8>>,
      revision: 10,
      created: timestamp.from_unix_seconds_and_nanoseconds(
        1_700_000_000,
        123_456_789,
      ),
      change: kv.Put,
    )

  let assert #(tracker, [kv.Deliver(entry)]) =
    kv.observe(tracker, delivery("b", "a.b", 11, 2, 0, option.Some("DEL")))
  assert entry.key == "a.b"
  assert entry.change == kv.Delete
  let assert #(_, [kv.Deliver(entry)]) =
    kv.observe(tracker, delivery("b", "a", 12, 3, 0, option.Some("PURGE")))
  assert entry.key == "a"
  assert entry.change == kv.Purge
}

pub fn consumer_created_empty_catches_up_test() {
  let #(tracker, _) = tracker(kv.watch("b", filter: ">"))
  let #(tracker, steps) = created(tracker, 0)
  assert steps == [kv.CaughtUp]
  assert created(tracker, 0).1 == []
}

pub fn replay_ends_despite_live_backlog_test() {
  let #(tracker, _) = tracker(kv.watch("b", filter: ">"))
  let #(tracker, _) = created(tracker, 1)
  let #(tracker, steps) =
    kv.observe(tracker, delivery("b", "k", 10, 1, 1, option.None))
  assert list.length(steps) == 2
  assert list.last(steps) == Ok(kv.CaughtUp)
  let #(_, steps) =
    kv.observe(tracker, delivery("b", "k", 11, 2, 0, option.None))
  assert !list.contains(steps, kv.CaughtUp)
}

pub fn suppressed_final_marker_ends_replay_test() {
  let #(tracker, _) = tracker(kv.keys("b"))
  let #(tracker, _) = created(tracker, 2)
  let #(tracker, steps) =
    kv.observe(tracker, delivery("b", "a", 10, 1, 5, option.None))
  assert !list.contains(steps, kv.CaughtUp)
  let #(tracker, steps) =
    kv.observe(tracker, status_100(option.Some("$JS.FC.x")))
  assert !list.contains(steps, kv.CaughtUp)
  let #(_, steps) =
    kv.observe(tracker, delivery("b", "k", 11, 2, 4, option.Some("DEL")))
  assert steps == [kv.CaughtUp]
}

fn created(tracker: kv.Tracker, pending: Int) -> #(kv.Tracker, List(kv.Step)) {
  kv.consumer_created(
    tracker,
    consumer.Info(
      stream: "KV_b",
      name: "c",
      config: consumer.ephemeral(),
      delivered: consumer.SequencePair(0, 0),
      ack_floor: consumer.SequencePair(0, 0),
      pending:,
      ack_pending: 0,
      redelivered: 0,
      waiting: 0,
    ),
  )
}

pub fn gap_recreates_from_last_sequence_test() {
  let #(tracker, _) = tracker(kv.watch("b", filter: ">"))
  let #(tracker, _) = created(tracker, 0)
  let #(tracker, _) =
    kv.observe(tracker, delivery("b", "k", 10, 1, 5, option.None))
  let assert #(tracker, [kv.Recreate]) =
    kv.observe(tracker, delivery("b", "k", 12, 3, 4, option.None))
  let #(_, operation) = kv.recreate(tracker, "_INBOX.w2")
  let sent = fake.sent(operation)
  let payload = sent.payload
  let decoder = {
    use policy <- decode.subfield(["config", "deliver_policy"], decode.string)
    use start <- decode.subfield(["config", "opt_start_seq"], decode.int)
    decode.success(#(policy, start))
  }
  assert json.parse_bits(payload, decoder) == Ok(#("by_start_sequence", 11))
  assert json.parse_bits(
      payload,
      decode.at(["config", "deliver_subject"], decode.string),
    )
    == Ok("_INBOX.w2")
}

pub fn control_messages_and_deletes_test() {
  let #(tracker, _) = kv.keys("b") |> tracker
  let assert #(_, [kv.Respond(response)]) =
    kv.observe(tracker, status_100(option.Some("$JS.FC.x")))
  assert response == jetgleam.message("$JS.FC.x", <<>>)
  let #(_, steps) =
    kv.observe(tracker, delivery("b", "k", 10, 1, 5, option.Some("DEL")))
  assert steps == []

  let stalled =
    jetgleam.set_header(
      status_100(option.None),
      "Nats-Consumer-Stalled",
      "$JS.FC.y",
    )
  let assert #(_, [kv.Respond(response)]) = kv.observe(tracker, stalled)
  assert response == jetgleam.message("$JS.FC.y", <<>>)

  let #(tracker, _) =
    kv.observe(tracker, delivery("b", "k", 10, 1, 5, option.None))
  let heartbeat = fn(last) {
    jetgleam.set_header(status_100(option.None), "Nats-Last-Consumer", last)
  }
  assert kv.observe(tracker, heartbeat("1")).1 == []
  assert kv.observe(tracker, heartbeat("5")).1 == [kv.Recreate]
}
