import fake
import gleam/bit_array
import gleam/dynamic/decode
import gleam/int
import gleam/json.{type Json}
import gleam/list
import gleam/option.{type Option}
import gleam/result
import gleam/string
import gleam/time/duration.{type Duration}
import gleam/time/timestamp
import jetgleam
import jetgleam/consumer
import qcheck

fn end_status_gen() -> qcheck.Generator(Int) {
  qcheck.from_generators(qcheck.return(404), [qcheck.return(408)])
}

fn duration_gen() -> qcheck.Generator(Duration) {
  qcheck.bounded_int(1, 1_000_000_000_000) |> qcheck.map(duration.nanoseconds)
}

fn delivery_gen() -> qcheck.Generator(consumer.Delivery) {
  qcheck.from_generators(qcheck.return(consumer.Pull), [
    qcheck.map3(
      qcheck.non_empty_string(),
      qcheck.option_from(duration_gen()),
      qcheck.bool(),
      fn(subject, heartbeat, flow_control) {
        consumer.Push(subject:, heartbeat:, flow_control:)
      },
    ),
  ])
}

fn deliver_policy_gen() -> qcheck.Generator(consumer.DeliverPolicy) {
  qcheck.from_generators(qcheck.return(consumer.DeliverAll), [
    qcheck.return(consumer.DeliverLast),
    qcheck.return(consumer.DeliverNew),
    qcheck.return(consumer.DeliverLastPerSubject),
    qcheck.bounded_int(1, 1_000_000)
      |> qcheck.map(fn(sequence) { consumer.DeliverFromSequence(sequence:) }),
    qcheck.bounded_int(0, 2_000_000_000)
      |> qcheck.map(fn(seconds) {
        consumer.DeliverFromTime(time: timestamp.from_unix_seconds(seconds))
      }),
  ])
}

fn ack_policy_gen() -> qcheck.Generator(consumer.AckPolicy) {
  qcheck.from_generators(qcheck.return(consumer.AckNone), [
    qcheck.return(consumer.AckAll),
    qcheck.return(consumer.AckExplicit),
  ])
}

fn config_gen() -> qcheck.Generator(consumer.Config) {
  let identity =
    qcheck.from_generators(qcheck.map(token_gen(), consumer.durable), [
      qcheck.map(qcheck.option_from(token_gen()), fn(name) {
        case name {
          option.Some(name) -> consumer.ephemeral() |> consumer.with_name(name)
          option.None -> consumer.ephemeral()
        }
      }),
    ])
  let policies =
    qcheck.tuple4(
      delivery_gen(),
      deliver_policy_gen(),
      ack_policy_gen(),
      qcheck.from_generators(qcheck.return(consumer.ReplayInstant), [
        qcheck.return(consumer.ReplayOriginal),
      ]),
    )
  let numbers =
    qcheck.tuple6(
      duration_gen(),
      qcheck.option_from(qcheck.bounded_int(1, 100)),
      qcheck.bounded_int(1, 100_000),
      qcheck.bounded_int(1, 100_000),
      qcheck.bounded_int(0, 5),
      duration_gen(),
    )
  let rest =
    qcheck.tuple3(
      qcheck.bool(),
      qcheck.generic_list(qcheck.non_empty_string(), qcheck.bounded_int(0, 3)),
      qcheck.option_from(qcheck.non_empty_string()),
    )
  qcheck.map4(
    identity,
    policies,
    numbers,
    rest,
    fn(base, policies, numbers, rest) {
      let #(delivery, deliver_policy, ack_policy, replay) = policies
      let #(ack_wait, max_deliver, max_ack_pending, max_waiting, replicas, idle) =
        numbers
      let #(headers_only, filters, description) = rest
      let config =
        base
        |> consumer.with_delivery(delivery)
        |> consumer.with_deliver_policy(deliver_policy)
        |> consumer.with_ack_policy(ack_policy)
        |> consumer.with_replay_policy(replay)
        |> consumer.with_ack_wait(ack_wait)
        |> consumer.with_replicas(replicas)
        |> consumer.with_inactive_threshold(idle)
        |> consumer.with_headers_only(headers_only)
        |> consumer.with_filter_subjects(filters)
      let config = case max_deliver {
        option.Some(max) -> consumer.with_max_deliver(config, max)
        option.None -> config
      }
      let config = case ack_policy {
        consumer.AckNone -> config
        _ -> consumer.with_max_ack_pending(config, max_ack_pending)
      }
      let config = case delivery {
        consumer.Pull -> consumer.with_max_waiting(config, max_waiting)
        consumer.Push(..) -> config
      }
      case description {
        option.Some(text) -> consumer.with_description(config, text)
        option.None -> config
      }
    },
  )
}

fn token_gen() -> qcheck.Generator(String) {
  qcheck.non_empty_string_from(qcheck.alphanumeric_ascii_codepoint())
}

fn counter_gen() -> qcheck.Generator(Int) {
  qcheck.bounded_int(0, 9_007_199_254_740_991)
}

fn fetch_gen() -> qcheck.Generator(#(Int, Int)) {
  qcheck.map2(qcheck.bounded_int(1, 20), qcheck.bounded_int(0, 19), fn(max, k) {
    #(max, k % max)
  })
}

fn time_gen() -> qcheck.Generator(#(Int, Int)) {
  qcheck.map2(
    qcheck.bounded_int(0, 4_102_444_800),
    qcheck.bounded_int(0, 999_999_999),
    fn(seconds, nanoseconds) { #(seconds, nanoseconds) },
  )
}

fn metadata_gen() -> qcheck.Generator(
  #(#(String, String), #(Int, Int, Int), #(#(Int, Int), Option(String))),
) {
  qcheck.map3(
    qcheck.map2(token_gen(), token_gen(), fn(stream, consumer) {
      #(stream, consumer)
    }),
    qcheck.map3(counter_gen(), counter_gen(), counter_gen(), fn(a, b, c) {
      #(a, b, c)
    }),
    qcheck.map2(time_gen(), qcheck.option_from(token_gen()), fn(a, b) {
      #(a, b)
    }),
    fn(names, counts, rest) { #(names, counts, rest) },
  )
}

fn info_reply(config: Json) -> jetgleam.Message {
  fake.reply(info_json("w", config))
}

fn info_json(name: String, config: Json) -> Json {
  let pair = fn(consumer, stream) {
    json.object([
      #("consumer_seq", json.int(consumer)),
      #("stream_seq", json.int(stream)),
    ])
  }
  json.object([
    #("stream_name", json.string("S")),
    #("name", json.string(name)),
    #("config", config),
    #("delivered", pair(4, 9)),
    #("ack_floor", pair(3, 8)),
    #("num_pending", json.int(10)),
    #("num_ack_pending", json.int(2)),
    #("num_redelivered", json.int(1)),
    #("num_waiting", json.int(5)),
  ])
}

fn echo_config(request: jetgleam.Message) -> jetgleam.Message {
  let assert Ok(config) =
    json.parse_bits(
      request.payload,
      decode.at(["config"], jetgleam.json_decoder()),
    )
  info_reply(config)
}

pub fn create_round_trips_config_test() {
  use config <- qcheck.given(config_gen())
  let assert Ok(info) =
    consumer.create(config, stream: "S") |> fake.run(echo_config)
  assert info.config == config
}

pub fn create_wire_format_test() {
  let named =
    fake.sent(
      consumer.durable("w")
      |> consumer.with_name("other")
      |> consumer.with_max_deliver(5)
      |> consumer.create(stream: "S"),
    )
  assert named.subject == "$JS.API.CONSUMER.CREATE.S.w"
  let text = fn(path) {
    json.parse_bits(named.payload, decode.at(path, decode.string))
  }
  let number = fn(path) {
    json.parse_bits(named.payload, decode.at(path, decode.int))
  }
  assert text(["action"]) == Ok("create")
  assert text(["stream_name"]) == Ok("S")
  assert text(["config", "durable_name"]) == Ok("w")
  assert number(["config", "ack_wait"]) == Ok(30_000_000_000)
  assert number(["config", "max_deliver"]) == Ok(5)

  let unnamed = fake.sent(consumer.ephemeral() |> consumer.create(stream: "S"))
  assert unnamed.subject == "$JS.API.CONSUMER.CREATE.S"
  assert json.parse_bits(
      unnamed.payload,
      decode.at(["config", "inactive_threshold"], decode.int),
    )
    == Ok(300_000_000_000)

  let none =
    fake.sent(
      consumer.durable("w")
      |> consumer.with_ack_policy(consumer.AckNone)
      |> consumer.create(stream: "S"),
    )
  let max_ack_pending = {
    use value <- decode.optional_field(
      "max_ack_pending",
      option.None,
      decode.optional(decode.int),
    )
    decode.success(value)
  }
  assert json.parse_bits(none.payload, decode.at(["config"], max_ack_pending))
    == Ok(option.None)
}

pub fn info_and_delete_test() {
  let request = fake.sent(consumer.info(stream: "S", consumer: "w"))
  assert request.subject == "$JS.API.CONSUMER.INFO.S.w"

  let config =
    json.object([
      #("name", json.string("w")),
      #("durable_name", json.string("w")),
    ])
  assert consumer.info(stream: "S", consumer: "w")
    |> fake.run(fn(_) { info_reply(config) })
    == Ok(consumer.Info(
      stream: "S",
      name: "w",
      config: consumer.durable("w"),
      delivered: consumer.SequencePair(consumer: 4, stream: 9),
      ack_floor: consumer.SequencePair(consumer: 3, stream: 8),
      pending: 10,
      ack_pending: 2,
      redelivered: 1,
      waiting: 5,
    ))

  let delete = consumer.delete(stream: "S", consumer: "w")
  assert fake.sent(delete).subject == "$JS.API.CONSUMER.DELETE.S.w"
  assert fake.run(delete, fn(_) {
      fake.reply(json.object([#("success", json.bool(True))]))
    })
    == Ok(Nil)
}

pub fn names_pages_test() {
  let page = fn(names) {
    fake.reply(
      json.object([
        #("total", json.int(3)),
        #("consumers", json.array(names, json.string)),
      ]),
    )
  }
  let respond = fn(request: jetgleam.Message) {
    let assert Ok(offset) =
      json.parse_bits(request.payload, decode.at(["offset"], decode.int))
    case offset {
      0 -> page(["a", "b"])
      _ -> page(["c"])
    }
  }
  let operation = consumer.names("S")
  assert fake.sent(operation).subject == "$JS.API.CONSUMER.NAMES.S"
  assert fake.run(operation, respond) == Ok(["a", "b", "c"])
}

pub fn list_pages_test() {
  let info = fn(name) { info_json(name, json.object([])) }
  let respond = fn(request: jetgleam.Message) {
    let assert Ok(offset) =
      json.parse_bits(request.payload, decode.at(["offset"], decode.int))
    let consumers = case offset {
      0 -> [info("a"), info("b")]
      _ -> [info("c")]
    }
    fake.reply(
      json.object([
        #("total", json.int(3)),
        #("consumers", json.preprocessed_array(consumers)),
      ]),
    )
  }
  let operation = consumer.list("S")
  assert fake.sent(operation).subject == "$JS.API.CONSUMER.LIST.S"
  let assert Ok(infos) = fake.run(operation, respond)
  assert list.map(infos, fn(info) { info.name }) == ["a", "b", "c"]
}

fn delivery(index: Int) -> jetgleam.Message {
  jetgleam.message("orders", <<index>>)
}

fn deliveries(count: Int) -> List(jetgleam.Message) {
  list.repeat(Nil, count)
  |> list.index_map(fn(_, index) { delivery(index + 1) })
}

fn status(code: Int, description: String) -> jetgleam.Message {
  jetgleam.Message(
    ..jetgleam.message("_INBOX.f", <<>>),
    status: option.Some(jetgleam.Status(code, option.Some(description))),
  )
}

fn heartbeat() -> jetgleam.Message {
  status(100, "Idle Heartbeat")
}

fn feed(
  fetching: consumer.Fetching,
  messages: List(jetgleam.Message),
) -> consumer.FetchStep {
  let assert [message, ..rest] = messages
  case consumer.fetch_received(fetching, message), rest {
    consumer.Continue(next), [_, ..] -> feed(next, rest)
    step, _ -> step
  }
}

fn start(max: Int) -> consumer.Fetching {
  let assert Ok(#(fetching, _, _)) =
    consumer.fetch(
      stream: "S",
      consumer: "C",
      max:,
      wait: duration.milliseconds(1000),
    )
    |> consumer.start_fetch("_INBOX.f")
  fetching
}

pub fn fetch_completes_on_max_test() {
  use #(max, _) <- qcheck.given(fetch_gen())
  let interleaved = fn(count) {
    deliveries(count)
    |> list.flat_map(fn(message) { [heartbeat(), message] })
  }
  let all = deliveries(max)
  assert feed(start(max), interleaved(max)) == consumer.Complete(all)
  case max {
    1 -> Nil
    _ -> {
      assert case feed(start(max), interleaved(max - 1)) {
        consumer.Continue(_) -> True
        _ -> False
      }
    }
  }
}

pub fn fetch_completes_on_404_or_408_test() {
  use #(#(max, k), code) <- qcheck.given(qcheck.tuple2(
    fetch_gen(),
    end_status_gen(),
  ))
  let received = deliveries(k)
  let messages = list.append(received, [status(code, "Done")])
  assert feed(start(max), messages) == consumer.Complete(received)
}

pub fn start_fetch_request_test() {
  let assert Ok(#(fetching, message, wait)) =
    consumer.fetch(
      stream: "S",
      consumer: "C",
      max: 5,
      wait: duration.milliseconds(2000),
    )
    |> consumer.with_domain("hub")
    |> consumer.with_heartbeat(duration.milliseconds(500))
    |> consumer.start_fetch("_INBOX.f")
  assert message.subject == "$JS.hub.API.CONSUMER.MSG.NEXT.S.C"
  assert message.reply_to == option.Some("_INBOX.f")
  assert wait == 2000
  let field = fn(name, decoder) {
    json.parse_bits(message.payload, decode.at([name], decoder))
  }
  assert field("batch", decode.int) == Ok(5)
  assert field("expires", decode.int) == Ok(1_800_000_000)
  assert field("idle_heartbeat", decode.int) == Ok(500_000_000)

  let without = {
    let assert Ok(#(_, message, _)) =
      consumer.fetch(
        stream: "S",
        consumer: "C",
        max: 5,
        wait: duration.milliseconds(2000),
      )
      |> consumer.start_fetch("_INBOX.f")
    message
  }
  assert without.subject == "$JS.API.CONSUMER.MSG.NEXT.S.C"
  assert json.parse_bits(
      without.payload,
      decode.at(["idle_heartbeat"], decode.int),
    )
    |> result.is_error

  let assert consumer.Continue(fetching) =
    consumer.fetch_received(fetching, delivery(1))
  assert consumer.fetch_messages(fetching) == [delivery(1)]
  assert consumer.fetch_received(fetching, status(409, "Consumer Deleted"))
    == consumer.Stop([delivery(1)], jetgleam.Api(409, 0, "Consumer Deleted"))
}

pub fn fetch_expiry_and_limits_test() {
  let start = fn(max, wait, heartbeat) {
    let fetch =
      consumer.fetch(
        stream: "S",
        consumer: "C",
        max:,
        wait: duration.milliseconds(wait),
      )
    case heartbeat {
      option.Some(heartbeat) ->
        consumer.with_heartbeat(fetch, duration.milliseconds(heartbeat))
      option.None -> fetch
    }
    |> consumer.start_fetch("_INBOX.f")
  }
  let expires = fn(wait) {
    let assert Ok(#(_, message, _)) = start(1, wait, option.None)
    let assert Ok(expires) =
      json.parse_bits(message.payload, decode.at(["expires"], decode.int))
    expires
  }
  assert expires(1) == 900_000
  assert expires(2) == 1_800_000
  assert expires(2_147_483_647) == 1_932_735_282_300_000

  let assert Ok(_) = start(1, 1000, option.Some(450))
  let assert Ok(_) = start(9_007_199_254_740_991, 1000, option.Some(1))
  let assert Error(jetgleam.InvalidConfig("heartbeat", _)) =
    start(1, 1000, option.Some(451))
  let assert Error(jetgleam.InvalidConfig("heartbeat", _)) =
    start(1, 1000, option.Some(0))
  let assert Error(jetgleam.InvalidArgument("max", _)) =
    start(0, 1000, option.None)
  let assert Error(jetgleam.InvalidArgument("wait", _)) =
    start(1, 0, option.None)
  let assert Error(jetgleam.InvalidArgument("wait", _)) =
    start(1, 2_147_483_648, option.None)
}

pub fn metadata_parses_reply_subject_test() {
  use
    #(
      #(stream, name),
      #(delivered, stream_seq, consumer_seq),
      #(#(seconds, nanoseconds), domain),
    )
  <- qcheck.given(metadata_gen())
  let time =
    int.to_string(seconds)
    <> string.pad_start(int.to_string(nanoseconds), 9, "0")
  let numbers = [
    int.to_string(delivered),
    int.to_string(stream_seq),
    int.to_string(consumer_seq),
    time,
    "7",
  ]
  let message = fn(subject) {
    jetgleam.message("orders", <<>>) |> jetgleam.set_reply_to(subject)
  }
  let expected = fn(domain) {
    Ok(consumer.Metadata(
      domain:,
      stream:,
      consumer: name,
      delivered:,
      stream_sequence: stream_seq,
      consumer_sequence: consumer_seq,
      time: timestamp.from_unix_seconds_and_nanoseconds(seconds, nanoseconds),
      pending: 7,
    ))
  }
  let v1 = ["$JS", "ACK", stream, name, ..numbers] |> string.join(".")
  assert consumer.metadata(message(v1)) == expected(option.None)
  let v2 =
    ["$JS", "ACK", option.unwrap(domain, "_"), "acct", stream, name, ..numbers]
    |> string.join(".")
  assert consumer.metadata(message(v2)) == expected(domain)
  assert consumer.metadata(jetgleam.message("orders", <<>>)) == Error(Nil)
}

pub fn metadata_numbers_are_exact_test() {
  let parse = fn(numbers) {
    jetgleam.message("orders", <<>>)
    |> jetgleam.set_reply_to("$JS.ACK.S.C." <> numbers)
    |> consumer.metadata
  }
  let time = fn(numbers) {
    let assert Ok(meta) = parse(numbers)
    timestamp.to_unix_seconds_and_nanoseconds(meta.time)
  }
  assert time("1.2.3.1700000000123456789.0") == #(1_700_000_000, 123_456_789)
  assert time("1.2.3.1700000000000000007.0") == #(1_700_000_000, 7)
  assert time("1.2.3.42.0") == #(0, 42)
  let assert Ok(meta) = parse("1.9007199254740991.3.0.0")
  assert meta.stream_sequence == 9_007_199_254_740_991

  use numbers <- list.each([
    "9007199254740992.2.3.0.0",
    "1.9007199254740993.3.0.0",
    "1.2.9007199254740993.0.0",
    "1.2.3.0.9007199254740993",
    "1.2.3.9007199254740993000000000.0",
    "-1.2.3.0.0",
    "1.2.3.-5.0",
    "1.2.3.x.0",
    "1.2.3..0",
  ])
  assert parse(numbers) == Error(Nil)
}

pub fn start_sequence_must_be_safe_test() {
  let from = fn(sequence) {
    consumer.durable("w")
    |> consumer.with_deliver_policy(consumer.DeliverFromSequence(sequence:))
    |> consumer.create(stream: "S")
  }
  let sent = fake.sent(from(9_007_199_254_740_991))
  assert json.parse_bits(
      sent.payload,
      decode.at(["config", "opt_start_seq"], decode.int),
    )
    == Ok(9_007_199_254_740_991)
  let assert Error(jetgleam.InvalidConfig("sequence", _)) =
    from(9_007_199_254_740_991 + 1)
    |> fake.run(fake.no_io)

  let unsafe =
    json.object([
      #("durable_name", json.string("w")),
      #("deliver_policy", json.string("by_start_sequence")),
      #("opt_start_seq", json.int(9_007_199_254_740_991 + 1)),
    ])
  let assert Error(jetgleam.BadResponse(..)) =
    consumer.info(stream: "S", consumer: "w")
    |> fake.run(fn(_) { info_reply(unsafe) })
}

pub fn info_rejects_unknown_enum_values_test() {
  let reply = fn(fields) {
    consumer.info(stream: "S", consumer: "w")
    |> fake.run(fn(_) {
      info_reply(json.object([#("durable_name", json.string("w")), ..fields]))
    })
  }
  let assert Error(jetgleam.BadResponse(..)) =
    reply([#("deliver_policy", json.string("by_magic"))])
  let assert Error(jetgleam.BadResponse(..)) =
    reply([#("deliver_policy", json.string("by_start_time"))])
  let assert Error(jetgleam.BadResponse(..)) =
    reply([#("deliver_policy", json.string("by_start_sequence"))])
  let assert Error(jetgleam.BadResponse(..)) =
    reply([#("ack_policy", json.string("sometimes"))])
  let assert Error(jetgleam.BadResponse(..)) =
    reply([#("replay_policy", json.string("rewind"))])
  let assert Ok(_) = reply([])
}

pub fn ack_test() {
  let msg =
    jetgleam.message("orders", <<>>)
    |> jetgleam.set_reply_to("$JS.ACK.S.C.1.2.3.4.5")
  let body = fn(kind) {
    let sent = fake.sent(consumer.ack(msg, kind))
    assert sent.subject == "$JS.ACK.S.C.1.2.3.4.5"
    let assert Ok(text) = bit_array.to_string(sent.payload)
    text
  }
  assert body(consumer.Ack) == "+ACK"
  assert body(consumer.Nak) == "-NAK"
  assert body(consumer.Term) == "+TERM"
  assert body(consumer.InProgress) == "+WPI"
  let assert "-NAK " <> delay = body(consumer.NakAfter(duration.seconds(2)))
  assert json.parse(delay, decode.at(["delay"], decode.int))
    == Ok(2_000_000_000)

  assert fake.run(consumer.ack(msg, consumer.Ack), fn(_) {
      panic as "publish only"
    })
    == Ok(Nil)
  let plain = jetgleam.message("orders", <<>>)
  assert fake.run(consumer.ack(plain, consumer.Ack), fn(_) { panic })
    == Error(jetgleam.NotAcknowledgeable)

  let sync = consumer.ack_sync(msg, consumer.Term)
  assert fake.sent(sync).payload == <<"+TERM":utf8>>
  assert fake.run(sync, fn(_) { jetgleam.message("_INBOX.reply", <<>>) })
    == Ok(Nil)
  assert fake.run(consumer.ack_sync(plain, consumer.Ack), fn(_) { panic })
    == Error(jetgleam.NotAcknowledgeable)
}

pub fn invalid_names_without_io_test() {
  let assert Error(jetgleam.InvalidConfig("name", _)) =
    consumer.durable("a.b")
    |> consumer.create(stream: "S")
    |> fake.run(fake.no_io)
  let assert Error(jetgleam.InvalidArgument("stream", _)) =
    consumer.names("a.b")
    |> fake.run(fake.no_io)
  let fetch = fn(stream, consumer) {
    consumer.fetch(
      stream:,
      consumer:,
      max: 1,
      wait: duration.milliseconds(1000),
    )
    |> consumer.start_fetch("_INBOX.f")
  }
  let assert Error(jetgleam.InvalidArgument("stream", _)) = fetch("a.b", "C")
  let assert Error(jetgleam.InvalidArgument("consumer", _)) = fetch("S", "a.b")
}
