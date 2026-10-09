import fake
import gleam/bit_array
import gleam/dynamic/decode
import gleam/json.{type Json}
import gleam/list
import gleam/option.{type Option}
import gleam/time/duration.{type Duration}
import gleam/time/timestamp
import jetgleam
import jetgleam/stream
import qcheck

fn duration_gen() -> qcheck.Generator(Duration) {
  qcheck.bounded_int(1, 1_000_000_000_000) |> qcheck.map(duration.nanoseconds)
}

fn limit_gen() -> qcheck.Generator(Option(Int)) {
  qcheck.option_from(qcheck.bounded_int(1, 1_000_000))
}

fn config_gen() -> qcheck.Generator(stream.Config) {
  let limits =
    qcheck.tuple6(
      limit_gen(),
      limit_gen(),
      limit_gen(),
      limit_gen(),
      limit_gen(),
      qcheck.bounded_int(1, 1_000_000),
    )
  let modes =
    qcheck.tuple6(
      qcheck.from_generators(qcheck.return(stream.Limits), [
        qcheck.return(stream.Interest),
        qcheck.return(stream.WorkQueue),
      ]),
      qcheck.from_generators(qcheck.return(stream.File), [
        qcheck.return(stream.Memory),
      ]),
      qcheck.from_generators(qcheck.return(stream.Old), [
        qcheck.return(stream.New),
      ]),
      qcheck.bool(),
      qcheck.bool(),
      qcheck.bool(),
    )
  let rest =
    qcheck.tuple5(
      qcheck.bool(),
      qcheck.option_from(duration_gen()),
      qcheck.option_from(duration_gen()),
      qcheck.option_from(qcheck.non_empty_string()),
      qcheck.option_from(qcheck.map2(
        qcheck.non_empty_string(),
        qcheck.non_empty_string(),
        stream.SubjectTransform,
      )),
    )
  qcheck.map3(limits, modes, rest, fn(limits, modes, rest) {
    let #(consumers, messages, bytes, per_subject, size, replicas) = limits
    let #(retention, storage, discard, direct, rollup, deny_delete) = modes
    let #(deny_purge, max_age, window, description, transform) = rest
    stream.config("S", ["s.>", "t"])
    |> set(consumers, stream.with_max_consumers)
    |> set(messages, stream.with_max_messages)
    |> set(bytes, stream.with_max_bytes)
    |> set(per_subject, stream.with_max_messages_per_subject)
    |> set(size, stream.with_max_message_size)
    |> stream.with_replicas(replicas)
    |> stream.with_retention(retention)
    |> stream.with_storage(storage)
    |> stream.with_discard(discard)
    |> stream.with_allow_direct(direct)
    |> stream.with_allow_rollup(rollup)
    |> stream.with_deny_delete(deny_delete)
    |> stream.with_deny_purge(deny_purge)
    |> set(max_age, stream.with_max_age)
    |> set(window, stream.with_duplicate_window)
    |> set(description, stream.with_description)
    |> set(transform, stream.with_subject_transform)
  })
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

fn echo_config(request: jetgleam.Message) -> jetgleam.Message {
  let assert Ok(config) =
    json.parse_bits(request.payload, jetgleam.json_decoder())
  info_reply(config)
}

pub fn create_round_trips_config_test() {
  use config <- qcheck.given(config_gen())
  let assert Ok(info) = stream.create(config) |> fake.run(echo_config)
  assert info.config == config
  assert info.state
    == stream.State(
      messages: 3,
      bytes: 30,
      first_sequence: 1,
      last_sequence: 3,
      consumers: 2,
    )
}

pub fn create_wire_format_test() {
  let config =
    stream.config("S", ["s.>"]) |> stream.with_max_age(duration.hours(1))
  let message = fake.sent(stream.create(config))
  assert message.subject == "$JS.API.STREAM.CREATE.S"
  let int_field = fn(name) {
    json.parse_bits(message.payload, decode.at([name], decode.int))
  }
  let string_field = fn(name) {
    json.parse_bits(message.payload, decode.at([name], decode.string))
  }
  assert int_field("max_age") == Ok(3_600_000_000_000)
  assert int_field("max_msgs") == Ok(-1)
  assert string_field("retention") == Ok("limits")
  assert string_field("storage") == Ok("file")
}

pub fn info_test() {
  let message = fake.sent(stream.info("S"))
  assert message.subject == "$JS.API.STREAM.INFO.S"
  assert message.payload == <<>>

  let error = fake.error_reply(404, 10_059, "stream not found")
  assert stream.info("S") |> fake.run(fn(_) { fake.reply(error) })
    == Error(jetgleam.NotFound)
}

fn names_reply(total: Int, streams: Json) -> jetgleam.Message {
  fake.reply(json.object([#("total", json.int(total)), #("streams", streams)]))
}

pub fn names_pages_test() {
  let respond = fn(request: jetgleam.Message) {
    let assert Ok(offset) =
      json.parse_bits(request.payload, decode.at(["offset"], decode.int))
    case offset {
      0 -> names_reply(3, json.array(["A", "B"], json.string))
      _ -> names_reply(3, json.array(["C"], json.string))
    }
  }
  assert stream.names() |> fake.run(respond) == Ok(["A", "B", "C"])
  assert stream.names() |> fake.run(fn(_) { names_reply(0, json.null()) })
    == Ok([])
}

pub fn list_pages_test() {
  let info = fn(name) { info_json(json.object([#("name", json.string(name))])) }
  let respond = fn(request: jetgleam.Message) {
    let assert Ok(offset) =
      json.parse_bits(request.payload, decode.at(["offset"], decode.int))
    let streams = case offset {
      0 -> [info("A"), info("B")]
      _ -> [info("C")]
    }
    fake.reply(
      json.object([
        #("total", json.int(3)),
        #("streams", json.preprocessed_array(streams)),
      ]),
    )
  }
  let operation = stream.list()
  assert fake.sent(operation).subject == "$JS.API.STREAM.LIST"
  let assert Ok(infos) = fake.run(operation, respond)
  let expected = fn(name) {
    let assert Ok(info) =
      stream.info(name) |> fake.run(fn(_) { fake.reply(info(name)) })
    info
  }
  assert infos == list.map(["A", "B", "C"], expected)
}

pub fn get_message_test() {
  let encode = fn(text) { bit_array.base64_encode(<<text:utf8>>, True) }
  let reply =
    fake.reply(
      json.object([
        #(
          "message",
          json.object([
            #("subject", json.string("a.b")),
            #("seq", json.int(7)),
            #("hdrs", json.string(encode("NATS/1.0\r\nA: 1\r\n\r\n"))),
            #("data", json.string(encode("hi"))),
            #("time", json.string("2024-01-02T03:04:05Z")),
          ]),
        ),
      ]),
    )
  let operation = stream.get_message(stream: "S", sequence: 7)
  let request = fake.sent(operation)
  assert request.subject == "$JS.API.STREAM.MSG.GET.S"
  assert json.parse_bits(request.payload, decode.at(["seq"], decode.int))
    == Ok(7)

  let assert Ok(time) = timestamp.parse_rfc3339("2024-01-02T03:04:05Z")
  assert fake.run(operation, fn(_) { reply })
    == Ok(stream.StoredMessage(
      subject: "a.b",
      sequence: 7,
      headers: [#("A", "1")],
      payload: <<"hi":utf8>>,
      time:,
    ))
}

pub fn get_message_rejects_unsafe_sequence_test() {
  let result =
    stream.get_message(stream: "S", sequence: 9_007_199_254_740_991 + 1)
    |> fake.run(fake.no_io)
  let assert Error(jetgleam.InvalidArgument("sequence", _)) = result
}

pub fn last_message_delete_and_purge_test() {
  let request = fake.sent(stream.get_last_message(stream: "S", subject: "a.b"))
  assert request.subject == "$JS.API.STREAM.MSG.GET.S"
  assert json.parse_bits(
      request.payload,
      decode.at(["last_by_subj"], decode.string),
    )
    == Ok("a.b")

  let ok = fn(_) { fake.reply(json.object([#("success", json.bool(True))])) }
  assert fake.sent(stream.delete("S")).subject == "$JS.API.STREAM.DELETE.S"
  assert fake.sent(stream.purge("S")).subject == "$JS.API.STREAM.PURGE.S"
  assert stream.delete("S") |> fake.run(ok) == Ok(Nil)
  assert stream.purge("S") |> fake.run(ok) == Ok(Nil)
}

pub fn invalid_names_without_io_test() {
  let assert Error(jetgleam.InvalidArgument("name", _)) =
    stream.delete("a.b")
    |> fake.run(fake.no_io)
  let assert Error(jetgleam.InvalidConfig("name", _)) =
    stream.config("a.b", ["a.>"])
    |> stream.create
    |> fake.run(fake.no_io)
}

fn info_reply(config: Json) -> jetgleam.Message {
  fake.reply(info_json(config))
}

fn info_json(config: Json) -> Json {
  json.object([
    #("config", config),
    #(
      "state",
      json.object([
        #("messages", json.int(3)),
        #("bytes", json.int(30)),
        #("first_seq", json.int(1)),
        #("last_seq", json.int(3)),
        #("consumer_count", json.int(2)),
      ]),
    ),
    #("created", json.string("2024-01-02T03:04:05Z")),
  ])
}

pub fn info_rejects_unknown_enum_values_test() {
  let reply = fn(field, value) {
    stream.info("S")
    |> fake.run(fn(_) {
      info_reply(
        json.object([#("name", json.string("S")), #(field, json.string(value))]),
      )
    })
  }
  let assert Error(jetgleam.BadResponse(..)) = reply("retention", "forever")
  let assert Error(jetgleam.BadResponse(..)) = reply("storage", "tape")
  let assert Error(jetgleam.BadResponse(..)) = reply("discard", "middle")
}

pub fn config_round_trip_keeps_unknown_fields_test() {
  let raw_config =
    json.object([
      #("name", json.string("S")),
      #("subjects", json.array(["input.*"], json.string)),
      #(
        "subject_transform",
        json.object([
          #("src", json.string("input.*")),
          #("dest", json.string("stored.{{wildcard(1)}}")),
        ]),
      ),
      #("future_setting", json.object([#("enabled", json.bool(True))])),
    ])
  let assert Ok(info) =
    stream.info("S")
    |> fake.run(fn(_) { info_reply(raw_config) })
  let request =
    info.config
    |> stream.with_description("changed")
    |> stream.update
    |> fake.sent
  let known_fields = {
    use source <- decode.then(decode.at(
      ["subject_transform", "src"],
      decode.string,
    ))
    use destination <- decode.then(decode.at(
      ["subject_transform", "dest"],
      decode.string,
    ))
    use description <- decode.field("description", decode.string)
    decode.success(#(source, destination, description))
  }
  assert json.parse_bits(request.payload, known_fields)
    == Ok(#("input.*", "stored.{{wildcard(1)}}", "changed"))
  assert json.parse_bits(
      request.payload,
      decode.at(["future_setting", "enabled"], decode.bool),
    )
    == Ok(True)
}
