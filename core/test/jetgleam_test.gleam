import fake
import gleam/int
import gleam/json.{type Json}
import gleam/list
import gleam/option
import jetgleam
import qcheck
import unitest

fn name() -> qcheck.Generator(String) {
  qcheck.bounded_int(0, 2) |> qcheck.map(int.to_string)
}

fn value() -> qcheck.Generator(String) {
  qcheck.string_from(qcheck.alphanumeric_ascii_codepoint())
}

fn message() -> qcheck.Generator(jetgleam.Message) {
  qcheck.list_from(qcheck.tuple2(name(), value()))
  |> qcheck.map(fn(headers) {
    list.fold(headers, jetgleam.message("a", <<>>), fn(m, h) {
      jetgleam.add_header(m, h.0, h.1)
    })
  })
}

pub fn main() -> Nil {
  unitest.main()
}

pub fn set_header_test() {
  use #(m, n, v) <- qcheck.given(qcheck.tuple3(message(), name(), value()))
  let m = jetgleam.set_header(m, n, v)
  assert jetgleam.get_headers(m, n) == [v]
  assert jetgleam.get_header(m, n) == Ok(v)
}

pub fn add_header_test() {
  use #(m, n, v) <- qcheck.given(qcheck.tuple3(message(), name(), value()))
  assert jetgleam.get_headers(jetgleam.add_header(m, n, v), n)
    == list.append(jetgleam.get_headers(m, n), [v])
}

pub fn remove_header_test() {
  use #(m, n) <- qcheck.given(qcheck.tuple2(message(), name()))
  let removed = jetgleam.remove_header(m, n)
  assert jetgleam.get_headers(removed, n) == []
  assert removed.headers == list.filter(m.headers, fn(h) { h.0 != n })
}

pub fn run_follows_operation_in_order_test() {
  let operation =
    jetgleam.request(jetgleam.message("ping", <<>>), fn(reply) {
      jetgleam.send(
        jetgleam.message("out", reply.payload),
        jetgleam.done(Ok(1)),
      )
    })
  let result =
    jetgleam.run(
      operation,
      publish: fn(message) {
        assert message == jetgleam.message("out", <<"pong">>)
        Ok(Nil)
      },
      request: fn(_, _) { Ok(jetgleam.message("r", <<"pong">>)) },
      error: fn(error) { error },
    )
  assert result == Ok(1)
}

pub fn run_maps_done_error_test() {
  let result =
    jetgleam.run(
      jetgleam.done(Error(jetgleam.NotFound)),
      publish: fn(_) { Ok(Nil) },
      request: fn(_, _) { Ok(jetgleam.message("r", <<>>)) },
      error: fn(_) { "mapped" },
    )
  assert result == Error("mapped")
}

pub fn with_timeout_applies_to_chained_requests_test() {
  let operation =
    jetgleam.request(jetgleam.message("one", <<>>), fn(_) {
      jetgleam.request(jetgleam.message("two", <<>>), fn(_) {
        jetgleam.done(Ok(Nil))
      })
    })
    |> jetgleam.with_timeout(7)
  let result =
    jetgleam.run(
      operation,
      publish: fn(_) { Ok(Nil) },
      request: fn(message, timeout) {
        case message.subject {
          "one" -> Ok(jetgleam.message("r", <<>>))
          _ -> Error(timeout)
        }
      },
      error: fn(_) { -1 },
    )
  assert result == Error(7)
}

pub fn with_domain_rewrites_api_subjects_only_test() {
  let operation =
    jetgleam.request(jetgleam.message("orders.new", <<>>), fn(_) {
      jetgleam.request(jetgleam.message("$JS.API.STREAM.INFO.X", <<>>), fn(_) {
        jetgleam.done(Ok(Nil))
      })
    })
    |> jetgleam.with_domain("hub")
  let result =
    fake.run(operation, fn(message) {
      assert list.contains(
        ["orders.new", "$JS.hub.API.STREAM.INFO.X"],
        message.subject,
      )
      jetgleam.message("r", <<>>)
    })
  assert result == Ok(Nil)

  let ack =
    jetgleam.send(jetgleam.message("$JS.ACK.a", <<>>), jetgleam.done(Ok(Nil)))
    |> jetgleam.with_domain("hub")
  assert fake.sent(ack).subject == "$JS.ACK.a"
}

fn publish_result(body: Json) -> Result(jetgleam.PubAck, jetgleam.Error) {
  fake.run(jetgleam.publish(jetgleam.message("a", <<>>)), fn(_) {
    fake.reply(body)
  })
}

pub fn publish_decodes_ack_test() {
  assert publish_result(
      json.object([#("stream", json.string("S")), #("seq", json.int(7))]),
    )
    == Ok(jetgleam.PubAck("S", 7, False))

  use #(seq, duplicate) <- qcheck.given(qcheck.tuple2(
    qcheck.bounded_int(0, 1_000_000),
    qcheck.bool(),
  ))
  let body =
    json.object([
      #("stream", json.string("S")),
      #("seq", json.int(seq)),
      #("duplicate", json.bool(duplicate)),
    ])
  assert publish_result(body) == Ok(jetgleam.PubAck("S", seq, duplicate))
}

pub fn publish_maps_server_errors_test() {
  [
    #(fake.error_reply(404, 10_059, "stream not found"), jetgleam.NotFound),
    #(fake.error_reply(404, 10_014, "consumer not found"), jetgleam.NotFound),
    #(fake.error_reply(404, 10_037, "no message found"), jetgleam.NotFound),
    #(fake.error_reply(404, 0, "x"), jetgleam.NotFound),
    #(fake.error_reply(400, 10_058, "in use"), jetgleam.AlreadyExists),
    #(fake.error_reply(400, 10_013, "consumer exists"), jetgleam.AlreadyExists),
    #(fake.error_reply(400, 10_148, "consumer exists"), jetgleam.AlreadyExists),
    #(
      fake.error_reply(400, 10_071, "wrong last sequence: 5"),
      jetgleam.WrongLastSequence(option.Some(5)),
    ),
    #(
      fake.error_reply(400, 10_071, "wrong last sequence: x"),
      jetgleam.WrongLastSequence(option.None),
    ),
    #(
      fake.error_reply(400, 10_071, "wrong last sequence: 9007199254740992"),
      jetgleam.WrongLastSequence(option.None),
    ),
    #(fake.error_reply(500, 10_999, "boom"), jetgleam.Api(500, 10_999, "boom")),
  ]
  |> list.each(fn(c) {
    assert publish_result(c.0) == Error(c.1)
  })
}

pub fn publish_bad_and_status_replies_test() {
  let assert Error(jetgleam.BadResponse(_)) =
    publish_result(json.object([#("nope", json.int(1))]))
  let assert Error(jetgleam.BadResponse(_)) =
    publish_result(
      json.object([
        #("stream", json.string("S")),
        #("seq", json.int(9_007_199_254_740_991 + 1)),
      ]),
    )
  let status =
    jetgleam.Message(
      ..jetgleam.message("r", <<>>),
      status: option.Some(jetgleam.Status(503, option.None)),
    )
  assert fake.run(jetgleam.publish(jetgleam.message("a", <<>>)), fn(_) {
      status
    })
    == Error(jetgleam.Api(503, 0, ""))
}

pub fn api_subject_test() {
  assert jetgleam.api_subject("CONSUMER.INFO", [
      jetgleam.Argument("stream", "S"),
      jetgleam.Argument("consumer", "c-1"),
    ])
    == Ok("CONSUMER.INFO.S.c-1")
  assert jetgleam.api_subject("STREAM.NAMES", []) == Ok("STREAM.NAMES")

  use name <- list.each(["", "a.b", "a*", "a>", "a b", "a\tb", "a\r\nb"])
  let assert Error(jetgleam.InvalidArgument("consumer", _)) =
    jetgleam.api_subject("CONSUMER.INFO", [
      jetgleam.Argument("stream", "S"),
      jetgleam.Argument("consumer", name),
    ])
  let assert Error(jetgleam.InvalidConfig("name", _)) =
    jetgleam.api_subject("STREAM.CREATE", [jetgleam.Setting("name", name)])
}

pub fn json_decoder_test() {
  let value =
    json.preprocessed_array([
      json.string("a"),
      json.int(1),
      json.float(1.5),
      json.bool(True),
      json.null(),
      json.object([#("l", json.preprocessed_array([json.int(1)]))]),
    ])
  let text = json.to_string(value)
  let assert Ok(decoded) = json.parse(text, jetgleam.json_decoder())
  assert json.to_string(decoded) == text
}
