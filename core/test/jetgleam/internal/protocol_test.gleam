import gleam/bit_array
import gleam/bytes_tree
import gleam/dynamic/decode
import gleam/int
import gleam/json.{type Json}
import gleam/list
import gleam/option.{type Option}
import gleam/result
import gleam/set
import gleam/string
import jetgleam
import jetgleam/internal/nkey
import jetgleam/internal/protocol
import qcheck

fn buffer_size() -> qcheck.Generator(Int) {
  qcheck.bounded_int(0, 120)
}

fn payload_sizes() -> qcheck.Generator(List(Int)) {
  qcheck.list_from(qcheck.bounded_int(0, 20))
}

fn inbox_count() -> qcheck.Generator(Int) {
  qcheck.bounded_int(1, 50)
}

fn failure_count() -> qcheck.Generator(Int) {
  qcheck.bounded_int(1, 10)
}

fn backoff_settings() -> qcheck.Generator(#(Int, Int)) {
  qcheck.bounded_int(1, 1000)
  |> qcheck.bind(fn(initial) {
    qcheck.map2(
      qcheck.return(initial),
      qcheck.bounded_int(initial, 10_000),
      fn(initial, max) { #(initial, max) },
    )
  })
}

fn token() -> qcheck.Generator(String) {
  qcheck.non_empty_string_from(qcheck.alphanumeric_ascii_codepoint())
}

fn subject() -> qcheck.Generator(String) {
  qcheck.map2(token(), qcheck.list_from(token()), fn(first, rest) {
    string.join([first, ..rest], ".")
  })
}

fn header() -> qcheck.Generator(#(String, String)) {
  qcheck.tuple2(
    token(),
    qcheck.string_from(qcheck.alphanumeric_ascii_codepoint()),
  )
}

fn message() -> qcheck.Generator(jetgleam.Message) {
  qcheck.map3(
    subject(),
    qcheck.list_from(header()),
    qcheck.byte_aligned_bit_array(),
    fn(subject, headers, payload) {
      jetgleam.Message(..jetgleam.message(subject, payload), headers: headers)
    },
  )
}

fn status() -> qcheck.Generator(Option(jetgleam.Status)) {
  qcheck.option_from(qcheck.map2(
    qcheck.bounded_int(100, 999),
    qcheck.option_from(token()),
    jetgleam.Status,
  ))
}

fn server_message() -> qcheck.Generator(jetgleam.Message) {
  qcheck.map4(
    qcheck.tuple2(subject(), qcheck.option_from(subject())),
    qcheck.list_from(header()),
    status(),
    qcheck.byte_aligned_bit_array(),
    fn(addressing, headers, status, payload) {
      jetgleam.Message(
        subject: addressing.0,
        reply_to: addressing.1,
        headers:,
        status:,
        payload:,
      )
    },
  )
}

fn config(url: String) -> protocol.Config {
  let assert Ok(config) = protocol.config(url)
  config
}

fn entropy() -> BitArray {
  <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16>>
}

fn opened(
  effects: List(protocol.Effect),
) -> #(protocol.Generation, protocol.Server, Int) {
  let assert [protocol.Open(generation, server, after), ..] = effects
  #(generation, server, after)
}

fn timers(effects: List(protocol.Effect)) -> List(#(protocol.Timer, Int)) {
  list.filter_map(effects, fn(effect) {
    case effect {
      protocol.StartTimer(timer, after) -> Ok(#(timer, after))
      _ -> Error(Nil)
    }
  })
}

fn connect(url: String) -> #(protocol.Connection, List(protocol.Effect)) {
  let conn = config(url) |> protocol.new(entropy())
  protocol.open(conn)
}

pub fn open_parses_urls_test() {
  let #(_, effects) = connect("nats://h:1234")
  let #(_, server, after) = opened(effects)
  assert server == protocol.Server("h", 1234)
  assert after == 0
  assert timers(effects) == [#(protocol.Handshake, 2000)]

  let conn =
    protocol.Config(..config("h"), connect_timeout: 500)
    |> protocol.new(entropy())
  assert timers(protocol.open(conn).1) == [#(protocol.Handshake, 500)]

  let #(_, effects) = connect("h")
  assert opened(effects).1 == protocol.Server("h", 4222)

  let #(_, effects) = connect("h:5")
  assert opened(effects).1 == protocol.Server("h", 5)
}

pub fn config_rejects_bad_urls_test() {
  let invalid_url = fn(url) {
    let assert Error(protocol.InvalidUrl(url: u, reason: _)) =
      protocol.config(url)
    assert u == url
    let assert Error(protocol.InvalidUrl(url: u, reason: _)) =
      config("h") |> protocol.add_server(url)
    assert u == url
  }
  invalid_url("tls://h")
  invalid_url("nats://u%zz:p@h")
  invalid_url("nats://u:p%@h")
  invalid_url("nats://tok%4@h")
  invalid_url("nats://:4222")
  invalid_url("nats://h:0")
}

pub fn new_starts_connecting_test() {
  let conn = config("h") |> protocol.new(entropy())
  assert conn.status == protocol.Connecting
  assert conn.info == option.None
}

pub fn new_inbox_is_unique_test() {
  use n <- qcheck.given(inbox_count())
  let conn = config("h") |> protocol.new(entropy())
  let #(_, subjects) =
    list.fold(list.repeat(Nil, n), #(conn, []), fn(acc, _) {
      let #(conn, subjects) = acc
      let #(conn, subject) = protocol.new_inbox(conn)
      #(conn, [subject, ..subjects])
    })
  assert set.size(set.from_list(subjects)) == n
  assert list.all(subjects, string.starts_with(_, "_INBOX."))
}

fn sent(effects: List(protocol.Effect)) -> String {
  list.filter_map(effects, fn(effect) {
    case effect {
      protocol.Transmit(bytes) -> Ok(bytes_tree.to_bit_array(bytes))
      _ -> Error(Nil)
    }
  })
  |> bit_array.concat
  |> bit_array.to_string
  |> result.unwrap("")
}

fn emitted(effects: List(protocol.Effect)) -> List(protocol.Event) {
  list.filter_map(effects, fn(effect) {
    case effect {
      protocol.Emit(event) -> Ok(event)
      _ -> Error(Nil)
    }
  })
}

fn next_open(
  effects: List(protocol.Effect),
) -> #(protocol.Generation, protocol.Server, Int) {
  let assert Ok(open) =
    list.find_map(effects, fn(effect) {
      case effect {
        protocol.Open(generation, server, after) ->
          Ok(#(generation, server, after))
        _ -> Error(Nil)
      }
    })
  open
}

fn info_line(fields: List(#(String, Json))) -> BitArray {
  let defaults = [
    #("server_id", json.string("id")),
    #("server_name", json.string("srv")),
    #("version", json.string("2.10.18")),
    #("proto", json.int(1)),
    #("host", json.string("h")),
    #("port", json.int(4222)),
    #("max_payload", json.int(1_048_576)),
    #("headers", json.bool(True)),
    #("jetstream", json.bool(True)),
    #("client_id", json.int(7)),
  ]
  let body =
    defaults
    |> list.filter(fn(field) { list.key_find(fields, field.0) == Error(Nil) })
    |> list.append(fields)
    |> json.object
  bit_array.from_string("INFO " <> json.to_string(body) <> "\r\n")
}

fn expected_info(nonce: Option(String)) -> protocol.ServerInfo {
  protocol.ServerInfo(
    max_payload: 1_048_576,
    headers: True,
    tls_required: False,
    nonce:,
    connect_urls: [],
  )
}

fn connect_line(
  echo_on: Bool,
  name: Option(String),
  auth: List(#(String, Json)),
) -> String {
  let name = case name {
    option.Some(name) -> [#("name", json.string(name))]
    option.None -> []
  }
  let body =
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
          #("echo", json.bool(echo_on)),
          #("headers", json.bool(True)),
          #("no_responders", json.bool(True)),
        ],
        auth,
      ]),
    )
  "CONNECT " <> json.to_string(body) <> "\r\nPING\r\n"
}

fn handshaking(
  config: protocol.Config,
) -> #(protocol.Connection, protocol.Generation) {
  let conn = protocol.new(config, entropy())
  let #(conn, effects) = protocol.open(conn)
  let generation = opened(effects).0
  #(conn, generation)
}

fn connected(
  config: protocol.Config,
) -> #(protocol.Connection, protocol.Generation) {
  let #(conn, generation) = handshaking(config)
  let #(conn, _) = protocol.received(conn, generation, info_line([]))
  let #(conn, _) = protocol.received(conn, generation, <<"PONG\r\n":utf8>>)
  #(conn, generation)
}

pub fn headers_follow_server_support_test() {
  let #(conn, generation) = handshaking(config("h"))
  let #(conn, effects) =
    protocol.received(
      conn,
      generation,
      info_line([#("headers", json.bool(False))]),
    )
  let assert "CONNECT " <> rest = sent(effects)
  let assert [body, ..] = string.split(rest, "\r\n")
  let flag = fn(name) { json.parse(body, decode.at([name], decode.bool)) }
  assert flag("headers") == Ok(False)
  assert flag("no_responders") == Ok(False)
  let #(conn, effects) =
    protocol.received(conn, generation, <<"PONG\r\n":utf8>>)
  assert list.count(emitted(effects), fn(event) {
      case event {
        protocol.Ready -> True
        _ -> False
      }
    })
    == 1

  let plain = jetgleam.message("s", <<"hi":utf8>>)
  let assert Ok(_) = protocol.publish(conn, plain)
  assert protocol.publish(conn, jetgleam.set_header(plain, "A", "1"))
    == Error(protocol.HeadersNotSupported)
}

pub fn handshake_test() {
  let #(conn, generation) = handshaking(config("h"))
  let #(conn, effects) = protocol.received(conn, generation, info_line([]))
  assert sent(effects) == connect_line(True, option.None, [])
  assert emitted(effects) == []

  let #(conn, effects) =
    protocol.received(conn, generation, <<"PONG\r\n":utf8>>)
  assert emitted(effects) == [protocol.Ready]
  assert list.contains(timers(effects), #(protocol.Keepalive, 120_000))
  assert list.contains(effects, protocol.CancelTimer(protocol.Handshake))
  assert conn.status == protocol.Connected
  assert conn.info == option.Some(expected_info(option.None))
}

pub fn later_info_updates_server_details_test() {
  let #(conn, generation) = handshaking(config("h"))
  let flags = [#("max_payload", json.int(64))]
  let #(conn, _) = protocol.received(conn, generation, info_line([]))
  let #(conn, _) = protocol.received(conn, generation, info_line(flags))
  let #(conn, effects) =
    protocol.received(conn, generation, <<"PONG\r\n":utf8>>)
  assert emitted(effects) == [protocol.Ready]
  assert conn.info
    == option.Some(
      protocol.ServerInfo(..expected_info(option.None), max_payload: 64),
    )
}

pub fn duplicate_info_connects_once_test() {
  let signer =
    nkey.Signer(public_key: "UKEY", sign: fn(data) { <<"signed:", data:bits>> })
  let #(conn, generation) =
    protocol.Config(..config("h"), auth: option.Some(protocol.Nkey(signer)))
    |> handshaking
  let assert Ok(#(conn, _, _)) = protocol.subscribe(conn, "s", option.None)
  let nonce = fn(value) { info_line([#("nonce", json.string(value))]) }
  let #(conn, effects) =
    [nonce("abc"), nonce("abc"), <<"PONG\r\n":utf8>>, <<"PONG\r\n":utf8>>]
    |> list.fold(#(conn, []), fn(acc, bytes) {
      let #(conn, effects) = protocol.received(acc.0, generation, bytes)
      #(conn, list.append(acc.1, effects))
    })
  let count = fn(effects, text) {
    list.length(string.split(sent(effects), text)) - 1
  }
  assert count(effects, "CONNECT ") == 1
  assert count(effects, "SUB s ") == 1
  assert list.count(emitted(effects), fn(event) {
      case event {
        protocol.Ready -> True
        _ -> False
      }
    })
    == 1

  let #(conn, effects) = protocol.transport_closed(conn, generation)
  let #(generation, _, _) = next_open(effects)
  let #(_, effects) = protocol.received(conn, generation, nonce("xyz"))
  let signed = bit_array.base64_url_encode(<<"signed:xyz">>, False)
  assert count(effects, "CONNECT ") == 1
  assert string.contains(sent(effects), "\"sig\":\"" <> signed <> "\"")
}

pub fn ping_interval_test() {
  let #(conn, generation) =
    protocol.Config(..config("h"), ping_interval: 1000) |> handshaking
  let #(conn, _) = protocol.received(conn, generation, info_line([]))
  let #(_, effects) = protocol.received(conn, generation, <<"PONG\r\n":utf8>>)
  assert list.contains(timers(effects), #(protocol.Keepalive, 1000))
}

pub fn handshake_buffers_split_bytes_test() {
  let #(conn, generation) = handshaking(config("h"))
  let assert <<head:bytes-size(10), tail:bytes>> = info_line([])
  let #(conn, effects) = protocol.received(conn, generation, head)
  assert effects == []
  let #(_, effects) = protocol.received(conn, generation, tail)
  assert sent(effects) == connect_line(True, option.None, [])
}

pub fn connect_auth_test() {
  let signer =
    nkey.Signer(public_key: "UKEY", sign: fn(data) { <<"signed:", data:bits>> })
  let sig = #(
    "sig",
    json.string(bit_array.base64_url_encode(<<"signed:abc">>, False)),
  )
  let cases = [
    #(
      protocol.Config(
        ..config("h"),
        name: option.Some("n"),
        auth: option.Some(protocol.Token("t")),
      ),
      True,
      option.Some("n"),
      [#("auth_token", json.string("t"))],
    ),
    #(
      protocol.Config(
        ..config("h"),
        auth: option.Some(protocol.UserPassword("u", "p")),
        echo_enabled: False,
      ),
      False,
      option.None,
      [#("user", json.string("u")), #("pass", json.string("p"))],
    ),
    #(config("nats://u:p@h"), True, option.None, [
      #("user", json.string("u")),
      #("pass", json.string("p")),
    ]),
    #(config("nats://al%40ice:p%3Ass@h"), True, option.None, [
      #("user", json.string("al@ice")),
      #("pass", json.string("p:ss")),
    ]),
    #(config("nats://tok%3Aen@h"), True, option.None, [
      #("auth_token", json.string("tok:en")),
    ]),
    #(
      protocol.Config(..config("h"), auth: option.Some(protocol.Nkey(signer))),
      True,
      option.None,
      [
        #("nkey", json.string("UKEY")),
        sig,
      ],
    ),
    #(
      protocol.Config(
        ..config("h"),
        auth: option.Some(protocol.Jwt(jwt: "the.jwt", signer:)),
      ),
      True,
      option.None,
      [#("jwt", json.string("the.jwt")), sig],
    ),
  ]
  list.each(cases, fn(c) {
    let #(config, echo_on, name, auth) = c
    let #(conn, generation) = handshaking(config)
    let nonce = [#("nonce", json.string("abc"))]
    let #(_, effects) = protocol.received(conn, generation, info_line(nonce))
    assert sent(effects) == connect_line(echo_on, name, auth)
  })
}

pub fn rejected_during_handshake_test() {
  let #(conn, generation) = handshaking(config("h"))
  let #(conn, effects) =
    protocol.received(conn, generation, <<
      "-ERR 'Authorization Violation'\r\n":utf8,
    >>)
  assert emitted(effects)
    == [protocol.Lost(protocol.Rejected("Authorization Violation"))]
  assert conn.status == protocol.Connecting
  assert next_open(effects).2 == 100
}

pub fn tls_required_is_rejected_test() {
  let #(conn, generation) = handshaking(config("h"))
  let #(_, effects) =
    protocol.received(
      conn,
      generation,
      info_line([#("tls_required", json.bool(True))]),
    )
  let assert [protocol.Lost(protocol.Rejected(_))] = emitted(effects)
}

pub fn connected_server_ping_and_warning_test() {
  let #(conn, generation) = connected(config("h"))
  let #(conn, effects) =
    protocol.received(conn, generation, <<"PING\r\n":utf8>>)
  assert sent(effects) == "PONG\r\n"
  let #(conn, effects) =
    protocol.received(conn, generation, <<
      "+OK\r\n-ERR 'Slow Consumer'\r\n":utf8,
    >>)
  assert emitted(effects) == [protocol.Warned("Slow Consumer")]
  assert conn.status == protocol.Connected
}

pub fn violation_reconnects_to_advertised_server_test() {
  let #(conn, generation) = connected(config("h"))
  let urls = json.array(["h:4222", "x:5", "x:5"], json.string)
  let #(conn, effects) =
    protocol.received(conn, generation, info_line([#("connect_urls", urls)]))
  assert effects == []

  let #(conn, effects) =
    protocol.received(conn, generation, <<"BOGUS\r\n":utf8>>)
  let assert [protocol.Lost(protocol.ProtocolViolation(_))] = emitted(effects)
  assert conn.status == protocol.Reconnecting
  assert next_open(effects).1 == protocol.Server("x", 5)
  assert list.contains(effects, protocol.CloseTransport(generation))

  assert protocol.received(conn, generation, <<"PING\r\n":utf8>>).1 == []

  let #(conn, generation) = connected(config("h"))
  let #(_, effects) = protocol.received(conn, generation, <<"INFO {\r\n":utf8>>)
  let assert [protocol.Lost(protocol.ProtocolViolation(_))] = emitted(effects)
}

pub fn backoff_doubles_up_to_max_test() {
  use n <- qcheck.given(failure_count())
  let #(conn, generation) = handshaking(config("h"))
  let rejected = <<"-ERR 'no'\r\n":utf8>>
  let #(_, after) =
    list.fold(list.repeat(Nil, n), #(#(conn, generation), 0), fn(acc, _) {
      let #(#(conn, generation), _) = acc
      let #(conn, effects) = protocol.received(conn, generation, rejected)
      let #(generation, _, after) = next_open(effects)
      #(#(conn, generation), after)
    })
  let expected = int.min(100 * int.bitwise_shift_left(1, n - 1), 2000)
  assert after == expected
}

fn payload(size: Int) -> BitArray {
  bit_array.from_string(string.repeat("x", size))
}

fn publish_frame(size: Int) -> String {
  "PUB a "
  <> int.to_string(size)
  <> "\r\n"
  <> string.repeat("x", size)
  <> "\r\n"
}

fn msg_line(sid: Int) -> BitArray {
  <<"MSG a ":utf8, int.to_string(sid):utf8, " 2\r\nhi\r\n":utf8>>
}

pub fn publish_while_connected_transmits_test() {
  let #(conn, _) = connected(config("h"))
  let assert Ok(#(_, effects)) =
    protocol.publish(conn, jetgleam.message("a", <<"hi":utf8>>))
  assert sent(effects) == "PUB a 2\r\nhi\r\n"
}

pub fn publish_errors_test() {
  let #(conn, _) = connected(config("h"))
  let publish_error = fn(conn, message) {
    let assert Error(error) = protocol.publish(conn, message)
    error
  }
  assert publish_error(conn, jetgleam.message("a b", <<>>))
    == protocol.InvalidSubject("a b")
  let bad_header =
    jetgleam.Message(..jetgleam.message("a", <<>>), headers: [#("a:b", "v")])
  assert publish_error(conn, bad_header) == protocol.InvalidHeaderName("a:b")
  assert publish_error(conn, jetgleam.message("a", payload(1_048_577)))
    == protocol.PayloadTooLarge(1_048_576)
}

pub fn publish_buffers_until_full_then_replays_test() {
  use #(buffer_size, sizes) <- qcheck.given(qcheck.tuple2(
    buffer_size(),
    payload_sizes(),
  ))
  let config = protocol.Config(..config("h"), buffer_size:)
  let #(conn, generation) = handshaking(config)
  let assert Ok(#(conn, _, effects)) =
    protocol.subscribe(conn, "s", option.None)
  assert effects == []

  let #(conn, accepted, _) =
    list.fold(sizes, #(conn, [], 0), fn(acc, size) {
      let #(conn, accepted, used) = acc
      let frame = publish_frame(size)
      let length = string.byte_size(frame)
      let fits = used + length <= buffer_size
      case protocol.publish(conn, jetgleam.message("a", payload(size))) {
        Ok(#(conn, effects)) -> {
          assert fits
          assert effects == []
          #(conn, [frame, ..accepted], used + length)
        }
        Error(error) -> {
          assert fits == False
          assert error == protocol.BufferFull
          #(conn, accepted, used)
        }
      }
    })

  let #(conn, _) = protocol.received(conn, generation, info_line([]))
  let #(_, effects) = protocol.received(conn, generation, <<"PONG\r\n":utf8>>)
  assert sent(effects) == "SUB s 1\r\n" <> string.concat(list.reverse(accepted))
}

pub fn replay_drops_publishes_the_server_cannot_take_test() {
  let buffer = fn(conn, message) {
    let assert Ok(#(conn, [])) = protocol.publish(conn, message)
    conn
  }
  let handshake = fn(conn, generation, max) {
    let info =
      info_line([
        #("max_payload", json.int(max)),
        #("headers", json.bool(False)),
      ])
    let #(conn, _) = protocol.received(conn, generation, info)
    protocol.received(conn, generation, <<"PONG\r\n":utf8>>)
  }

  let #(conn, generation) = handshaking(config("h"))
  let conn =
    conn
    |> buffer(jetgleam.message("a", payload(30)))
    |> buffer(jetgleam.message("a", payload(1)))
    |> buffer(
      jetgleam.message("a", payload(1)) |> jetgleam.set_header("A", "1"),
    )
    |> buffer(jetgleam.message("a", payload(2)))
  let #(conn, effects) = handshake(conn, generation, 20)
  assert sent(effects) == publish_frame(1) <> publish_frame(2)
  let assert [
    protocol.PublishDropped(protocol.MaxPayload(20)),
    protocol.PublishDropped(protocol.NoHeaders),
    protocol.Ready,
  ] = emitted(effects)

  let #(conn, effects) = protocol.transport_closed(conn, generation)
  let #(generation, _, _) = next_open(effects)
  let conn =
    conn
    |> buffer(jetgleam.message("a", payload(4)))
    |> buffer(jetgleam.message("a", payload(1)))
  let #(_, effects) = handshake(conn, generation, 2)
  assert sent(effects) == publish_frame(1)
  let assert [protocol.PublishDropped(protocol.MaxPayload(2)), protocol.Ready] =
    emitted(effects)
}

pub fn parse_error_keeps_earlier_ops_test() {
  let #(conn, generation) = connected(config("h"))
  let assert Ok(#(conn, sid, _)) = protocol.subscribe(conn, "s", option.None)
  let msg = "MSG s " <> int.to_string(sid) <> " 2\r\nhi\r\n"
  let bytes = bit_array.from_string(msg <> "BOGUS\r\n" <> msg)
  let size = bit_array.byte_size(bytes)

  let cuts = list.repeat(Nil, size + 1) |> list.index_map(fn(_, at) { at })
  use at <- list.each(cuts)
  let assert Ok(head) = bit_array.slice(bytes, 0, at)
  let assert Ok(tail) = bit_array.slice(bytes, at, size - at)
  let #(conn, first) = protocol.received(conn, generation, head)
  let #(_, second) = protocol.received(conn, generation, tail)
  let assert [
    protocol.Delivered(delivered, message),
    protocol.Lost(protocol.ProtocolViolation(_)),
  ] = emitted(list.append(first, second))
  assert delivered == sid && message.payload == <<"hi":utf8>>
}

pub fn parse_error_after_loss_is_not_a_second_loss_test() {
  let #(conn, generation) = connected(config("h"))
  let #(_, effects) =
    protocol.received(conn, generation, <<"INFO {\r\nBOGUS\r\n":utf8>>)
  assert emitted(effects)
    == [protocol.Lost(protocol.ProtocolViolation("invalid INFO"))]
}

pub fn subscribe_validates_test() {
  let #(conn, _) = connected(config("h"))
  assert protocol.subscribe(conn, "a b", option.None)
    == Error(protocol.InvalidSubject("a b"))
  assert protocol.subscribe(conn, "a", option.Some("q r"))
    == Error(protocol.InvalidQueueGroup("q r"))
}

pub fn subscription_delivers_until_unsubscribed_test() {
  let #(conn, generation) = connected(config("h"))
  let assert Ok(#(conn, sid, effects)) =
    protocol.subscribe(conn, "a", option.Some("q"))
  assert sent(effects) == "SUB a q 1\r\n"
  assert sid == 1

  let #(conn, effects) = protocol.received(conn, generation, msg_line(sid))
  assert emitted(effects)
    == [protocol.Delivered(sid, jetgleam.message("a", <<"hi":utf8>>))]

  let #(conn, effects) = protocol.unsubscribe(conn, sid)
  assert sent(effects) == "UNSUB 1\r\n"
  let #(conn, effects) = protocol.received(conn, generation, msg_line(sid))
  assert emitted(effects) == []
  assert protocol.unsubscribe(conn, sid).1 == []
}

pub fn flush_emits_after_pong_test() {
  let #(conn, generation) = connected(config("h"))
  let #(conn, token, effects) = protocol.flush(conn)
  assert sent(effects) == "PING\r\n"
  let #(_, effects) = protocol.received(conn, generation, <<"PONG\r\n":utf8>>)
  assert emitted(effects) == [protocol.Flushed(token)]
}

pub fn flush_while_connecting_waits_for_ready_test() {
  let #(conn, generation) = handshaking(config("h"))
  let #(conn, token, effects) = protocol.flush(conn)
  assert effects == []
  let #(conn, _) = protocol.received(conn, generation, info_line([]))
  let #(conn, effects) =
    protocol.received(conn, generation, <<"PONG\r\n":utf8>>)
  assert sent(effects) == "PING\r\n"
  assert emitted(effects) == [protocol.Ready]
  let #(_, effects) = protocol.received(conn, generation, <<"PONG\r\n":utf8>>)
  assert emitted(effects) == [protocol.Flushed(token)]
}

pub fn cancelled_offline_flush_is_not_replayed_test() {
  let config = protocol.Config(..config("h"), buffer_size: 30)
  let #(conn, generation) = handshaking(config)
  let assert Ok(#(conn, [])) =
    protocol.publish(conn, jetgleam.message("a", <<"1">>))
  let #(conn, cancelled, _) = protocol.flush(conn)
  let #(conn, kept, _) = protocol.flush(conn)
  let assert Ok(#(conn, [])) =
    protocol.publish(conn, jetgleam.message("b", <<"2">>))
  let #(conn, cancelled_last, _) = protocol.flush(conn)
  let conn =
    conn
    |> protocol.cancel_flush(cancelled)
    |> protocol.cancel_flush(cancelled_last)

  assert protocol.publish(conn, jetgleam.message("c", <<"3">>))
    == Error(protocol.BufferFull)
  let #(conn, _) = protocol.received(conn, generation, info_line([]))
  let #(conn, effects) =
    protocol.received(conn, generation, <<"PONG\r\n":utf8>>)
  assert sent(effects) == "PUB a 1\r\n1\r\nPING\r\nPUB b 1\r\n2\r\n"
  let #(_, effects) = protocol.received(conn, generation, <<"PONG\r\n":utf8>>)
  assert emitted(effects) == [protocol.Flushed(kept)]
}

pub fn cancelled_sent_flush_preserves_pong_alignment_test() {
  let #(conn, generation) = connected(config("h"))
  let #(conn, first, _) = protocol.flush(conn)
  let #(conn, second, _) = protocol.flush(conn)
  let conn = protocol.cancel_flush(conn, first)
  let #(conn, effects) =
    protocol.received(conn, generation, <<"PONG\r\n":utf8>>)
  assert !list.contains(emitted(effects), protocol.Flushed(second))
  let #(_, effects) = protocol.received(conn, generation, <<"PONG\r\n":utf8>>)
  assert emitted(effects) == [protocol.Flushed(second)]
}

pub fn flush_fails_when_lost_test() {
  let #(conn, generation) = connected(config("h"))
  let #(conn, first, _) = protocol.flush(conn)
  let #(conn, second, _) = protocol.flush(conn)
  let #(_, effects) = protocol.transport_closed(conn, generation)
  let assert [
    protocol.Lost(_),
    protocol.FlushFailed(a),
    protocol.FlushFailed(b),
  ] = emitted(effects)
  assert #(a, b) == #(first, second)
}

pub fn transport_loss_backs_off_test() {
  use #(#(initial, max), n) <- qcheck.given(qcheck.tuple2(
    backoff_settings(),
    failure_count(),
  ))
  let config =
    protocol.Config(
      ..config("h"),
      reconnect_initial: initial,
      reconnect_max: max,
    )
  let #(conn, first) = handshaking(config)
  let #(_, _, afters) =
    list.fold(list.repeat(Nil, n), #(conn, first, []), fn(acc, _) {
      let #(conn, generation, afters) = acc
      let #(conn, effects) = protocol.transport_failed(conn, generation, "x")
      assert protocol.transport_failed(conn, generation, "x").1 == []
      let #(next, _, after) = next_open(effects)
      #(conn, next, [after, ..afters])
    })
  let expected =
    list.repeat(Nil, n)
    |> list.index_map(fn(_, i) {
      int.min(initial * int.bitwise_shift_left(1, i), max)
    })
  assert list.reverse(afters) == expected
}

pub fn stale_transport_events_are_ignored_test() {
  let #(conn, generation) = connected(config("h"))
  assert protocol.is_current(conn, generation)
  let #(conn, effects) = protocol.transport_closed(conn, generation)
  assert protocol.transport_closed(conn, generation).1 == []
  assert protocol.transport_failed(conn, generation, "x").1 == []
  assert emitted(effects) == [protocol.Lost(protocol.TransportClosed)]
  let #(next, _, _) = next_open(effects)
  assert !protocol.is_current(conn, generation)
  assert protocol.is_current(conn, next)
}

pub fn loss_reconnects_to_next_server_test() {
  let assert Ok(config) = config("a") |> protocol.add_server("b")
  let #(conn, generation) = connected(config)
  let #(conn, effects) = protocol.transport_closed(conn, generation)
  assert emitted(effects) == [protocol.Lost(protocol.TransportClosed)]
  assert conn.status == protocol.Reconnecting
  let #(_, server, after) = next_open(effects)
  assert server == protocol.Server("b", 4222)
  assert after == 100
}

pub fn failure_count_resets_after_handshake_test() {
  let #(conn, generation) = connected(config("h"))
  let #(conn, effects) = protocol.transport_failed(conn, generation, "x")
  let #(generation, _, _) = next_open(effects)
  let #(conn, _) = protocol.received(conn, generation, info_line([]))
  let #(conn, _) = protocol.received(conn, generation, <<"PONG\r\n":utf8>>)
  let #(_, effects) = protocol.transport_failed(conn, generation, "x")
  assert next_open(effects).2 == 100
}

pub fn reconnect_resends_subscriptions_before_buffer_test() {
  let #(conn, generation) = connected(config("h"))
  let assert Ok(#(conn, _, _)) = protocol.subscribe(conn, "s", option.None)
  let #(conn, effects) = protocol.transport_closed(conn, generation)
  let #(generation, _, _) = next_open(effects)
  let assert Ok(#(conn, _)) =
    protocol.publish(conn, jetgleam.message("a", payload(2)))
  let #(conn, _) = protocol.received(conn, generation, info_line([]))
  let #(_, effects) = protocol.received(conn, generation, <<"PONG\r\n":utf8>>)
  assert sent(effects) == "SUB s 1\r\n" <> publish_frame(2)
}

pub fn ping_timer_detects_stale_connection_test() {
  let config = config("h")
  let #(conn, _) = connected(config)
  let #(conn, effects) = protocol.timer_fired(conn, protocol.Keepalive)
  assert sent(effects) == "PING\r\n"
  assert timers(effects) == [#(protocol.Keepalive, 120_000)]
  let #(conn, _) = protocol.timer_fired(conn, protocol.Keepalive)
  let #(_, effects) = protocol.timer_fired(conn, protocol.Keepalive)
  assert emitted(effects) == [protocol.Lost(protocol.Stale)]

  let #(conn, generation) = connected(config)
  let #(conn, _) = protocol.timer_fired(conn, protocol.Keepalive)
  let #(conn, _) = protocol.timer_fired(conn, protocol.Keepalive)
  let #(conn, _) = protocol.received(conn, generation, <<"PONG\r\n":utf8>>)
  let #(_, effects) = protocol.timer_fired(conn, protocol.Keepalive)
  assert sent(effects) == "PING\r\n"

  let #(conn, generation) = connected(config)
  let #(conn, _) = protocol.timer_fired(conn, protocol.Keepalive)
  let #(conn, token, _) = protocol.flush(conn)
  let #(conn, effects) =
    protocol.received(conn, generation, <<"PONG\r\n":utf8>>)
  assert emitted(effects) == []
  let #(_, effects) = protocol.received(conn, generation, <<"PONG\r\n":utf8>>)
  assert emitted(effects) == [protocol.Flushed(token)]
}

pub fn handshake_timer_test() {
  let #(conn, _) = handshaking(config("h"))
  let #(_, effects) = protocol.timer_fired(conn, protocol.Handshake)
  assert emitted(effects)
    == [protocol.Lost(protocol.TransportFailed("connect timed out"))]
  assert protocol.timer_fired(conn, protocol.Keepalive).1 == []

  let #(conn, _) = connected(config("h"))
  assert protocol.timer_fired(conn, protocol.Handshake).1 == []
}

pub fn advertised_server_joins_pool_after_loss_test() {
  let #(conn, generation) = handshaking(config("h"))
  let urls = json.array(["other:4333"], json.string)
  let #(conn, _) =
    protocol.received(conn, generation, info_line([#("connect_urls", urls)]))
  let #(conn, _) = protocol.received(conn, generation, <<"PONG\r\n":utf8>>)
  let #(_, effects) = protocol.transport_closed(conn, generation)
  assert next_open(effects).1 == protocol.Server("other", 4333)
}

pub fn reply_and_headers_encode_as_hpub_test() {
  let assert Ok(valid) =
    jetgleam.Message(
      ..jetgleam.message("a.b", <<"hi":utf8>>),
      reply_to: option.Some("inbox.1"),
    )
    |> jetgleam.add_header("X", "1")
    |> jetgleam.add_header("X", "2")
    |> protocol.validate_publish
  assert bytes_tree.to_bit_array(protocol.encode_publish(valid))
    == <<
      "HPUB a.b inbox.1 24 26\r\nNATS/1.0\r\nX: 1\r\nX: 2\r\n\r\nhi\r\n":utf8,
    >>
}

pub fn validation_outcomes_test() {
  let msg = jetgleam.message
  let publish_err = fn(m) {
    protocol.validate_publish(m)
    |> result.unwrap_error(protocol.InvalidSubject("ok"))
  }

  assert publish_err(msg("a b", <<>>)) == protocol.InvalidSubject("a b")
  assert publish_err(msg("a..b", <<>>)) == protocol.InvalidSubject("a..b")
  assert publish_err(msg("a.>", <<>>)) == protocol.InvalidSubject("a.>")
  assert publish_err(msg("a.*", <<>>)) == protocol.InvalidSubject("a.*")
  assert publish_err(
      jetgleam.Message(..msg("a", <<>>), reply_to: option.Some("r r")),
    )
    == protocol.InvalidReplySubject("r r")
  assert publish_err(jetgleam.add_header(msg("a", <<>>), "Bad:Name", "v"))
    == protocol.InvalidHeaderName("Bad:Name")
  assert publish_err(jetgleam.add_header(msg("a", <<>>), "", "v"))
    == protocol.InvalidHeaderName("")
  assert publish_err(jetgleam.add_header(msg("a", <<>>), "N", "v\nx"))
    == protocol.InvalidHeaderValue("N")

  assert protocol.validate_subscription("a.>", option.None)
    == Ok(protocol.ValidSubscription("a.>", option.None))
  assert protocol.validate_subscription("a.*.b", option.None)
    == Ok(protocol.ValidSubscription("a.*.b", option.None))
  assert protocol.validate_subscription("a.>.b", option.None)
    == Error(protocol.InvalidSubject("a.>.b"))
  assert protocol.validate_subscription("a", option.Some("q g"))
    == Error(protocol.InvalidQueueGroup("q g"))
  assert protocol.validate_subscription("a", option.Some(""))
    == Error(protocol.InvalidQueueGroup(""))
}

pub fn publish_size_matches_encoding_test() {
  use msg <- qcheck.given(message())
  let assert Ok(valid) = protocol.validate_publish(msg)
  let frame = protocol.encode_publish(valid) |> bytes_tree.to_bit_array
  assert valid.size
    == bit_array.byte_size(frame) - control_line_length(frame, 0) - 2
}

fn control_line_length(frame: BitArray, at: Int) -> Int {
  case bit_array.slice(frame, at, 2) {
    Ok(<<"\r\n":utf8>>) -> at + 2
    _ -> control_line_length(frame, at + 1)
  }
}

fn frame(sid: Int, message: jetgleam.Message) -> BitArray {
  let jetgleam.Message(subject:, reply_to:, headers:, status:, payload:) =
    message
  let reply = case reply_to {
    option.Some(r) -> " " <> r
    option.None -> ""
  }
  let head = subject <> " " <> int.to_string(sid) <> reply <> " "
  let size = int.to_string(bit_array.byte_size(payload))
  case headers, status {
    [], option.None -> <<
      "MSG ":utf8,
      head:utf8,
      size:utf8,
      "\r\n":utf8,
      payload:bits,
      "\r\n":utf8,
    >>
    _, _ -> {
      let first = case status {
        option.Some(jetgleam.Status(code:, description:)) ->
          "NATS/1.0 "
          <> int.to_string(code)
          <> case description {
            option.Some(d) -> " " <> d
            option.None -> ""
          }
        option.None -> "NATS/1.0"
      }
      let lines =
        list.map(headers, fn(h) { h.0 <> ": " <> h.1 <> "\r\n" })
        |> string.concat
      let block = <<first:utf8, "\r\n":utf8, lines:utf8, "\r\n":utf8>>
      let hdr = bit_array.byte_size(block)
      let total = int.to_string(hdr + bit_array.byte_size(payload))
      <<
        "HMSG ":utf8,
        head:utf8,
        int.to_string(hdr):utf8,
        " ":utf8,
        total:utf8,
        "\r\n":utf8,
        block:bits,
        payload:bits,
        "\r\n":utf8,
      >>
    }
  }
}

pub fn parse_chunking_invariant_test() {
  use #(#(messages, broken), cut) <- qcheck.given(qcheck.tuple2(
    qcheck.tuple2(
      qcheck.generic_list(server_message(), qcheck.bounded_int(0, 8)),
      qcheck.bool(),
    ),
    qcheck.bounded_int(0, 10_000),
  ))
  let indexed = list.index_map(messages, fn(m, i) { #(i, m) })
  let expected = list.map(indexed, fn(p) { protocol.Msg(p.0, p.1) })
  let valid = list.map(indexed, fn(p) { frame(p.0, p.1) }) |> bit_array.concat
  let buffer = case broken {
    True -> bit_array.concat([valid, <<"BOGUS\r\n":utf8>>, valid])
    False -> valid
  }
  let ended = fn(rest) {
    case rest, broken {
      Ok(<<>>), False | Error(_), True -> True
      _, _ -> False
    }
  }

  let #(ops, rest) = protocol.parse(buffer)
  assert ops == expected
  assert ended(rest)

  let at = cut % { bit_array.byte_size(buffer) + 1 }
  let assert Ok(head) = bit_array.slice(buffer, 0, at)
  let assert Ok(tail) =
    bit_array.slice(buffer, at, bit_array.byte_size(buffer) - at)
  let #(first, rest) = protocol.parse(head)
  let #(ops, rest) = case rest {
    Error(_) -> #(first, rest)
    Ok(leftover) -> {
      let #(second, rest) = protocol.parse(bit_array.append(leftover, tail))
      #(list.append(first, second), rest)
    }
  }
  assert ops == expected
  assert ended(rest)
}

pub fn parse_control_ops_test() {
  let info = json.object([#("server_id", json.string("x"))]) |> json.to_string
  let buffer = <<
    "INFO ":utf8,
    info:utf8,
    "\r\nping\r\nPONG\r\n+OK\r\n-ERR 'Authorization Violation'\r\n":utf8,
  >>
  assert protocol.parse(buffer)
    == #(
      [
        protocol.Info(info),
        protocol.Ping,
        protocol.Pong,
        protocol.Ack,
        protocol.ServerError("Authorization Violation"),
      ],
      Ok(<<>>),
    )
}

pub fn parse_status_only_hmsg_test() {
  let block = <<"NATS/1.0 408 Request Timeout\r\n\r\n":utf8>>
  let size = bit_array.byte_size(block) |> int.to_string
  let buffer = <<
    "HMSG s 1 ":utf8,
    size:utf8,
    " ":utf8,
    size:utf8,
    "\r\n":utf8,
    block:bits,
    "\r\n":utf8,
  >>
  let assert #([protocol.Msg(1, message)], Ok(<<>>)) = protocol.parse(buffer)
  assert message.status
    == option.Some(jetgleam.Status(408, option.Some("Request Timeout")))
  assert message.headers == []
  assert message.payload == <<>>
}

pub fn parse_unknown_op_test() {
  let assert #([], Error(_)) = protocol.parse(<<"FOO bar\r\n":utf8>>)
}

pub fn decode_headers_test() {
  assert protocol.decode_headers(<<
      "NATS/1.0\r\nA: 1\r\nB:2\r\nA: 3\r\n\r\n":utf8,
    >>)
    == Ok(#(option.None, [#("A", "1"), #("B", "2"), #("A", "3")]))
  assert protocol.decode_headers(<<"NATS/1.0 503\r\n\r\n":utf8>>)
    == Ok(#(option.Some(jetgleam.Status(503, option.None)), []))

  use block <- list.each([
    "NATS/1.0\r\nA: 1\r\nno colon\r\n\r\n",
    "NATS/1.0\r\nA: 1\r\n",
    "NATS/1.0\r\nA: 1",
    "NATS/1.0100\r\n\r\n",
    "NATS/1.0\r\n\r\nA: 1\r\n\r\n",
    "NATS/1.0\r\n: v\r\n\r\n",
    "NATS/1.0 abc\r\n\r\n",
    "NATS/1.1\r\n\r\n",
  ])
  assert protocol.decode_headers(bit_array.from_string(block)) == Error(Nil)
}
