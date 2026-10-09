import gleam/bit_array
import gleam/dict
import gleam/dynamic
import gleam/erlang/atom
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/otp/static_supervisor
import gleam/result
import gleam/string
import gleam/time/duration
import jetgleam
import jetgleam/consumer
import jetgleam/internal/protocol
import jetgleam/kv
import jetgleam/stream
import jetgleam_erlang/nats
import mug
import nats_server

pub fn connect_reports_status_test() {
  use server <- nats_server.with([])
  let events = process.new_subject()
  let assert Ok(conn) =
    nats_server.config(server)
    |> nats.with_events(events)
    |> nats.connect

  assert nats.status(conn) == nats.Connected
  assert process.receive(events, 1000) == Ok(nats.StatusChanged(nats.Connected))
}

pub fn start_test() {
  use server <- nats_server.with(["--user", "u", "--pass", "p"])
  let events = process.new_subject()
  let assert Ok(started) =
    nats_server.config(server)
    |> nats.with_name("worker")
    |> nats.with_user_password(user: "u", password: "p")
    |> nats.with_echo(False)
    |> nats.with_ping_interval(60_000)
    |> nats.with_buffer_size(1024)
    |> nats.with_events(events)
    |> nats.start

  assert process.receive(events, 1000) == Ok(nats.StatusChanged(nats.Connected))
  let assert Ok(sub) = nats.subscribe(started.data, "x")
  let assert Ok(Nil) = nats.publish(started.data, "x", <<>>)
  assert process.receive(nats.messages(sub), 200) == Error(Nil)

  assert nats.new_inbox(started.data) != nats.new_inbox(started.data)
  let assert Error(nats.NoResponders) =
    nats.request_message(
      started.data,
      jetgleam.message("nobody.home", <<>>),
      timeout: 1000,
    )
}

pub fn connect_failures_test() {
  let assert Error(nats.InvalidUrl(url: "tls://x", ..)) = nats.config("tls://x")
  let port = int.to_string(nats_server.free_port())
  let assert Ok(dead) = nats.config("nats://127.0.0.1:" <> port)
  let assert Error(nats.InvalidUrl(url: "tls://x", ..)) =
    nats.add_server(dead, "tls://x")
  let assert Error(nats.InvalidCredentials(..)) = nats.with_nkey(dead, "junk")
  let assert Error(nats.InvalidCredentials(..)) =
    nats.with_credentials(dead, "junk")

  let dead = nats.with_connect_timeout(dead, 300)
  let assert Error(nats.Unreachable(..)) = nats.connect(dead)

  use server <- nats_server.with(["--auth", "secret"])
  let assert Ok(failover) = nats.add_server(dead, nats_server.url(server))
  let assert Ok(_) = failover |> nats.with_token("secret") |> nats.connect
  let assert Error(nats.Rejected(..)) =
    nats_server.config(server) |> nats.with_token("wrong") |> nats.connect
  let assert Ok(_) =
    nats_server.config(server) |> nats.with_token("secret") |> nats.connect
}

fn connect(server: nats_server.Server) -> nats.Connection {
  let assert Ok(conn) = nats_server.config(server) |> nats.connect
  conn
}

pub fn pub_sub_round_trip_test() {
  use server <- nats_server.with([])
  let conn = connect(server)
  let assert Ok(sub) = nats.subscribe(conn, "orders.*")
  let message =
    jetgleam.message("orders.new", <<"hello":utf8>>)
    |> jetgleam.set_reply_to("replies.1")
    |> jetgleam.add_header("Tag", "a")
    |> jetgleam.add_header("Tag", "b")
    |> jetgleam.add_header("Other", "c")
  let assert Ok(Nil) = nats.publish_message(conn, message)

  let assert Ok(received) = process.receive(nats.messages(sub), 1000)
  assert received.subject == message.subject
  assert received.reply_to == message.reply_to
  assert received.headers == message.headers
  assert received.payload == message.payload

  nats.unsubscribe(sub)
  let assert Ok(Nil) = nats.flush(conn, timeout: 1000)
  let assert Ok(Nil) = nats.publish(conn, "orders.late", <<>>)
  let assert Ok(Nil) = nats.flush(conn, timeout: 1000)
  assert process.receive(nats.messages(sub), 100) == Error(Nil)
}

pub fn queue_subscribe_delivers_once_test() {
  use server <- nats_server.with([])
  let conn = connect(server)
  let assert Ok(a) = nats.queue_subscribe(conn, "jobs", group: "workers")
  let assert Ok(b) = nats.queue_subscribe(conn, "jobs", group: "workers")
  list.each(list.repeat(Nil, 10), fn(_) {
    let assert Ok(Nil) = nats.publish(conn, "jobs", <<>>)
  })
  let assert Ok(Nil) = nats.flush(conn, timeout: 1000)

  let count = fn(sub) {
    let messages = nats.messages(sub)
    list.count(list.repeat(Nil, 10), fn(_) {
      process.receive(messages, 100) != Error(Nil)
    })
  }
  assert count(a) + count(b) == 10
}

pub fn dead_owner_leaves_queue_group_test() {
  use exit <- list.each([fn() { Nil }, fn() { panic as "worker crashed" }])
  let server = nats_server.start([])
  let events = process.new_subject()
  let assert Ok(conn) =
    nats_server.config(server)
    |> nats.with_reconnect_delay(initial: 50, max: 200)
    |> nats.with_events(events)
    |> nats.connect
  assert process.receive(events, 1000) == Ok(nats.StatusChanged(nats.Connected))

  let ready = process.new_subject()
  let worker =
    process.spawn_unlinked(fn() {
      let assert Ok(_) = nats.queue_subscribe(conn, "jobs", group: "workers")
      process.send(ready, Nil)
      exit()
    })
  let down =
    process.new_selector()
    |> process.select_specific_monitor(process.monitor(worker), fn(_) { Nil })
  let assert Ok(Nil) = process.receive(ready, 1000)
  let assert Ok(survivor) = nats.queue_subscribe(conn, "jobs", group: "workers")
  let assert Ok(Nil) = process.selector_receive(down, 1000)

  let assert Ok(Nil) = nats.flush(conn, timeout: 1000)
  assert publish_jobs(conn, survivor) == 20

  let server = nats_server.restart(server)
  assert process.receive(events, 5000)
    == Ok(nats.StatusChanged(nats.Reconnecting))
  assert process.receive(events, 10_000)
    == Ok(nats.StatusChanged(nats.Connected))
  assert publish_jobs(conn, survivor) == 20

  nats_server.stop(server)
}

fn publish_jobs(
  conn: nats.Connection,
  sub: nats.Subscription(jetgleam.Message),
) -> Int {
  list.each(list.repeat(Nil, 20), fn(_) {
    let assert Ok(Nil) = nats.publish(conn, "jobs", <<>>)
  })
  let assert Ok(Nil) = nats.flush(conn, timeout: 1000)
  list.count(list.repeat(Nil, 20), fn(_) {
    process.receive(nats.messages(sub), 100) != Error(Nil)
  })
}

pub fn owner_exit_stops_connection_test() {
  use server <- nats_server.with([])
  use exit <- list.each([fn() { Nil }, fn() { panic as "owner crashed" }])
  let started = process.new_subject()
  process.spawn_unlinked(fn() {
    let assert Ok(connection) = nats_server.config(server) |> nats.start
    process.send(started, connection.pid)
    exit()
  })
  let assert Ok(pid) = process.receive(started, 1000)
  let down =
    process.new_selector()
    |> process.select_specific_monitor(process.monitor(pid), fn(_) { Nil })
  assert process.selector_receive(down, 1000) == Ok(Nil)
}

pub fn request_reply_test() {
  use server <- nats_server.with([])
  let conn = connect(server)

  assert nats.request(conn, "svc", <<>>, timeout: 500)
    == Error(nats.NoResponders)

  let parent = process.new_subject()
  process.spawn(fn() {
    let assert Ok(sub) = nats.subscribe(conn, "svc")
    process.send(parent, Nil)
    let assert Ok(request) = process.receive_forever(nats.messages(sub)) |> Ok
    let assert option.Some(reply_to) = request.reply_to
    let assert Ok(Nil) = nats.publish(conn, reply_to, <<"pong":utf8>>)
  })
  let assert Ok(Nil) = process.receive(parent, 1000)
  let assert Ok(reply) =
    nats.request(conn, "svc", <<"ping":utf8>>, timeout: 1000)
  assert reply.payload == <<"pong":utf8>>

  process.spawn(fn() {
    let assert Ok(_) = nats.subscribe(conn, "silent")
    process.send(parent, Nil)
    process.sleep(1000)
  })
  let assert Ok(Nil) = process.receive(parent, 1000)
  let assert Ok(Nil) = nats.flush(conn, timeout: 1000)
  assert nats.request(conn, "silent", <<>>, timeout: 200) == Error(nats.Timeout)
}

pub fn invalid_subject_test() {
  use server <- nats_server.with([])
  let conn = connect(server)
  assert nats.publish(conn, "bad subject", <<>>)
    == Error(nats.InvalidSubject("bad subject"))
  assert nats.flush(conn, timeout: 1000) == Ok(Nil)
}

pub fn nkey_auth_test() {
  use server <- nats_server.with(["-c", "test/fixtures/nkey.conf"])
  let seed = "SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY"
  let other = "SUAAABYOCUOCGKRRHA7UMTKULNRGS4DXP2CYZE42UGUK7NV5YTF5FWIXJQ"
  let assert Ok(config) = nats.with_nkey(nats_server.config(server), seed)
  let assert Ok(_) = nats.connect(config)
  let assert Ok(config) = nats.with_nkey(nats_server.config(server), other)
  let assert Error(nats.Rejected(..)) = nats.connect(config)
}

@external(erlang, "file", "read_file")
fn read_file(path: String) -> Result(BitArray, dynamic.Dynamic)

pub fn credentials_auth_test() {
  use server <- nats_server.with(["-c", "test/fixtures/jwt.conf"])
  let assert Ok(bits) = read_file("test/fixtures/user.creds")
  let assert Ok(contents) = bit_array.to_string(bits)
  let assert Ok(config) =
    nats.with_credentials(nats_server.config(server), contents)
  let assert Ok(conn) = nats.connect(config)
  assert round_trip(conn)

  let swapped =
    string.replace(
      contents,
      "SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY",
      "SUAAABYOCUOCGKRRHA7UMTKULNRGS4DXP2CYZE42UGUK7NV5YTF5FWIXJQ",
    )
  let assert Ok(config) =
    nats.with_credentials(nats_server.config(server), swapped)
  let assert Error(nats.Rejected(..)) = nats.connect(config)
}

pub fn flush_crashes_on_stopped_connection_test() {
  use server <- nats_server.with([])
  let assert Ok(started) = nats_server.config(server) |> nats.start
  process.unlink(started.pid)
  suspend_process(started.pid)
  let flusher =
    process.spawn_unlinked(fn() {
      let _ = nats.flush(started.data, timeout: 60_000)
    })
  let down =
    process.new_selector()
    |> process.select_specific_monitor(process.monitor(flusher), fn(down) {
      down
    })
  process.kill(started.pid)
  let assert Ok(process.ProcessDown(reason: process.Abnormal(_), ..)) =
    process.selector_receive(down, 1000)
}

pub fn flush_outcomes_test() {
  let server = nats_server.start([])
  let events = process.new_subject()
  let assert Ok(conn) =
    nats_server.config(server)
    |> nats.with_reconnect_delay(initial: 50, max: 200)
    |> nats.with_events(events)
    |> nats.connect
  assert process.receive(events, 1000) == Ok(nats.StatusChanged(nats.Connected))

  nats_server.signal(server, "STOP")
  assert nats.flush(conn, timeout: 100) == Error(nats.Timeout)
  nats_server.signal(server, "CONT")
  assert nats.flush(conn, timeout: 1000) == Ok(Nil)

  nats_server.signal(server, "STOP")
  process.spawn(fn() {
    process.sleep(200)
    nats_server.signal(server, "KILL")
  })
  assert nats.flush(conn, timeout: 10_000) == Error(nats.Disconnected)
  assert process.receive(events, 1000)
    == Ok(nats.StatusChanged(nats.Reconnecting))

  let result = process.new_subject()
  process.spawn(fn() { process.send(result, nats.flush(conn, timeout: 10_000)) })
  let server = nats_server.restart(server)
  assert process.receive(result, 10_000) == Ok(Ok(Nil))

  nats_server.stop(server)
}

pub fn requests_leave_no_messages_test() {
  use server <- nats_server.with([])
  let conn = connect(server)
  let ready = process.new_subject()
  let responder = fn(subject, delay) {
    process.spawn(fn() {
      let assert Ok(sub) = nats.subscribe(conn, subject)
      process.send(ready, Nil)
      respond(conn, sub, delay)
    })
  }
  responder("svc", 0)
  responder("svc", 0)
  responder("slow", 150)
  list.each([1, 2, 3], fn(_) {
    let assert Ok(Nil) = process.receive(ready, 1000)
  })
  let assert Ok(Nil) = nats.flush(conn, timeout: 1000)

  assert nats_server.leftover_messages(fn() {
      let assert Ok(_) = nats.request(conn, "svc", <<>>, timeout: 1000)
    })
    == 0
  assert nats_server.leftover_messages(fn() {
      let assert Error(nats.Timeout) =
        nats.request(conn, "slow", <<>>, timeout: 100)
    })
    == 0
}

fn respond(
  conn: nats.Connection,
  sub: nats.Subscription(jetgleam.Message),
  delay: Int,
) -> Nil {
  let request = process.receive_forever(nats.messages(sub))
  process.sleep(delay)
  let assert option.Some(reply_to) = request.reply_to
  let assert Ok(Nil) = nats.publish(conn, reply_to, <<"pong":utf8>>)
  respond(conn, sub, delay)
}

pub fn supervised_test() {
  use server <- nats_server.with([])
  use name, conn <- supervise(nats_server.config(server))

  let assert Ok(sub) = nats.subscribe(conn, "sup")
  let assert Ok(Nil) = nats.publish(conn, "sup", <<"hi":utf8>>)
  let assert Ok(received) = process.receive(nats.messages(sub), 1000)
  assert received.payload == <<"hi":utf8>>

  let assert Ok(child) = process.named(name)
  process.kill(child)
  await_restart(name, child, 100)
  let assert Ok(sub) = nats.subscribe(conn, "sup")
  let assert Ok(Nil) = nats.publish(conn, "sup", <<"again":utf8>>)
  let assert Ok(received) = process.receive(nats.messages(sub), 1000)
  assert received.payload == <<"again":utf8>>
}

pub fn stale_async_cleanup_does_not_unsubscribe_replacement_test() {
  use server <- nats_server.with([])
  use name, conn <- supervise(nats_server.config(server))

  let assert Ok(old) = nats.subscribe(conn, "sup")
  let assert Ok(Nil) = nats.publish(conn, "sup", <<"old":utf8>>)
  let assert Ok(received) = process.receive(nats.messages(old), 1000)
  assert received.payload == <<"old":utf8>>

  let assert Ok(child) = process.named(name)
  process.kill(child)
  await_restart(name, child, 100)

  let assert Ok(replacement) = nats.subscribe(conn, "sup")
  let assert Ok(Nil) = nats.publish(conn, "sup", <<"before":utf8>>)
  let assert Ok(before) = process.receive(nats.messages(replacement), 1000)
  assert before.payload == <<"before":utf8>>

  nats.unsubscribe(old)
  let assert Ok(Nil) = nats.flush(conn, timeout: 1000)
  let assert Ok(Nil) = nats.publish(conn, "sup", <<"after":utf8>>)
  let assert Ok(after) = process.receive(nats.messages(replacement), 1000)
  assert after.payload == <<"after":utf8>>
}

pub fn dead_request_cleanup_does_not_unsubscribe_replacement_test() {
  use server <- nats_server.with([])
  use name, conn <- supervise(nats_server.config(server))
  let responder = connect(server)
  let assert Ok(service) = nats.subscribe(responder, "service")
  let requester =
    process.spawn_unlinked(fn() {
      let _ = nats.request(conn, "service", <<"request":utf8>>, timeout: 500)
    })
  let down =
    process.new_selector()
    |> process.select_specific_monitor(process.monitor(requester), fn(down) {
      down
    })
  let assert Ok(request) = process.receive(nats.messages(service), 1000)
  assert request.payload == <<"request":utf8>>
  suspend_process(requester)

  let assert Ok(child) = process.named(name)
  process.kill(child)
  await_restart(name, child, 100)
  let assert Ok(replacement) = nats.subscribe(conn, "sup")
  let assert Ok(Nil) = nats.publish(conn, "sup", <<"before":utf8>>)
  let assert Ok(before) = process.receive(nats.messages(replacement), 1000)
  assert before.payload == <<"before":utf8>>

  resume_process(requester)
  let assert Ok(process.ProcessDown(reason: process.Abnormal(_), ..)) =
    process.selector_receive(down, 5000)

  let assert Ok(Nil) = nats.publish(conn, "sup", <<"after":utf8>>)
  let assert Ok(Nil) = nats.flush(conn, timeout: 1000)
  let assert Ok(after) = process.receive(nats.messages(replacement), 1000)
  assert after.payload == <<"after":utf8>>

  nats.unsubscribe(service)
}

pub fn request_crashes_on_unconfirmed_close_test() {
  use server <- nats_server.with([])
  let assert Ok(started) = nats_server.config(server) |> nats.start
  let responder = connect(server)
  let assert Ok(service) = nats.subscribe(responder, "service")
  let requester =
    process.spawn_unlinked(fn() {
      let _ = nats.request(started.data, "service", <<>>, timeout: 1000)
    })
  let down =
    process.new_selector()
    |> process.select_specific_monitor(process.monitor(requester), fn(down) {
      down
    })
  let assert Ok(request) = process.receive(nats.messages(service), 1000)

  suspend_process(requester)
  let assert option.Some(reply_to) = request.reply_to
  let assert Ok(Nil) = nats.publish(responder, reply_to, <<"pong":utf8>>)
  let assert Ok(Nil) = nats.flush(responder, timeout: 1000)
  let assert Ok(Nil) = nats.flush(started.data, timeout: 1000)
  suspend_process(started.pid)
  resume_process(requester)
  let outcome = process.selector_receive(down, 6000)

  resume_process(started.pid)
  process.unlink(started.pid)
  process.kill(started.pid)
  let assert Ok(process.ProcessDown(reason: process.Abnormal(_), ..)) = outcome
}

@external(erlang, "erlang", "suspend_process")
fn suspend_process(pid: process.Pid) -> Bool

@external(erlang, "erlang", "resume_process")
fn resume_process(pid: process.Pid) -> Bool

pub fn stale_connect_keeps_current_socket_test() {
  let server = nats_server.start([])
  let events = process.new_subject()
  let assert Ok(conn) =
    nats_server.config(server)
    |> nats.with_reconnect_delay(initial: 50, max: 200)
    |> nats.with_events(events)
    |> nats.connect
  assert process.receive(events, 1000) == Ok(nats.StatusChanged(nats.Connected))
  let server = nats_server.restart(server)
  assert process.receive(events, 5000)
    == Ok(nats.StatusChanged(nats.Reconnecting))
  assert process.receive(events, 10_000)
    == Ok(nats.StatusChanged(nats.Connected))

  nats.deliver_connect(
    conn,
    protocol.Generation(0),
    protocol.Server("127.0.0.1", server.port),
  )
  assert round_trip(conn)

  let assert Ok(stale) = mug.new("127.0.0.1", port: server.port) |> mug.connect
  nats.deliver_attempt(conn, protocol.Generation(0), stale)
  assert round_trip(conn)
  assert closed(stale) == mug.Closed

  nats_server.stop(server)
}

fn closed(socket: mug.Socket) -> mug.Error {
  case mug.receive(socket, timeout_milliseconds: 1000) {
    Ok(_) -> closed(socket)
    Error(error) -> error
  }
}

pub fn stalled_writes_reconnect_without_crashing_callers_test() {
  let server = nats_server.start([])
  let events = process.new_subject()
  let assert Ok(conn) =
    nats_server.config(server)
    |> nats.with_reconnect_delay(initial: 50, max: 200)
    |> nats.with_buffer_size(0)
    |> nats.with_events(events)
    |> nats.connect
  let assert Ok(nats.StatusChanged(nats.Connected)) =
    process.receive(events, 1000)

  nats_server.signal(server, "STOP")
  let published = process.new_subject()
  process.spawn(fn() {
    let payload = bit_array.from_string(string.repeat("x", 524_288))
    let result =
      list.try_each(list.repeat(Nil, 64), fn(_) {
        nats.publish(conn, "large", payload)
      })
    process.send(published, result)
  })
  let lost = process.receive(events, 2000)
  nats_server.signal(server, "CONT")
  let outcome = process.receive(published, 1000)
  let flushed = nats.flush(conn, timeout: 1000)
  let recovered = round_trip(conn)
  nats_server.stop(server)

  assert lost == Ok(nats.StatusChanged(nats.Reconnecting))
  assert outcome == Ok(Error(nats.BufferFull))
  assert flushed == Ok(Nil)
  assert recovered
}

pub fn slow_connect_attempt_keeps_connection_responsive_test() {
  let assert Ok(config) = nats.config("nats://192.0.2.1:4222")
  let assert Ok(started) =
    config |> nats.with_connect_timeout(1000) |> nats.start
  process.sleep(100)
  assert elapsed(fn() { nats.status(started.data) }) < 200

  let assert Error(nats.Unreachable(_)) = nats.connect(config)
  assert elapsed(fn() { nats.connect(config |> nats.with_connect_timeout(300)) })
    < 1000

  process.unlink(started.pid)
  process.kill(started.pid)
}

fn elapsed(run: fn() -> a) -> Int {
  let millisecond = atom.create("millisecond")
  let start = monotonic_time(millisecond)
  run()
  monotonic_time(millisecond) - start
}

@external(erlang, "erlang", "monotonic_time")
fn monotonic_time(unit: atom.Atom) -> Int

pub fn restarted_named_connection_ignores_old_callbacks_test() {
  let server = nats_server.start([])
  let events = process.new_subject()
  let config =
    nats_server.config(server)
    |> nats.with_reconnect_delay(initial: 3000, max: 3000)
    |> nats.with_connect_timeout(500)
    |> nats.with_events(events)
  use name, conn <- supervise(config)
  assert process.receive(events, 1000) == Ok(nats.StatusChanged(nats.Connected))

  nats_server.halt(server)
  assert process.receive(events, 5000)
    == Ok(nats.StatusChanged(nats.Reconnecting))
  let server = nats_server.restart(server)
  let assert Ok(old) = process.named(name)
  process.kill(old)
  await_restart(name, old, 100)
  assert process.receive(events, 1000) == Ok(nats.StatusChanged(nats.Connected))

  assert process.receive(events, 4000) == Error(Nil)
  assert round_trip(conn)
  nats_server.stop(server)
}

fn round_trip(conn: nats.Connection) -> Bool {
  let assert Ok(sub) = nats.subscribe(conn, "round.trip")
  let assert Ok(Nil) = nats.publish(conn, "round.trip", <<"ok":utf8>>)
  process.receive(nats.messages(sub), 1000)
  |> result.map(fn(message) { message.payload })
  == Ok(<<"ok":utf8>>)
}

pub fn invalid_timing_fails_startup_test() {
  let assert Ok(base) = nats.config("nats://127.0.0.1:1")
  let limit = 4_294_967_295
  use #(config, field) <- list.each([
    #(base |> nats.with_ping_interval(-1), "ping_interval"),
    #(base |> nats.with_ping_interval(0), "ping_interval"),
    #(base |> nats.with_ping_interval(limit + 1), "ping_interval"),
    #(base |> nats.with_connect_timeout(0), "connect_timeout"),
    #(base |> nats.with_connect_timeout(limit - 999), "connect_timeout"),
    #(
      base |> nats.with_reconnect_delay(initial: 0, max: 100),
      "reconnect_initial",
    ),
    #(
      base |> nats.with_reconnect_delay(initial: 200, max: 100),
      "reconnect_initial",
    ),
    #(
      base |> nats.with_reconnect_delay(initial: 1, max: limit - 1999),
      "reconnect_max",
    ),
  ])
  let assert Error(nats.InvalidConfig(field: rejected, ..)) =
    nats.connect(config)
  assert rejected == field
  let assert Error(actor.InitFailed(_)) = nats.start(config)
}

pub fn timing_limits_connect_test() {
  use server <- nats_server.with([])
  let limit = 4_294_967_295
  let assert Ok(conn) =
    nats_server.config(server)
    |> nats.with_ping_interval(limit)
    |> nats.with_connect_timeout(limit - 1000)
    |> nats.with_reconnect_delay(initial: 1000, max: 1000)
    |> nats.connect
  assert round_trip(conn)
}

pub fn server_without_headers_test() {
  use server <- nats_server.with(["-c", "test/fixtures/no_headers.conf"])
  let conn = connect(server)
  assert round_trip(conn)

  assert jetgleam.message("s", <<>>)
    |> jetgleam.set_header("A", "1")
    |> nats.publish_message(conn, _)
    == Error(nats.HeadersNotSupported)
  assert round_trip(conn)
}

pub fn escaped_url_credentials_test() {
  use server <- nats_server.with(["--user", "al@ice", "--pass", "p:ss"])
  let assert Ok(config) =
    nats.config(
      "nats://al%40ice:p%3Ass@127.0.0.1:" <> int.to_string(server.port),
    )
  let assert Ok(conn) = nats.connect(config)
  assert round_trip(conn)
}

fn supervise(
  config: nats.Config,
  run: fn(process.Name(nats.Command), nats.Connection) -> a,
) -> a {
  let name = process.new_name("nats")
  let assert Ok(started) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(nats.supervised(config, name:))
    |> static_supervisor.start
  let result = run(name, nats.named_connection(name))
  process.unlink(started.pid)
  process.kill(started.pid)
  result
}

fn await_restart(
  name: process.Name(nats.Command),
  old: process.Pid,
  attempts: Int,
) -> Nil {
  case process.named(name) {
    Ok(pid) if pid != old -> Nil
    _ if attempts > 0 -> {
      process.sleep(10)
      await_restart(name, old, attempts - 1)
    }
    _ -> panic as "connection was not restarted"
  }
}

pub fn reconnect_test() {
  let server = nats_server.start([])
  let events = process.new_subject()
  let assert Ok(conn) =
    nats_server.config(server)
    |> nats.with_reconnect_delay(initial: 50, max: 200)
    |> nats.with_events(events)
    |> nats.connect
  let assert Ok(sub) = nats.subscribe(conn, "again")
  let assert Ok(Nil) = nats.flush(conn, timeout: 1000)
  assert process.receive(events, 1000) == Ok(nats.StatusChanged(nats.Connected))

  nats_server.halt(server)
  assert process.receive(events, 5000)
    == Ok(nats.StatusChanged(nats.Reconnecting))
  let server = nats_server.restart(server)
  assert process.receive(events, 10_000)
    == Ok(nats.StatusChanged(nats.Connected))

  let other = connect(server)
  let assert Ok(Nil) = nats.publish(other, "again", <<"back":utf8>>)
  let assert Ok(received) = process.receive(nats.messages(sub), 2000)
  assert received.payload == <<"back":utf8>>

  nats_server.stop(server)
}

pub fn subscribe_denied_emits_warning_test() {
  use server <- nats_server.with(["-c", "test/fixtures/perms.conf"])
  let events = process.new_subject()
  let assert Ok(conn) =
    nats_server.config(server)
    |> nats.with_user_password(user: "u", password: "p")
    |> nats.with_events(events)
    |> nats.connect

  assert process.receive(events, 1000) == Ok(nats.StatusChanged(nats.Connected))
  let assert Ok(_) = nats.subscribe(conn, "secret")
  let assert Ok(nats.Warning(_)) = process.receive(events, 1000)
}

fn run(op: jetgleam.Operation(a), conn: nats.Connection) -> a {
  let assert Ok(value) = nats.execute(op, on: conn)
  value
}

fn publish(
  conn: nats.Connection,
  subject: String,
  payload: BitArray,
) -> jetgleam.PubAck {
  run(jetgleam.publish(jetgleam.message(subject, payload)), conn)
}

fn orders_settings(config: stream.Config) -> stream.Config {
  config
  |> stream.with_subjects(["orders.>"])
  |> stream.with_description("all orders")
  |> stream.with_retention(stream.Limits)
  |> stream.with_storage(stream.Memory)
  |> stream.with_discard(stream.New)
  |> stream.with_max_consumers(5)
  |> stream.with_max_messages(100)
  |> stream.with_max_bytes(1_000_000)
  |> stream.with_max_age(duration.hours(1))
  |> stream.with_max_messages_per_subject(10)
  |> stream.with_max_message_size(4096)
  |> stream.with_duplicate_window(duration.seconds(30))
  |> stream.with_allow_direct(True)
  |> stream.with_allow_rollup(True)
  |> stream.with_deny_delete(True)
}

pub fn stream_settings_round_trip_test() {
  use server <- nats_server.with([])
  let conn = connect(server)
  let config = stream.config("ORDERS", ["orders.>"]) |> orders_settings
  run(stream.create(config), conn)

  let initial = run(stream.info("ORDERS"), conn)
  assert orders_settings(initial.config) == initial.config

  let expected =
    initial.config
    |> stream.with_max_age(duration.hours(2))
    |> stream.with_subjects(["orders.>", "refunds.>"])
  let updated = run(stream.update(expected), conn)
  assert updated.config == expected
  assert run(stream.info("ORDERS"), conn).config == updated.config
  assert list.contains(run(stream.names(), conn), "ORDERS")
  let listed = run(stream.list(), conn)
  assert list.map(listed, fn(info) { info.created }) == [updated.created]

  assert nats.execute(
      stream.create(stream.config("ORDERS", ["other.>"])),
      on: conn,
    )
    == Error(nats.JetStream(jetgleam.AlreadyExists))

  run(stream.delete("ORDERS"), conn)
  assert nats.execute(stream.info("ORDERS"), on: conn)
    == Error(nats.JetStream(jetgleam.NotFound))
}

pub fn subject_transform_survives_description_update_test() {
  use server <- nats_server.with([])
  let conn = connect(server)
  let subject_transform =
    stream.SubjectTransform(
      source: "input.*",
      destination: "stored.{{wildcard(1)}}",
    )
  run(
    stream.config("TRANSFORM", ["input.*"])
      |> stream.with_storage(stream.Memory)
      |> stream.with_subject_transform(subject_transform)
      |> stream.create,
    conn,
  )

  let initial = run(stream.info("TRANSFORM"), conn)
  let before = publish(conn, "input.a", <<"before":utf8>>)
  assert before.sequence == 1
  let before_stored =
    run(stream.get_last_message("TRANSFORM", "stored.a"), conn)
  assert before_stored.subject == "stored.a"
  assert before_stored.payload == <<"before":utf8>>

  run(
    initial.config
      |> stream.with_description("description changed")
      |> stream.update,
    conn,
  )
  publish(conn, "input.b", <<"after":utf8>>)

  let after_stored = run(stream.get_last_message("TRANSFORM", "stored.b"), conn)
  assert after_stored.subject == "stored.b"
  assert after_stored.payload == <<"after":utf8>>
}

pub fn publish_and_get_message_test() {
  use server <- nats_server.with([])
  let conn = connect(server)
  run(
    stream.config("ORDERS", ["orders.>"])
      |> stream.with_storage(stream.Memory)
      |> stream.create,
    conn,
  )

  let first =
    jetgleam.message("orders.new", <<"one":utf8>>)
    |> jetgleam.set_header("Nats-Msg-Id", "a")
    |> jetgleam.set_header("Tag", "x")
  assert publish_message(conn, first).sequence == 1
  let again = publish_message(conn, first)
  assert again.sequence == 1
  assert again.duplicate
  assert publish(conn, "orders.new", <<"two":utf8>>).sequence == 2

  let stale =
    jetgleam.message("orders.new", <<"three":utf8>>)
    |> jetgleam.set_header("Nats-Expected-Last-Sequence", "99")
  assert nats.execute(jetgleam.publish(stale), on: conn)
    == Error(nats.JetStream(jetgleam.WrongLastSequence(option.Some(2))))

  let stored = run(stream.get_message(stream: "ORDERS", sequence: 1), conn)
  assert stored.payload == <<"one":utf8>>
  assert list.contains(stored.headers, #("Tag", "x"))
  let last = run(stream.get_last_message("ORDERS", "orders.new"), conn)
  assert last.payload == <<"two":utf8>>

  run(stream.purge("ORDERS"), conn)
  assert nats.execute(
      stream.get_message(stream: "ORDERS", sequence: 1),
      on: conn,
    )
    == Error(nats.JetStream(jetgleam.NotFound))
}

fn publish_message(
  conn: nats.Connection,
  message: jetgleam.Message,
) -> jetgleam.PubAck {
  run(jetgleam.publish(message), conn)
}

pub fn consumer_settings_round_trip_test() {
  use server <- nats_server.with([])
  let conn = connect(server)
  run(
    stream.config("ORDERS", ["orders.>"])
      |> stream.with_storage(stream.Memory)
      |> stream.create,
    conn,
  )
  let config =
    consumer.durable("worker")
    |> consumer.with_description("the worker")
    |> consumer.with_deliver_policy(consumer.DeliverNew)
    |> consumer.with_ack_policy(consumer.AckExplicit)
    |> consumer.with_ack_wait(duration.seconds(10))
    |> consumer.with_max_deliver(3)
    |> consumer.with_filter_subjects(["orders.new", "orders.paid"])
    |> consumer.with_replay_policy(consumer.ReplayOriginal)
    |> consumer.with_max_ack_pending(50)
    |> consumer.with_max_waiting(7)
    |> consumer.with_headers_only(True)
    |> consumer.with_replicas(1)
  run(consumer.create(config, stream: "ORDERS"), conn)

  assert run(consumer.info(stream: "ORDERS", consumer: "worker"), conn).config
    == config

  let ephemeral_config =
    consumer.ephemeral()
    |> consumer.with_name("temp")
    |> consumer.with_inactive_threshold(duration.seconds(30))
  let ephemeral = run(consumer.create(ephemeral_config, stream: "ORDERS"), conn)
  assert ephemeral.name == "temp"
  assert ephemeral.config == ephemeral_config
  let names = run(consumer.names("ORDERS"), conn)
  assert list.contains(names, "worker") && list.contains(names, "temp")
  let listed = run(consumer.list("ORDERS"), conn)
  assert list.sort(list.map(listed, fn(info) { info.name }), string.compare)
    == ["temp", "worker"]

  run(consumer.delete(stream: "ORDERS", consumer: "worker"), conn)
  assert nats.execute(
      consumer.info(stream: "ORDERS", consumer: "worker"),
      on: conn,
    )
    == Error(nats.JetStream(jetgleam.NotFound))
}

pub fn fetch_and_ack_test() {
  use server <- nats_server.with([])
  let conn = connect(server)
  run(
    stream.config("ORDERS", ["orders.>"])
      |> stream.with_storage(stream.Memory)
      |> stream.create,
    conn,
  )
  run(consumer.create(consumer.durable("worker"), stream: "ORDERS"), conn)
  let fetch = fn(max, wait) {
    consumer.fetch(
      stream: "ORDERS",
      consumer: "worker",
      max:,
      wait: duration.milliseconds(wait),
    )
    |> nats.fetch(on: conn)
  }

  assert fetch(10, 300) == Ok([])

  publish(conn, "orders.new", <<"1":utf8>>)
  publish(conn, "orders.new", <<"2":utf8>>)
  publish(conn, "orders.new", <<"3":utf8>>)
  let assert Ok([a, b]) = fetch(2, 1000)
  assert a.payload == <<"1":utf8>>
  let assert Ok(meta) = consumer.metadata(a)
  assert meta.stream == "ORDERS" && meta.consumer == "worker"
  run(consumer.ack(a, consumer.Ack), conn)
  run(consumer.ack_sync(b, consumer.Ack), conn)

  let assert Ok([third]) = fetch(10, 500)
  assert third.payload == <<"3":utf8>>
  run(consumer.ack(third, consumer.Nak), conn)

  let assert Ok([again]) = fetch(1, 1000)
  let assert Ok(meta) = consumer.metadata(again)
  assert meta.delivered == 2

  assert consumer.fetch(
      stream: "ORDERS",
      consumer: "worker",
      max: 1,
      wait: duration.seconds(1),
    )
    |> consumer.with_heartbeat(duration.milliseconds(450))
    |> nats.fetch(on: conn)
    == Ok([])

  run(consumer.delete(stream: "ORDERS", consumer: "worker"), conn)
  let assert Error(nats.FetchError(received: [], error: nats.JetStream(_))) =
    fetch(1, 500)
}

pub fn fetch_timeout_keeps_partial_batch_test() {
  let server = nats_server.start([])
  let conn = connect(server)
  run(
    stream.config("ORDERS", ["orders.>"])
      |> stream.with_storage(stream.Memory)
      |> stream.create,
    conn,
  )
  run(consumer.create(consumer.durable("worker"), stream: "ORDERS"), conn)
  publish(conn, "orders.new", <<"1":utf8>>)

  process.spawn(fn() {
    process.sleep(150)
    nats_server.signal(server, "STOP")
    process.sleep(700)
    nats_server.signal(server, "CONT")
  })
  assert nats_server.leftover_messages(fn() {
      let assert Error(nats.FetchError([message], nats.Timeout)) =
        consumer.fetch(
          stream: "ORDERS",
          consumer: "worker",
          max: 5,
          wait: duration.milliseconds(500),
        )
        |> nats.fetch(on: conn)
      assert message.payload == <<"1":utf8>>
      process.sleep(500)
    })
    == 0

  nats_server.stop(server)
}

pub fn fetch_deadline_includes_setup_test() {
  use server <- nats_server.with([])
  let assert Ok(started) = nats_server.config(server) |> nats.start
  let conn = started.data
  let assert Ok(Nil) = nats.flush(conn, timeout: 2000)
  run(
    stream.config("ORDERS", ["orders.>"])
      |> stream.with_storage(stream.Memory)
      |> stream.create,
    conn,
  )
  run(consumer.create(consumer.durable("worker"), stream: "ORDERS"), conn)

  let suspended = process.new_subject()
  process.spawn(fn() {
    suspend_process(started.pid)
    process.send(suspended, Nil)
    process.sleep(300)
    resume_process(started.pid)
  })
  let assert Ok(Nil) = process.receive(suspended, 1000)
  let millisecond = atom.create("millisecond")
  let start = monotonic_time(millisecond)
  let result =
    consumer.fetch(
      stream: "ORDERS",
      consumer: "worker",
      max: 1,
      wait: duration.milliseconds(500),
    )
    |> nats.fetch(on: conn)
  let elapsed = monotonic_time(millisecond) - start
  assert result == Error(nats.FetchError([], nats.Timeout))
  assert elapsed < 650

  process.unlink(started.pid)
  process.kill(started.pid)
}

fn bucket(conn: nats.Connection, storage: stream.Storage) -> Nil {
  kv.config("b")
  |> kv.with_history(5)
  |> kv.with_storage(storage)
  |> kv.create_bucket
  |> run(conn)
}

pub fn key_operations_test() {
  use server <- nats_server.with([])
  let conn = connect(server)
  bucket(conn, stream.Memory)

  assert run(kv.put(bucket: "b", key: "k", value: <<"v1":utf8>>), conn) == 1
  assert run(kv.get(bucket: "b", key: "k"), conn).value == <<"v1":utf8>>
  kv.update(bucket: "b", key: "k", value: <<"x":utf8>>, revision: 99)
  |> assert_wrong_sequence(conn)
  kv.create(bucket: "b", key: "k", value: <<"x":utf8>>)
  |> assert_wrong_sequence(conn)

  run(kv.delete(bucket: "b", key: "k"), conn)
  assert nats.execute(kv.get(bucket: "b", key: "k"), on: conn)
    == Error(nats.JetStream(jetgleam.NotFound))
  let assert Ok(_) =
    nats.execute(
      kv.create(bucket: "b", key: "k", value: <<"v2":utf8>>),
      on: conn,
    )

  run(kv.purge(bucket: "b", key: "k"), conn)
  assert nats.execute(kv.get(bucket: "b", key: "k"), on: conn)
    == Error(nats.JetStream(jetgleam.NotFound))

  assert nats.execute(
      kv.config("b") |> kv.with_history(3) |> kv.create_bucket,
      on: conn,
    )
    == Error(nats.JetStream(jetgleam.AlreadyExists))
  run(kv.delete_bucket("b"), conn)
  assert nats.execute(kv.get(bucket: "b", key: "k"), on: conn)
    == Error(nats.JetStream(jetgleam.NotFound))
}

pub fn dotted_bucket_cannot_alias_keys_test() {
  use server <- nats_server.with([])
  let conn = connect(server)
  bucket(conn, stream.Memory)
  run(kv.put(bucket: "b", key: "ui.theme", value: <<"old":utf8>>), conn)

  let assert Error(nats.JetStream(jetgleam.InvalidArgument("bucket", _))) =
    nats.execute(
      kv.put(bucket: "b.ui", key: "theme", value: <<"new":utf8>>),
      on: conn,
    )
  assert run(kv.get(bucket: "b", key: "ui.theme"), conn).value == <<"old":utf8>>
}

fn assert_wrong_sequence(
  op: jetgleam.Operation(Int),
  conn: nats.Connection,
) -> Nil {
  let assert Error(nats.JetStream(jetgleam.WrongLastSequence(option.Some(_)))) =
    nats.execute(op, on: conn)
  Nil
}

pub fn collect_test() {
  use server <- nats_server.with([])
  let conn = connect(server)
  bucket(conn, stream.Memory)

  assert nats.collect(kv.keys("b"), on: conn, timeout: 5000) == Ok([])

  run(kv.put(bucket: "b", key: "a", value: <<"1":utf8>>), conn)
  run(kv.put(bucket: "b", key: "b", value: <<"2":utf8>>), conn)
  run(kv.delete(bucket: "b", key: "b"), conn)
  let assert Ok(keys) = nats.collect(kv.keys("b"), on: conn, timeout: 5000)
  assert list.map(keys, fn(entry) { #(entry.key, entry.value) })
    == [#("a", <<>>)]

  run(kv.put(bucket: "b", key: "a", value: <<"2":utf8>>), conn)
  run(kv.put(bucket: "b", key: "a", value: <<"3":utf8>>), conn)
  let assert Ok(history) =
    nats.collect(kv.history(bucket: "b", key: "a"), on: conn, timeout: 5000)
  let revisions = list.map(history, fn(entry) { entry.revision })
  assert list.length(revisions) == 3
  assert revisions == list.sort(revisions, by: int.compare)
}

pub fn snapshot_gap_keeps_latest_revision_of_every_key_test() {
  use server <- nats_server.with([])
  let conn = connect(server)
  bucket(conn, stream.Memory)
  list.each(["a", "b", "b", "b", "c"], fn(key) {
    run(kv.put(bucket: "b", key:, value: <<"v">>), conn)
  })

  let inbox = nats.new_inbox(conn)
  let assert Ok(sub) = nats.subscribe(conn, inbox)
  let #(tracker, operation) = kv.track(kv.keys("b"), inbox)
  let assert #(tracker, []) = kv.consumer_created(tracker, run(operation, conn))
  let assert Ok(first) = process.receive(nats.messages(sub), 1000)
  let assert #(tracker, [kv.Deliver(first)]) = kv.observe(tracker, first)
  let assert Ok(_) = process.receive(nats.messages(sub), 1000)
  let assert Ok(after_gap) = process.receive(nats.messages(sub), 1000)
  let assert #(tracker, [kv.Recreate]) = kv.observe(tracker, after_gap)
  nats.unsubscribe(sub)

  let inbox = nats.new_inbox(conn)
  let assert Ok(sub) = nats.subscribe(conn, inbox)
  let #(tracker, operation) = kv.recreate(tracker, inbox)
  let assert #(tracker, []) = kv.consumer_created(tracker, run(operation, conn))
  let entries = finish_replay(sub, tracker, [first])
  nats.unsubscribe(sub)
  assert list.map(entries, fn(entry) { #(entry.key, entry.revision) })
    == [#("a", 1), #("b", 4), #("c", 5)]
}

fn finish_replay(
  sub: nats.Subscription(jetgleam.Message),
  tracker: kv.Tracker,
  entries: List(kv.Entry),
) -> List(kv.Entry) {
  let assert Ok(message) = process.receive(nats.messages(sub), 1000)
  let #(tracker, steps) = kv.observe(tracker, message)
  let entries =
    list.fold(steps, entries, fn(entries, step) {
      case step {
        kv.Deliver(entry) -> [entry, ..entries]
        kv.CaughtUp | kv.Respond(_) | kv.Recreate -> entries
      }
    })
  case list.contains(steps, kv.CaughtUp) {
    True -> list.reverse(entries)
    False -> finish_replay(sub, tracker, entries)
  }
}

pub fn watch_test() {
  use server <- nats_server.with([])
  let conn = connect(server)
  bucket(conn, stream.Memory)

  let assert Ok(empty) = nats.watch(kv.watch("b", filter: ">"), on: conn)
  assert process.receive(nats.messages(empty), 5000) == Ok(nats.CaughtUp)
  nats.unsubscribe(empty)

  run(kv.put(bucket: "b", key: "a", value: <<"1":utf8>>), conn)
  let assert Ok(sub) = nats.watch(kv.watch("b", filter: ">"), on: conn)
  let assert Ok(nats.Changed(entry)) = process.receive(nats.messages(sub), 5000)
  assert entry.key == "a"
  assert process.receive(nats.messages(sub), 5000) == Ok(nats.CaughtUp)

  run(kv.put(bucket: "b", key: "a", value: <<"2":utf8>>), conn)
  let assert Ok(nats.Changed(entry)) = process.receive(nats.messages(sub), 5000)
  assert entry.value == <<"2":utf8>>
  run(kv.delete(bucket: "b", key: "a"), conn)
  let assert Ok(nats.Changed(entry)) = process.receive(nats.messages(sub), 5000)
  assert entry.change == kv.Delete

  nats.unsubscribe(sub)
  process.sleep(200)
  run(kv.put(bucket: "b", key: "a", value: <<"3":utf8>>), conn)
  assert process.receive(nats.messages(sub), 500) == Error(Nil)
}

pub fn watch_survives_restart_test() {
  let server = nats_server.start([])
  let conn = connect(server)
  bucket(conn, stream.File)
  let assert Ok(sub) = nats.watch(kv.watch("b", filter: ">"), on: conn)
  assert process.receive(nats.messages(sub), 5000) == Ok(nats.CaughtUp)

  let server = nats_server.restart(server)
  let assert Ok(Nil) = wait_put(conn, 150)
  let assert Ok(nats.Changed(entry)) =
    process.receive(nats.messages(sub), 15_000)
  assert entry.key == "a"

  nats_server.stop(server)
}

pub fn collect_leaves_no_messages_test() {
  use server <- nats_server.with([])
  let conn = connect(server)
  bucket(conn, stream.Memory)
  run(kv.put(bucket: "b", key: "a", value: <<"0":utf8>>), conn)

  let started = process.new_subject()
  process.spawn(fn() {
    let stop = process.new_subject()
    process.send(started, stop)
    write(conn, stop, 1)
  })
  let assert Ok(writing) = process.receive(started, 1000)
  assert nats_server.leftover_messages(fn() {
      let assert Ok(_) =
        nats.collect(kv.watch("b", filter: ">"), on: conn, timeout: 5000)
    })
    == 0
  process.send(writing, Nil)
}

fn write(conn: nats.Connection, stop: process.Subject(Nil), n: Int) -> Nil {
  case process.receive(stop, 0) {
    Ok(Nil) -> Nil
    Error(Nil) -> {
      let _ =
        nats.execute(
          kv.put(bucket: "b", key: "a", value: <<int.to_string(n):utf8>>),
          on: conn,
        )
      write(conn, stop, n + 1)
    }
  }
}

fn wait_put(conn: nats.Connection, attempts: Int) -> Result(Nil, Nil) {
  case
    nats.execute(kv.put(bucket: "b", key: "a", value: <<"1":utf8>>), on: conn)
  {
    Ok(_) -> Ok(Nil)
    Error(_) if attempts > 0 -> {
      process.sleep(100)
      wait_put(conn, attempts - 1)
    }
    Error(_) -> Error(Nil)
  }
}

pub fn watch_fails_when_bucket_deleted_test() {
  use server <- nats_server.with([])
  let conn = connect(server)
  bucket(conn, stream.Memory)
  let assert Ok(sub) = nats.watch(kv.watch("b", filter: ">"), on: conn)
  assert process.receive(nats.messages(sub), 5000) == Ok(nats.CaughtUp)

  run(kv.delete_bucket("b"), conn)
  assert process.receive(nats.messages(sub), 15_000)
    == Ok(nats.Failed(nats.JetStream(jetgleam.NotFound)))
}

type Event {
  Subscribed(inbox: String, messages: process.Subject(jetgleam.Message))
  Unsubscribed(inbox: String)
  Entry(revision: Int)
  CaughtUp
  Failed(String)
  RecreationWaiting(release: process.Subject(Nil))
}

fn fake_io(events: process.Subject(Event)) -> nats.WatchIo(String) {
  nats.WatchIo(
    subscribe: fn(inbox) {
      let messages = process.new_subject()
      process.send(events, Subscribed(inbox, messages))
      Ok(#(messages, fn() { process.send(events, Unsubscribed(inbox)) }))
    },
    publish: fn(_) { Ok(Nil) },
    execute: fn(_) {
      Ok(consumer.Info(
        stream: "KV_b",
        name: "c",
        config: consumer.ephemeral(),
        delivered: consumer.SequencePair(0, 0),
        ack_floor: consumer.SequencePair(0, 0),
        pending: 1,
        ack_pending: 0,
        redelivered: 0,
        waiting: 0,
      ))
    },
    new_inbox: fn() { "_INBOX." <> int.to_string(int.random(1_000_000_000)) },
    fatal: fn(_) { False },
  )
}

fn delivery(stream: Int, consumer: Int) -> jetgleam.Message {
  let reply =
    ["$JS.ACK.KV_b.c.1", int.to_string(stream), int.to_string(consumer), "0.0"]
    |> string.join(".")
  jetgleam.message("$KV.b.k", <<>>) |> jetgleam.set_reply_to(reply)
}

fn heartbeat(last_consumer: Int) -> jetgleam.Message {
  jetgleam.Message(
    ..jetgleam.message("_INBOX", <<>>),
    status: option.Some(jetgleam.Status(100, option.None)),
  )
  |> jetgleam.set_header("Nats-Last-Consumer", int.to_string(last_consumer))
}

fn recreates(operation: jetgleam.Operation(consumer.Info)) -> Bool {
  let assert Error(message) =
    jetgleam.run(
      operation,
      publish: fn(message) { Error(message) },
      request: fn(message, _) { Error(message) },
      error: fn(_) { panic as "operation finished without sending" },
    )
  let assert Ok(payload) = bit_array.to_string(message.payload)
  string.contains(payload, "by_start_sequence")
}

fn flow_control() -> jetgleam.Message {
  jetgleam.Message(
    ..jetgleam.message("_INBOX", <<>>),
    reply_to: option.Some("$JS.FC.x"),
    status: option.Some(jetgleam.Status(100, option.None)),
  )
}

fn start_watch(
  events: process.Subject(Event),
  io: nats.WatchIo(String),
) -> Result(fn() -> Nil, String) {
  nats.start_watch(
    kv.watch("b", filter: ">"),
    io,
    fn(entry) { process.send(events, Entry(entry.revision)) },
    fn() { process.send(events, CaughtUp) },
    fn(error) { process.send(events, Failed(error)) },
  )
}

fn next(events: process.Subject(Event)) -> Event {
  let assert Ok(event) = process.receive(events, 1000)
  event
}

pub fn recreation_ignores_previous_generation_test() {
  let events = process.new_subject()
  let assert Ok(stop) =
    nats.start_watch(
      kv.watch("b", filter: ">"),
      fake_io(events),
      fn(entry) { process.send(events, Entry(entry.revision)) },
      fn() { process.send(events, CaughtUp) },
      fn(_) { panic as "watch failed" },
    )
  let assert Subscribed(old_inbox, old) = next(events)

  process.send(old, delivery(10, 1))
  assert next(events) == Entry(10)
  assert next(events) == CaughtUp

  process.send(old, delivery(12, 3))
  let assert Subscribed(new_inbox, new) = next(events)
  assert new_inbox != old_inbox
  assert next(events) == Unsubscribed(old_inbox)

  process.send(old, delivery(11, 2))
  process.send(old, heartbeat(9))
  assert process.receive(events, 100) == Error(Nil)

  process.send(new, delivery(11, 1))
  process.send(new, delivery(12, 2))
  process.send(new, heartbeat(2))
  assert next(events) == Entry(11)
  assert next(events) == Entry(12)
  assert process.receive(events, 100) == Error(Nil)

  stop()
  assert next(events) == Unsubscribed(new_inbox)
}

pub fn recreation_drains_retired_inbox_before_live_delivery_test() {
  let events = process.new_subject()
  let base = fake_io(events)
  let io =
    nats.WatchIo(..base, execute: fn(operation) {
      case recreates(operation) {
        True -> {
          let release = process.new_subject()
          process.send(events, RecreationWaiting(release))
          let assert Ok(Nil) = process.receive(release, 5000)
          base.execute(operation)
        }
        False -> base.execute(operation)
      }
    })
  let assert Ok(stop) =
    nats.start_watch(
      kv.watch("b", filter: ">"),
      io,
      fn(entry) {
        let remaining =
          process.new_selector()
          |> process.select_other(fn(_) { Nil })
          |> process.selector_receive(0)
        assert remaining == Error(Nil)
        process.send(events, Entry(entry.revision))
      },
      fn() { process.send(events, CaughtUp) },
      fn(_) { panic as "watch failed" },
    )
  let assert Subscribed(old_inbox, old) = next(events)

  process.send(old, delivery(10, 1))
  assert next(events) == Entry(10)
  assert next(events) == CaughtUp

  process.send(old, delivery(12, 3))
  let assert Subscribed(new_inbox, new) = next(events)
  assert new_inbox != old_inbox
  let assert RecreationWaiting(release) = next(events)

  list.each(list.repeat(Nil, 1000), fn(_) {
    process.send(old, delivery(100, 100))
  })
  process.send(release, Nil)

  assert next(events) == Unsubscribed(old_inbox)

  process.send(new, delivery(11, 1))
  assert next(events) == Entry(11)
  process.send(new, delivery(12, 2))
  assert next(events) == Entry(12)

  stop()
  assert next(events) == Unsubscribed(new_inbox)
}

pub fn receiver_exit_stops_watch_test() {
  let events = process.new_subject()
  process.spawn_unlinked(fn() {
    let assert Ok(_) =
      nats.start_watch(
        kv.watch("b", filter: ">"),
        fake_io(events),
        fn(_) { Nil },
        fn() { Nil },
        fn(_) { Nil },
      )
  })
  let assert Subscribed(inbox, _) = next(events)
  assert next(events) == Unsubscribed(inbox)
}

pub fn failed_create_unsubscribes_test() {
  let events = process.new_subject()
  let io = nats.WatchIo(..fake_io(events), execute: fn(_) { Error("boom") })
  assert start_watch(events, io) == Error("boom")
  let assert Subscribed(inbox, _) = next(events)
  assert next(events) == Unsubscribed(inbox)
}

pub fn fatal_error_fails_watch_test() {
  let events = process.new_subject()
  let io =
    nats.WatchIo(
      ..fake_io(events),
      publish: fn(_) { Error("closed") },
      fatal: fn(_) { True },
    )
  let assert Ok(_) = start_watch(events, io)
  let assert Subscribed(inbox, messages) = next(events)
  process.send(messages, flow_control())
  assert next(events) == Failed("closed")
  assert next(events) == Unsubscribed(inbox)
}

pub fn non_fatal_error_keeps_watching_test() {
  let events = process.new_subject()
  let io = nats.WatchIo(..fake_io(events), publish: fn(_) { Error("busy") })
  let assert Ok(stop) = start_watch(events, io)
  let assert Subscribed(inbox, messages) = next(events)
  process.send(messages, flow_control())
  process.send(messages, delivery(10, 1))
  assert next(events) == Entry(10)
  assert next(events) == CaughtUp
  stop()
  assert next(events) == Unsubscribed(inbox)
}

pub fn failed_recreate_keeps_old_consumer_test() {
  let events = process.new_subject()
  let base = fake_io(events)
  let io =
    nats.WatchIo(..base, execute: fn(operation) {
      case recreates(operation) {
        True -> Error("busy")
        False -> base.execute(operation)
      }
    })
  let assert Ok(stop) = start_watch(events, io)
  let assert Subscribed(old_inbox, old) = next(events)
  process.send(old, delivery(10, 1))
  assert next(events) == Entry(10)
  assert next(events) == CaughtUp

  process.send(old, delivery(12, 3))
  let assert Subscribed(new_inbox, _) = next(events)
  assert next(events) == Unsubscribed(new_inbox)
  process.send(old, delivery(11, 2))
  assert next(events) == Entry(11)

  stop()
  assert next(events) == Unsubscribed(old_inbox)
}

pub fn replaced_timer_ignores_stale_callback_test() {
  let fired = process.new_subject()
  let start = fn(running) {
    nats.start_timer(running, "t", 0, to: fired, message: fn(token) { token })
  }
  let running = start(dict.new())
  let assert Ok(a) = process.receive(fired, 100)
  let running = start(running)
  let assert Ok(b) = process.receive(fired, 100)

  assert nats.claim_timer(running, "t", a) == Error(Nil)
  let assert Ok(after_b) = nats.claim_timer(running, "t", b)
  assert after_b == dict.new()
  assert nats.claim_timer(after_b, "t", b) == Error(Nil)
}
