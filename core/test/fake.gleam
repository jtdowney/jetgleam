import gleam/bit_array
import gleam/json.{type Json}
import jetgleam

pub fn run(
  operation: jetgleam.Operation(a),
  respond: fn(jetgleam.Message) -> jetgleam.Message,
) -> Result(a, jetgleam.Error) {
  jetgleam.run(
    operation,
    publish: fn(_) { Ok(Nil) },
    request: fn(message, _) { Ok(respond(message)) },
    error: fn(error) { error },
  )
}

pub fn sent(operation: jetgleam.Operation(a)) -> jetgleam.Message {
  let assert Error(message) =
    jetgleam.run(
      operation,
      publish: fn(message) { Error(message) },
      request: fn(message, _) { Error(message) },
      error: fn(_) { panic as "operation finished without sending" },
    )
  message
}

pub fn no_io(_: jetgleam.Message) -> jetgleam.Message {
  panic as "no request expected"
}

pub fn reply(body: Json) -> jetgleam.Message {
  jetgleam.message("_INBOX.reply", bit_array.from_string(json.to_string(body)))
}

pub fn error_reply(code: Int, err_code: Int, description: String) -> Json {
  json.object([
    #(
      "error",
      json.object([
        #("code", json.int(code)),
        #("err_code", json.int(err_code)),
        #("description", json.string(description)),
      ]),
    ),
  ])
}
