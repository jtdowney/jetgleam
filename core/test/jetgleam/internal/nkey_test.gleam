import gleam/list
import gleam/string
import jetgleam/internal/nkey

const seed = "SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY"

const public_key = "UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4"

const raw_public_key = <<
  0xef, 0x4e, 0x44, 0x52, 0x4b, 0x72, 0xe4, 0x40, 0xf9, 0x6d, 0xae, 0x7b, 0xba,
  0x23, 0x7c, 0x66, 0x66, 0xa4, 0x07, 0x01, 0xcb, 0x13, 0x65, 0xa4, 0x22, 0x44,
  0xf8, 0x4e, 0x7a, 0x67, 0xda, 0xb1,
>>

pub fn public_key_encodes_with_seed_type_test() {
  let assert Ok(decoded) = nkey.from_seed("  " <> seed <> "\n")
  assert nkey.public_key(decoded, raw_public_key) == public_key
}

pub fn from_seed_rejects_invalid_test() {
  let flipped = string.slice(seed, 0, 10) <> "A" <> string.drop_start(seed, 11)
  assert nkey.from_seed(flipped) == Error(Nil)
  assert nkey.from_seed(public_key) == Error(Nil)
  assert nkey.from_seed("not a seed!") == Error(Nil)
}

const account_seed = "SAACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCTIMI"

pub fn from_seed_requires_canonical_signing_seed_test() {
  let assert Ok(account) = nkey.from_seed(account_seed)
  assert string.starts_with(nkey.public_key(account, raw_public_key), "A")

  use invalid <- list.each([
    seed <> "A",
    "SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VZ",
    "SXACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCUYBM",
    "SBACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCTJAE",
    "SUASSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCSHEM",
  ])
  assert nkey.from_seed(invalid) == Error(Nil)
}

fn creds(jwt: String, seed: String, newline: String) -> String {
  [
    "-----BEGIN NATS USER JWT-----",
    jwt,
    "------END NATS USER JWT------",
    "",
    "************************* IMPORTANT *************************",
    "NKEY Seed printed below can be used to sign and prove identity.",
    "",
    "-----BEGIN USER NKEY SEED-----",
    seed,
    "------END USER NKEY SEED------",
    "",
  ]
  |> string.join(newline)
}

pub fn from_credentials_reads_bounded_sections_test() {
  let assert Ok(expected) = nkey.from_seed(seed)
  assert nkey.from_credentials(creds("the.jwt", seed, "\n"))
    == Ok(#("the.jwt", expected))
  assert nkey.from_credentials(creds("the.jwt", seed, "\r\n"))
    == Ok(#("the.jwt", expected))
  assert nkey.from_credentials(
      "Paste after the BEGIN NATS USER JWT line\n"
      <> creds("the.jwt", seed, "\n"),
    )
    == Ok(#("the.jwt", expected))

  assert nkey.from_credentials(creds("", seed, "\n")) == Error(Nil)
  assert nkey.from_credentials(creds("the.jwt", "", "\n")) == Error(Nil)
  let unterminated =
    "-----BEGIN NATS USER JWT-----\nthe.jwt\n"
    <> "-----BEGIN USER NKEY SEED-----\n"
    <> seed
    <> "\n------END USER NKEY SEED------\n"
  assert nkey.from_credentials(unterminated) == Error(Nil)
}
