import gleam/bool
import gleam/int
import gleam/list
import gleam/result
import gleam/string
import thirtytwo

pub type Seed {
  Seed(key_type: KeyType, raw: BitArray)
}

pub type KeyType {
  Account
  Cluster
  Server
  Operator
  User
}

fn prefix_bits(key_type: KeyType) -> Int {
  case key_type {
    Account -> 0
    Cluster -> 2
    Server -> 13
    Operator -> 14
    User -> 20
  }
}

fn key_type_from(bits: Int) -> Result(KeyType, Nil) {
  [Account, Cluster, Server, Operator, User]
  |> list.find(fn(key_type) { prefix_bits(key_type) == bits })
}

pub type Signer {
  Signer(public_key: String, sign: fn(BitArray) -> BitArray)
}

const seed_prefix = 18

pub fn from_seed(seed: String) -> Result(Seed, Nil) {
  use decoded <- result.try(thirtytwo.decode(string.trim(seed)))
  use #(head, crc) <- result.try(case decoded {
    <<head:bytes-size(34), crc:16-little>> -> Ok(#(head, crc))
    _ -> Error(Nil)
  })
  use <- bool.guard(when: crc != crc16(head), return: Error(Nil))
  case head {
    <<prefix:5, key_type:5, 0:6, raw:bytes>> if prefix == seed_prefix -> {
      use key_type <- result.map(key_type_from(key_type))
      Seed(key_type:, raw:)
    }
    _ -> Error(Nil)
  }
}

pub fn public_key(seed: Seed, raw_public_key: BitArray) -> String {
  let body = <<prefix_bits(seed.key_type):5, 0:3, raw_public_key:bits>>
  thirtytwo.encode(<<body:bits, crc16(body):16-little>>, padding: False)
}

pub fn from_credentials(contents: String) -> Result(#(String, Seed), Nil) {
  let lines = string.split(contents, "\n") |> list.map(string.trim)
  use jwt <- result.try(section(lines, "NATS USER JWT"))
  use seed <- result.try(section(lines, "USER NKEY SEED"))
  use key <- result.map(from_seed(seed))
  #(jwt, key)
}

fn section(lines: List(String), name: String) -> Result(String, Nil) {
  let marker = fn(line, word) {
    string.starts_with(line, "---")
    && string.replace(line, "-", "") == word <> " " <> name
  }
  let after =
    list.drop_while(lines, fn(line) { !marker(line, "BEGIN") }) |> list.drop(1)
  let body = list.take_while(after, fn(line) { !marker(line, "END") })
  let closed = list.length(body) < list.length(after)
  case closed, list.filter(body, fn(line) { line != "" }) {
    True, [value] -> Ok(value)
    _, _ -> Error(Nil)
  }
}

// CRC-16/XMODEM
fn crc16(bytes: BitArray) -> Int {
  crc16_loop(bytes, 0)
}

fn crc16_loop(bytes: BitArray, crc: Int) -> Int {
  case bytes {
    <<byte, rest:bits>> ->
      crc16_loop(rest, crc16_byte(int.bitwise_exclusive_or(crc, byte * 256), 8))
    _ -> crc
  }
}

fn crc16_byte(crc: Int, bits_left: Int) -> Int {
  use <- bool.guard(when: bits_left == 0, return: crc)
  let shifted = int.bitwise_and(crc * 2, 0xFFFF)
  case int.bitwise_and(crc, 0x8000) {
    0 -> crc16_byte(shifted, bits_left - 1)
    _ -> crc16_byte(int.bitwise_exclusive_or(shifted, 0x1021), bits_left - 1)
  }
}
