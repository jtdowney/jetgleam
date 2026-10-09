# jetgleam

A NATS and JetStream client for Gleam, split into two packages:

| Package                                 | Target | What it does                                                 |
| --------------------------------------- | ------ | ------------------------------------------------------------ |
| [`core`](core/) (`jetgleam`)            | any    | Messages, and JetStream and KV operations as values. No I/O. |
| [`erlang`](erlang/) (`jetgleam_erlang`) | erlang | Connects to the server and runs those values.                |
