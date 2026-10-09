-module(jetgleam_erlang_ffi).
-export([ed25519_public_key/1, ed25519_sign/2, set_socket_options/2]).

ed25519_public_key(Seed) ->
    {Pub, _} = crypto:generate_key(eddsa, ed25519, Seed),
    Pub.

ed25519_sign(Seed, Data) ->
    crypto:sign(eddsa, none, Data, [Seed, ed25519]).

set_socket_options(Socket, WriteTimeout) ->
    case inet:setopts(Socket, [{nodelay, true},
                              {send_timeout, WriteTimeout},
                              {send_timeout_close, true}]) of
        ok -> {ok, nil};
        {error, Reason} -> {error, Reason}
    end.
