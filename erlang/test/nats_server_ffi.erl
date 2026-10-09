-module(nats_server_ffi).
-export([free_port/0, new_store/0, remove_store/1, start/3, stop/2, signal/2]).

free_port() ->
    {ok, Socket} = gen_tcp:listen(0, []),
    {ok, Port} = inet:port(Socket),
    gen_tcp:close(Socket),
    Port.

new_store() ->
    list_to_binary(string:trim(os:cmd("mktemp -d \"${TMPDIR:-/tmp}/jetgleam.XXXXXX\""))).

remove_store(Dir) ->
    _ = file:del_dir_r(Dir),
    nil.

start(Port, Dir, Args) ->
    Nats = case os:find_executable("nats-server") of
        false -> erlang:error({nats_server_not_found,
                               "install nats-server and put it on PATH"});
        Path -> Path
    end,
    Log = filename:join(binary_to_list(Dir), "server.log"),
    Script = "log=$1; shift; \"$0\" \"$@\" >>\"$log\" 2>&1 & pid=$!; "
             "read _; kill $pid",
    ServerArgs = ["-js", "-a", "127.0.0.1", "-p", integer_to_list(Port),
                  "-sd", binary_to_list(Dir) | [binary_to_list(A) || A <- Args]],
    Handle = open_port({spawn_executable, "/bin/sh"},
                       [{args, ["-c", Script, Nats, Log | ServerArgs]}]),
    case wait(Port, true, 250) of
        ok -> Handle;
        timeout ->
            try port_close(Handle) catch _:_ -> ok end,
            {_, Output} = file:read_file(Log),
            erlang:error({nats_server_did_not_start, Port, Output})
    end.

signal(Port, Signal) ->
    os:cmd("pkill -" ++ binary_to_list(Signal) ++ " -f 'nats-server .*-p "
           ++ integer_to_list(Port) ++ " '"),
    nil.

stop(Handle, Port) ->
    try port_close(Handle) catch _:_ -> ok end,
    case wait(Port, false, 250) of
        ok -> nil;
        timeout -> erlang:error({nats_server_did_not_stop, Port})
    end.

wait(Port, Up, Tries) ->
    Reachable = case gen_tcp:connect({127, 0, 0, 1}, Port, [], 100) of
        {ok, Socket} -> gen_tcp:close(Socket), true;
        {error, _} -> false
    end,
    if
        Reachable =:= Up -> ok;
        Tries =:= 0 -> timeout;
        true -> timer:sleep(20), wait(Port, Up, Tries - 1)
    end.
