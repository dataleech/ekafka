-module(ekafka).

%% API
-export([start/0]).

start() ->
    ok = application:start(ezk),
    ok = application:start(ekafka).
