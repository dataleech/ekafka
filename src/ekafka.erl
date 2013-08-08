-module(ekafka).

%% API
-export([start/0]).
-export([produce/2, produce/3]).

start() ->
    ok = application:start(ezk),
    ok = application:start(ekafka).

-spec produce(binary(), [binary()]) -> ok.
produce(Topic, Messages) ->
    produce(Topic, 0, Messages).

-spec produce(binary(), non_neg_integer(), [binary()]) -> ok.
produce(Topic, Partition, Messages) ->
    F = fun(W) ->
        gen_server:cast(W, {produce, Topic, Partition, Messages})
    end,
    poolboy:transaction(pool(), F).

%% Internal functions
pool() ->
    Pools = [Name || {Name, _, _, _} <- supervisor:which_children(ekafka_sup)],
    lists:nth(random:uniform(length(Pools)), Pools).
