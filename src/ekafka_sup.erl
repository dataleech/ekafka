-module(ekafka_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% supervisor callbacks
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    PoolSpecs = lists:map(fun({Id, IP, Port}) ->
        PoolName = list_to_atom("broker_" ++ Id),
        PoolArg = [
            {name, {local, PoolName}},
            {worker_module, ekafka_client}
        ] ++ application:get_env(ekafka, pool, []),
        poolboy:child_spec(PoolName, PoolArg, [IP, Port])
    end, brokers()),
    {ok, {{one_for_one, 5, 10}, PoolSpecs}}.

brokers() ->
    {ok, C} = ezk:start_connection(),
    {ok, Ids} = ezk:ls(C, "/brokers/ids"),
    Brokers = lists:foldr(
        fun(Id, Acc) ->
            {ok, {B1, _}} = ezk:get(C, "/brokers/ids/" ++ binary_to_list(Id)),
            StrIP = lists:nth(2, string:tokens(binary_to_list(B1), ":")),
            Port = lists:nth(3, string:tokens(binary_to_list(B1), ":")),
            {ok, IP} = inet_parse:address(StrIP),
            [{binary_to_list(Id), IP, list_to_integer(Port)} | Acc]
        end, [], Ids),
    ezk:end_connection(C, ""),
    Brokers.
