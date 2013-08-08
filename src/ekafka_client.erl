-module(ekafka_client).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-record(state, {socket :: inet:socket()}).

-define(MAGIC, 1).
-define(COMPRESSION, 0).

-define(PRODUCE, 0).

start_link([IP, Port]) ->
    gen_server:start_link(?MODULE, [IP, Port], []).

init([IP, Port]) ->
    {ok, Socket} = gen_tcp:connect(IP, Port, [binary, {active, false}]),
    {ok, #state{socket = Socket}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({produce, Topic, Partition, Messages} , State) ->
    TopicSize = byte_size(Topic), 
    MessagesSize = lists:foldl(
        fun(Message, Size) ->
            Size + 4 + 1 + 1 + 4 + byte_size(Message)
        end, 0, Messages),
    PacketSize = 2 + 2 + TopicSize + 4 + 4 + MessagesSize,
    ProducedMessages = lists:foldr(
        fun(Message, Acc) ->
            Length = 1 + 1 + 4 + byte_size(Message),
            CheckSum = erlang:crc32(Message),
            Msg = <<
                Length:32,
                ?MAGIC:8,
                ?COMPRESSION:8,
                CheckSum:32,
                Message/binary
            >>,
            <<Msg/binary, Acc/binary>>
        end, <<>>, Messages),
    Packet = <<
        PacketSize:32, 
        ?PRODUCE:16,
        TopicSize:16,
        Topic/binary,
        Partition:32,
        MessagesSize:32,
        ProducedMessages/binary
    >>,
    ok = gen_tcp:send(State#state.socket, Packet),
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, State) ->
    gen_tcp:close(State#state.socket).
