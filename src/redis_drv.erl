-module(redis_drv).

-behaviour(gen_server).

-export([start_link/0]).

-export([set/2, get/1, del/1, sadd/2, srem/2, smembers/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, 
  terminate/2]).

-export([message/2, message/3]).

-define(REC_TIMEOUT, 5000).

-define(CREDIS_INVALID_COMMAND, 255).
-define(CREDIS_SET, 0).
-define(CREDIS_GET, 1).
-define(CREDIS_DEL, 2).
-define(CREDIS_SADD, 3).
-define(CREDIS_SREM, 4).
-define(CREDIS_SMEMBERS, 5).

-define(SERVER, ?MODULE).
-define(DRIVER_NAME, "redis_drv").

-record(state, {port}).

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).
    
init([]) ->
  SearchDir = filename:join([filename:dirname(code:which(?MODULE)), "..", "priv"]),
  case erl_ddll:load(SearchDir, "redis_drv") of
    ok ->
      {ok, #state{port=open_port({spawn, ?DRIVER_NAME}, [binary])}};
    Error ->
      Error
    end.

set(Key, Value) ->
  gen_server:call(?MODULE, {set, Key, Value}).
  
get(Key) ->
  gen_server:call(?MODULE, {get, Key}).
  
del(Key) ->
  gen_server:call(?MODULE, {del, Key}).
  
sadd(Key, Value) ->
  gen_server:call(?MODULE, {sadd, Key, Value}).
  
srem(Key, Value)->
  gen_server:call(?MODULE, {srem, Key, Value}).
  
smembers(Key)->
  gen_server:call(?MODULE, {smembers, Key}).

handle_call({set, Key, Value}, _From, State)
  when is_binary(Key) and is_binary(Value) ->
    Message = message(?CREDIS_SET, Key, Value),
    Reply = send_command(State#state.port, Message),
    {reply, Reply, State};
    
handle_call({get, Key}, _From, State)
  when is_binary(Key) ->
    Message = message(?CREDIS_GET, Key),
    Reply = send_command(State#state.port, Message),
    {reply, Reply, State};
    
handle_call({del, Key}, _From, State)
  when is_binary(Key) ->
    Message = message(?CREDIS_DEL, Key),
    Reply = send_command(State#state.port, Message),
    {reply, Reply, State};
    
handle_call({sadd, Key, Value}, _From, State)
  when is_binary(Key) and is_binary(Value)->
    Message = message(?CREDIS_SADD, Key, Value),
    Reply = send_command(State#state.port, Message),
    {reply, Reply, State};
    
handle_call({srem, Key, Value}, _From, State)
  when is_binary(Key) and is_binary(Value)->
    Message = message(?CREDIS_SREM, Key, Value),
    Reply = send_command(State#state.port, Message),
    {reply, Reply, State};
    
handle_call({smembers, Key}, _From, State)
  when is_binary(Key) ->
    Message = message(?CREDIS_SMEMBERS, Key),
    Reply = send_command(State#state.port, Message),
    {reply, Reply, State};
    
handle_call(_Request, _From, State) ->
    Reply = {error, unkown_call},
    {reply, Reply, State}.
    
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    port_close(State#state.port),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

message(Command, Key) ->
  KeySize = size(Key),
  <<Command/native, KeySize:64/native, Key/binary>>.

message(Command, Key, Value) ->
  KeySize = size(Key),
  ValueSize = size(Value),
  <<Command/native, KeySize:64/native, Key/binary, ValueSize:64/native, Value/binary>>.
    
send_command(Port, Command) ->
    port_command(Port, Command),
    receive
      Data ->
        Data
    after ?REC_TIMEOUT ->
      io:format("Received nothing!~n"),
      {error, timeout}
    end.