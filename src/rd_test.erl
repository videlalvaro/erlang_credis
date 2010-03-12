-module(rd_test).

-export([test/0]).

test() ->
  redis_drv:start_link(),
  Key = <<"key">>,
  Value = <<"value">>,
  log(redis_drv:set(Key, Value)),
  log(redis_drv:get(Key)).
  
log(Msg) ->
  io:format("Log: ~p~n", [Msg]).