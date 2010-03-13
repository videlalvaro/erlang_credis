-module(rd_test).

-export([test/0, test/1]).

test() ->
  redis_drv:start_link(),
  Key = <<"key">>,
  Value = <<"value">>,
  log(redis_drv:set(Key, Value)),
  log(redis_drv:get(Key)).
  
log(Msg) ->
  io:format("Log: ~p~n", [Msg]).

test(riak) ->
  riak_test_util:standard_backend_test(riak_redis_backend, []),
  fprof:apply(riak_test_util,standard_backend_test, [riak_redis_backend, []]),
  fprof:profile(),
  fprof:analyse([no_callers, no_details, totals]).