-module(rd_test).

-export([test/0]).

test() ->
  redis_drv:start_link(),
  redis_drv:set(<<"Key">>, <<"Value">>).