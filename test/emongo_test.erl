-module(emongo_test).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

-define(NUM_PROCESSES,     500).
-define(NUM_TESTS_PER_PID, 10).
-define(POOL,              pool1).
-define(COLL,              <<"test">>).
-define(TIMEOUT,           5000).

setup() ->
  ensure_started(sasl),
  ensure_started(emongo),
  emongo:add_pool(?POOL, "localhost", 27017, "TEST", 10),
  ok.

cleanup(_) ->
  ok.

run_test_() ->
  [{setup,
    fun setup/0,
    fun cleanup/1,
    [
      fun test_performance/0
    ]
  }].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_performance() ->
  ?debugFmt("Testing performance.", []),
  Start = cur_time_ms(),

  try
    start_processes(?NUM_PROCESSES),
    block_until_done(?NUM_PROCESSES)
  after
    % Clean up in case something failed.
    emongo:delete_sync(?POOL, ?COLL)
  end,

  End = cur_time_ms(),
  ?debugFmt("Test passed in ~p ms\n", [End - Start]).

start_processes(X) when X =< 0 -> ok;
start_processes(X) ->
  Pid = self(),
  proc_lib:spawn(fun() ->
    run_tests(Pid, X, ?NUM_TESTS_PER_PID)
  end),
  start_processes(X - 1).

run_tests(Pid, _, Y) when Y =< 0 ->
  Pid ! done;
run_tests(Pid, X, Y) ->
  Num = (X bsl 16) bor Y, % Make up a unique number for this run
  try
    ok = emongo:insert_sync(?POOL, ?COLL, [{"_id", Num}]),

    FMRes = emongo:find_and_modify(?POOL, ?COLL, [{"_id", Num}],
      [{<<"$set">>, [{<<"fm">>, Num}]}], [{new, true}]),
    [[{<<"value">>, [{<<"_id">>, Num}, {<<"fm">>, Num}]},
      {<<"ok">>, 1.0}]] = FMRes,

    ok = emongo:update_sync(?POOL, ?COLL, [{"_id", Num}],
      [{<<"$set">>, [{<<"us">>, Num}]}]),

    FARes = emongo:find_all(?POOL, ?COLL, [{"_id", Num}]),
    [[{<<"_id">>, Num}, {<<"fm">>, Num}, {<<"us">>, Num}]] = FARes,

    ok = emongo:delete_sync(?POOL, ?COLL, [{"_id", Num}])
  catch _:E ->
    ?debugFmt("Exception occurred for test ~.16b: ~p\n~p\n",
              [Num, E, erlang:get_stacktrace()]),
    throw(test_failed)
  end,
  run_tests(Pid, X, Y - 1).

block_until_done(X) when X =< 0 -> ok;
block_until_done(X) ->
  receive done -> ok
  after ?TIMEOUT ->
    ?debugFmt("No response\n", []),
    throw(test_failed)
  end,
  block_until_done(X - 1).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

ensure_started(App) ->
  case application:start(App) of
    ok -> ok;
    {error, {already_started, App}} -> ok
  end.

cur_time_ms() ->
  {MegaSec, Sec, MicroSec} = erlang:now(),
  MegaSec * 1000000000 + Sec * 1000 + erlang:round(MicroSec / 1000).
