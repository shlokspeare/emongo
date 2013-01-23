%% Copyright (c) 2009 Jacob Vorreuter <jacob.vorreuter@gmail.com>
%%
%% Permission is hereby granted, free of charge, to any person
%% obtaining a copy of this software and associated documentation
%% files (the "Software"), to deal in the Software without
%% restriction, including without limitation the rights to use,
%% copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following
%% conditions:
%%
%% The above copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%% OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
%% NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
%% HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.
-module(emongo_conn).
-include("emongo.hrl").
-export([start_link/3, stop/1, send/3, send_sync/5, send_recv/4, queue_lengths/1, write_pid/1]).
-export([init_writer/3, init_listener/2]).
-record(conn, {listen_pid, write_pid}).

% The underlying socket is going to be handled by two Pids: one to write to it and one to read from it.  Besides the
% maintenance nightmare of having to coordinate these two Pids, it should maximize the throughput of this connection.
start_link(PoolId, Host, Port) ->
  start_writer(PoolId, Host, Port).

stop(#conn{write_pid = WritePid}) ->
  WritePid ! emongo_conn_close.
  % The WritePid will tell the ListenPid to exit.

send(Conn, ReqId, Packet) ->
  gen_call(Conn, emongo_conn_send, ReqId, {ReqId, Packet}, ?TIMEOUT).

send_sync(Conn, ReqId, Packet1, Packet2, Timeout) ->
  Resp = gen_call(Conn, emongo_conn_send_sync, ReqId,
                  {ReqId, Packet1, Packet2}, Timeout),
  Documents = emongo_bson:decode(Resp#response.documents),
  Resp#response{documents=Documents}.

send_recv(Conn, ReqId, Packet, Timeout) ->
  Resp = gen_call(Conn, emongo_conn_send_recv, ReqId, {ReqId, Packet},
                  Timeout),
  Documents = emongo_bson:decode(Resp#response.documents),
  Resp#response{documents=Documents}.

queue_lengths(#conn{listen_pid = ListenPid, write_pid = WritePid}) ->
  {_, ListenQueueLen} = erlang:process_info(ListenPid, message_queue_len),
  {_, WriteQueueLen}  = erlang:process_info(WritePid,  message_queue_len),
  {ListenQueueLen, WriteQueueLen}.

write_pid(#conn{write_pid = WritePid}) -> WritePid.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_writer(PoolId, Host, Port) ->
  {ok, _} = proc_lib:start_link(?MODULE, init_writer, [PoolId, Host, Port], ?TIMEOUT).

init_writer(PoolId, Host, Port) ->
  Socket = open_socket(Host, Port),
  % Note that the Ets table name is not unique since several connections will be made.  However, this is not a
  % named_table, so that should be okay.  The name should not be used.
  EtsTid = ets:new(?MODULE, [public, {write_concurrency, true}]),
  WritePid = self(),
  {ok, ListenPid} = start_listener(EtsTid, WritePid),
  ok = gen_tcp:controlling_process(Socket, ListenPid),
  ok = proc_lib:init_ack({ok, #conn{listen_pid = ListenPid, write_pid = WritePid}}),
  socket_writer(PoolId, Socket, EtsTid, ListenPid).

socket_writer(PoolId, Socket, EtsTid, ListenPid) ->
  try
    receive
      % FromRef = {From, Mref}
      {emongo_conn_send, FromRef, {_ReqId, Packet}} ->
        ok = gen_tcp:send(Socket, Packet),
        gen:reply(FromRef, ok);
      {emongo_conn_send_sync, FromRef, {ReqId, Packet1, Packet2}} ->
        true = ets:insert_new(EtsTid, {ReqId, FromRef}),
        % Packet2 is the packet containing getlasterror.
        % Send both packets in the same TCP packet for performance reasons.
        % It's about 3 times faster.
        ok = gen_tcp:send(Socket, <<Packet1/binary, Packet2/binary>>);
      {emongo_conn_send_recv, FromRef, {ReqId, Packet}} ->
        true = ets:insert_new(EtsTid, {ReqId, FromRef}),
        ok = gen_tcp:send(Socket, Packet);
      emongo_listen_exited -> exit(emongo_listen_exited);
      emongo_conn_close    -> exit(emongo_conn_close)
    end
  catch _:Error ->
    ListenPid ! emongo_conn_close,
    gen_tcp:close(Socket),
    ets:delete(EtsTid),
    case Error of
      emongo_conn_close ->
        exit(normal);
      _ ->
        ?EXCEPTION("~s: Writer exiting: ~p", [?MODULE, Error]),
        exit({?MODULE, PoolId, Error})
    end
  end,
  socket_writer(PoolId, Socket, EtsTid, ListenPid).

start_listener(EtsTid, WritePid) ->
  proc_lib:start_link(?MODULE, init_listener, [EtsTid, WritePid], ?TIMEOUT).

init_listener(EtsTid, WritePid) ->
  proc_lib:init_ack({ok, self()}),
  socket_listener(EtsTid, <<>>, WritePid).

socket_listener(EtsTid, Leftover, WritePid) ->
  NewLeftover = try
    receive
      {tcp, _Socket, Data} ->
        _NewLeftovers = process_bin(EtsTid, <<Leftover/binary, Data/binary>>);
      {emongo_recv_timeout, FromRef, ReqId} ->
        ets:delete(EtsTid, ReqId),
        gen:reply(FromRef, ok),
        Leftover;
      {tcp_closed, _Socket}        -> exit(emongo_tcp_closed);
      {tcp_error, _Socket, Reason} -> exit({emongo, Reason});
      emongo_conn_close            -> exit(emongo_conn_close)
    end
  catch _:Error ->
    WritePid ! emongo_listen_exited,
    case Error of
      emongo_conn_close -> ok;
      _ ->
        ?EXCEPTION("~s: Reader exiting: ~p", [?MODULE, Error])
    end,
    exit(normal)
  end,
  socket_listener(EtsTid, NewLeftover, WritePid).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

open_socket(Host, Port) ->
  case gen_tcp:connect(Host, Port, [binary, {active, true}, {nodelay, true}]) of
    {ok, Sock} ->
      Sock;
    {error, Reason} ->
      exit({emongo_failed_to_open_socket, Reason})
  end.

gen_call(#conn{write_pid = WritePid, listen_pid = ListenPid}, Label, ReqId, Request, Timeout) ->
  case catch gen:call(WritePid, Label, Request, Timeout) of
    {ok, Result} -> Result;
    {'EXIT', timeout} ->
      % Clear the ets table from the timed out call
      try
        gen:call(ListenPid, emongo_recv_timeout, ReqId, Timeout)
      catch
        _:{'EXIT', timeout} ->
          % If a timeout occurred while trying to communicate with the
          % connection, something is really backed up.  However, if this
          % happens after a connection goes down, it's expected.
          exit({emongo_conn_error, overloaded});
        _:E -> E % Let the original error bubble up.
      end,
      exit({emongo_conn_error, timeout});
    Error -> exit({emongo_conn_error, Error})
  end.

process_bin(_EtsTid, <<>>) -> <<>>;
process_bin(EtsTid, Bin) ->
  case emongo_packet:decode_response(Bin) of
    undefined -> Bin;
    {Resp = #response{header = #header{response_to = ResponseTo}}, Tail} ->
      case ets:lookup(EtsTid, ResponseTo) of
        [{_, FromRef} | _] ->
          ets:delete(EtsTid, ResponseTo),
          gen:reply(FromRef, Resp);
        _ ->
          % Request already timed out.
          ok
      end,
      % Continue processing Tail in case there's another complete message in it.
      process_bin(EtsTid, Tail)
  end.
