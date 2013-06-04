-include_lib("emongo_public.hrl").

-record(pool, {id, host, port, database, size=1, user, pass_hash, max_pipeline_depth = 0, socket_options = [],
               conns=queue:new(), req_id=1}).
-record(header, {message_length, request_id, response_to, op_code}).
-record(emo_query, {opts=0, offset=0, limit=16#7FFFFFFF, q=[], field_selector=[]}).

-define(IS_DOCUMENT(Doc), (is_list(Doc) andalso (Doc == [] orelse (is_tuple(hd(Doc)) andalso tuple_size(hd(Doc)) == 2)))).
-define(IS_LIST_OF_DOCUMENTS(Docs), (
	is_list(Docs) andalso (
		Docs == [] orelse (
			is_list(hd(Docs)) andalso (
				hd(Docs) == [] orelse (
					is_tuple(hd(hd(Docs))) andalso
					tuple_size(hd(hd(Docs))) == 2
				)
			)
		)
	))).

-define(TIMEOUT, 5000).

-define(OP_REPLY, 1).
-define(OP_MSG, 1000).
-define(OP_UPDATE, 2001).
-define(OP_INSERT, 2002).
-define(OP_QUERY, 2004).
-define(OP_GET_MORE, 2005).
-define(OP_DELETE, 2006).
-define(OP_KILL_CURSORS, 2007).

-define(WARN(Fmt, Args),      io:format("WARNING (~p:~p): "   Fmt "\n",     [?MODULE, ?LINE | Args])).
-define(EXCEPTION(Fmt, Args), io:format("EXCEPTION (~p:~p): " Fmt "\n~p\n", [?MODULE, ?LINE | Args] ++
                                                                            [erlang:get_stacktrace()])).
-define(ERROR(Fmt, Args),     io:format("ERROR (~p:~p): "     Fmt "\n",     [?MODULE, ?LINE | Args])).
-define(DEBUG(Fmt, Args),     io:format("DEBUG (~p:~p): "     Fmt "\n",     [?MODULE, ?LINE | Args])).
-define(DUMP(X),              ?DEBUG("~p = ~p", [??X, X])).
