{application, emongo, [
    {description, "Erlang MongoDB Driver"},
    {vsn, "1.0.0"},
    {modules, [
		emongo,
		emongo_app,
		emongo_bson,
		emongo_conn,
		emongo_packet,
		emongo_utils
    ]},
    {registered, []},
    {mod, {emongo_app, []}},
    {applications, [kernel, stdlib, sasl]}
]}.
