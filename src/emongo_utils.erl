-module(emongo_utils).

-compile(export_all).

as_binary(V) when is_binary(V) -> V;
as_binary(V) when is_atom(V) -> as_binary(atom_to_list(V));
as_binary(V) when is_list(V) -> list_to_binary(V);
as_binary(V) when is_integer(V) -> list_to_binary(integer_to_list(V)).

as_list(L) when is_list(L) -> L;
as_list(V) when is_atom(V) -> atom_to_list(V);
as_list(B) when is_binary(B) -> binary_to_list(B).
