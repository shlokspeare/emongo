#### The goal of emongo is to be stable, fast and easy to use.

## Build

	make

## Run Unit Tests

	make test
	make check

## Start emongo

	application:start(emongo).

## Connecting to mongodb

#### Option 1 - Config file

example.config:

	[{emongo, [
		{pools, [
			{pool1, [
				{size, 1},
				{host, "localhost"},
				{port, 27017},
				{database, "testdatabase"}
			]}
		]}
	]}].

specify the config file path when starting Erlang

	erl -config priv/example

start the application

	application:start(emongo).

#### Option 2 - Add pool

start the app and then add as many pools as you like

	application:start(emongo).
	emongo:add_pool(pool1, "localhost", 27017, "testdatabase", 1).

## API Type Reference

__PoolId__ = atom()
__Host__ = string()
__Port__ = integer()
__Database__ = string()
__PoolSize__ = integer()
__User__ = string()
__Pass__ = string()
__CollectionName__ = string()
__Selector__ = Document
__Document__ = [{Key, Val}]
__Documents__ = [Document]
__Upsert__ = true | false (insert a new document if the selector does not match an existing document)
__Key__ = string() | atom() | binary() | integer()
__Val__ = float() | string() | binary() | Document | {array, [term()]} | {binary, BinSubType, binary()} | {oid, binary()} | {oid, string()} | bool() | now() | datetime() | undefined | {regexp, string(), string()} | integer()
__BinSubType__ = integer() <http://www.mongodb.org/display/DOCS/BSON#BSON-noteondatabinary>
__Options__ = {timeout, Timeout} | {limit, Limit} | {offset, Offset} | {orderby, Orderby} | {fields, Fields} | response_options
__Timeout__ = integer (timeout in milliseconds)
__Limit__ = integer
__Offset__ = integer
__Orderby__ = [{Key, Direction}]
__Direction__ = 1 | -1 | asc | desc
__Fields__ = [Key] = specifies a list of fields to return in the result set
__response_options__ = return #response{header, response_flag, cursor_id, offset, limit, documents}
__Result__ = [Document] | response()
__Pipeline__ = Document
__Oid__ = binary()
__Pool__ = #pool{id, host, port, database, size, user, pass_hash, conn_pids, req_id}

## Add Pool

	emongo:add_pool(PoolId, Host, Port, Database, PoolSize) -> ok
	emongo:add_pool(PoolId, Host, Port, Database, PoolSize, User, Pass) -> ok

## Remove Pool

	emongo:remove_pool(PoolId) -> ok

## Pools

	emongo:pools() -> [Pool]

## Oid

	emongo:oid() -> Oid

## Oid Generation Time

	emongo:oid_generation_time({oid, Oid}) -> int() (32-bit Unix time)

## Insert

	emongo:insert(PoolId, CollectionName, Document) -> ok
	emongo:insert(PoolId, CollectionName, Documents) -> ok

### Examples

	%% insert a single document with two fields into the "collection" collection
	emongo:insert(test, "collection", [{"field1", "value1"}, {"field2", "value2"}]).

	%% insert two documents, each with a single field into the "collection" collection
	emongo:insert(test, "collection", [[{"document1_field1", "value1"}], [{"document2_field1", "value1"}]]).

## Update

	%% by default upsert == false
	emongo:update(PoolId, CollectionName, Selector, Document) -> ok
	emongo:update(PoolId, CollectionName, Selector, Document, Upsert) -> ok

### Examples

	%% update the document that matches "field1" == "value1"
	emongo:update(test, "collection", [{"field1", "value1"}], [{"field1", "value1"}, {"field2", "value2"}]).

## Update All

	%% update all is the same as update, except that the multi flag is set to true.
	emongo:update_all(PoolId, CollectionName, Selector, Document) -> ok

## Delete

	%% delete all documents in a collection
	emongo:delete(PoolId, CollectionName) -> ok

	%% delete all documents in a collection that match a selector
	emongo:delete(PoolId, CollectionName, Selector) -> ok

## Get Last Error

Insert, update, and delete each have a counter part, insert_sync, update_sync, and delete_sync, that calls getLastError after performing the action.  These calls block until the action has completed on the Mongo server.

	emongo:insert_sync(PoolId, CollectionName, Documents) -> ok
	emongo:insert_sync(PoolId, CollectionName, Documents, Options) -> ok

	emongo:update_sync(PoolId, CollectionName, Selector, Document) -> ok | {emongo_no_match_found, Error}
	emongo:update_sync(PoolId, CollectionName, Selector, Document, Upsert) -> ok | {emongo_no_match_found, Error}
	emongo:update_sync(PoolId, CollectionName, Selector, Document, Upsert, Options) -> ok | {emongo_no_match_found, Error}

	emongo:update_all_sync(PoolId, Collection, Selector, Document) -> ok
	emongo:update_all_sync(PoolId, Collection, Selector, Document, Options) -> ok

	emongo:delete_sync(PoolId, CollectionName) -> ok | {emongo_no_match_found, Error}
	emongo:delete_sync(PoolId, CollectionName, Selector) -> ok | {emongo_no_match_found, Error}
	emongo:delete_sync(PoolId, Collection, Selector, Options) -> ok | {emongo_no_match_found, Error}

## Find

	emongo:find(PoolId, CollectionName, Selector, Options) -> Result

### Examples

__limit, offset, timeout, orderby, fields__

	%% find documents from 'collection' where field1 equals 1 and abort the query if it takes more than 5 seconds
	%% limit the number of results to 100 and offset the first document 10 documents from the beginning
	%% return documents in ascending order, sorted by the value of field1
	%% limit the fields in the return documents to field1 (the _id field is always included in the results)
	emongo:find(test, "collection", [{"field1", 1}], [{limit, 100}, {offset, 10}, {timeout, 5000}, {orderby, [{"field1", asc}]}, {fields, ["field1"]}]).

__great than, less than, great than or equal, less than or equal__

	%% find documents where field1 is greater than 5 and less than 10
	emongo:find(test, "collection", [{"field1", [{gt, 5}, {lt, 10}]}], []).

	%% find documents where field1 is greater than or equal to 5 and less than or equal to 10
	emongo:find(test, "collection", [{"field1", [{gte, 5}, {lte, 10}]}], []).

	%% find documents where field1 is greater than 5 and less than 10
	emongo:find(test, "collection", [{"field1", [{'>', 5}, {'<', 10}]}], []).

	%% find documents where field1 is greater than or equal to 5 and less than or equal to 10
	emongo:find(test, "collection", [{"field1", [{'>=', 5}, {'=<', 10}]}], []).

__not equal__

	%% find documents where field1 is not equal to 5 or 10
	emongo:find(test, "collection", [{"field1", [{ne, 5}, {ne, 10}]}], []).

	%% find documents where field1 is not equal to 5
	emongo:find(test, "collection", [{"field1", [{'=/=', 5}]}], []).

	%% find documents where field1 is not equal to 5
	emongo:find(test, "collection", [{"field1", [{'/=', 5}]}], []).

__in__

	%% find documents where the value of field1 is one of the values in the list [1,2,3,4,5]
	emongo:find(test, "collection", [{"field1", [{in, [1,2,3,4,5]}]}], []).

__not in__

	%% find documents where the value of field1 is NOT one of the values in the list [1,2,3,4,5]
	emongo:find(test, "collection", [{"field1", [{nin, [1,2,3,4,5]}]}], []).

__all__

	%% find documents where the value of field1 is an array and contains all of the values in the list [1,2,3,4,5]
	emongo:find(test, "collection", [{"field1", [{all, [1,2,3,4,5]}]}], []).

__size__

	%% find documents where the value of field1 is an array of size 10
	emongo:find(test, "collection", [{"field1", [{size, 10}]}], []).

__exists__

	%% find documents where field1 exists
	emongo:find(test, "collection", [{"field1", [{exists, true}]}], []).

__where__

	%% find documents where the value of field1 is greater than 10
	emongo:find(test, "collection", [{where, "this.field1 > 10"}], []).

__nested queries__

	%% find documents with an address field containing a sub-document
	%% with street equal to "Maple Drive".
	%% ie: [{"address", [{"street", "Maple Drive"}, {"zip", 94114}]
	emongo:find(test, "people", [{"address.street", "Maple Drive"}], []).

## Find All

Find all matching documents, using the returned cursor to get more data as necessary.

	emongo:find_all(PoolId, Collection) -> Result
	emongo:find_all(PoolId, Collection, Selector) -> Result
	emongo:find_all(PoolId, Collection, Selector, Options) -> Result

## Find One

Find only the first matching document.

	emongo:find_one(PoolId, Collection, Selector) -> Result
	emongo:find_one(PoolId, Collection, Selector, Options) -> Result

## Ensure Index

	emongo:ensure_index(PoolId, Collection, Keys, Unique) -> ok

## Count

	emongo:count(PoolId, Collection) -> int() | undefined
	emongo:count(PoolId, Collection, Selector) -> int() | undefined
	emongo:count(PoolId, Collection, Selector, Options) -> int() | undefined

## Aggregate

	emongo:aggregate(PoolId, Collection, Pipeline) -> Result
	emongo:aggregate(PoolId, Collection, Pipeline, Options) -> Result

## Find and Modify

	emongo:find_and_modify(PoolId, Collection, Selector, Document) -> Result
	emongo:find_and_modify(PoolId, Collection, Selector, Document, Options) -> Result
