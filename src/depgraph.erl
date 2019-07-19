%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Georges Younes, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc A simple Erlang implementation of a causal dependency graph.

-module(depgraph).
-author("Georges Younes <georges.r.younes@gmail.com>").

-include("camus.hrl").

-export([new/0, add_with/3, update/3, delete/2,
         get/3, is_element/2, add/2,
         delete_list/2, to_binary/1, from_binary/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([dotstruct/0, dg/0, binary_depgraph/0]).

-record(dotstruct, {stage :: atom(),
                bitstr :: integer() | 'undefined',
                pred :: context(),
                succ :: context(),
                pyld :: term()
                }).

% -opaque dotstruct() :: #dotstruct{}.
-type dg() :: maps:map(dot(), dotstruct()).
-type binary_depgraph() :: binary().

% @doc Create a new depgraph.
-spec new() -> dg().
new() ->
    maps:new().

% @doc Add a new entry in depgraph.
-spec add_with(dot(), [{term(), term()}], dg()) -> dg().
add_with(Dot, List, Depgraph) ->
    Depgraph1 = add(Dot, Depgraph),
    update(Dot, List, Depgraph1).

% @doc Add a new entry in depgraph.
-spec add(dot(), dg()) -> dg() | {badmap, term()}.
add(Dot, Depgraph) ->
    maps:put(Dot, #dotstruct{bitstr=0, pred=maps:new(), succ=maps:new()}, Depgraph).

% @doc Update an entry in depgraph.
-spec update(dot(), [{term(), term()}], dg()) -> dg().
update(Dot, List, Depgraph) ->
    maps:update_with(
        Dot,
        fun(Record) ->
            lists:foldl(
                fun({K, V}, Acc) ->
                    update_record_val(K, V, Acc)
                end,
                Record,
                List
            )
        end,
        Depgraph
    ).

% @doc Delete an entry in depgraph.
-spec delete(dot(), dg()) -> dg().
delete(Dot, Depgraph) ->
    maps:remove(Dot, Depgraph).

% @doc Delete a list of entries in depgraph.
-spec delete_list([dot()], dg()) -> dg().
delete_list(List, Depgraph) ->
    lists:foldl(
        fun(Dot, Acc) ->
            maps:remove(Dot, Acc)
        end,
        Depgraph,
        List).

% @doc Get a certain value from record in depgraph.
-spec get(dot(), term(), dg()) -> term().
get(Dot, Key, Depgraph) ->
    case maps:is_key(Dot, Depgraph) of
        false ->
            get_record_def(Key);
        true ->
            get_record_val(Key, maps:get(Dot, Depgraph))
    end.

%% @doc Check if a dot is in the Depgraph.
-spec is_element(dot(), dg()) -> boolean().
is_element(Dot, Depgraph) ->
    maps:is_key(Dot, Depgraph).

%% @doc an effecient format for disk / wire.
%% @see from_binary/1
-spec to_binary(dg()) -> binary_depgraph().
to_binary(Depgraph) ->
    term_to_binary(Depgraph).

%% @doc takes the output of `to_binary/1' and returns a depgraph
-spec from_binary(binary_depgraph()) -> dg().
from_binary(Bin) ->
    binary_to_term(Bin).

%% internal
update_record_val(Key, Val, Record) ->
    case Key of
        stage ->
            Record#dotstruct{stage=Val};
        bitstr ->
            Record#dotstruct{bitstr=Val};
        pred ->
            Record#dotstruct{pred=Val};
        succ ->
            Record#dotstruct{succ=Val};
        pyld ->
            Record#dotstruct{pyld=Val}
    end.

%% internal
get_record_val(Key, Record) ->
    case Key of
        stage ->
            Record#dotstruct.stage;
        bitstr ->
            Record#dotstruct.bitstr;
        pred ->
            Record#dotstruct.pred;
        succ ->
            Record#dotstruct.succ;
        pyld ->
            Record#dotstruct.pyld
    end.

%% internal
get_record_def(Key) ->
    case Key of
        stage ->
            undefined;
        bitstr ->
            0;
        pred ->
            maps:new();
        succ ->
            maps:new();
        pyld ->
            undefined
    end.
