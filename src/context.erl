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

%% @doc A simple Erlang implementation of a context.

-module(context).
-author("Georges Younes <georges.r.younes@gmail.com>").

-include("camus.hrl").

-export([new/0, set_val/1, purge/2, add/2,
         to_binary/1, from_binary/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([context/0, binary_context/0]).

-type ctxt() :: maps:map(id(), counter()).
-type binary_context() :: binary().

%% @doc Create a new context.
-spec new() -> ctxt().
new() ->
    maps:new().

% @doc Set a context value to a dot.
-spec set_val(dot()) -> ctxt().
set_val({Id, Counter}) ->
    #{Id => Counter}.

% @doc purge keys in first context that have
% same values in second context.
-spec purge(ctxt(), ctxt()) -> ctxt().
purge(LocalContext, RemoteContext) ->
    maps:fold(
        fun(LocalId, LocalCtr, Acc0) ->
            RemoteCtr = maps:get(LocalId, RemoteContext, 0),
            case LocalCtr == RemoteCtr of
                true ->
                    maps:remove(LocalId, Acc0);
                false ->
                    Acc0
            end
        end,
        LocalContext,
        LocalContext).

% @doc Add a {dot, context} to context.
-spec add({dot(), ctxt()}, ctxt()) -> ctxt().
add({{Id, Ctr}, RemoteContext}, LocalContext) ->
    NewContext = purge(LocalContext, RemoteContext),
    maps:put(Id, Ctr, NewContext).

%% @doc an effecient format for disk / wire.
%5 @see from_binary/1
-spec to_binary(ctxt()) -> binary_context().
to_binary(Context) ->
    term_to_binary(Context).

%% @doc takes the output of `to_binary/1' and returns a context
-spec from_binary(binary_context()) -> ctxt().
from_binary(Bin) ->
    binary_to_term(Bin).

-ifdef(TEST).

set_val_test() ->
    Dot = {c, 3},
    Expected = #{c => 3},
    ?assertEqual(Expected, set_val(Dot)).

purge_test() ->
    Bottom = #{},
    Context1 = #{a => 3, b => 3,  c => 3},
    Context2 = #{a => 3, b => 3,  c => 3},
    Context3 = #{a => 2, c => 3},
    Expected23 = #{a => 3, b => 3},
    ?assertEqual(Bottom, purge(Context1, Context2)),
    ?assertEqual(Bottom, purge(Context2, Context1)),
    ?assertEqual(Expected23, purge(Context2, Context3)).

add_test() ->
    Dot = {b, 3},
    RemoteContext = #{a => 2, c => 2},
    LocalContext = #{c => 2, b =>2, a => 4},
    Expected = #{a => 4, b => 3},
    ?assertEqual(Expected, add({Dot, RemoteContext}, LocalContext)).

-endif.
