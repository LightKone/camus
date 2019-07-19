%%
%% Copyright (c) 2018 Vitor Enes.  All Rights Reserved.
%% Copyright (c) 2019 Georges Younes.  All Rights Reserved.
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

-module(camus_forward).
-author("Vitor Enes <vitorenesduarte@gmail.com").
-author("Georges Younes <georges.r.younes@gmail.com>").

-include("camus.hrl").

-export([all_shards/0,
         update_members/1]).

%% number of shards
-define(SHARD_NUMBER, 1).

%% @doc
-spec all_shards() -> list(atom()).
all_shards() ->
    lists:map(
        fun(Index) -> camus_util:integer_to_atom(Index) end,
        lists:seq(0, ?SHARD_NUMBER - 1)
    ).

%% @doc Update members.
-spec update_members(list(camus_id())) -> [ok].
update_members(Members) ->
    forward(all, cast, {update_members, Members}).

%% @private
-spec forward(all, atom(), term()) -> term().
forward(all, What, Msg) ->
    lists:map(
        fun(Name) -> do_forward(Name, What, Msg) end,
        all_shards()
    ).

%% @private Forward by call or cast.
-spec do_forward(atom(), atom(), term()) -> term().
% do_forward(Name, call, Msg) ->
%     gen_server:call(Name, Msg, infinity);
do_forward(Name, cast, Msg) ->
    gen_server:cast(Name, Msg).
