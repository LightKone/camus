%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Georges Younes.  All Rights Reserved.
%% Copyright (c) 2017 IMDEA Software Institute.  All Rights Reserved.
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

%% @doc VClock "Matrix".

-module(mclock).
-author("Georges Younes <georges.r.younes@gmail.com>").
-author("Vitor Enes <vitorenesduarte@gmail.com>").

-include("camus.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/1,
         update/2,
         update/3,
         stable/1]).

-export_type([mc/0]).

%% vclock "matrix"
-type mc() :: maps:map(id(), vclock()).


%% @doc Create an empty VClock Matrix.
-spec new([id()]) -> mc().
new(Members) ->
    VC = vclock:new(Members),
    maps:from_list([{Node, VC} || Node <- Members]).

%% @doc Update clock for a given sender.
-spec update(dot(), mc()) -> mc().
update({Id, _}=Dot, M) ->
    VC = maps:get(Id, M),
    maps:put(Id, vclock:add_dot(Dot, VC), M).

%% @doc Update clock for a given sender.
-spec update(id(), vclock(), mc()) -> mc().
update(Id, Clock, M) ->
    maps:put(Id, Clock, M).

%% @doc Get the stable exception clock.
-spec stable(mc()) -> vclock().
stable(M) ->
    intersection(maps:values(M)).

%% @private
-spec intersection([vclock()]) -> vclock().
intersection([A, B|L]) -> intersection([vclock:intersection(A, B)|L]);
intersection([A]) -> A.