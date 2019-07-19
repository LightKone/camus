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

%% @doc A simple Erlang implementation of a dot.

-module(dot).
-author("Georges Younes <georges.r.younes@gmail.com>").

-include("camus.hrl").

-export([new/2, new_dot/1, inc/1,
         to_binary/1, from_binary/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([d/0, binary_dot/0]).

-type d() :: {id(), counter()}.
-type binary_dot() :: binary().

% @doc Create a brand new dot.
-spec new(id(), counter()) -> d().
new(Id, Counter) ->
    {Id, Counter}.

-spec new_dot(id()) -> d().
new_dot(Id) ->
    {Id, 0}.

-spec inc(d()) -> d().
inc({Id, Counter}) ->
    {Id, Counter+1}.

%% @doc an effecient format for disk / wire.
%% @see from_binary/1
-spec to_binary(d()) -> binary_dot().
to_binary(Dot) ->
    term_to_binary(Dot).

%% @doc takes the output of `to_binary/1' and returns a dot
-spec from_binary(binary_dot()) -> d().
from_binary(Bin) ->
    binary_to_term(Bin).