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

%% @doc A simple Erlang implementation of a map of unacked messages.

-module(unacked).
-author("Georges Younes <georges.r.younes@gmail.com>").

-include("camus.hrl").

-export([new/0, add/5, ack/3,
         to_binary/1, from_binary/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([ua/0, binary_ua/0]).

-type ua() :: maps:map(dot(), {vclock(), term(), non_neg_integer()}).
-type binary_ua() :: binary().

% @doc Create a new unacked.
-spec new() -> ua().
new() ->
    maps:new().

% @doc Add a new entry in unacked.
-spec add(dot(), context(), term(), non_neg_integer(), ua()) -> ua() | {badmap, term()}.
add(Dot, Ctxt, Msg, Bits, Unacked) ->
    maps:put(Dot, {Ctxt, Msg, Bits}, Unacked).

% @doc Ack an entry in unacked.
-spec ack(rcvd(), non_neg_integer(), ua()) -> {term(), ua()} | {badmap, term()} | {badkey, term()}.
ack({LastCtr, UnRcvd}, NegBits, Unacked) ->
    %% NegBits are N-AckerBits

    L = rcvd:get_exceptions(UnRcvd),

    lists:foldl(
        fun(Dot={_, Ctr}, {A, B}) ->
            {Ctxt, Msg, Bits} = maps:get(Dot, B),
            case lists:member(Ctr, L) of
                true ->
                    M = {Dot, Ctxt, Msg},
                    {[M|A], B};
                false ->
                    case Bits band NegBits of
                        0 ->
                            {A, maps:remove(Dot, B)};
                        N ->
                            {A, maps:update(Dot, {Ctxt, Msg, N}, B)}
                    end
            end
        end,
    {[], Unacked},
    [D || {_, C}=D <- maps:keys(Unacked), C =< LastCtr]).

%% @doc an effecient format for disk / wire.
%% @see from_binary/1
-spec to_binary(ua()) -> binary_ua().
to_binary(Unacked) ->
    term_to_binary(Unacked).

%% @doc takes the output of `to_binary/1' and returns a Unacked
-spec from_binary(binary_ua()) -> ua().
from_binary(Bin) ->
    binary_to_term(Bin).
