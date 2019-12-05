%% -------------------------------------------------------------------
%%
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

%% @doc map of rcvd dots.

-module(rcvd).
-author("Georges Younes <georges.r.younes@gmail.com>").

-include("camus.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0,
         init/2,
         add_dot/2,
         is_unrcvd/2,
         is_rcvd/2,
         get_exceptions/1,
         is_exception/2,
         to_binary/1,
         from_binary/1]).

-export_type([received/0, binary_rcvd/0]).

-type received() :: maps:map(id(), {counter(), [{counter(), counter()}]}).
-type binary_rcvd() :: binary().

%% @doc Create a rcvd map.
-spec new() -> received().
new() ->
    maps:new().

%% @doc Create a rcvd map.
-spec init([id()], received()) -> received().
init(Ids, Rcvd) ->
    lists:foldl(
        fun(Id, Acc) ->
            maps:put(Id, {0, []}, Acc)
        end,
    Rcvd,
    Ids).

%% @doc Add a dot to the rcvd map.
-spec add_dot(dot(), received()) -> received().
add_dot({Id, Ctr}, Rcvd) ->
    {LastCtr, UnRcvdList} = maps:get(Id, Rcvd),
    Entry = case Ctr-LastCtr of
        1 ->
            {Ctr, UnRcvdList};
        N when N > 0 ->
            {Ctr, [{LastCtr, N}|UnRcvdList]};
        N when N < 0 ->
            lists:foldl(
                fun({A, B}, Acc) ->
                    case lists:member(Ctr, lists:seq(A+1, A+B)) of
                        true when A+1 == A+B ->
                            {LastCtr, lists:delete({A, B}, Acc)};
                        true when A+1 == Ctr ->
                            {LastCtr, lists:keyreplace(A, 1, Acc, {Ctr, B-1})};
                        true when Ctr == A+B ->
                            {LastCtr, lists:keyreplace(A, 1, Acc, {A, B-1})};
                        true ->
                            {LastCtr, [{N, B-1}|[{A, N-A-1}|lists:delete({A, B}, Acc)]]};
                        false ->
                            Acc
                    end
                end,
                UnRcvdList,
                UnRcvdList)
    end,
    maps:update(Id, Entry, Rcvd).

%% @doc Check if a dot is an exception of the Rcvd.
-spec is_exception(dot(), [{counter(), counter()}]) -> boolean().
is_exception({_Id, Ctr}, UnRcvdList) ->
    L = get_exceptions(UnRcvdList),
    lists:member(Ctr, L).

%% @doc Returns exceptions of the Rcvd.
-spec get_exceptions([{counter(), counter()}]) -> [dot()].
get_exceptions(UnRcvdList) ->
    lists:foldl(
        fun({A, B}, Acc) ->
            [lists:seq(A+1, A+B)|Acc]
        end,
        [],
        UnRcvdList
    ).

%% @doc Check if a dot is in the Rcvd.
-spec is_rcvd(dot(), received()) -> boolean() | {error, term()}.
is_rcvd({Id, Ctr}=Dot, Rcvd) ->
    case maps:get(Id, Rcvd) of
        {LastCtr, UnRcvdList} when is_integer(LastCtr)->
            Ctr =< LastCtr andalso not is_exception(Dot, UnRcvdList);
        Ret ->
            {error, Ret}
    end.

%% @doc Check if a dot is unrcvd.
-spec is_unrcvd(dot(), received()) -> boolean().
is_unrcvd({Id, Ctr}=Dot, Rcvd) ->
    case maps:get(Id, Rcvd, notfound) of
        notfound ->
            false;
        {LastCtr, UnRcvdList} ->
            Ctr > LastCtr orelse is_exception(Dot, UnRcvdList)
    end.

%% @doc an effecient format for disk / wire.
%% @see from_binary/1
-spec to_binary(received()) -> binary_rcvd().
to_binary(Rcvd) ->
    term_to_binary(Rcvd).

%% @doc takes the output of `to_binary/1' and returns a Rcvd
-spec from_binary(binary_rcvd()) -> received().
from_binary(Bin) ->
    binary_to_term(Bin).

