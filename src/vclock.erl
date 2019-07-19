%% -------------------------------------------------------------------
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

%% @doc VClock.

-module(vclock).
-author("Vitor Enes <vitorenesduarte@gmail.com>").
-author("Georges Younes <georges.r.younes@gmail.com>").

-include("camus.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0,
         new/1,
         from_list/1,
         get_next_dot/2,
         get_dot/2,
         get_dot_ctr/2,
         add_dot/2,
         get_next_vv/2,
         update_dot/2,
         is_element/2,
         past_barrier/2,
         precedes/2,
         can_be_delivered/2,
         can_be_delivered/3,
         can_deliver/3,
         union/2,
         intersection/2,
         subtract/2,
         merge_all/3,
         to_binary/1,
         from_binary/1,
         size/1]).

-export_type([vc/0]).

-type vc() :: maps:map(id(), counter()).
-type binary_vclock() :: binary().

%% @doc Create an vclock.
-spec new() -> vc().
new() ->
    maps:new().

%% @doc Create an vclock.
-spec new([id()]) -> vc().
new(Members) ->
    lists:foldl(
        fun(Node, Acc) ->
            maps:put(Node, 0, Acc)
        end,
    maps:new(),
    Members).

%% @doc Create a vclock from a list of {id, counter}.
-spec from_list([{id(), counter()}]) -> vc().
from_list(L) ->
    maps:from_list(L).

%% @doc Return the next vv for a given id.
-spec get_next_vv(id(), vc()) -> vc().
get_next_vv(Id, VClock) ->
    Ctr = maps:get(Id, VClock, 0),
    maps:put(Id, Ctr + 1, VClock).

%% @doc Return the next dot for a given id.
-spec get_next_dot(id(), vc()) -> dot().
get_next_dot(Id, VClock) ->
    Ctr = maps:get(Id, VClock, 0),
    NewCtr = Ctr + 1,
    {Id, NewCtr}.

%% @doc Return the dot for a given id.
-spec get_dot(id(), vc()) -> dot().
get_dot(Id, VClock) ->
    Ctr = maps:get(Id, VClock, 0),
    {Id, Ctr}.

%% @doc Return the ctr given id.
-spec get_dot_ctr(id(), vc()) -> counter().
get_dot_ctr(Id, VClock) ->
    maps:get(Id, VClock, 0).

%% @doc Add a dot to the VClock.
-spec add_dot(dot(), vc()) -> vc().
add_dot({Id, Ctr}, VClock) ->
    %% maybe if it is less error
    %% instead of Max
    maps:update_with(
        Id,
        fun(CurrentCtr) -> max(Ctr, CurrentCtr) end,
        Ctr,
        VClock
    ).

%% @doc Update a dot entry in the VClock.
-spec update_dot(dot(), vc()) -> vc().
update_dot({Id, Ctr}, VClock) ->
    maps:put(Id, Ctr, VClock).

%% @doc Check if a dot is in the VClock.
-spec is_element(dot(), vc()) -> boolean().
is_element({Id, Ctr}, VClock) ->
    CurrentCtr = maps:get(Id, VClock, 0),
    Ctr =< CurrentCtr.

%% @doc Check if a dot is strictly less than the VClock.
-spec past_barrier(dot(), vc()) -> boolean().
past_barrier({Id, Ctr}, VClock) ->
    CurrentCtr = maps:get(Id, VClock, 0),
    Ctr < CurrentCtr.

%% @doc Check is a `VClockA' precedes or equal `VClockB'.
-spec precedes(vc(), vc()) -> boolean().
precedes(VClockA, VClockB) ->
    precedes_loop(maps:to_list(VClockA), VClockB).

%% @private
-spec precedes_loop([{id(), counter()}], vc()) -> boolean().
precedes_loop([], _) ->
    true;
precedes_loop([Dot|Rest], VClock) ->
    case is_element(Dot, VClock) of
        true -> precedes_loop(Rest, VClock);
        false -> false
    end.

%% @doc Checks if LocalClock preceded RemoteClock with the exception for RemoteDot entry.
-spec can_deliver(dot(), vc(), vc()) -> boolean().
can_deliver({Id, Ctr}=_RemoteDot, RemoteClock, LocalClock) ->
    case is_element({Id, Ctr - 1}, LocalClock) of
        true -> precedes(maps:remove(Id, RemoteClock), LocalClock);
        false -> false
    end.

%% @doc Checks if LocalClock preceded RemoteClock with the exception for RemoteDot entry.
-spec can_be_delivered(id(), vc(), vc()) -> boolean().
can_be_delivered(Id, RemoteClock, LocalClock) ->
    can_deliver({Id, get_dot_ctr(Id, RemoteClock)}, RemoteClock, LocalClock).

%% @doc Checks if LocalClock preceded RemoteClock with the exception for RemoteDot entry.
-spec can_be_delivered(context(), vc()) -> boolean().
can_be_delivered(Context, LocalClock) ->
    maps:fold(
        fun(K, V, Acc) ->
            case is_element({K, V}, LocalClock) of
                true ->
                    true andalso Acc;
                false ->
                    false
            end
        end,
        true,
    Context).

%% @doc
merge_all(MergeFun, MapA, MapB) ->
    %% merge A and with B
    %% (what's in B that's not in A won't be in `Map0')
    Map0 = maps:map(
        fun(Key, ValueA) ->
            case maps:find(Key, MapB) of
                {ok, ValueB} -> MergeFun(Key, ValueA, ValueB);
                error -> ValueA
            end
        end,
        MapA
    ),
    %% merge B with `Map0'
    maps:merge(MapB, Map0).

%% @doc Union clocks.
-spec union(vc(), vc()) ->vc().
union(VClockA, VClockB) ->
    merge_all(
        fun(_, CtrA, CtrB) -> max(CtrA, CtrB) end,
        VClockA,
        VClockB
    ).

%% @doc Intersect vclocks.
-spec intersection(vc(), vc()) -> vc().
intersection(VClockA, VClockB) ->
    VClock0 = maps:filter(
        fun(Id, _) -> maps:is_key(Id, VClockB) end,
        VClockA
    ),
    maps:map(
        fun(Id, CtrA) ->
            CtrB = maps:get(Id, VClockB),
            min(CtrA, CtrB)
        end,
        VClock0
    ).

%% @doc Subtract vclocks.
-spec subtract(vc(), vc()) -> list(dot()).
subtract(VClockA, VClockB) ->
    maps:fold(
        fun(Id, CtrA, Acc0) ->
            CtrB = maps:get(Id, VClockB, 0),
            case CtrB >= CtrA of
                true ->
                    Acc0;
                false ->
                    lists:foldl(
                        fun(Ctr, Acc1) -> [{Id, Ctr} | Acc1] end,
                        Acc0,
                        lists:seq(CtrB + 1, CtrA)
                    )
            end
        end,
        [],
        VClockA
    ).

%% @doc Size of vclock.
-spec size(vclock()) -> non_neg_integer().
size(VClock) ->
    maps:size(VClock).

%% @doc an effecient format for disk / wire.
%5 @see from_binary/1
-spec to_binary(vc()) -> binary_vclock().
to_binary(VClock) ->
    term_to_binary(VClock).

%% @doc takes the output of `to_binary/1' and returns a vclock
-spec from_binary(binary_vclock()) -> vc().
from_binary(Bin) ->
    binary_to_term(Bin).

-ifdef(TEST).

precedes_test() ->
    Bottom = #{},
    VClockA = #{a => 4, b => 1},
    VClockB = #{a => 6, c => 3},
    VClockC = #{a => 6, b => 1, c => 3},
    ?assert(precedes(Bottom, VClockA)),
    ?assert(precedes(Bottom, VClockB)),
    ?assert(precedes(Bottom, VClockC)),
    ?assert(precedes(VClockA, VClockA)),
    ?assertNot(precedes(VClockA, VClockB)),
    ?assertNot(precedes(VClockB, VClockA)),
    ?assert(precedes(VClockA, VClockC)),
    ?assert(precedes(VClockB, VClockC)),
    ?assertNot(precedes(VClockC, VClockA)),
    ?assertNot(precedes(VClockC, VClockB)).

can_deliver_test() ->
    VClockA = #{b => 1},
    VClockB = #{a => 6},
    VClockC = #{a => 4, c => 4},
    Local = #{a => 5, c => 3},
    ?assert(can_deliver({b, 1}, VClockA, Local)),
    ?assert(can_deliver({a, 6}, VClockB, Local)),
    ?assertNot(can_deliver({c, 5}, VClockB, Local)),
    ?assert(can_deliver({c, 4}, VClockC, Local)).

union_test() ->
    Bottom = #{},
    VClockA = #{a => 4, b => 1},
    VClockB = #{a => 6, c => 3},
    Expected = #{a => 6, b => 1, c => 3},

    ?assertEqual(VClockA, union(Bottom, VClockA)),
    ?assertEqual(VClockA, union(VClockA, Bottom)),
    ?assertEqual(Expected, union(VClockA, VClockB)),
    ?assertEqual(Expected, union(VClockB, VClockA)),
    ok.

intersection_test() ->
    Bottom = #{},
    VClockA = #{a => 4, b => 1},
    VClockB = #{a => 6, c => 3},
    Expected = #{a => 4},

    ?assertEqual(Bottom, intersection(Bottom, VClockA)),
    ?assertEqual(Bottom, intersection(VClockA, Bottom)),
    ?assertEqual(Expected, intersection(VClockA, VClockB)),
    ?assertEqual(Expected, intersection(VClockB, VClockA)),
    ok.

subtract_test() ->
    Bottom = #{},
    VClockA = #{a => 4, b => 1},
    VClockB = #{a => 6, c => 3},

    ?assertEqual([], subtract(Bottom, VClockA)),
    ?assertEqual([{a, 1}, {a, 2}, {a, 3}, {a, 4}, {b, 1}], lists:sort(subtract(VClockA, Bottom))),
    ?assertEqual([{b, 1}], subtract(VClockA, VClockB)),
    ?assertEqual([{a, 5}, {a, 6}, {c, 1}, {c, 2}, {c, 3}], lists:sort(subtract(VClockB, VClockA))),
    ok.

-endif.
