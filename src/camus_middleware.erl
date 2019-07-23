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

-module(camus_middleware).
-author("Georges Younes <georges.r.younes@gmail.com").

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% client callbacks
-export([cbcast/2,
         setnotifyfun/1,
         setmembership/1,
         getstate/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("camus.hrl").

-record(state, {actor :: id(),
                vclock :: vclock(),
                depgraph :: depgraph(),
                nodebitmap :: orddict:orddict(),
                latency :: term(),
                bits :: non_neg_integer(),
                unacked_list :: unacked(),
                rcvd :: rcvd(),
                notify_function :: fun()}).

-type state_t() :: #state{}.

%%%===================================================================
%%% client callbacks
%%%===================================================================

%% Broadcast message.
-spec cbcast(message(), {dot(), context()}) -> term().
cbcast(Msg, {Dot, Ctxt}) ->
    gen_server:call(?MODULE, {cbcast, Dot, Ctxt, Msg}, infinity).

%% Register the delivery function.
-spec setnotifyfun(fun()) -> ok.
setnotifyfun(Fun) ->
    gen_server:call(?MODULE, {setnotifyfun, Fun}, infinity).

%% Set membership.
-spec setmembership([id()]) -> ok.
setmembership(NodeList) ->
    gen_server:call(?MODULE, {setmembership, NodeList}, infinity).

%% Get state.
-spec getstate() -> term().
getstate() ->
    gen_server:call(?MODULE, getstate, infinity).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Same as start_link([]).
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init(list()) -> {ok, state_t()}.
init([]) ->

    F = fun(Msg) ->
        lager:info("Message unhandled: ~p", [Msg]),
        ok
    end,
    {ok, #state{actor=?UTIL:get_node(),
                vclock=vclock:new(),
                depgraph=depgraph:new(),
                nodebitmap=orddict:new(),
                bits=0,
                unacked_list=unacked:new(),
                rcvd=rcvd:new(),
                latency=camus_config:get(camus_latency, undefined),
                notify_function=F}}.

%% @private
-spec handle_call(term(), {pid(), term()}, state_t()) ->
    {reply, term(), state_t()}.

handle_call({setmembership, NodeList}, _From, #state{actor=Actor, rcvd=Rcvd0, latency=Latency0}=State) ->
    Sorted = lists:usort([Actor|NodeList]),
    Length = length(Sorted),
    Membership = lists:foldl(
        fun(X, Acc) ->
            orddict:append(lists:nth(X, Sorted), round(math:pow(2, Length-X)), Acc)
        end,
        orddict:new(),
        lists:seq(1, Length)),
    N = round(math:pow(2, Length) - 1),
    Rcvd = rcvd:init(NodeList, Rcvd0),
    Latency = ?UTIL:generate_latency(Latency0, Sorted),
    ?LOG("generate_latency gave the following matrixL ~p", [Latency]),
    {reply, ok, State#state{nodebitmap=Membership, bits=N, rcvd=Rcvd, latency=Latency}};

handle_call(getstate, _From,
    #state{actor=Actor,
                   vclock=VClock,
                   depgraph=Depgraph,
                   nodebitmap=Nodebitmap}=State) ->
    {reply, {Actor, VClock, Depgraph, Nodebitmap}, State};

handle_call({cbcast, Dot, Ctxt, Msg}, _From,
            #state{actor=Actor,
                   vclock=VClock,
                   depgraph=Depgraph,
                   latency=Latency,
                   unacked_list=Unacked0,
                   rcvd=Rcvd,
                   bits=N,
                   notify_function=F,
                   nodebitmap=Nodebitmap}=State) ->

    ?LOG("handle_call cbcast"),

    %% update VV
    VClock1 = vclock:update_dot(Dot, VClock),

    %% calculate bitstr
    [Bi] = orddict:fetch(Actor, Nodebitmap),
    B = N - Bi,
    %% add to unacked

    Unacked = unacked:add(Dot, Ctxt, Msg, B, Unacked0),

    %% bcast and record transmission
    M = {remotemsg, {Id, Ctr}=Dot, Ctxt, Msg, Rcvd},
    Peers = orddict:fetch_keys(Nodebitmap),
    ?UTIL:bcast(M, ?MODULE, Peers, Latency),

    %% filter stable deps
    Filt = fun(K, V) -> not is_stable({K, V}, VClock1, Depgraph) end,
    Preds = maps:filter(Filt, Ctxt),

    %% add to to graph
    Depgraph1 = maps:fold(
        fun(K, V, Acc) ->
            Succ0 = depgraph:get({K, V}, succ, Acc),
            Succ1 = maps:put(Id, Ctr, Succ0),
            depgraph:update({K, V}, [{succ, Succ1}], Acc)
        end,
        Depgraph,
        Preds
    ),

    %% Update bitstr
    List = [{stage, ?DLV}, {bitstr, B}, {pred, Preds}, {succ, maps:new()}, {pyld, Msg}],
    Depgraph2 = depgraph:add_with(Dot, List, Depgraph1),

    %% update stability
    Depgraph3 = update_stability(B, Dot, Depgraph2, F),

    ?LOG("Delivered locally dot ~p, vclock is ~p, ctxt is ~p", [Dot, VClock1, Ctxt]),

    {reply, ok, State#state{vclock=VClock1, depgraph=Depgraph3, unacked_list=Unacked}};

handle_call({setnotifyfun, F}, _From, State) ->
    {reply, ok, State#state{notify_function=F}}.

%% @private
-spec handle_cast(term(), state_t()) -> {noreply, state_t()}.
handle_cast({remotemsg, {Id, Ctr}=Dot, P, Pyld, RRcvd},
            #state{vclock=VClock0,
                   notify_function=F,
                   depgraph=Depgraph0,
                   latency=Latency,
                   unacked_list=Unacked0,
                   rcvd=LRcvd,
                   bits=N,
                   nodebitmap=Nodebitmap}=State) ->

    % Case dot not received/delivered before
    {VClockX, DepgraphX, UnackedX} = case rcvd:is_rcvd(Dot, LRcvd) of
        true ->
            {VClock0, Depgraph0, Unacked0};
        false ->
            %% TODO ack unacked
            [B0] = orddict:fetch(Id, Nodebitmap),
            {ResendList, Unacked1} = unacked:ack(RRcvd, N - B0, Unacked0),

            %% TODO prepend remotemsg, append Rcvd, resend
            prepare_resend(ResendList, LRcvd, Latency, Id),

            %% filter stable
            Filt = fun(K, V) -> not is_stable({K, V}, VClock0, Depgraph0) end,
            Preds = maps:filter(Filt, P),

            % For each predecessor, if not in graph add SLT
            % then add Dot to the succ of every pred
            % Bor/sum the provenenance/node bits of every undelivered pred
            {Depgraph2, B} = maps:fold(
                fun(K, V, {AccG, AccB}) ->

                    [Bx] = orddict:fetch(K, Nodebitmap),

                    {Depgraph1, B} = case depgraph:is_element({K, V}, AccG) of
                        true ->
                            {AccG, case depgraph:get({K, V}, stage, AccG) =/= ?DLV of
                                true ->
                                    AccB bor Bx;
                                false ->
                                    AccB
                            end};
                        false ->
                            {depgraph:add_with({K, V}, [{stage, ?SLT}, {succ, maps:new()}], AccG), AccB bor Bx}
                    end,
                    Succ0 = depgraph:get({K, V}, succ, Depgraph1),
                    Succ1 = maps:put(Id, Ctr, Succ0),
                    {depgraph:update({K, V}, [{succ, Succ1}], Depgraph1), B}
                end,
                {Depgraph0, 0},
                Preds
            ),
            % prepare values list
            List = [{stage, ?RCV}, {bitstr, B}, {pred, P}, {pyld, Pyld}],
            % if SLT update, else add with succ empty
            Depgraph3 = case depgraph:is_element(Dot, Depgraph2) of
                true ->
                    depgraph:update(Dot, List, Depgraph2);
                false ->
                    depgraph:add_with(Dot, [{succ, maps:new()}|List], Depgraph2)
            end,
            % if B=0 means all preds of Dot are delivered and so deliver Dot
            {VClock1, Depgraph4} = case B == 0 of
                true ->
                    deliver(Dot, VClock0, Depgraph3, F, Nodebitmap, N);
                false ->
                    {VClock0, Depgraph3}
            end,
            {VClock1, Depgraph4, Unacked1}
    end,

    {noreply, State#state{vclock=VClockX, depgraph=DepgraphX, unacked_list=UnackedX}};

handle_cast({delstable, Dot},
            #state{depgraph=Depgraph0}=State) ->

    Depgraph1 = deletestable(Dot, Depgraph0),

    {noreply, State#state{depgraph=Depgraph1}};

handle_cast(Msg, State) ->
    lager:warning("Unhandled cast messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), state_t()) -> {noreply, state_t()}.
handle_info(Msg, State) ->
    lager:warning("Unhandled info messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), state_t()) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, state_t(), term()) -> {ok, state_t()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% middleware internal private functions and procedures
%%%===================================================================

%% @private
-spec deliver(dot(), vclock(), depgraph(), fun(), orddict:orddict(), non_neg_integer()) -> {vclock(), depgraph()}.
deliver({Id, _}=Dot, VClock, Depgraph, F, Nodebitmap, N) ->
    P = depgraph:get(Dot, pred, Depgraph),
    Pyld = depgraph:get(Dot, pyld, Depgraph),
    F({camus, {deliver, Pyld, {Dot, P}}}),
    %% update vv
    VClock1 = vclock:update_dot(Dot, VClock),
    %% Update stage and bitstr
    [Bi] = orddict:fetch(?UTIL:get_node(), Nodebitmap),
    [Bj] = orddict:fetch(Id, Nodebitmap),
    %% ts TF is the timestamp used sinnce something is delivered until stabilized
    Depgraph1 = depgraph:update(Dot, [{stage, ?DLV}, {bitstr, N - (Bi bor Bj)}], Depgraph),
    %% update stability
    B1 = N - Bj,
    Depgraph2 = update_stability(B1, Dot, Depgraph1, F),

    maps:fold(
        fun(K, V, {AccVC, AccDepgraph}) ->
            B2 = depgraph:get({K, V}, bitstr, AccDepgraph),
            NewB = B2 band B1,
            Depgraph3 = depgraph:update({K, V}, [{bitstr, NewB}], AccDepgraph),
            case NewB == 0 of
                true ->
                    deliver({K, V}, AccVC, AccDepgraph, F, Nodebitmap, N);
                false ->
                    {AccVC, Depgraph3}
            end
        end,
        {VClock1, Depgraph2},
        depgraph:get(Dot, succ, Depgraph2)
    ).

%% @private
-spec update_stability(non_neg_integer(), dot(), depgraph(), fun()) -> depgraph().
update_stability(B, Dot, Depgraph0, F) ->
    maps:fold(
        fun(K, V, Acc) ->
            case depgraph:get({K, V}, stage, Acc) of
                ?STB ->
                    Acc;
                _ ->
                    B1 = depgraph:get({K, V}, bitstr, Acc),
                    B2 = B1 band B,
                    case B1 =/= B2 of
                        true ->
                            Depgraph1 = depgraph:update({K, V}, [{bitstr, B2}], Acc),
                            case B2 == 0 of
                                true ->
                                    stabilize({K, V}, Depgraph1, F);
                                false ->
                                    update_stability(B, {K, V}, Depgraph1, F)
                            end;
                        false ->
                            Acc
                    end
            end
        end,
        Depgraph0,
        depgraph:get(Dot, pred, Depgraph0)
    ).

%% @private
-spec stabilize(dot(), depgraph(), fun()) -> depgraph().
stabilize(Dot, Depgraph0, F) ->
    Preds = depgraph:get(Dot, pred, Depgraph0),
    Depgraph1 = maps:fold(
        fun(K, V, Acc) ->
            case depgraph:get(Dot, stage, Acc) of
                ?STB ->
                    Acc;
                _ ->
                    stabilize({K, V}, Acc, F)
            end
        end,
        Depgraph0,
        Preds
    ),
    Pyld = depgraph:get(Dot, pyld, Depgraph1),
    F({camus, {stable, Pyld, {Dot, Preds}}}),
    depgraph:update(Dot, [{stage, ?STB}], Depgraph1).

%% @private
-spec deletestable(dot(), depgraph()) -> depgraph().
deletestable(Dot, Depgraph0) ->
    Depgraph1 = maps:fold(
        fun(K, V, Acc) ->
            Preds = maps:remove(Dot, depgraph:get({K, V}, pred, Acc)),
            depgraph:update({K, V}, [{pred, Preds}], Acc)
        end,
        Depgraph0,
        depgraph:get(Dot, succ, Depgraph0)
    ),
    depgraph:delete(Dot, Depgraph1).

%% @private
-spec is_stable(dot(), vclock(), depgraph()) -> boolean().
%% checks if a dot D is stable.
%% D stable if D already delivered and:
%% - ready to be stable i.e. STB
%% - already stablize and not in G
is_stable(D, V, G) ->
    vclock:is_element(D, V)
    andalso case depgraph:is_element(D, G) of
        true ->
            depgraph:get(D, stage, G) == ?STB;
        false ->
            true
    end.

%% @private
prepare_resend(ResendList, Rcvd, Latency, SenderId) ->
    lists:foreach(
        fun({Dot, Ctxt, Msg}) ->
            M = {remotemsg, Dot, Ctxt, Msg, Rcvd},
            ?UTIL:send(M, ?MODULE, SenderId, Latency)
        end,
    ResendList).
