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

%% @doc camus peer service.

-module(camus_ps).
-author("Vitor Enes <vitorenesduarte@gmail.com>").
-author("Georges Younes <georges.r.younes@gmail.com>").

-include("camus.hrl").

-behaviour(gen_server).

%% API
-export([start_link/0,
         myself/0,
         join/1,
         members/0,
         forward_message/3,
         forward_message_with_latency/4,
         exit/2]).

%% gen_server
-export([init/1,
         handle_call/3,
         handle_cast/2]).

-record(state, {connections :: camus_ps_connections:connections()}).

-spec start_link() -> {ok, pid()} | ignore | error().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec myself() -> node_spec().
myself() ->
    Id = camus_config:id(),
    Ip = camus_config:get(camus_ip),
    Port = camus_config:get(camus_port),
    {Id, Ip, Port}.

-spec join(node_spec()) -> ok | error().
join(Spec) ->
    gen_server:call(?MODULE, {join, Spec}, infinity).

-spec members() -> {ok, list(camus_id())}.
members() ->
    gen_server:call(?MODULE, members, infinity).

-spec forward_message(camus_id(), atom(), message()) -> ok.
forward_message(Id, Mod, Message) ->
    %% pick a random connection
    Name = camus_util:connection_name(Id),
    gen_server:cast(Name, {forward_message, Mod, Message}).

-spec forward_message_with_latency(camus_id(), atom(), message(), non_neg_integer()) -> ok.
forward_message_with_latency(Id, Mod, Message, Latency) ->
    %% pick a random connection
    Name = camus_util:connection_name(Id),
    gen_server:cast(Name, {forward_message, Mod, Message, Latency}).

-spec exit(camus_id(), pid()) -> ok.
exit(Id, Pid) ->
    gen_server:cast(?MODULE, {exit, Id, Pid}).

init([]) ->
    lager:info("camus peer service initialized!"),

    %% create connections
    Connections = camus_ps_connections:new(),

    {ok, #state{connections=Connections}}.

handle_call({join, {Id, Ip, Port}=Spec}, _From,
            #state{connections=Connections0}=State) ->

    lager:info("Will connect to ~p", [Spec]),

    %% try to connect
    {Result, Connections} = camus_ps_connections:connect(Id, Ip, Port,
                                                        Connections0),

    notify_app(true, Connections),

    {reply, Result, State#state{connections=Connections}};

handle_call(members, _From, #state{connections=Connections}=State) ->
    Members = camus_ps_connections:members(Connections),
    {reply, {ok, Members}, State}.

handle_cast({exit, Id, Pid}, #state{connections=Connections0}=State) ->
    lager:info("EXIT from ~p.", [Id]),

    {NewMembers, Connections} = camus_ps_connections:exit(Id, Pid, Connections0),

    notify_app(NewMembers, Connections),

    {noreply, State#state{connections=Connections}}.

%% @private
-spec notify_app(boolean(), camus_ps_connections:connections()) -> ok.
notify_app(false, _) ->
    ok;
notify_app(true, Connections) ->
    Members = camus_ps_connections:members(Connections),
    camus_forward:update_members(Members),
    ok.
