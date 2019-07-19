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

-module(camus_sup).
-author("Georges Younes <georges.r.younes@gmail.com>").

-behaviour(supervisor).

-include("camus.hrl").

-export([start_link/0]).

-export([init/1]).

-define(CHILD(Name, Mod, Args),
        {Name, {Mod, start_link, Args}, permanent, 5000, worker, [Mod]}).
-define(CHILD(I),
        ?CHILD(I, I, [])).

-define(LISTENER_OPTIONS(Port),
        [{port, Port},
         %% we probably won't have more than 32 servers
         {max_connections, 32},
         %% one acceptor should be enough since the number
         %% of servers is small
         {num_acceptors, 1}]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Port = configure(),

    %% start server tcp acceptor
    {ok, _} = ranch:start_listener(camus_ps_server,
                                   ranch_tcp,
                                   ?LISTENER_OPTIONS(Port),
                                   camus_ps_client,
                                   []),

    Children = [?CHILD(camus_ps), ?CHILD(camus_middleware)],

    RestartStrategy = {one_for_one, 5, 10},
    {ok, {RestartStrategy, Children}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
configure() ->
    %% configure node number
    configure_int("NODE_NUMBER",
                  node_number,
                  1),

    %% configure node number
    configure_var("TEST_MODE",
                  test_mode,
                  false),

    %% configure event interval
    configure_int("EVENT_INTERVAL",
                  camus_event_interval,
                  1000),

    %% configure server latency
    configure_latency("LATENCY",
                         camus_latency,
                         undefined),

    %% configure server ip
    configure_ip("CAMUS_IP",
                 camus_ip,
                 {127, 0, 0, 1}),

    %% configure server port
    configure_int("CAMUS_PORT",
                         camus_port,
                         5000).

%% @private
configure_latency(Env, Var, Default) ->
    Current = camus_config:get(Var, Default),
    ?LOG("Current: ~p", [Current]),
    case Current of
        undefined ->
            configure_var(Env, Var, Default);
        L when is_integer(L) ->
            configure_int(Env, Var, Default);
        List when is_list(List) ->
            camus_config:set(Var, List)
    end.

%% @private
configure_var(Env, Var, Default) ->
    To = fun(V) -> atom_to_list(V) end,
    From = fun(V) -> list_to_atom(V) end,
    configure(Env, Var, Default, To, From).

%% @private
configure_int(Env, Var, Default) ->
    To = fun(V) -> integer_to_list(V) end,
    From = fun(V) -> list_to_integer(V) end,
    configure(Env, Var, Default, To, From).

%% @private
configure_ip(Env, Var, Default) ->
    To = fun(V) -> inet_parse:ntoa(V) end,
    From = fun(V) -> {ok, IP} = inet_parse:address(V), IP end,
    configure(Env, Var, Default, To, From).

%% @private
configure(Env, Var, Default, To, From) ->
    Current = camus_config:get(Var, Default),
    Val = From(
        os:getenv(Env, To(Current))
    ),
    camus_config:set(Var, Val),
    Val.
