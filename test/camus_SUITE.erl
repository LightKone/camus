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
%%

-module(camus_SUITE).
-author("Georges Younes <georges.r.younes@gmail.com>").

%% common_test callbacks
-export([%% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-compile([nowarn_export_all, export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").


-include("camus.hrl").

-define(PORT, 9000).

suite() ->
[{timetrap, {minutes, 2}}].

%% ===================================================================
%% common_test callbacks
%% ===================================================================

init_per_suite(_Config) ->
    _Config.

end_per_suite(_Config) ->
    _Config.

init_per_testcase(Case, _Config) ->
    ct:pal("Beginning test case ~p", [Case]),

    _Config.

end_per_testcase(Case, _Config) ->
    ct:pal("Ending test case ~p", [Case]),

    _Config.

all() ->
    [
     test1,
     test2,
     test3,
     test4,
     test5,
     test6,
     test7,
     test8,
     test9
    ].

%% ===================================================================
%% Tests.
%% ===================================================================

%% Test causal delivery and stability with full membership
test1(_Config) ->
  test(fullmesh, 5, 50, 100, 50, z),
  ok.
test2(_Config) ->
  test(fullmesh, 5, 50, 100, undefined, z),
  ok.
test3(_Config) ->
  test(fullmesh, 5, 50, 100, [10, 20, 30, 40, 50, 60, 70, 80, 90, 100], z),
  ok.
test4(_Config) ->
  test(fullmesh, 5, 100, 50, 50, z),
  ok.
test5(_Config) ->
  test(fullmesh, 5, 100, 50, undefined, z),
  ok.
test6(_Config) ->
  test(fullmesh, 5, 100, 50, [10, 20, 30, 40, 50, 60, 70, 80, 90, 100], z),
  ok.
test7(_Config) ->
  test(fullmesh, 5, 100, 200, 50, z),
  ok.
test8(_Config) ->
  test(fullmesh, 5, 100, 200, undefined, z),
  ok.
test9(_Config) ->
  test(fullmesh, 5, 100, 200, [10, 20, 30, 40, 50, 60, 70, 80, 90, 100], z),
  ok.
%% Test causal delivery and stability with full membership
test(Overlay, NodesNumber, MaxMsgNumber, MaxRate, Latency, _DropRatio) ->
  Options = [{node_number, NodesNumber},
             {overlay, Overlay},
             {msg_number, MaxMsgNumber},
             {msg_rate, MaxRate},
             % {drop_ratio, DropRatio},
             {latency, Latency}],
  IdToNode = start(Options),
  ct:pal("started"),
  construct_overlay(Options, IdToNode),
  ct:pal("overlay constructed"),
  %% start causal delivery and stability test
  fun_causal_test(IdToNode, Options),
  ct:pal("causality checked"),
  stop(IdToNode),
  ct:pal("node stopped"),
  ok.

%% ===================================================================
%% Internal functions.
%% ===================================================================

%% @private Start nodes.
start(Options) ->
    ok = start_erlang_distribution(),
    NodeNumber = proplists:get_value(node_number, Options),
    Latency = proplists:get_value(latency, Options),

    InitializerFun = fun(I, Acc) ->
        %% Start node
        Config = [{monitor_master, true},
                  {startup_functions, [{code, set_path, [codepath()]}]}],

        Name = get_node_name(I),
        case ct_slave:start(Name, Config) of
            {ok, Node} ->
                orddict:store(I, Node, Acc);
            Error ->
                ct:fail(Error)
        end
    end,

    IdToNode = lists:foldl(InitializerFun,
                           orddict:new(),
                           lists:seq(0, NodeNumber - 1)),

    % ct:pal("InitializerFun done"),

    LoaderFun = fun({_Id, Node}) ->
        %% Load exp
        ok = rpc:call(Node, application, load, [?APP]),

        %% Set lager log dir
        PrivDir = code:priv_dir(?APP),
        NodeDir = filename:join([PrivDir, "lager", Node]),
        ok = rpc:call(Node,
                      application,
                      set_env,
                      [lager, log_root, NodeDir])
    end,
    lists:foreach(LoaderFun, IdToNode),

    % ct:pal("LoaderFun done"),

    ConfigureFun = fun({Id, Node}) ->
        %% Configure camus
        lists:foreach(
            fun({Property, Value}) ->
                ok = rpc:call(Node,
                              camus_config,
                              set,
                              [Property, Value])
            end,
            [{node_number, NodeNumber},
             {camus_port, get_port(Id)},
             {camus_latency, Latency},
             {test_mode, false}]
        )
    end,
    lists:foreach(ConfigureFun, IdToNode),

    % ct:pal("ConfigureFun done"),

    StartFun = fun({_Id, Node}) ->
        {ok, _} = rpc:call(Node,
                           application,
                           ensure_all_started,
                           [?APP])
    end,
    lists:foreach(StartFun, IdToNode),

    % ct:pal("StartFun done"),

    IdToNode.

%% @private Stop nodes.
stop(IdToNode) ->
    StopFun = fun({I, _Node}) ->
        Name = get_node_name(I),
        case ct_slave:stop(Name) of
            {ok, _} ->
                ok;
            Error ->
                ct:fail(Error)
        end
    end,
    lists:foreach(StopFun, IdToNode).

%% @private Start erlang distribution.
start_erlang_distribution() ->
    os:cmd(os:find_executable("epmd") ++ " -daemon"),
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@" ++ Hostname), shortnames]) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok
    end.

get(fullmesh, N) ->
    All = lists:seq(0, N - 1),
    lists:foldl(
        fun(I, Acc) ->
            orddict:store(I, All -- [I], Acc)
        end,
        orddict:new(),
        All
    ).

%% @private Connect each node to its peers.
construct_overlay(Options, IdToNode) ->
  IdToNodeSpec = lists:map(
      fun({Id, Node}) ->
          Spec = rpc:call(Node, camus_ps, myself, []),
          {Id, Spec}
      end,
      IdToNode
  ),

  NodeNumber = orddict:size(IdToNode),
  Overlay = proplists:get_value(overlay, Options),
  Graph = get(Overlay, NodeNumber),

  lists:foreach(
      fun({I, Peers}) ->
          Node = orddict:fetch(I, IdToNode),

          lists:foreach(
              fun(Peer) ->
                  PeerSpec = orddict:fetch(Peer, IdToNodeSpec),

                  ok = rpc:call(Node,
                                camus_ps,
                                join,
                                [PeerSpec])
              end,
              Peers
          )
      end,
      Graph
  ).

%% @private
get_port(Id) ->
    ?PORT + Id.

%% @private
get_node_name(I) ->
    list_to_atom("n" ++ integer_to_list(I)).

%% @private
codepath() ->
    lists:filter(fun filelib:is_dir/1, code:get_path()).

fun_receive(Me, TotalMessages, TotalDelivered, State, Q, Receiver) ->
  receive
    {camus, Something} ->
      {NewQ, NewTotalDelivered, NewState} = case Something of
        cbcast ->
          ct:pal("cbcast at node ~p", [Me]),
          State1 = rpc:call(Me, camus, cbcast, [pyld, State]),
          {_Dot, Ctxt}=State,
          {NewDot, _NewCtxt}=State1,
          Q1 = [{deliver, {NewDot, Ctxt}}|Q],
          TotalDelivered1 = TotalDelivered + 1,
          {Q1, TotalDelivered1, State1};
        _ ->
          {Type, {_Pyld, TS}, State1} = rpc:call(Me, camus, handle, [Something, State]),
          TotalDelivered1 = case Type of
            deliver ->
              TotalDelivered + 1;
            stable ->
              TotalDelivered
          end,
          Q1 = [{Type, TS}|Q],
          {Q1, TotalDelivered1, State1}
      end,
      ct:pal("delivery ~p of ~p at node ~p", [NewTotalDelivered, TotalMessages, Me]),
      case TotalMessages =:= NewTotalDelivered of
        true ->
          Receiver ! {done, Me, NewQ};
        false ->
          fun_receive(Me, TotalMessages, NewTotalDelivered, NewState, NewQ, Receiver)
      end;
    M ->
      ct:fail("UNKWONN ~p", [M])
  end.

%% @private
fun_ready_to_check(Nodes, N, Dict, Runner) ->
  receive
    {done, Node, Q} ->
      Data = lists:reverse(Q),
      ct:pal("Node ~p, Queue: ~p", [Node, Data]),
      Dict1 = dict:store(Node, Data, Dict),
      case N > 1 of
        true -> 
          fun_ready_to_check(Nodes, N-1, Dict1, Runner);
        false ->
          Runner ! {done, Nodes, Dict1}
      end;
    M ->
      ct:fail("fun_ready_to_check :: received incorrect message: ~p", [M])
  end.

%% @private
get_next_poisson_distribution(MaxRate) ->
    Rate = 1/MaxRate,
    round(-math:log(1.0 - rand:uniform())/Rate).

%% @private
fun_send(_NodeReceiver, _MaxRate, 0) ->
  ok;
fun_send(NodeReceiver, MaxRate, Times) ->
  % ct:pal("sending cbcast ~p to ~p", [Times, NodeReceiver]),
  X = get_next_poisson_distribution(MaxRate),
  timer:sleep(X),
  NodeReceiver ! {camus, cbcast},
  fun_send(NodeReceiver, MaxRate, Times - 1).

%% @private
fun_check_delivery(Nodes, Dict) ->
  NewDict = dict:fold(
    fun(Node, Q, Acc) ->
      dict:store(Node, {Q, 0, vclock:new(Nodes), mclock:new(Nodes)}, Acc)
    end,
    Dict,
  Dict),

  Test = testDot(Nodes, NewDict, maps:new()),

  case Test of
    true ->
      ok;
    false ->
      ct:fail("fun_check_delivery")
  end.

%% @private
testDot([], _Dict, _DotToVV) ->
  true;
testDot([Node|Rest], Dict, DotToVV) ->
  {Seq, Last, _VV, _MC} = dict:fetch(Node, Dict),
  case iterate(lists:nthtail(Last, Seq), {local, Node, Dict, DotToVV}) of
    false ->
      ct:pal("testDot Node: ~p, false", [Node]),
      false;
    {NewDict, NewDotToVV} ->
      testDot(Rest, NewDict, NewDotToVV)
  end.

iterate([], {local, _Node, NodeDict, DotToVV}) ->
  {NodeDict, DotToVV};
iterate([{deliver, {Dot, _}}|_T], {Dot, Node, NodeDict, DotToVV}) ->
  update(Dot, Node, NodeDict, DotToVV);
iterate([{Type, {{Id, _}=Dot, Ctxt}}|T], {StopCond, Node, NodeDict, DotToVV}) ->
  case Type of
    deliver ->
      case Id == Node of
        true ->
          {NewNodeDict, NewDotToVV} = update(Dot, Node, NodeDict, DotToVV),
          iterate(T, {StopCond, Node, NewNodeDict, NewDotToVV});
        false ->
          case maps:is_key(Dot, DotToVV) of
            true ->
              {Seq, Last, VV, MC} = dict:fetch(Node, NodeDict),
              RemoteVV = maps:get(Dot, DotToVV),
              case vclock:can_deliver(Dot, RemoteVV, VV) of
                true ->
                  {NewNodeDict, _NewDotToVV} = update2(Seq, Last, VV, MC, Dot, Node, NodeDict, DotToVV),
                  iterate(T, {StopCond, Node, NewNodeDict, DotToVV});
                false ->
                  false
              end;
            false ->
              {Seq, Last, _, _} = dict:fetch(Id, NodeDict),
              case iterate(lists:nthtail(Last, Seq), {Dot, Id, NodeDict, DotToVV}) of
                false ->
                  false;
                {NewNodeDict, NewDotToVV} ->
                  iterate([{deliver, {Dot, Ctxt}}|T], {StopCond, Node, NewNodeDict, NewDotToVV})
              end
          end
      end;
    stable ->
      {Seq, Last, VV, MC} = dict:fetch(Node, NodeDict),
      Stable = mclock:stable(MC),
      case maps:get(Dot, DotToVV, error) of
        error ->
          false;
        DotVV ->
          case vclock:precedes(DotVV, Stable) of
            true ->
              NewNodeDict = dict:store(Node, {Seq, Last + 1, VV, MC}, NodeDict),
              iterate(T, {StopCond, Node, NewNodeDict, DotToVV});
            false ->
              false
          end
      end
  end.

update(Dot, Node, NodeDict, DotToVV) ->
  {Seq, Last, VV, MC} = dict:fetch(Node, NodeDict),
  update2(Seq, Last, VV, MC, Dot, Node, NodeDict, DotToVV).

update2(Seq, Last, VV, MC, {Id, _}=Dot, Node, NodeDict, DotToVV) ->
  NewVV = vclock:add_dot(Dot, VV),
  NewLast = Last + 1,
  MC1 = mclock:update(Node, NewVV, MC),
  %% this should not be updated unless it was not originally there
  {NewDotToVV, NewMC} = case maps:get(Dot, DotToVV, new) of
    new ->
      DotToVV2 = maps:put(Dot, NewVV, DotToVV),
      ct:pal("449 - At Node ~p Delivered. Dot: ~p, VV: ~p", [Node, Dot, NewVV]),
      {DotToVV2, MC1};
    MsgVV ->
      {DotToVV, mclock:update(Id, MsgVV, MC1)}
  end,
  NewNodeDict = dict:store(Node, {Seq, NewLast, NewVV, NewMC}, NodeDict),
  {NewNodeDict, NewDotToVV}.

%% @private
fun_check() ->
  receive
    {done, Nodes, Dict} ->
      fun_check_delivery(Nodes, Dict);
    M ->
      ct:fail("fun_check :: received incorrect message: ~p", [M])
  end.

fun_init_dict_info(Nodes, MaxMsgNumber) ->
  lists:foldl(
  fun(Node, Acc) ->
    dict:store(Node, MaxMsgNumber, Acc)
    % dict:store(Node, rand:uniform(MaxMsgNumber), Acc)
  end,
  dict:new(),
  Nodes).

%% private   
fun_setmembership(Nodes) ->   
  lists:foreach(fun(Node) ->    
      ok = rpc:call(Node, camus, setmembership, [Nodes])    
  end, Nodes).

fun_causal_test(IdToNode, Options) ->
  MaxMsgNumber = proplists:get_value(msg_number, Options),
  MaxRate = proplists:get_value(msg_rate, Options),
  % DropRatio = proplists:get_value(drop_ratio, Options),

  Nodes = [Node || {_Id, Node} <- IdToNode],

  fun_setmembership(Nodes),

  timer:sleep(1000),

  DictInfo = fun_init_dict_info(Nodes, MaxMsgNumber),

  %% Calculate the number of Messages that will be delivered by each node
  %% result = sum of msgs sent per node * number of nodes (Broadcast)
  TotNumMsgToRecv = dict:fold(
    fun(_Key, V, Acc)->
      Acc + V
    end,
    0,
    DictInfo),

  Self = self(),

  %% Spawn a receiver process to collect all delivered msgs dots and stabilized msgs per node
  Receiver = spawn(?MODULE, fun_ready_to_check, [Nodes, length(Nodes), DictInfo, Self]),
     
  timer:sleep(1000),

  NewDictInfo = lists:foldl(
  fun(Node, Acc) ->
    NumMsgToSend = dict:fetch(Node, Acc),
    
    %% Spawn a receiver process for each node to tbcast and deliver msgs
    NodeReceiver = spawn(?MODULE, fun_receive, [Node, TotNumMsgToRecv, 0, {dot:new_dot(Node), context:new()}, [], Receiver]),
    
    %% define a delivery function that notifies the Receiver upon delivery
    F=fun(M) -> 
      NodeReceiver ! M
    end,
    ok = rpc:call(Node,
      camus,
      setnotifyfun,
      [F]),
    
    dict:store(Node, {NumMsgToSend, NodeReceiver}, Acc)
  end,
  DictInfo,
  Nodes),

  timer:sleep(1000),

  %% Sending random messages and recording on delivery the VV of the messages in delivery order per Node
  lists:foreach(fun(Node) ->
    {MsgNumToSend, NodeReceiver} = dict:fetch(Node, NewDictInfo),
    spawn(?MODULE, fun_send, [NodeReceiver, MaxRate, MsgNumToSend])
  end, Nodes),

  fun_check().
