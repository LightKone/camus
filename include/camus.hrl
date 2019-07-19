-define(APP, camus).

%% data types
-type message() :: term().
-type id() :: term().
-type counter() :: non_neg_integer().
-type dot() :: dot:d().
-type context() :: context:ctxt().
-type vclock() :: vclock:vc().
-type mclock() :: mclock:mc().
-type depgraph() :: depgraph:dg().
-type depdot() :: {dot(), context()}.
-type dotstruct() :: depgraph:dotstruct().
-type unacked() :: unacked:ua().
-type rcvd() :: rcvd:received().

%% peer service
-type camus_id() :: node().
-type node_ip() :: inet:ip_address().
-type node_port() :: non_neg_integer().
-type node_spec() :: {camus_id(), node_ip(), node_port()}.
% -type handler() :: term(). %% module
% -type timestamp() :: non_neg_integer().
-type error() :: {error, term()}.

-define(CONNECTIONS, 1).

%% Processes
-define(UTIL, camus_util).
-define(MIDDLEWARE, camus_middleware).
-define(PS, camus_ps).

%% define stages for record
-define(SLT, 0).
-define(RCV, 1).
-define(DLV, 2).
-define(STB, 3).

%% logging
-ifdef(debug).
-define(LOG(M), lager:info(M)).
-define(LOG(M, A), lager:info(M, A)).
-else.
-define(LOG(_M), ok).
-define(LOG(_M, _A), ok).
-endif.
