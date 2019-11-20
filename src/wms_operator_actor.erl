%%%-------------------------------------------------------------------
%%% @author Attila Makra
%%% @copyright (C) 2019, OTP Bank Nyrt.
%%% @doc
%%%
%%% @end
%%% Created : 12. Nov 2019 14:01
%%%-------------------------------------------------------------------
-module(wms_operator_actor).
-author("Attila Makra").

-behaviour(gen_server).

-include("wms_operator.hrl").
-include_lib("wms_common/include/wms_common.hrl").
-include_lib("wms_logger/include/wms_logger.hrl").
-include_lib("wms_state/include/wms_state.hrl").

%% API

-export([start_link/0,
         interaction/3,
         login/0, logout/0]).
-export([init/1,
         handle_info/2,
         handle_call/3,
         handle_cast/2,
         interaction_wrapper/4]).

%% =============================================================================
%% Types
%% =============================================================================

%% @formatter:off
-type interaction_data() :: #{
  id := binary(),
  request_id := binary(),
  parameters := map(),
  started := timestamp(),
  module := module(),
  description := binary()
}.

-record(state, {
  phase = started :: started | logged_in,
  interactions :: #{
    binary() => #{
        module := module(),
        description := binary()
      }
  },
  processes = #{} :: #{
    pid() => interaction_data()
  }
}).
%% @formatter:on
-type state() :: #state{}.

-callback execute(InteractionID :: identifier_name(),
                  Parameters :: [{identifier_name(), literal()}]) ->
                   {ok, map()} | {error, term()}.

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() ->
  {ok, pid()} | ignore | {error, {already_started, pid()} | term()}.
start_link() ->
  Ret = gen_server:start_link({local, ?MODULE}, ?MODULE, [], []),
  ?info("Operator Actor stared."),
  Ret.

-spec interaction(identifier_name(), identifier_name(),
                  [{identifier_name(), literal()}]) ->
                   ok.
interaction(InteractionRequestID, InteractionID, Parameters) ->
  gen_server:cast(?MODULE, {interaction, InteractionRequestID,
                            InteractionID, Parameters}).

-spec login() ->
  ok.
login() ->
  gen_server:cast(?MODULE, login).

-spec logout() ->
  ok.
logout() ->
  gen_server:cast(?MODULE, logout).

%% =============================================================================
%% gen_server behaviour
%% =============================================================================

-spec init(Args :: term()) ->
  {ok, State :: state()}.
init(_) ->
  Interactions = wms_cfg:get(?APP_NAME, interactions, #{}),
  case map_size(Interactions) of
    0 ->
      ?warning("No interactions are defined for operator_actor on node ~s",
               [node()]);
    _ ->
      self() ! start,
      ?info("~p interactions are defined for operator_actor on node ~s",
            [maps:keys(Interactions), node()])
  end,
  {ok, #state{phase        = started,
              interactions = Interactions}}.

-spec handle_info(Info :: any(), State :: state()) ->
  {noreply, State :: state()}.

handle_info(start, #state{phase        = started,
                          interactions = Interactions} = State) ->
  InteractionIDS = maps:keys(Interactions),

  NewState =
    case wms_dist:call(wms_distributor_actor,
                       login,
                       [node(), InteractionIDS]) of
      ok ->
        ?info("Operator actor was loggen in on node ~s for interactions: ~p",
              [node(), InteractionIDS]),
        State#state{phase = logged_in};
      _ ->
        ?warning("Operator actor unable to login on node ~s", [node()]),
        erlang:send_after(1000, self(), start),
        State
    end,
  {noreply, NewState};
handle_info({'DOWN', Reference, process, Pid, Reason},
            #state{processes = Processes} = State) ->
  erlang:demonitor(Reference),

  NewState =
    case maps:get(Pid, Processes, undefined) of
      undefined ->
        State;
      #{request_id := InteractionRequestID} ->
        ok = wms_dist:call(wms_distributor_actor,
                           interaction_reply,
                           [node(), InteractionRequestID, Reason]),
        State#state{
          processes = maps:remove(Pid, Processes)}
    end,

  {noreply, NewState};

handle_info(Msg, State) ->
  ?warning("Unknown message: ~0p", [Msg]),
  {noreply, State}.

-spec handle_call(Info :: any(), From :: {pid(), term()}, State :: state()) ->
  {reply, term(), State :: state()}.
handle_call(_Cmd, _From, _State) ->
  throw(not_impl).

-spec handle_cast(Request :: any(), State :: state()) ->
  {noreply, State :: state()}.
handle_cast({interaction, InteractionRequestID, InteractionID, Parameters},
            #state{interactions = Interactions,
                   processes    = Processes} = State) ->
  NewState =
    case maps:get(InteractionID, Interactions, undefined) of
      undefined ->
        ?error("OPA-0001",
               "~s interaction not defined in operator_actor on node ~s",
               [InteractionID, node()]),
        ok = wms_dist:call(wms_distributor_actor,
                           interaction_reply,
                           [node(),
                            InteractionRequestID,
                            {error, {not_defined, InteractionID}}]),
        State;
      #{module := Module, description := Desc} ->
        {Pid, _} = spawn_monitor(?MODULE,
                                 interaction_wrapper,
                                 [Module, InteractionRequestID, InteractionID, Parameters]),
        State#state{
          processes = Processes#{
            Pid => #{
              id => InteractionID,
              request_id => InteractionRequestID,
              parameters => Parameters,
              started => wms_common:timestamp(),
              module => Module,
              description => Desc
            }
          }
        }
    end,
  {noreply, NewState};
handle_cast(login, State) ->
  self() ! start,
  {noreply, State#state{phase = started}};
handle_cast(logout, State) ->
  ok = wms_dist:call(wms_distributor_actor,
                     logout,
                     [node()]),
  ?info("Operator actor was logged out on node ~s", [node()]),
  {noreply, State#state{phase = started}};
handle_cast(_, State) ->
  {noreply, State}.

%% =============================================================================
%% Private functions
%% =============================================================================

-spec interaction_wrapper(module(), identifier_name(), identifier_name(), [{identifier_name(), literal()}]) ->
  no_return().
interaction_wrapper(Module, InteractionRequestID, InteractionID, Parameters) ->
  process_flag(trap_exit, true),

  % process to send `keepalive` signal
  FAlive =
    fun FAlive() ->
      ok = wms_dist:call(wms_distributor_actor,
                         keepalive,
                         [node(), InteractionRequestID]),
      timer:sleep(5000),
      FAlive()
    end,
  spawn_link(FAlive),

  Result =
    try
      ?info("~s operator actor started for ~s interaction, with ~s
      request ID", [Module,
                    InteractionID,
                    InteractionRequestID]),

      Ret = apply(Module, execute, [InteractionID, Parameters]),

      ?info("~s operator actor executed for ~s interaction, ~s
      request ID, with result: ~p", [Module,
                                     InteractionID,
                                     InteractionRequestID,
                                     Ret]),
      Ret
    catch
      C:E:St ->
        ?error("OPA-0002",
               "~s operator actor error for ~s interaction, on node ~s "
               "with ~s request ID: ~s:~p~n~p", [Module,
                                                 InteractionID,
                                                 node(),
                                                 InteractionRequestID,
                                                 C, E, St]),
        {error, E}
    end,
  exit(Result).