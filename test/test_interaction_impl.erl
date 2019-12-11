%%%-------------------------------------------------------------------
%%% @author Attila Makra
%%% @copyright (C) 2019, Attila Makra.
%%% @doc
%%%
%%% @end
%%% Created : 13. Nov 2019 15:53
%%%-------------------------------------------------------------------
-module(test_interaction_impl).
-author("Attila Makra").

-behavior(wms_operator_actor).
-include_lib("wms_state/include/wms_state.hrl").
%% API
-export([execute/2]).


-spec execute(InteractionID :: identifier_name(),
              Parameters :: [{identifier_name(), literal()}]) ->
               {ok, map()} | {error, term()}.
execute(<<"test_interaction_add">>, [{_, Number1}, {_, Number2}]) ->
  {ok, #{<<"Result">> => Number1 + Number2}}.