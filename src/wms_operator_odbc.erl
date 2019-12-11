%%%-------------------------------------------------------------------
%%% @author Attila Makra
%%% @copyright (C) 2019, Attila Makra.
%%% @doc
%%%
%%% @end
%%% Created : 19. Nov 2019 08:51
%%%-------------------------------------------------------------------
-module(wms_operator_odbc).
-author("Attila Makra").

%% API
-export([select/2,
         select/4,
         execute/2,
         execute/4,
         block/4,
         connect/3,
         disconnect/1]).

%% =============================================================================
%% Types
%% =============================================================================
-export_type([transaction_fun/0]).

-type db_record() :: #{
FieldName :: binary() => FieldValue :: term()
}.

-type transaction_fun() :: fun((Ref :: term()) ->
  {ok, term()} | {error, term()}).

-type db_cursor() :: [db_record()].

%% =============================================================================
%% API
%% =============================================================================

-spec connect(string(), string(), string()) ->
  {ok, Ref :: term()} | {error, term()}.
connect(DSN, UID, PWD) ->
  ConnStr = io_lib:format("DSN=~s;UID=~s;PWD=~s", [DSN, UID, PWD]),
  odbc:connect(ConnStr, [{scrollable_cursors, off},
                         {tuple_row, off},
                         {binary_strings, on}]).

-spec disconnect(term()) ->
  ok | {error, term()}.
disconnect(Ref) ->
  odbc:disconnect(Ref).

-spec block(string(), string(), string(), transaction_fun()) ->
  {ok, term()} | {error, term()}.
block(DSN, UID, PWD, TFun) ->
  case connect(DSN, UID, PWD) of
    {ok, Ref} ->
      execute_block(Ref, TFun);
    Error ->
      Error
  end.

-spec select(term(), string()) ->
  {ok, db_cursor()} | {error, term()}.
select(Ref, SQLQuery) ->
  cursor_to_map(odbc:sql_query(Ref, SQLQuery)).

-spec select(term(), string(), string(), [{FieldName :: string(), Value :: term()}]) ->
  {ok, db_cursor()} | {error, term()}.
select(Ref, TableName, SQLQuery, Params) ->
  SpecParams = get_column_types(Ref, TableName, Params),
  cursor_to_map(odbc:param_query(Ref, SQLQuery, SpecParams)).

-spec execute(term(), string()) ->
  {ok, non_neg_integer() | undefined} | {error, term()}.
execute(Ref, SqlCommand) ->
  case odbc:sql_query(Ref, SqlCommand) of
    {updated, Count} ->
      {ok, Count};
    Error ->
      Error
  end.

-spec execute(term(), string(), string(), [{FieldName :: string(), Value :: term()}]) ->
  {ok, non_neg_integer() | undefined} | {error, term()}.
execute(Ref, TableName, SqlCommand, Params) ->
  SpecParams = get_column_types(Ref, TableName, Params),
  case odbc:param_query(Ref, SqlCommand, SpecParams) of
    {updated, Count} ->
      {ok, Count};
    Error ->
      Error
  end.

%% -----------------------------------------------------------------------------
%% Private functions
%% -----------------------------------------------------------------------------

-spec cursor_to_map({selected, [string()], [[term()]]}) ->
  {ok, db_cursor()} | {error, term()}.
cursor_to_map({selected, ColumnNames, Rows}) ->
  {ok, lists:map(
    fun(Record) ->
      record_to_map(Record, ColumnNames)
    end, Rows)};
cursor_to_map(Other) ->
  Other.

-spec record_to_map([term()], [string()]) ->
  db_record().
record_to_map(Record, ColumnNames) ->
  Record1 = lists:zip(ColumnNames, Record),
  lists:foldl(
    fun({Name, Value}, Accu) ->
      Accu#{list_to_binary(Name) => Value}
    end, #{}, Record1
  ).

-spec get_column_types(term(), string(), [{string(), term()}]) ->
  [{term(), [term()]}].
get_column_types(Ref, TableName, Params) ->
  {ok, ColumnSpecs} = odbc:describe_table(Ref, TableName),
  lists:map(
    fun({FieldName, Value}) ->
      case proplists:lookup(FieldName, ColumnSpecs) of
        none ->
          throw({error, {not_found, TableName ++ "." ++ FieldName}});
        {_, FieldType} ->
          {FieldType, [Value]}
      end
    end, Params).

-spec execute_block(term(), transaction_fun()) ->
  {ok, term()} | {error, term()}.
execute_block(Ref, TFun) ->
  try
    TFun(Ref)
  after
    disconnect(Ref)
  end.