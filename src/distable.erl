-module(distable).

-export([
  start/2, stop/1,
  register/4, register_property/2, unregister/1,
  whereis/1, get_by_property/1,
  call/2, call/3, cast/2
]).

-define(TRACKER, 'Elixir.Distable.Tracker').

%% @doc You shouldn't need this if you've added the
%% the `distable` application to your applications
%% list, but it's here if you need it.
%% @end
start(Type, Args) ->
    'Elixir.Distable':start(Type, Args).

%% Same as above, use if you need it.
stop(State) ->
    'Elixir.Distable':stop(State).

%% @doc Registers a name to a process started by the provided
%% module/function/args. If the MFA does not start a process,
%% an error will be returned.
%% @end
-spec register(term(), atom(), atom(), [term()]) -> {ok, pid()} | {error, term()}.
register(Name, Module, Function, Args) ->
    ?TRACKER:register(Name, {Module, Function, Args}).

%% @doc Unregisters a name.
-spec unregister(term()) -> ok.
unregister(Name) ->
    ?TRACKER:unregister(Name).

%% @doc Add metadata to a registered process.
-spec register_property(pid(), term()) -> ok.
register_property(Pid, Prop) ->
    ?TRACKER:register_property(Pid, Prop).

%% @doc Get the pid of a registered name.
-spec whereis(term()) -> pid() | undefined.
whereis(Name) ->
    ?TRACKER:whereis(Name).

%% @doc Get a list of pids which have the given property in their metadata.
-spec get_by_property(term()) -> [pid].
get_by_property(Prop) ->
    ?TRACKER:get_by_property(Prop).

%% @doc Call the server associated with a given name.
%% Use like `gen_server:call/3`
%% @end
-spec call(term(), term()) -> term() | {error, term()}.
call(Name, Message) ->
    call(Name, Message, 5000).

%% @doc Call the server associated with a given name.
%% Same as `call/2`, but takes a timeout value for the call.
%% Use like `gen_server:call/3`
%% @end
-spec call(term(), term(), pos_integer()) -> term() | {error, term()}.
call(Name, Message, Timeout) ->
    ?TRACKER:call(Name, Message, Timeout).

%% @doc Cast a message to a server associated with the given name.
%% Use like `gen_server:cast/2`
%% @end
-spec cast(term(), term()) -> ok.
cast(Name, Message) ->
    ?TRACKER:cast(Name, Message).
