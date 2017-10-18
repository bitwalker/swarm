-module(swarm).

-export([
  start/2, stop/1,
  register_name/2, register_name/4, unregister_name/1, whereis_name/1,
  join/2, leave/2, members/1,
  publish/2, multi_call/2, multi_call/3, send/2
]).

-define(SWARM, 'Elixir.Swarm').

%% @doc You shouldn't need this if you've added the
%% the `swarm` application to your applications
%% list, but it's here if you need it.
%% @end
start(Type, Args) ->
    ?SWARM:start(Type, Args).

%% Same as above, use if you need it.
stop(State) ->
    ?SWARM:stop(State).

%% @doc Registers a name to a pid. Should not be used directly,
%% should only be used with `{via, swarm, Name}`
-spec register_name(term(), pid()) -> yes | no.
register_name(Name, Pid) ->
    ?SWARM:register_name(Name, Pid).

%% @doc Registers a name to a process started by the provided
%% module/function/args. If the MFA does not start a process,
%% an error will be returned.
%% @end
-spec register_name(term(), atom(), atom(), [term()]) -> {ok, pid()} | {error, term()}.
register_name(Name, Module, Function, Args) ->
    ?SWARM:register_name(Name, Module, Function, Args).

%% @doc Unregisters a name.
-spec unregister_name(term()) -> ok.
unregister_name(Name) ->
    ?SWARM:unregister_name(Name).

%% @doc Get the pid of a registered name.
-spec whereis_name(term()) -> pid() | undefined.
whereis_name(Name) ->
    ?SWARM:whereis_name(Name).

%% @doc Join a process to a group
-spec join(term(), pid()) -> ok.
join(Group, Pid) ->
    ?SWARM:join(Group, Pid).

%% @doc Part a process from a group
-spec leave(term(), pid()) -> ok.
leave(Group, Pid) ->
    ?SWARM:leave(Group, Pid).

%% @doc Get a list of pids which are members of the given group
-spec members(term()) -> [pid()].
members(Group) ->
    ?SWARM:members(Group).

%% @doc Publish a message to all members of a group
-spec publish(term(), term()) -> ok.
publish(Group, Message) ->
    ?SWARM:publish(Group, Message).

%% @doc Call all members of a group with the given message
%% and return the results as a list.
%% @end
-spec multi_call(term(), term()) -> [any()].
multi_call(Group, Message) ->
    multi_call(Group, Message, 5000).

%% @doc Same as multi_call/2, but takes a timeout.
%% Any responses not received within that period are
%% ignored.
%% @end
-spec multi_call(term(), term(), pos_integer()) -> [any()].
multi_call(Group, Message, Timeout) ->
    ?SWARM:multi_call(Group, Message, Timeout).

%% @doc This function sends a message to the process registered to the given name.
%% It is intended to be used by GenServer when using `GenServer.cast/2`, but you
%% may use it to send any message to the desired process.
%% @end
-spec send(term(), term()) -> ok.
 send(Name, Msg) ->
    ?SWARM:send(Name, Msg).
