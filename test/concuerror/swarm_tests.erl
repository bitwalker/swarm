-module(swarm_tests).

-export([simple_test/0]).

simple_test() ->
    {ok, _} = 'Elixir.Logger.App':start(permanent, []),
    {ok, _} = gen_state_machine:start(permanent, []),
    {ok, _} = libring:start(permanent, []),
    {ok, _} = swarm:start(permanent, []),
    Pid1 = spawn(fun() -> loop() end),
    yes = swarm:register_name(test, Pid1),
    Pid1 = swarm:whereis_name(test),
    ok = swarm:unregister_name(test, Pid1),
    undefined = swarm:whereis_name(test).

loop() ->
    receive
        stop ->
            ok;
        _ ->
            loop()
    after 10000 ->
            ok
    end.
