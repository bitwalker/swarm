#!/bin/bash

mix compile

erl \
    -connect_all false \
    -hidden \
    -sname "$1" \
    -setcookie swarm_test \
    -config ./test/support/sys.config \
    -pa /Users/paulschoenfelder/erlang/19.1/*/ebin \
    -pa /Users/paulschoenfelder/src/github.com/bitwalker/swarm/_build/test/consolidated \
    -pa /Users/paulschoenfelder/src/github.com/bitwalker/swarm/_build/test/lib/*/ebin \
    -user Elixir.IEx.CLI \
    -extra --no-halt +iex
