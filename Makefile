.PHONY: help build

APP_NAME ?= swarm
VERSION ?= `cat mix.exs | grep "version:" | cut -d '"' -f2`

help:
	@echo "$(APP_NAME):$(VERSION)"
	@perl -nle'print $& if m{^[a-zA-Z_-]+:.*?## .*$$}' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

clean: ## Clean build artifacts
	rm -rf _build

deps: ## Fetch deps
	mix deps.get

build: deps ## Build the app
	mix compile

test: ## Execute tests
	mix test

concuerror: deps ## Execute concuerror tests
	deps/concuerror/concuerror -f test/concuerror/swarm_tests.erl -m swarm_tests -t simple_test \
		--pa _build/dev/lib/swarm/ebin \
		--pa _build/dev/lib/gen_state_machine/ebin \
		--pa _build/dev/lib/libring/ebin \
		--pa ~/.asdf/installs/elixir/1.3.4/lib/elixir/ebin \
		--pa ~/.asdf/installs/elixir/1.3.4/lib/logger/ebin

test-coverage: ## Generate coverage report
	mix test --cover
