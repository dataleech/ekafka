REBAR = rebar
LIBS = ERL_LIBS=deps

.PHONY: deps

compile:
	@$(REBAR) compile

deps:
	@$(REBAR) get-deps

run:
	@$(REBAR) compile skip_deps=true
	@$(LIBS) erl -pa ebin -s ekafka
