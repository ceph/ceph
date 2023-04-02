all: fmt generate lint test

fmt:
	./lint-jsonnet.sh -i

generate: dashboards_out
	tox -ealerts-fix

vendor: jsonnetfile.lock.json
	tox -ejsonnet-bundler-install

dashboards_out: vendor dashboards
	tox -ejsonnet-fix

lint:
	tox -ejsonnet-lint
	tox -ealerts-lint

test: generate
	tox -ejsonnet-check
	tox -epromql-query-test
	tox -ealerts-test
check: test

.PHONY: all fmt generate lint test check
