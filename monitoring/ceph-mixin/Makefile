# Main development workflow
all: fmt generate lint test

# Format Jsonnet code automatically
fmt:
	./lint-jsonnet.sh -i

# Generate all dashboards and alerts
generate: dashboards_out
	tox -ealerts-fix

# Install Jsonnet dependencies
vendor: jsonnetfile.lock.json
	tox -ejsonnet-bundler-install

# Generate JSON dashboards from Jsonnet templates
dashboards_out: vendor dashboards
	tox -ejsonnet-fix

# Run all linters (Jsonnet and alerts)
lint:
	tox -ejsonnet-lint
	tox -ealerts-lint

# Run all tests (verifies generated files match templates)
test: generate
	tox -ejsonnet-check
	tox -epromql-query-test
	tox -ealerts-test

# Alias for test
check: test

# Show this help message
help:
	@echo "Ceph Mixin Development Commands:"
	@echo ""
	@echo "  all       - Format, generate, lint, and test everything"
	@echo "  fmt       - Format Jsonnet code automatically"
	@echo "  generate  - Generate all dashboards and alerts"
	@echo "  vendor    - Install Jsonnet dependencies"
	@echo "  lint      - Run all linters (Jsonnet and alerts)"
	@echo "  test      - Run all tests (requires generated files)"
	@echo "  check     - Alias for test"
	@echo ""
	@echo "Quick start: 'make all' to run the complete workflow"

.PHONY: all fmt generate lint test check help
