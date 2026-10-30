# Development entry points. Everything here runs on macOS except `supervisor`,
# which needs Linux with KVM — see docs/local-development.md.

.DEFAULT_GOAL := help
.PHONY: help up down logs fmt lint test check docs doctor doctor-check api api-setup api-generate sdk-setup sdk-check reset-db clean

# The compose stack binds non-default ports so a natively installed PostgreSQL
# cannot be reached by mistake. See compose.yaml.
export HUDSON_OPENAPI_PYTHON ?= $(CURDIR)/.venv-openapi/bin/python

export DATABASE_URL ?= postgres://sandbox:sandbox@127.0.0.1:55432/sandbox

help: ## Show this help
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-8s %s\n", $$1, $$2}'

up: ## Start PostgreSQL and MinIO
	docker compose up -d --wait

down: ## Stop them, keeping data
	docker compose down

logs: ## Follow the dependency logs
	docker compose logs -f

fmt: ## Format
	cargo fmt --all

lint: ## Format check and clippy, as CI runs them
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets -- -D warnings

test: ## Run the test suite (schema tests need `make up` first)
	cargo test --workspace

reset-db: ## Drop and recreate the development schema
	docker compose exec -T postgres psql -U sandbox -d sandbox -q \
		-c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"

check: runtime-artifacts-test guest-image-check host-image-check api sdk-check mcp-check lint test docs doctor-check source-install-check ## Everything CI checks

.PHONY: source-install-check
source-install-check: ## Test native source installation without compiling or starting services
	python3 -m unittest discover -s scripts -p test_install_source.py

docs: ## Documentation link and fence checks
	python3 scripts/check_docs.py

doctor: ## Diagnose native prerequisites without starting services
	python3 scripts/doctor.py

doctor-check: ## Test read-only diagnosis without tools, services or KVM
	python3 -m unittest discover -s scripts -p test_doctor.py

clean: ## Remove build output and dependency volumes
	cargo clean
	docker compose down -v

api-setup: ## Install pinned OpenAPI validation tools in an isolated environment
	python3 -m venv .venv-openapi
	.venv-openapi/bin/python -m pip install -r scripts/api-requirements.txt

api-generate: ## Regenerate Rust, Python and TypeScript HTTP models and requests
	python3 scripts/generate_api.py

api: ## Validate OpenAPI, generation drift and contract checker regressions
	$(HUDSON_OPENAPI_PYTHON) scripts/check_api.py
	python3 scripts/generate_api.py --check
	$(HUDSON_OPENAPI_PYTHON) -m unittest discover -s scripts -p test_api_contract.py


sdk-setup: ## Install pinned Python and Node SDK development dependencies
	python3 -m venv .venv-sdk
	.venv-sdk/bin/python -m pip install -r scripts/sdk-requirements.txt
	npm ci --prefix sdk/typescript --ignore-scripts

SDK_PYTHON_SOURCES = sdk/python/src sdk/python/tests sdk/python/examples sdk/tests/python_runner.py scripts/generate_clients.py scripts/check_sdk_packages.py scripts/guest_image.py scripts/build_guest_image.py scripts/test_guest_image.py scripts/launch_verified_host.py scripts/test_host_images.py
SDK_GENERATED_EXCLUDES = --exclude models.py --exclude _requests.py --exclude _schema.py

sdk-check: ## Check SDK parsing, types and unpublished package installation
	.venv-sdk/bin/ruff check $(SDK_GENERATED_EXCLUDES) $(SDK_PYTHON_SOURCES)
	.venv-sdk/bin/ruff format --check $(SDK_GENERATED_EXCLUDES) $(SDK_PYTHON_SOURCES)
	PYTHONPATH=sdk/python/src .venv-sdk/bin/python -m unittest discover -s sdk/python/tests
	npm test --prefix sdk/typescript
	.venv-sdk/bin/python scripts/check_sdk_packages.py

.PHONY: runtime-artifacts-test
runtime-artifacts-test: ## Test pinned runtime preparation without network or privilege
	python3 -m unittest discover -s scripts -p test_runtime_artifacts.py

.PHONY: mcp-setup mcp-check
mcp-setup: ## Install separate pinned optional MCP development dependencies
	python3 -m venv .venv-mcp
	.venv-mcp/bin/python -m pip install -r scripts/mcp-requirements.txt

mcp-check: ## Validate bounded local MCP tools and subprocess interoperability
	.venv-mcp/bin/ruff check sdk/mcp scripts/check_mcp_package.py
	.venv-mcp/bin/ruff format --check sdk/mcp scripts/check_mcp_package.py
	PYTHONPATH=sdk/python/src:sdk/mcp/src .venv-mcp/bin/python -m unittest discover -s sdk/mcp/tests
	.venv-mcp/bin/python scripts/check_mcp_package.py

.PHONY: guest-image-check
guest-image-check: ## Test guest artifact integrity and unprivileged ext4 reproducibility
	python3 -m unittest discover -s scripts -p test_guest_image.py

.PHONY: host-image-check
host-image-check: ## Test verified image admission and immutable host launch configuration
	python3 -m unittest discover -s scripts -p test_host_images.py
