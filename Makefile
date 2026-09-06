COMPOSE := docker compose -f extra/docker-compose.yml
BIN     := backend/bin

.DEFAULT_GOAL := help

.PHONY: help fmt vet test build check schema producer baseline quality \
        cluster-up cluster-down dashboard charts clean

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
	  awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-13s\033[0m %s\n", $$1, $$2}'

fmt: ## Format Go code
	cd backend && gofmt -w .

vet: ## Run go vet
	cd backend && go vet ./...

test: ## Run unit tests
	cd backend && go test ./...

build: ## Compile all commands to backend/bin/
	cd backend && \
	  go build -o bin/schema   ./cmd/schema   && \
	  go build -o bin/producer ./cmd/producer && \
	  go build -o bin/baseline ./cmd/baseline && \
	  go build -o bin/quality  ./cmd/quality
	@echo "built: $(BIN)/{schema,producer,baseline,quality}"

check: fmt vet test build ## Format, vet, test, and build everything

schema: build ## Create the keyspace + tables (run once after cluster-up)
	cd backend && ./bin/schema

producer: build ## Stream transactions and evaluate rules (Ctrl-C stops it)
	cd backend && ./bin/producer

baseline: build ## Recompute account baselines (feeds rule A1)
	cd backend && ./bin/baseline

quality: build ## Precision/recall vs planted ground truth (experiment E6)
	cd backend && ./bin/quality

cluster-up: ## Start the 3-node cluster and wait for all nodes UN
	bash scripts/cluster_up.sh

cluster-down: ## Stop the cluster (keeps data; make cluster-down ARGS=-v wipes it)
	$(COMPOSE) down $(ARGS)

dashboard: ## Run the read-only Streamlit dashboard
	cd frontend && streamlit run app.py

charts: ## Regenerate the benchmark SVG charts from the CSVs
	python3 benchmarks/chart.py

clean: ## Remove built binaries
	rm -rf $(BIN)
