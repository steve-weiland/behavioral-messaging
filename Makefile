.DEFAULT_GOAL := help

COMPOSE ?= docker compose

.PHONY: help up down logs ps rebuild seed seed-people mysql events-count people-count

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-12s %s\n", $$1, $$2}'

up: ## Bring the V1 stack up (compose build + up -d)
	$(COMPOSE) up -d --build

down: ## Tear it all down (containers + volumes)
	$(COMPOSE) down --volumes --remove-orphans

logs: ## Tail logs from app services only
	$(COMPOSE) logs -f track-api stub-receiver

ps: ## List running services
	$(COMPOSE) ps

rebuild: ## Rebuild + restart Go services only
	$(COMPOSE) up -d --build track-api stub-receiver

seed: ## POST 10 events at 1 req/s
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
		curl -fsS -X POST \
			-H 'Content-Type: application/json' \
			-H 'X-Workspace-ID: ws_alpha' \
			-d "{\"person_id\":\"p_demo\",\"event_name\":\"seeded\",\"payload\":{\"i\":$$i}}" \
			http://localhost:8090/events && echo; \
		sleep 1; \
	done

seed-people: ## Identify p_alice with a small attribute set, then read back (V1 PR 2)
	@curl -fsS -X POST \
		-H 'Content-Type: application/json' \
		-H 'X-Workspace-ID: ws_alpha' \
		-d '{"person_id":"p_alice","attributes":{"plan":"pro","city":"Sydney","signup_year":2026}}' \
		http://localhost:8090/people && echo
	@curl -fsS -X POST \
		-H 'Content-Type: application/json' \
		-H 'X-Workspace-ID: ws_alpha' \
		-d '{"person_id":"p_alice","attributes":{"city":"Melbourne"}}' \
		http://localhost:8090/people && echo
	@echo "--- final state of p_alice ---"
	@curl -fsS -H 'X-Workspace-ID: ws_alpha' http://localhost:8090/people/p_alice

mysql: ## Interactive mysql shell against the bm database
	$(COMPOSE) exec mysql mysql -ubm -pbm bm

events-count: ## SELECT count(*) FROM events
	$(COMPOSE) exec -T mysql mysql -ubm -pbm bm -e 'SELECT count(*) FROM events;'

people-count: ## SELECT count(*) FROM people
	$(COMPOSE) exec -T mysql mysql -ubm -pbm bm -e 'SELECT count(*) FROM people;'
