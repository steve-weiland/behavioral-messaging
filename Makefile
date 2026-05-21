.DEFAULT_GOAL := help

COMPOSE ?= docker compose

.PHONY: help up down logs ps rebuild seed seed-people seed-segments \
	mysql events-count people-count segments-count

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

segments-count: ## SELECT count(*) FROM segments
	$(COMPOSE) exec -T mysql mysql -ubm -pbm bm -e 'SELECT count(*) FROM segments;'

seed-segments: ## Define active_pro + two people + check membership before/after event (V1 PR 3)
	@echo "1. define active_pro = plan=pro AND viewed_pricing"
	@curl -fsS -X POST -H 'Content-Type: application/json' -H 'X-Workspace-ID: ws_alpha' \
		-d '{"segment_id":"active_pro","name":"Active pro users","definition":{"op":"and","conditions":[{"op":"attr_eq","key":"plan","value":"pro"},{"op":"event_seen","name":"viewed_pricing"}]}}' \
		http://localhost:8090/segments && echo
	@echo
	@echo "2. identify p_alice (plan=pro) and p_bob (plan=free)"
	@curl -fsS -X POST -H 'Content-Type: application/json' -H 'X-Workspace-ID: ws_alpha' \
		-d '{"person_id":"p_alice","attributes":{"plan":"pro"}}' http://localhost:8090/people > /dev/null && echo "  p_alice ok"
	@curl -fsS -X POST -H 'Content-Type: application/json' -H 'X-Workspace-ID: ws_alpha' \
		-d '{"person_id":"p_bob","attributes":{"plan":"free"}}' http://localhost:8090/people > /dev/null && echo "  p_bob   ok"
	@echo
	@echo "3. check membership BEFORE event (expect both false)"
	@printf "  alice: "; curl -fsS -H 'X-Workspace-ID: ws_alpha' 'http://localhost:8090/segments/active_pro/check?person_id=p_alice'; echo
	@printf "  bob:   "; curl -fsS -H 'X-Workspace-ID: ws_alpha' 'http://localhost:8090/segments/active_pro/check?person_id=p_bob'; echo
	@echo
	@echo "4. track viewed_pricing for BOTH"
	@curl -fsS -X POST -H 'Content-Type: application/json' -H 'X-Workspace-ID: ws_alpha' \
		-d '{"person_id":"p_alice","event_name":"viewed_pricing","payload":{}}' http://localhost:8090/events > /dev/null && echo "  p_alice ok"
	@curl -fsS -X POST -H 'Content-Type: application/json' -H 'X-Workspace-ID: ws_alpha' \
		-d '{"person_id":"p_bob","event_name":"viewed_pricing","payload":{}}' http://localhost:8090/events > /dev/null && echo "  p_bob   ok"
	@echo
	@echo "5. check membership AFTER event (expect alice true, bob false)"
	@printf "  alice: "; curl -fsS -H 'X-Workspace-ID: ws_alpha' 'http://localhost:8090/segments/active_pro/check?person_id=p_alice'; echo
	@printf "  bob:   "; curl -fsS -H 'X-Workspace-ID: ws_alpha' 'http://localhost:8090/segments/active_pro/check?person_id=p_bob'; echo
