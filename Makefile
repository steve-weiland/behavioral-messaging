.DEFAULT_GOAL := help

COMPOSE ?= docker compose

.PHONY: help up down logs ps rebuild test seed seed-people seed-segments seed-campaign showcase \
	load-prep load-quick load-soak \
	mysql events-count people-count segments-count campaigns-count enrollments-count

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

test: ## Run all Go unit tests (no docker required)
	go test ./...

load-prep: ## Pre-seed welcome_pro + 50 plan=pro people for load runs
	WORKSPACE=ws_alpha PEOPLE=50 DURATION=0 ./chaos/load-v1.sh

load-quick: ## 10s burst @ concurrency=5 (sanity)
	WORKSPACE=ws_alpha PEOPLE=50 DURATION=10 CONCURRENCY=5 ./chaos/load-v1.sh

load-soak: ## 60s steady @ concurrency=20 (ceiling search — V1 writeup data)
	WORKSPACE=ws_alpha PEOPLE=50 DURATION=60 CONCURRENCY=20 ./chaos/load-v1.sh

showcase: ## Run every V1 surface end-to-end (people → segment → campaign → 10 events) — BM-52
	@$(MAKE) -s seed-people
	@$(MAKE) -s seed-segments
	@$(MAKE) -s seed-campaign
	@$(MAKE) -s seed

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

campaigns-count: ## SELECT count(*) FROM campaigns
	$(COMPOSE) exec -T mysql mysql -ubm -pbm bm -e 'SELECT count(*) FROM campaigns;'

enrollments-count: ## SELECT count(*) FROM journey_enrollments
	$(COMPOSE) exec -T mysql mysql -ubm -pbm bm -e 'SELECT count(*) FROM journey_enrollments;'

seed-segments: ## Define active_pro + two people + check membership before/after event (V1 PR 3)
	@echo "1. ensure active_pro = plan=pro AND viewed_pricing (idempotent)"
	@if ! curl -fsS -H 'X-Workspace-ID: ws_alpha' http://localhost:8090/segments/active_pro > /dev/null 2>&1; then \
		curl -fsS -X POST -H 'Content-Type: application/json' -H 'X-Workspace-ID: ws_alpha' \
			-d '{"segment_id":"active_pro","name":"Active pro users","definition":{"op":"and","conditions":[{"op":"attr_eq","key":"plan","value":"pro"},{"op":"event_seen","name":"viewed_pricing"}]}}' \
			http://localhost:8090/segments && echo "   created"; \
	else echo "   already exists"; fi
	@echo
	@echo "2. identify p_alice (plan=pro) and p_bob (plan=free)"
	@curl -fsS -X POST -H 'Content-Type: application/json' -H 'X-Workspace-ID: ws_alpha' \
		-d '{"person_id":"p_alice","attributes":{"plan":"pro"}}' http://localhost:8090/people > /dev/null && echo "  p_alice ok"
	@curl -fsS -X POST -H 'Content-Type: application/json' -H 'X-Workspace-ID: ws_alpha' \
		-d '{"person_id":"p_bob","attributes":{"plan":"free"}}' http://localhost:8090/people > /dev/null && echo "  p_bob   ok"
	@echo
	@echo "3. check membership BEFORE event (expect both false)"
	@sleep 1   # bitmap index is eventually consistent — let people.changes propagate
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
	@sleep 1   # let the viewed_pricing events propagate through campaigns.fanout to the index
	@printf "  alice: "; curl -fsS -H 'X-Workspace-ID: ws_alpha' 'http://localhost:8090/segments/active_pro/check?person_id=p_alice'; echo
	@printf "  bob:   "; curl -fsS -H 'X-Workspace-ID: ws_alpha' 'http://localhost:8090/segments/active_pro/check?person_id=p_bob'; echo

seed-campaign: ## Define welcome_pro + identify two people + fire signed_up + verify enrollments (V1 PR 4)
	@echo "1. ensure welcome_pro = (plan=pro AND event_seen signed_up) (idempotent)"
	@if ! curl -fsS -H 'X-Workspace-ID: ws_alpha' http://localhost:8090/campaigns/welcome_pro > /dev/null 2>&1; then \
		curl -fsS -X POST -H 'Content-Type: application/json' -H 'X-Workspace-ID: ws_alpha' \
			-d '{"campaign_id":"welcome_pro","name":"Welcome Pro users","trigger":{"op":"and","conditions":[{"op":"attr_eq","key":"plan","value":"pro"},{"op":"event_seen","name":"signed_up"}]},"template":"Welcome {{.Person.PersonID}} — your {{.Attrs.plan}} plan is live."}' \
			http://localhost:8090/campaigns && echo "   created"; \
	else echo "   already exists"; fi
	@echo
	@echo "2. identify pr4a (plan=pro) and pr4b (plan=free)"
	@curl -fsS -X POST -H 'Content-Type: application/json' -H 'X-Workspace-ID: ws_alpha' \
		-d '{"person_id":"pr4a","attributes":{"plan":"pro"}}' http://localhost:8090/people > /dev/null && echo "  pr4a ok"
	@curl -fsS -X POST -H 'Content-Type: application/json' -H 'X-Workspace-ID: ws_alpha' \
		-d '{"person_id":"pr4b","attributes":{"plan":"free"}}' http://localhost:8090/people > /dev/null && echo "  pr4b ok"
	@echo
	@echo "3. track signed_up for BOTH (only pr4a should match)"
	@curl -fsS -X POST -H 'Content-Type: application/json' -H 'X-Workspace-ID: ws_alpha' \
		-d '{"person_id":"pr4a","event_name":"signed_up","payload":{}}' http://localhost:8090/events && echo
	@curl -fsS -X POST -H 'Content-Type: application/json' -H 'X-Workspace-ID: ws_alpha' \
		-d '{"person_id":"pr4b","event_name":"signed_up","payload":{}}' http://localhost:8090/events && echo
	@echo
	@sleep 1
	@echo "4. journey_enrollments for welcome_pro (expect ONE row, person_id=pr4a):"
	@$(COMPOSE) exec -T mysql mysql -ubm -pbm bm -e \
		"SELECT campaign_id, person_id, triggered_by FROM journey_enrollments WHERE campaign_id='welcome_pro' ORDER BY enrolled_at DESC LIMIT 5;"
	@echo
	@echo "5. stub-receiver log tail (the rendered template):"
	@$(COMPOSE) logs --tail=20 stub-receiver | grep -E 'received|rendered|Welcome' || true
