.DEFAULT_GOAL := help

COMPOSE ?= docker compose

.PHONY: help up down logs ps rebuild test fmt vet seed seed-people seed-segments seed-campaign showcase \
	load load-prep load-quick load-soak seed-journey journey-runs \
	dlq-inspect dlq-replay dlq-purge retry-queues \
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

fmt: ## gofmt the tree
	gofmt -w cmd/ internal/

vet: ## go vet
	go vet ./...

load: load-soak ## BM-52 alias — the standard ceiling-search run (= load-soak)

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

seed-journey: ## V3-1a: define a journey, fire a trigger, then REDELIVER it — expect exactly 1 run
	@echo "1. ensure onboard_pro exists (delay 60s -> branch on plan -> send):"
	@if ! curl -fsS -H 'X-Workspace-ID: ws_alpha' http://localhost:8090/journeys/onboard_pro >/dev/null 2>&1; then \
	  curl -fsS -X POST http://localhost:8090/journeys \
	    -H "X-Workspace-ID: ws_alpha" -H "Content-Type: application/json" \
	    -d '{"journey_id":"onboard_pro","name":"Pro onboarding","trigger":{"op":"event_seen","name":"signed_up"},"steps":[{"type":"delay","seconds":10},{"type":"branch_on_condition","condition":{"op":"attr_eq","key":"plan","value":"pro"},"if_true":2,"if_false":3},{"type":"send","template":"Your {{.Attrs.plan}} trial ends soon.","then":"end"},{"type":"send","template":"Upgrade to pro."}]}' && echo "   created"; \
	else echo "   already exists"; fi
	@echo "2. identify pj_alice (plan=pro):"
	@curl -fsS -X POST http://localhost:8090/people \
	  -H "X-Workspace-ID: ws_alpha" -H "Content-Type: application/json" \
	  -d '{"person_id":"pj_alice","attributes":{"plan":"pro"}}' && echo
	@echo "3. fire signed_up (creates the run):"
	@EV=$$(curl -fsS -X POST http://localhost:8090/events \
	  -H "X-Workspace-ID: ws_alpha" -H "Content-Type: application/json" \
	  -d '{"person_id":"pj_alice","event_name":"signed_up","payload":{}}' | python3 -c "import sys,json;print(json.load(sys.stdin)['event_id'])"); \
	  echo "   event_id=$$EV"; sleep 4; \
	  echo "4. run after first delivery:"; \
	  curl -fsS -H "X-Workspace-ID: ws_alpha" http://localhost:8090/journeys/onboard_pro/runs/pj_alice | python3 -m json.tool; \
	  echo "5. REDELIVER the identical event (same event_id) straight to the exchange:"; \
	  curl -fsS -u guest:guest -H "Content-Type: application/json" \
	    -X POST http://localhost:15672/api/exchanges/%2f/campaigns.fanout/publish \
	    -d "$$(python3 -c 'import json,sys; ev={"workspace_id":"ws_alpha","event_id":sys.argv[1],"person_id":"pj_alice","event_name":"signed_up","payload":{},"received_at":"2026-07-29T00:00:00Z"}; print(json.dumps({"properties":{"delivery_mode":2},"routing_key":"ws_alpha","payload":json.dumps(ev),"payload_encoding":"string"}))' "$$EV")" && echo; \
	  sleep 4
	@echo "6. run count for onboard_pro (BM-113 — expect exactly 1):"
	@$(MAKE) -s journey-runs

dlq-inspect: ## V3-2: DLQ depth + why each message gave up (BM-124)
	@./chaos/dlq.sh inspect

dlq-replay: ## V3-2: drain the DLQ back onto the work exchange, attempts reset (BM-124)
	@./chaos/dlq.sh replay

dlq-purge: ## Empty the DLQ (destructive — inspect first)
	@./chaos/dlq.sh purge

retry-queues: ## Show retry-ladder queue depths
	@./chaos/dlq.sh inspect | head -6

journey-runs: ## Count + show journey_runs rows
	@$(COMPOSE) exec -T mysql mysql -ubm -pbm bm -e \
	  "SELECT journey_id, person_id, step_index, status, journey_version, COUNT(*) OVER () AS total_rows FROM journey_runs ORDER BY created_at DESC LIMIT 10;" 2>/dev/null

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
