# Makefile — one answer to "how do I run this"
# Core services only by default (fits 8-16GB machines); UI targets are opt-in.

CORE = zookeeper broker minio mc rest jobmanager taskmanager sql-client producer

.PHONY: up down clean logs ps test test-full trino superset query

up:            ## start the core pipeline
	docker compose up -d $(CORE)

trino:         ## add the query engine
	docker compose up -d trino

superset:      ## add dashboards (heaviest service)
	docker compose up -d --build superset

down:          ## stop everything, keep data volumes
	docker compose down

clean:         ## stop everything AND wipe data (full reset)
	docker compose down -v

logs:          ## follow all logs
	docker compose logs -f

ps:            ## show container states
	docker compose ps -a

test:          ## fast smoke test (~1 min)
	./smoke-test.sh

test-full:     ## end-to-end smoke test incl. Trino growth check (~3 min)
	./smoke-test.sh --full

query:         ## open a Trino SQL shell
	docker compose exec trino trino
