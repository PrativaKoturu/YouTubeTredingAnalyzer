PYTHON ?= python
export PYTHONPATH := $(CURDIR)

.PHONY: up down logs topics index producer replay bulk-fetch bulk-trending bulk-dry-run jsonl-replay consumer analytics queries benchmark search test

up:
	docker compose up -d
	@echo "Waiting for Elasticsearch..."
	@for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do \
		curl -sf http://localhost:9200 >/dev/null && echo "Elasticsearch is up" && break; \
		sleep 3; \
	done

down:
	docker compose down

logs:
	docker compose logs -f

topics:
	$(PYTHON) -c "from config.settings import create_topics; create_topics()"

index:
	$(PYTHON) elastic/create_index.py

producer:
	$(PYTHON) producer.producer

replay:
	$(PYTHON) producer.csv_replay

bulk-fetch:
	$(PYTHON) producer/bulk_fetch.py --comments

bulk-trending:
	$(PYTHON) producer/bulk_fetch.py --trending

bulk-dry-run:
	$(PYTHON) producer/bulk_fetch.py --dry-run --comments

jsonl-replay:
	$(PYTHON) producer/jsonl_replay.py

consumer:
	$(PYTHON) consumer/consumer.py

analytics:
	$(PYTHON) analytics/run_all.py

queries:
	$(PYTHON) elastic/run_queries.py

benchmark:
	$(PYTHON) analytics/benchmark.py

search:
	$(PYTHON) -m streamlit run search_demo/app.py

test:
	$(PYTHON) -m pytest -q
