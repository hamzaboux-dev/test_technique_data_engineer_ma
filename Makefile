.PHONY: up down build logs trigger test ps clean

up:         ## Demarre tous les services (build si necessaire)
	docker compose up -d --build

down:       ## Arrete les services (garde les volumes/donnees)
	docker compose down

clean:      ## Arrete tout et supprime aussi les volumes (reset complet)
	docker compose down -v

ps:         ## Etat des services
	docker compose ps

logs:       ## Logs en direct de tous les services
	docker compose logs -f

trigger:    ## Declenche le DAG taxi_pipeline
	docker compose exec airflow-webserver airflow dags trigger taxi_pipeline

test:       ## Lance les tests unitaires (ingestion)
	pip install -q pytest requests boto3 pandas
	python -m pytest tests/ -v
