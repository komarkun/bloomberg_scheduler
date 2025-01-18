# This Makefile script is only for local development. For Docker usage, please refer to the README.md.

# Load .env variables
ifneq (,$(wildcard .env))
    include .env
    export $(shell sed 's/=.*//' .env)
endif

DIR=

# Linux Example 
POSTGRES_SETUP_CMD = sudo -u postgres psql -c

.PHONY: all db_migrate create_user webserver scheduler setup setup_database setup_airflow_connections install_airflow 

all: scheduler webserver


db_migrate:
	airflow db migrate


create_user:
	@echo "Creating user for the site..."
	airflow users create \
		--username $(AIRFLOW_WEB_USER) \
		--firstname $(AIRFLOW_WEB_FIRSTNAME) \
		--lastname $(AIRFLOW_WEB_LASTNAME) \
		--role Admin \
		--email $(AIRFLOW_WEB_EMAIL) \
		--password $(AIRFLOW_WEB_PASSWORD)


webserver:
	@echo "Starting Airflow webserver..."
	airflow webserver


scheduler:
	@echo "Starting Airflow scheduler..."
	airflow scheduler -D


revert_database:
	@echo "Reverting PostgreSQL setup..."
	@$(POSTGRES_SETUP_CMD) "BEGIN; \
	REVOKE ALL PRIVILEGES ON DATABASE $(POSTGRES_DB) FROM $(POSTGRES_USER); \
	REVOKE ALL ON SCHEMA public FROM $(POSTGRES_USER); \
	GRANT CONNECT ON DATABASE $(POSTGRES_DB) TO postgres; \
	DROP USER IF EXISTS $(POSTGRES_USER); \
	COMMIT;"

	@$(POSTGRES_SETUP_CMD) "DROP DATABASE IF EXISTS $(POSTGRES_DB);"

	@if [ $$? -eq 0 ]; then \
		echo "PostgreSQL revert completed successfully."; \
	else \
		echo "PostgreSQL revert failed. Please check the logs."; \
		exit 1; \
	fi


setup_database:
	@echo "Setting up PostgreSQL database for Airflow..."

	@$(POSTGRES_SETUP_CMD) "CREATE DATABASE $(POSTGRES_DB);"
	@$(POSTGRES_SETUP_CMD) "BEGIN; \
	CREATE USER $(POSTGRES_USER) WITH PASSWORD '$(POSTGRES_PASSWORD)'; \
	GRANT ALL PRIVILEGES ON DATABASE $(POSTGRES_DB) TO $(POSTGRES_USER); \
	GRANT ALL ON SCHEMA public TO $(POSTGRES_USER); \
	GRANT postgres TO $(POSTGRES_USER); \
	COMMIT;"

	@if [ $$? -eq 0 ]; then \
		echo "PostgreSQL setup completed successfully."; \
	else \
		echo "PostgreSQL setup failed. Please check the logs."; \
		exit 1; \
	fi


setup_airflow_connections:
	@echo "Setting up Airflow connections..."
	@airflow connections add 'postgres_conn' \
		--conn-uri 'postgresql+psycopg2://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@$(POSTGRES_HOST):$(POSTGRES_PORT)/$(POSTGRES_DB)'


setup_local: setup_database install_airflow setup_airflow_connections db_migrate create_user scheduler webserver


install_airflow:
	./install.sh