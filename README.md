# Bloomberg Scheduler

## Local Setup

1. Create a virtual environment and activate it:

```bash
python3 -m venv .venv
source ./.venv/bin/activate
```

2. Run the following command to set up the application `make setup_local`:

    This command will:

    - Install dependencies.
    - Set up the database.
    - Apply migrations.
    - Start the webserver interface and scheduler.

3. After setup is complete, you can log in using the default credentials:

- **Username: johndoe (set by AIRFLOW_WEB_USER)**
- **Password: mysecret (set by AIRFLOW_WEB_PASSWORD)**

### Troubleshooting

If you encounter issues while creating the database and cannot re-run the setup_local script, you can reset the database by running:

```bash
make revert_database
```

## Docker Setup

1. Build and start the Docker containers:

```bash
docker compose up --build --scale airflow-worker=3 -d
```

2. Wait until the containers are fully running. You can then log in to the Airflow web interface using the following credentials:

- **Username: airflow**
- **Password: airflow**
