from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

conn = BaseHook.get_connection("weatherApiKey")
api_key = conn.extra_dejson.get("api_key")  # Récupère la valeur du JSON

with DAG(
    dag_id="kedro_pipeline",
    start_date=datetime(2026, 1, 26),
    schedule=timedelta(minutes=7),
    catchup=False,
) as dag:

    run_kedro = DockerOperator(
        task_id="run_kedro_pipeline",
        image="kedro-weather-app:latest",  # ou "kedro_project_kedro:latest" selon ton build
        api_version="auto",
        auto_remove=True,
        command="kedro run",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            "API_KEY": api_key
        },
        mounts=[
                Mount(
                    source="/c/Users/lucas/Documents/Projets persos/weatherDataProcessing/output",
                    target="/app/data",
                    type="bind",
                )
            ],
    )

    run_kedro
