"""DAG Airflow pour exécuter le pipeline Kedro de traitement des données météorologiques.

Ce DAG orchestre l'exécution du pipeline Kedro dans un conteneur Docker,
en récupérant la clé API depuis les connexions Airflow et en montant les
volumes de sortie pour la persistance des données.

Planification : toutes les 7 minutes
"""

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta


# Récupération de la clé API depuis les connexions Airflow
conn = BaseHook.get_connection("weatherApiKey")
api_key = conn.extra_dejson.get("api_key")

# Définition du DAG
with DAG(
    dag_id="kedro_pipeline",
    start_date=datetime(2026, 1, 26),
    schedule=timedelta(minutes=7),
    catchup=False,
    description="Exécute le pipeline Kedro de traitement des données météorologiques",
    tags=["kedro", "weather", "etl"],
) as dag:
    """
    DAG d'orchestration du pipeline de données météorologiques.
    
    Exécute un pipeline Kedro en conteneur Docker qui :
    1. Récupère les données météorologiques actuelles des villes configurées
    2. Transforme les données en format lisible
    3. Sauvegarde les résultats dans le répertoire de sortie
    """

    run_kedro = DockerOperator(
        task_id="run_kedro_pipeline",
        image="kedro-weather-app:latest",
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
        doc="""
        Exécute le pipeline Kedro dans un conteneur Docker.
        
        Le conteneur :
        - Utilise la clé API OpenWeatherMap pour récupérer les données
        - Monte le répertoire de sortie pour persister les résultats
        - S'auto-supprime après exécution
        """,
    )

    run_kedro
