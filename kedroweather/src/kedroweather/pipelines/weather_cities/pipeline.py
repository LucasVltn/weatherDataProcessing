"""Pipeline Kedro pour la récupération et le traitement des données météorologiques.

Définit le flux de travail complet :
1. Récupération des données météorologiques pour les villes configurées
2. Transformation des données en format lisible et structuré
"""

from kedro.pipeline import Node, Pipeline

from .nodes import get_city_weather_info, readable_weather_data


def create_pipeline(**kwargs) -> Pipeline:
    """Crée le pipeline Kedro pour le traitement des données météorologiques.

    Construit un pipeline à deux étapes :
    - D'abord, récupère les données météorologiques actuelles pour chaque ville
    - Ensuite, transforme ces données en format DataFrame lisible

    Args:
        **kwargs: Arguments supplémentaires passés par Kedro (non utilisés).

    Returns:
        Pipeline Kedro configuré avec les nœuds d'extraction et de transformation.
    """
    return Pipeline([
        Node(
            func=get_city_weather_info,
            inputs="params:cities",
            outputs="city_weather_info",
            name="city_weather_info_node",
        ),
        Node(
            func=readable_weather_data,
            inputs="city_weather_info",
            outputs="readable_city_weather_data",
            name="readable_city_weather_data_node",
        ),
    ])
