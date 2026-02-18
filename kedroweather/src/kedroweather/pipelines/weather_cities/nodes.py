"""Pipeline pour la récupération et le traitement des données météorologiques par ville.

Ce module contient les nœuds du pipeline Kedro qui gèrent :
- La récupération des coordonnées géographiques des villes
- La récupération des données météorologiques actuelles
- La transformation des données en format lisible
"""

import os
import pandas as pd
import requests


def get_city_coordinates(city_name: str, api_key: str) -> tuple[float, float]:
    """Récupère les coordonnées géographiques d'une ville via l'API OpenWeatherMap.

    Utilise l'endpoint Geocoding API pour obtenir la latitude et longitude
    d'une ville à partir de son nom.

    Args:
        city_name: Nom de la ville pour laquelle récupérer les coordonnées.
        api_key: Clé API OpenWeatherMap pour l'authentification.

    Returns:
        Tuple contenant (latitude, longitude) de la ville.

    Raises:
        ValueError: Si la ville n'est pas trouvée dans l'API.
        Exception: Si la requête API échoue (code HTTP non-200).
    """
    geo_url = "http://api.openweathermap.org/geo/1.0/direct"
    geo_params = {
        "q": city_name,
        "limit": 1,
        "appid": api_key
    }
    
    geo_response = requests.get(geo_url, params=geo_params)
    
    if geo_response.status_code == 200:
        geo_data = geo_response.json()
        if geo_data:
            return geo_data[0]['lat'], geo_data[0]['lon']
        else:
            raise ValueError(f"Ville '{city_name}' non trouvée")
    else:
        raise Exception(
            f"Erreur lors de la récupération des coordonnées: "
            f"{geo_response.status_code}, {geo_response.text}"
        )


def get_city_weather_info(city_names: list[str]) -> dict:
    """Récupère les données météorologiques actuelles pour une liste de villes.

    Pour chaque ville fournie, récupère ses coordonnées puis ses données
    météorologiques actuelles via l'API OpenWeatherMap.

    Args:
        city_names: Liste des noms des villes pour lesquelles récupérer les données.

    Returns:
        Dictionnaire avec structure {nom_ville: données_météo_json}.

    Raises:
        Exception: Si la requête API échoue pour une ville ou ses données.
    """
    result = {}
    api_key = os.environ.get("API_KEY")
    for city_name in city_names:
        lat, lon = get_city_coordinates(city_name, api_key)  # type: ignore
        
        weather_url = "https://api.openweathermap.org/data/3.0/onecall"
        weather_params = {
            "lat": lat,
            "lon": lon,
            "appid": api_key,
            "exclude": "minutely,hourly,daily,alerts",
            "units": "metric",
            "lang": "fr"
        }
        
        weather_response = requests.get(weather_url, params=weather_params)
        
        if weather_response.status_code == 200:
            result[city_name] = weather_response.json()
        else:
            raise Exception(
                f"Erreur lors de la récupération des données météo: "
                f"{weather_response.status_code}, {weather_response.text}"
            )
    
    return result


def readable_weather_data(weather_data: dict) -> pd.DataFrame:
    """Transforme les données météorologiques brutes en format lisible.

    Convertit les données JSON brutes de l'API en DataFrame pandas avec
    colonnes formatées (ville, timestamp, température, humidité, description).

    Args:
        weather_data: Dictionnaire des données météo brutes par ville.

    Returns:
        Dictionnaire avec pour clé un horodatage (format 'YYYY-MM-DD HHhMM.csv')
        et pour valeur le DataFrame contenant les données transformées.
    """
    result = pd.DataFrame()
    
    for city, data in weather_data.items():
        current = data['current']
        df = pd.DataFrame([{
            "city": city,
            'timestamp': pd.to_datetime(current['dt'], unit='s').strftime('%Y-%m-%d %H:%M'),
            'temperature': current['temp'],
            'humidity': current['humidity'],
            'weather_description': current['weather'][0]['description']
        }])
        result = pd.concat([result, df], ignore_index=True)
    
    # horodatage = str(result["timestamp"][0].strftime('%Y-%m-%d %Hh%M')) + ".csv"
    return result
