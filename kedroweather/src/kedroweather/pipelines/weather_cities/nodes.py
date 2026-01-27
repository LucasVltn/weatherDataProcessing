"""
This is a boilerplate pipeline 'weather_cities'
generated using Kedro 1.1.1
"""
import os
import pandas as pd
import requests
import yaml


# from kedro.framework.session import KedroSession

# with KedroSession.create() as session:
#     context = session.load_context()
#     creds = context.config_loader["credentials"]  # Load credentials
#     api_key = creds["api_key"]



def getCityCoordinates(city_name: str, api_key: str) -> tuple:
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
            raise ValueError("City not found")
    else:
        raise Exception(f"Error fetching coordinates: {geo_response.status_code}, {geo_response.text}")

def getCityWeatherInfo(city_names: list) -> dict:
    result = {}
    api_key = os.environ.get("API_KEY")
    for city_name in city_names:
        lat, lon = getCityCoordinates(city_name, api_key) # type: ignore
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
            raise Exception(f"Error fetching weather data: {weather_response.status_code}, {weather_response.text}")
    return result

def readable_weather_data(weather_data: dict) -> dict[str, pd.DataFrame]:
    result = pd.DataFrame()
    for city, data in weather_data.items():
        current = data['current']
        df = pd.DataFrame([{
            "city": city,
            'timestamp': pd.to_datetime(current['dt'], unit='s'),
            'temperature': current['temp'],
            'humidity': current['humidity'],
            'weather_description': current['weather'][0]['description']
        }])
        result = pd.concat([result, df], ignore_index=True)
    horodatage = str(result["timestamp"][0].strftime('%Y-%m-%d %Hh%M'))+".csv"
    return {
        horodatage: result
        }
