"""
This is a boilerplate pipeline 'weather_cities'
generated using Kedro 1.1.1
"""

from kedro.pipeline import Node, Pipeline

from .nodes import getCityWeatherInfo, readable_weather_data  # noqa



def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline([
        Node(
            func=getCityWeatherInfo,
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
