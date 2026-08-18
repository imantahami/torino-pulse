import requests

url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": 45.0703,
    "longitude": 7.6869,
    "current": "temperature_2m,precipitation,wind_speed_10m,weather_code"
}

response = requests.get(url, params=params)
print("Status code:", response.status_code)

data = response.json()
print(data["current"])