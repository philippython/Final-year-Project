import os
import csv
import requests
import psycopg2
import datetime
import sqlite3
from dotenv import load_dotenv

load_dotenv()

API_URL = "http://api.openweathermap.org/data/2.5/air_pollution/history"
db_params = {
    'host': os.getenv("DB_HOST"),
    'database': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USERNAME"),
    'password': os.getenv("DB_PASSWORD"),
}

# connection with PostgreSQL database
connection = psycopg2.connect(**db_params)
cursor = connection.cursor() 

# connection with sqlite data base
connection_sqlite = sqlite3.connect("air_pollution_data_for_ijebu_ode_june2023_to_dec2023.db")
cursor_sqlite = connection_sqlite.cursor()

def insert_data(parameters, csv_parameters):
    parameters["lat"] = csv_parameters["lat"]
    parameters["lon"] = csv_parameters["long"]
    api_response = requests.get(API_URL, params=parameters)
    for hourly_data in api_response.json()["list"]:
        normal_date = datetime.datetime.utcfromtimestamp(hourly_data["dt"])

        formatted_date = normal_date.strftime('%Y-%m-%d %H:%M:%S')
        aqi = hourly_data["main"]["aqi"]
        carbon_monoxide = hourly_data["components"]["co"]
        nitrogen_monoxide = hourly_data["components"]["no"]
        nitrogen_dioxide = hourly_data["components"]["no2"]
        ozone = hourly_data["components"]["o3"]
        sulphur_dioxide = hourly_data["components"]["so2"]
        fine_particles_matter = hourly_data["components"]["pm2_5"]
        coarse_particulate_matter = hourly_data["components"]["pm10"]
        ammonia = hourly_data["components"]["nh3"]

        # insertion query for postgresql
        insertion_query = f"""
                            INSERT INTO air_pollution_data_for_ijebu_ode_june2023_to_dec2023
                            (date, carbon_monoxide, nitrogen_monoxide, nitrogen_dioxide, ozone, sulphur_dioxide, fine_particles_matter, coarse_particulate_matter, ammonia, aqi)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
                            VALUES ('{formatted_date}', {carbon_monoxide}, {nitrogen_monoxide}, {nitrogen_dioxide}, {ozone}, {sulphur_dioxide}, {fine_particles_matter}, {coarse_particulate_matter}, {ammonia}, {aqi})
                        """
        
        # insertion query for sqlite
        query = f"""
                    INSERT INTO air_pollution_data
                    (aqi, date, carbon_monoxide, nitrogen_monoxide, nitrogen_dioxide, ozone, sulphur_dioxide, fine_particles_matter, coarse_particulate_matter, ammonia)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
                    VALUES ({aqi}, '{formatted_date}', {carbon_monoxide}, {nitrogen_monoxide}, {nitrogen_dioxide}, {ozone}, {sulphur_dioxide}, {fine_particles_matter}, {coarse_particulate_matter}, {ammonia})
                """

        # query execution
        # cursor_sqlite.execute(query)
        # connection_sqlite.commit()
        
        # creating csv file from data fetched
        with open("air_pollution_data.csv", "a", newline='') as data_file:
            writer = csv.writer(data_file)
            writer.writerow([aqi, formatted_date, csv_parameters["location"],csv_parameters["state"], csv_parameters["region"], carbon_monoxide, nitrogen_monoxide, nitrogen_dioxide, ozone, sulphur_dioxide, fine_particles_matter, coarse_particulate_matter, ammonia])

request_params = {
    "start" : 1685577600, # June 1st 2023
    "end" : 1701388800, # Decemeber 1st, 2023
    "appid" : "fbe358ae38b19d1e84de1525dbd432d7"
}

locations = [
    {
        "location": "Kano City",
        "state": "Kano State",
        "region": "North-West",
        "lat": 11.9964,
        "long": 8.5244
    },
    {
        "location": "Kaduna City",
        "state": "Kaduna State",
        "region": "North-West",
        "lat": 10.5235,
        "long": 7.438
    },
    {
        "location": "Katsina City",
        "state": "Katsina State",
        "region": "North-West",
        "lat": 12.9864,
        "long": 7.6018
    },
    {
        "location": "Maiduguri",
        "state": "Borno State",
        "region": "North-East",
        "lat": 11.8469,
        "long": 13.1609
    },
    {
        "location": "Yola",
        "state": "Adamawa State",
        "region": "North-East",
        "lat": 9.2094,
        "long": 12.4818
    },
    {
        "location": "Gombe",
        "state": "Gombe State",
        "region": "North-East",
        "lat": 10.2899,
        "long": 11.1676
    },
    {
        "location": "Makurdi",
        "state": "Benue State",
        "region": "North-Central",
        "lat": 7.7274,
        "long": 8.5431
    },
    {
        "location": "Jos",
        "state": "Plateau State",
        "region": "North-Central",
        "lat": 9.8922,
        "long": 8.8583
    },
    {
        "location": "Minna",
        "state": "Niger State",
        "region": "North-Central",
        "lat": 9.6111,
        "long": 6.5569
    },
    {
        "location": "Apapa",
        "state": "Lagos State",
        "region": "South-West",
        "lat": 6.447810,
        "long": 3.362530
    },
    {
        "location": "Ibadan",
        "state": "Oyo State",
        "region": "South-West",
        "lat": 7.3775,
        "long": 3.9470
    },
    {
        "location": "ilaro",
        "state": "Ogun State",
        "region": "South-West",
        "lat": 6.900140,
        "long": 3.018730
    },
    {
        "location": "Enugu",
        "state": "Enugu State",
        "region": "South-East",
        "lat": 6.4483,
        "long": 7.5464
    },
    {
        "location": "Owerri",
        "state": "Imo State",
        "region": "South-East",
        "lat": 5.4836,
        "long": 7.0332
    },
    {
        "location": "Umuahia",
        "state": "Abia State",
        "region": "South-East",
        "lat": 5.5260,
        "long": 7.4906
    },
    {
        "location": "Port Harcourt",
        "state": "Rivers State",
        "region": "South-South",
        "lat": 4.8156,
        "long": 7.0498
    },
    {
        "location": "Benin City",
        "state": "Edo State",
        "region": "South-South",
        "lat": 6.3350,
        "long": 5.6030
    },
    {
        "location": "Asaba",
        "state": "Delta State",
        "region": "South-South",
        "lat": 6.1453,
        "long": 6.7924
    }
]

for location in locations:
    print(1)
    insert_data(request_params, location)