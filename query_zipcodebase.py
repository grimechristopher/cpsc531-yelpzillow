import json
import requests
import os
from dotenv import load_dotenv
# import pyspark
from pyspark.sql import SparkSession

# print(pyspark.__version__)
load_dotenv()
ZIPCODEBASE_API_KEY = os.getenv('ZIPCODEBASE_API_KEY')

headers = { 
  "apikey": ZIPCODEBASE_API_KEY}

def request_codes_in_radius(center_code, radius):
    base_url = 'https://app.zipcodebase.com/api/v1/radius'
    params = {
        ('code', center_code),
        ('radius', radius), # Kilometers
        ('country', 'us')
    }

    # sample = '{"query": {"code": "92603", "unit": "km", "radius": "10", "country": "us"}, "results": [{"code": "92603", "city": "Irvine", "state": "CA", "city_en": null, "state_en": null, "distance": 0, "zhvi": "2754525.781233289"}, {"code": "92617", "city": "Irvine", "state": "CA", "city_en": null, "state_en": null, "distance": 4.85, "zhvi": "932524.1825747042"}, {"code": "92697", "city": "Irvine", "state": "CA", "city_en": null, "state_en": null, "distance": 4.92}, {"code": "92616", "city": "Irvine", "state": "CA", "city_en": null, "state_en": null, "distance": 4.95}, {"code": "92657", "city": "Newport Coast", "state": "CA", "city_en": null, "state_en": null, "distance": 4.96, "zhvi": "5639517.6815955555"}, {"code": "92612", "city": "Irvine", "state": "CA", "city_en": null, "state_en": null, "distance": 5.02, "zhvi": "1312679.337267474"}, {"code": "92619", "city": "Irvine", "state": "CA", "city_en": null, "state_en": null, "distance": 5.81}, {"code": "92698", "city": "Aliso Viejo", "state": "CA", "city_en": null, "state_en": null, "distance": 6.41}, {"code": "92637", "city": "Laguna Woods", "state": "CA", "city_en": null, "state_en": null, "distance": 6.55, "zhvi": "467778.47845988226"}, {"code": "92658", "city": "Newport Beach", "state": "CA", "city_en": null, "state_en": null, "distance": 6.69}, {"code": "92650", "city": "East Irvine", "state": "CA", "city_en": null, "state_en": null, "distance": 6.84}, {"code": "92660", "city": "Newport Beach", "state": "CA", "city_en": null, "state_en": null, "distance": 6.91, "zhvi": "3256674.984280741"}, {"code": "92614", "city": "Irvine", "state": "CA", "city_en": null, "state_en": null, "distance": 7.29, "zhvi": "1468808.5692724737"}, {"code": "92604", "city": "Irvine", "state": "CA", "city_en": null, "state_en": null, "distance": 7.3, "zhvi": "1474662.2627040492"}, {"code": "92654", "city": "Laguna Hills", "state": "CA", "city_en": null, "state_en": null, "distance": 7.62}, {"code": "92625", "city": "Corona Del Mar", "state": "CA", "city_en": null, "state_en": null, "distance": 7.84, "zhvi": "4510984.052714496"}, {"code": "92623", "city": "Irvine", "state": "CA", "city_en": null, "state_en": null, "distance": 7.94}, {"code": "92606", "city": "Irvine", "state": "CA", "city_en": null, "state_en": null, "distance": 8.28, "zhvi": "1576428.5946377795"}, {"code": "92652", "city": "Laguna Beach", "state": "CA", "city_en": null, "state_en": null, "distance": 9.14}, {"code": "92651", "city": "Laguna Beach", "state": "CA", "city_en": null, "state_en": null, "distance": 9.15, "zhvi": "3225949.6072385437"}, {"code": "92620", "city": "Irvine", "state": "CA", "city_en": null, "state_en": null, "distance": 9.18, "zhvi": "1894559.9626260558"}, {"code": "92662", "city": "Newport Beach", "state": "CA", "city_en": null, "state_en": null, "distance": 9.4, "zhvi": "4293098.181447449"}, {"code": "92609", "city": "El Toro", "state": "CA", "city_en": null, "state_en": null, "distance": 9.56}, {"code": "92653", "city": "Laguna Hills", "state": "CA", "city_en": null, "state_en": null, "distance": 9.57, "zhvi": "1373800.9015416235"}, {"code": "92656", "city": "Aliso Viejo", "state": "CA", "city_en": null, "state_en": null, "distance": 9.96, "zhvi": "1227441.559938194"}]}'
    # data = json.loads(sample)

    response = requests.get(base_url, headers=headers, params=params)
    data = response.json()

    # Use spark to read th csv
    spark = SparkSession.builder.appName('cluster_create').getOrCreate()
    df = spark.read.csv('input/zillow/singlefamily_by_zipcode.csv', header=True)

    def process_row(row):
        return row['RegionName'], row['City'], row['2024-03-31']

    # use spark to process the data 
    processed_data = df.rdd.map(process_row)

    results = processed_data.collect()

    # Add the ZVHI from zillow to the results of the zip and calculate the average zhvi for a cluster
    zhvi_sum = 0
    zhvi_count = 0
    for result in data['results']:
        code = result['code']
        for row in results:
            if row[0] == code:
                result['zhvi'] = row[2]
                if result['zhvi'] is not None:
                    zhvi_sum += float(result['zhvi'])
                    zhvi_count += 1
                break
    
    if zhvi_count > 0:
        average_zhvi = zhvi_sum / zhvi_count
        data['query']['average_zhvi'] = average_zhvi

    # Save the CSV to a file
    with open('input/cluster/'+ center_code +'.json', 'w') as file:
        json.dump(data, file)
