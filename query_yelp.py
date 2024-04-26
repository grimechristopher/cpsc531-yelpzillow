import requests
import json
import time
import os
from dotenv import load_dotenv


from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

load_dotenv()
YELP_API_KEY=os.getenv('YELP_API_KEY')

yelp_headers = {
  "Accept": "application/json", 
  "Authorization": "Bearer " + YELP_API_KEY
  }


def request_restaurants_in_zipcode(zipcode):
    base_url = 'https://api.yelp.com/v3/businesses/search'
    params = {
        ('location', zipcode),
        ('term', 'restaurants'),
        ('radius', 8000),
        ('sort_by', 'distance')
    }

    response = requests.get(base_url, headers=yelp_headers, params=params)
    data = response.json()

    with open('input/yelp/restaurants/'+ zipcode +'.json', 'w') as file:
        json.dump(data, file)

def request_starbucks_in_zipcode(zipcode):
    base_url = 'https://api.yelp.com/v3/businesses/search'
    params = {
        ('location', zipcode),
        ('term', 'starbucks'),
        ('radius', 8000),
        ('sort_by', 'distance')
    }

    response = requests.get(base_url, headers=yelp_headers, params=params)
    data = response.json()

    with open('input/yelp/starbucks/'+ zipcode +'.json', 'w') as file:
        json.dump(data, file)

def gather_restaurants():
  spark = SparkSession.builder.appName("Yelp Restaurant Collector").getOrCreate()

  # Save my API requests by only calling requests for zipcodes I havent checked before
  with open('cluster_input.json') as file:
      data = json.load(file) 
  restaurant_zipcodes = data['restaurants']
  starbucks_zipcodes = data['starbucks']

  broadcasted_restaurant_zipcodes = spark.sparkContext.broadcast(restaurant_zipcodes) # Broadcast list to workers
  broadcasted_starbucks_zipcodes = spark.sparkContext.broadcast(starbucks_zipcodes)

  # Load cluster json files into a DataFrame
  restaurant_cluster_df = spark.read.option("multiline",True).json("input/cluster") 
  restaurant_cluster_df = restaurant_cluster_df.select(explode("results").alias("result")) # Select the results section
  restaurant_cluster_df = restaurant_cluster_df.filter(col("result.zhvi").isNotNull()) # If I dont have the price for the zip its pointless to get restraurants
  restaurant_cluster_df = restaurant_cluster_df.filter(~col("result.code").isin(broadcasted_restaurant_zipcodes.value)) # Filter out zips I already have restaurants for
  restaurant_cluster_df = restaurant_cluster_df.select("result.code") # Give me just the zip codes
  restaurant_cluster_df = restaurant_cluster_df.dropDuplicates() # No duplicates please

  starbucks_cluster_df = spark.read.option("multiline",True).json("input/cluster")
  starbucks_cluster_df = starbucks_cluster_df.select(explode("results").alias("result"))
  starbucks_cluster_df = starbucks_cluster_df.filter(col("result.zhvi").isNotNull())
  starbucks_cluster_df = starbucks_cluster_df.filter(~col("result.code").isin(broadcasted_starbucks_zipcodes.value))
  starbucks_cluster_df = starbucks_cluster_df.select("result.code")
  starbucks_cluster_df = starbucks_cluster_df.dropDuplicates()

  # Format into a list
  zip_codes_list = restaurant_cluster_df.rdd.flatMap(lambda x: x).collect() 
  starbucks_zip_codes_list = starbucks_cluster_df.rdd.flatMap(lambda x: x).collect()

  spark.stop()

  # Run get_yelp_reviews for each zip code in the list
  for zip_code in zip_codes_list:
      request_restaurants_in_zipcode(zip_code)
      time.sleep(0.2)

  for zip_code in starbucks_zip_codes_list:
      request_starbucks_in_zipcode(zip_code)
      time.sleep(0.2)

  # Add to the list of zips we searched already
  data['restaurants'] = restaurant_zipcodes + zip_codes_list
  data['starbucks'] = starbucks_zipcodes + starbucks_zip_codes_list
  
  with open('cluster_input.json', 'w') as file:
      json.dump(data, file)