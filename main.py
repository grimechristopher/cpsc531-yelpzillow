from query_zipcodebase import request_codes_in_radius
from query_yelp import gather_restaurants
# from analyze import average_restaurant_rating_vs_home_value, price_correlation_between_starbucks_and_home_value, correlation_between_number_of_ratings_and_rating, restaurant_count_correlation_to_rating, ratings_vs_extreme
import json

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# Open clusters_input.json and load the array
with open('cluster_input.json') as file:
    data = json.load(file)

zip_codes = data['zip_codes']
data['zip_codes'] = []

print("Gathering zip code cluster data... ")
for zip_code in zip_codes:
    request_codes_in_radius(zip_code, 10)
# Clear input file if clusters have been gathered
with open('cluster_input.json', 'w') as file:
    json.dump(data, file)

print("Gathering restaurants in zip codes... ")
gather_restaurants() 

# Analysis in Jupyter Notebook