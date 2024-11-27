import requests
import json
import pandas as pd
from sqlalchemy import create_engine
# API endpoint
url = "http://172.20.51.16/ltab/TestAPI"
# Make the GET request to fetch the data from the API
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Parse the JSON data
    data = response.json()

    # Define the file path for saving the data
    file_path = "api_data.json"

    # Save the data to a JSON file
    with open(file_path, "w") as json_file:
        json.dump(data, json_file, indent=4)  # 'indent=4' is for pretty printing

    print(f"Data has been successfully saved to {file_path}")
else:
    print(f"Failed to retrieve data from API. Status code: {response.status_code}")
