from datetime import date,timedelta
#start_date=date.today()-timedelta(7)
#end_date=date.today()-timedelta(1)
url=url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}&endtime={end_date}"
print(url)


# Welcome to your new notebook
# Type here in the cell editor to add code!
import requests
import json
# Construct the API URL with start and end dates provided by Data Factory, formatted for geojson output.
url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}&endtime={end_date}"
# Make the GET request to fetch data
response = requests.get(url)
# Check if the request was successful
if response.status_code == 200:
    # Get the JSON response
    data = response.json()
    data = data['features']  
    # Specify the file name (and path if needed)
    file_path = f'/lakehouse/default/Files/{start_date}_earthquake_data.json'
    # Open the file in write mode ('w') and save the JSON data
    with open(file_path, 'w') as file:
        # The `json.dump` method serializes `data` as a JSON formatted stream to `file`
        # `indent=4` makes the file human-readable by adding whitespace
        json.dump(data, file, indent=4)
    print(f"Data successfully saved to {file_path}")
else:
    print("Failed to fetch data. Status code:", response.status_code)


df = spark.read.option("multiline", "true").json("Files/2025-05-10_earthquake_data.json")
# df now is a Spark DataFrame containing JSON data from "Files/2025-05-10_earthquake_data.json".
#display(df)
