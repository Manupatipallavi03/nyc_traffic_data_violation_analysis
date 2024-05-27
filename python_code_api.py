import pandas as pd
import requests

# Define the URL for the NYC Open Data API
dataset_url = "https://data.cityofnewyork.us/resource/nc67-uf89.json"

# Define parameters for sorting and selecting recent 10000 records
limit = 10000  # Number of records to fetch
params = {'$limit': limit, '$order': 'issue_date DESC'}

try:
    # Fetch the data with sorting and selecting top records
    response = requests.get(dataset_url, params=params)
    
    # Check if the request was successful
    response.raise_for_status()
    
    # Attempt to parse the JSON data
    data = response.json()
    
    # Load data into a pandas DataFrame
    df = pd.DataFrame(data)
    
    # Display or save the result
    print(df)

        # Specify the directory where you want to save the file
    save_directory = 'C:/documents/'

        # Save the file with the specified directory
    df.to_csv(f'{save_directory}recent_10000_records.csv', index=False)
    

except requests.exceptions.RequestException as e:
    print(f"Request error: {e}")
