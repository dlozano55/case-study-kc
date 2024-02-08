import json
from google.cloud import bigquery
from google.oauth2 import service_account

def read_bigquery_data(query, credentials_data):
    # Create a BigQuery client instance
    credentials = service_account.Credentials.from_service_account_info(credentials_data)
    client = bigquery.Client(credentials=credentials)

    # Execute the query and retrieve the results as a pandas DataFrame
    query_job = client.query(query)
    dataframe = query_job.to_dataframe()

    return dataframe

# Load credentials from the JSON file
json_file_path = '/Users/diegolozano/Desktop/CK Business Case/ck-case-study-c83094e812a0.json'
with open(json_file_path, 'r') as json_file:
    credentials_data = json.load(json_file)

# Specify the query
query = """SELECT * FROM `ck-sp-case-study-2024.case_study.orders` LIMIT 10"""

# Call the function with the query and credentials data
result = read_bigquery_data(query, credentials_data)

# Print the result
print(result)
