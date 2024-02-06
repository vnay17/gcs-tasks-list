from google.cloud import bigquery

# Specify the path to your service account key file
key_path = 'C:/Users/VinayKumarBandirajul/Desktop/sample/gcp-project-398805-f7812d799e68.json'

# Initialize the BigQuery client with the credentials
client = bigquery.Client.from_service_account_json(key_path)

table_name = "dataset.titanic"

# Set table ID
table_id = ".".join([client.project, table_name])

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True,
)

# file path
file_path = 'C:/Users/VinayKumarBandirajul/Downloads/titanic_dataset.csv'

with open(file_path, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)

job.result()  # Waits for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)