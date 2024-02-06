from google.cloud import bigquery
from google.oauth2 import service_account

# Specify the path to your service account key file
key_path = 'C:/Users/VinayKumarBandirajul/Desktop/sample/gcp-project-398805-f7812d799e68.json'

# Load the credentials from the key file
credentials = service_account.Credentials.from_service_account_file(key_path)

# Initialize the BigQuery client with the credentials
client = bigquery.Client(credentials=credentials)


dataset_id = "{}.dataset".format(client.project)


#  Set table_id to the ID of the table to create.
table_id = "{}.titanic".format(dataset_id)

schema = [
    bigquery.SchemaField("PassengerId", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("Survived", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("Pclass", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("Name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Sex", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Age", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("SibSp", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("Parch", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("Ticket", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Fare", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Cabin", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Embarked", "STRING", mode="NULLABLE"),

]

table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)