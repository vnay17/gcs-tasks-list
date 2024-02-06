from google.cloud import bigquery
from google.oauth2 import service_account

# Specify the path to your service account key file
key_path = 'C:/Users/VinayKumarBandirajul/Desktop/sample/gcp-project-398805-f7812d799e68.json'

# Load the credentials from the key file
credentials = service_account.Credentials.from_service_account_file(key_path)

# Initialize the BigQuery client with the credentials
client = bigquery.Client(credentials=credentials)

#  Set dataset_id to the ID of the dataset to create.
dataset_id = "{}.dataset".format(client.project)

# Construct a full Dataset object to send to the API.
dataset = bigquery.Dataset(dataset_id)

#  Specify the geographic location where the dataset should reside.
dataset.location = "us-central1"

# Send the dataset to the API for creation, with an explicit timeout.
# Raises google.api_core.exceptions.Conflict if the Dataset already
# exists within the project.
dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
print("Created dataset {}.{}".format(client.project, dataset.dataset_id))