 # Develop a dataflow job to export data from Bigquery table to GCS.

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery
import os
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.gcsio import GcsIO
# from google.cloud import storage      
import logging
import datetime
os.environ['GOOGLE_CLOUD_PROJECT'] = 'gcp-project-398805'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/VinayKumarBandirajul/Desktop/sample/gcp-project-398805-f7812d799e68.json'
def run_dataflow_job(project_id, temp_location,destination_file,file_name,save_main_session=True):
    # Create pipeline options
    options = PipelineOptions(
        runner='DataflowRunner',
        job_name = 'bq-to-gcs',
        project = 'gcp-project-398805',
        region='us-central1',
        staging_location='gs://bqs-gcs/staging/',
        temp_location = temp_location
    )
    options.view_as(SetupOptions).save_main_session = save_main_session
    # Create the pipeline
    with beam.Pipeline(options=options) as pipeline:
        class DeleteFile(beam.DoFn):
            def process(self, element):
                gcs = GcsIO()
                gcs.delete([element])
                yield
        # Read data from the source table
        source_data = (
            pipeline
            | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(query='SELECT * FROM gcp-project-398805.dataset.titanic',
                use_standard_sql=True)
            # |    beam.Map(print)
        )
 
        source_data | 'WriteToText' >> beam.io.WriteToText(destination_file,file_name)
       
 
 
project_id='gcp-project-398805'
dataset_id='gcp-project-398805.dataset'
table_id='titanic'
dataset_ref = bigquery.DatasetReference(project=project_id, dataset_id=dataset_id)
 
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    source_table = 'gcp-project-398805.dataset.titanic'
    destination_table = dataset_ref.table(table_id)
    file_name=f"orders_load_{datetime.datetime.now()}.csv"
    # destination_table = bigquery.TableReference(dataset_ref=dataset_id, table_id=table_id)
    # destination_table = 'gcp-training-386806.GCP_TRAINING.veg_plant_1'
    project_id = 'gcp-project-398805'
    temp_location = 'gs://bqs-gcs/temp/'  # Set your desired GCS bucket
    input_file=f"gs://bqs-gcs/{file_name}"
    destination_file=f"gs://bqs-gcs/dest/"
    run_dataflow_job(project_id, temp_location,destination_file,file_name)
 
# if __name__ == '__main__':
#     logging.getLogger().setLevel(logging.INFO)
#     run()