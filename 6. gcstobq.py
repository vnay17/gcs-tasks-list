import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
from apache_beam.io.gcp import bigquery, gcsfilesystem
import os
import logging
from apache_beam.options.pipeline_options import SetupOptions
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/VinayKumarBandirajul/Desktop/sample/gcp-project-398805-6afd4e0b4940.json'
 
def run(argv=None, save_main_session=True):
    pipeline_options = PipelineOptions(
        flags=None,
        runner='DataflowRunner',
        project='gcp-project-398805',
        region='us-central1',
        job_name='bg-to-bq-load',
        temp_location='gs://bqs-gcs/temp/',
        staging_location='gs://bqs-gcs/staging/'
    )
    # gcp-training-21585.sample_python.titanic_py
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    # Set the GCP project and bucket
    PROJECT = 'gcp-project-398805'
    BUCKET = 'bqs-gcs'
    # Define the input CSV file and BigQuery table
    INPUT_CSV = 'gs://{}/bqdatagcs.csv'.format(BUCKET)
    OUTPUT_TABLE = '{}.dataset.gcstobq'.format(PROJECT)
 
    # Define the output archive folder
    OUTPUT_ARCHIVE = 'gs://{}/archives/'.format(BUCKET)
 
    file_name = os.path.basename(INPUT_CSV)
    archive_file = '{}{}'.format(OUTPUT_ARCHIVE, file_name.split('.')[0])
    # archive_file = '{}'.format(OUTPUT_ARCHIVE)
    with beam.Pipeline(options=pipeline_options) as p:
        # Read the CSV file from GCS
        data = (p | 'Read from CSV' >> ReadFromText(INPUT_CSV, skip_header_lines=1))
 
        # Convert each row to a dictionary
        dicts = data | "Convert CSV to Dictionary" >> beam.Map(lambda row: dict(zip(('Duration', 'Pulse', 'Maxpulse', 'Calories'), row.split(','))))
        # Write the data to BigQuery
        dicts | 'Write to BigQuery' >> beam.io.WriteToBigQuery(OUTPUT_TABLE,
        schema = 'Duration:INTEGER,Pulse:INTEGER,Maxpulse:INTEGER,Calories:FLOAT',
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        # Move the input CSV file to the archive folder in GCS
        (data | 'Move file to archive' >> beam.io.WriteToText(
            archive_file,
            file_name_suffix='.csv',
            num_shards=0,
            shard_name_template=''))
 
 
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()