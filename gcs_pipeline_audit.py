# 	Develop a dataflow job, to read file data from GCS and load into bigquery	1 Week
#  	Once processing completed, move the file to another folder caller archive.	 
#  	Implement a audit log processing part of the complete code base.	 
#  	i.e create a audit log table and capture below records. 	 
#  	1. Filename ( With complete path )	 
#  	2. No of records in the file 	 
#  	3. No of inserted records in the file 	 
#  	4. No of rejected records in the file 	 
#  	5. Record inserted time	


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.options.pipeline_options import SetupOptions
from datetime import datetime
import datetime
from apache_beam.pvalue import TaggedOutput
from google.cloud import bigquery
import logging
import json
import os
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/VinayKumarBandirajul/Desktop/sample/gcp-project-398805-f7812d799e68.json'


# Define a custom DoFn to handle success and failure data
class DoFnMethods(beam.DoFn):
    def __init__(self,file_name):
        self.file_name = file_name
        self.success = 0
        self.error = 0
        self.total = 0
        self.window = beam.transforms.window.GlobalWindow()

    def process(self, element, window=beam.DoFn.WindowParam):
        element  = element.split(',')
        try:    
            element = {
                'index': int(element[0]),    
                'ean':int(element[1]),
                'stock':int(element[2]),
                'price':int(element(3))
                }

            self.success += 1
            yield element
        
        except Exception as e:  
            self.error +=1 

        finally :
            self.total +=1

    def finish_bundle(self):
        current_timestamp = datetime.datetime.now()
        yield beam.pvalue.TaggedOutput('auditlog',beam.utils.windowed_value.WindowedValue(
            value = {'file_name': self.file_name, 'success':self.success, 'error':self.error,'total':self.total, 'timestamp':current_timestamp},
            timestamp=0,
            windows=[self.window],
        ))

table_schema = 'index:INTEGER,ean:INTEGER,stock:INTEGER,price:INTEGER'

GCS_INPUT_FILE = "gs://gcs-titanic/offers-1000.txt"


BIGQUERY_PROJECT = "gcp-project-398805"
BIGQUERY_DATASET = "Bqdata"
BIGQUERY_TABLE = "bqtable"
BIGQUERY_AUDIT_TABLE = "bq-audit-table"
GCS_ARCHIVE_FOLDER = "gs://gcs-titanic/archive/"

# Define the input data source
input_file ="gs://gcs-titanic/offers-1000.txt"
input_file_path = 'gs://gcs-titanic'
input_file_name = 'offers-1000.txt'


audit_table_schema = 'file_name:STRING,success:INTEGER,error:INTEGER,total:INTEGER,timestamp:TIMESTAMP'

def run(GCS_INPUT_FILE,argv=None, save_main_session=True):
    pipeline_options = PipelineOptions(
        flags=None,
        runner='DataflowRunner',
        project='gcp-project-398805',
        region='us-central1',
        job_name='bq-data-job',
        temp_location='gs://gcs-titanic/tmp/',
        staging_location='gs://gcs-titanic/'
    )
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as pipeline:
        print('creating data')
        
        # Read data from the CSV file
        data = (
            pipeline
            | "Read CSV" >> beam.io.ReadFromText(GCS_INPUT_FILE, skip_header_lines=1))

        # Process the data and handle success and failure rows
        print('processing data')
        output,auditlog = data | 'DoFn methods' >> beam.ParDo(DoFnMethods(input_file_name)).with_outputs('auditlog', main='element')
        
        # writing ouput to bigquery table
        output | 'writing to bigquery' >> beam.io.WriteToBigQuery(
            table='gcp-project-398805.Bqdata.bqtable',
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

        # writing auditlog to bigquery table
        auditlog | 'writing to audit table' >> beam.io.WriteToBigQuery(
            table='gcp-project-398805.Bqdata.bq-audit-table',
            schema=audit_table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

        # # moving file to archive folder with tamestamp in name
        archive_folder = os.path.dirname(GCS_ARCHIVE_FOLDER)
        archive_file_path = os.path.join(
            archive_folder, f"{input_file_name.rstrip('.csv')}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        archive_file_path = archive_file_path.replace("\\", "/")  # Only for Windows
        fs = GCSFileSystem(pipeline_options=pipeline_options)
        # fs.rename([input_file], [archive_file_path])
        print(f"File {input_file_name} renamed and processed successfully.")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run(GCS_INPUT_FILE)