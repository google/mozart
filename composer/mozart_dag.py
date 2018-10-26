# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Mozart main DAG.

This DAG automates the process of downloading SA360 reports into Cloud Storage
and then processing it using DataFlow.

DAG requirements:
  Airflow variables:
    sa360_agency_id: SA360 agency ID (e.g.: 123456789)
    sa360_advertiser_id: SA360 advertiser ID (e.g.: 987654321)
    gcs_bucket: GCS bucket where SA360 reports will be stored for processing.
      Just the bucket name, no *gs://* prefix
    mozart_start_date: Date on which the workflow will begin processing. Format:
      YYYY-MM-DD
"""
import datetime

import airflow
from airflow import models
from airflow.contrib.hooks import gcs_hook
from airflow.contrib.hooks import ssh_hook
from airflow.contrib.operators import dataflow_operator

import gcs_to_sftp_operator
import sa360_create_report_operator as create_operator
import sa360_download_report_file_operator as download_operator
import sa360_report_file_available_sensor as available_sensor
import sa360_report_request_builder as request_builder
import sa360_reporting_hook

GCS_PATH_FORMAT = 'gs://%s/%s'
REPORT_FILENAME = 'report-{{ run_id }}.csv'
OUTPUT_FILENAME = 'output/' + REPORT_FILENAME

# Read configuration from Airflow variables
sa360_conn_id = 'google_cloud_default'
dataflow_conn_id = 'dataflow'
sftp_conn_id = 'sa360_sftp'
agency_id = models.Variable.get('sa360_agency_id')
advertiser_id = models.Variable.get('sa360_advertiser_id')
gcs_bucket = models.Variable.get('gcs_bucket')
start_date = datetime.datetime.strptime(
    models.Variable.get('mozart_start_date'), '%Y-%m-%d')
lookback_days = int(models.Variable.get('lookback_days'))
gcp_project = models.Variable.get('gcp_project')
gcp_zone = models.Variable.get('gcp_zone')
dataflow_staging = models.Variable.get('dataflow_staging')
dataflow_template = models.Variable.get('dataflow_template')

# Default args that will be applied to all tasks in the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=10),
}

# Hooks for connecting to GCS, SA360 API and SA360's sFTP endpoint
sa360_reporting_hook = sa360_reporting_hook.SA360ReportingHook(
    sa360_report_conn_id=sa360_conn_id)
gcs_hook = gcs_hook.GoogleCloudStorageHook()
ssh_hook = ssh_hook.SSHHook(
    remote_host='partnerupload.google.com',
    username='feeds-8c94k6',
    password='>$qAZWZ2!B',
    port=19321)

# SA360 request builder
request_builder = request_builder.SA360ReportRequestBuilder(
    agency_id, advertiser_id)

output_file_header = ','.join(request_builder.get_headers())

# DAG definition
dag = airflow.DAG(
    'mozart_dag',
    default_args=default_args,
    schedule_interval=datetime.timedelta(1),
    concurrency=3)

create_report = create_operator.SA360CreateReportOperator(
    task_id='create_kw_report',
    sa360_reporting_hook=sa360_reporting_hook,
    request_builder=request_builder,
    lookback_days=lookback_days,
    dag=dag)

wait_for_report = available_sensor.SA360ReportFileAvailableSensor(
    task_id='wait_for_kw_report',
    sa360_reporting_hook=sa360_reporting_hook,
    poke_interval=30,
    retries=500,
    dag=dag)
create_report.set_downstream(wait_for_report)

download_file = download_operator.SA360DownloadReportFileOperator(
    task_id='download_kw_report',
    sa360_reporting_hook=sa360_reporting_hook,
    gcs_hook=gcs_hook,
    gcs_bucket=gcs_bucket,
    filename=REPORT_FILENAME,
    write_header=False,
    dag=dag)
wait_for_report.set_downstream(download_file)

process_elements = dataflow_operator.DataflowTemplateOperator(
    task_id='process_elements',
    dataflow_default_options={
        'project': gcp_project,
        'zone': gcp_zone,
        'tempLocation': dataflow_staging,
    },
    parameters={
        'inputFile': GCS_PATH_FORMAT % (gcs_bucket, REPORT_FILENAME),
        'outputFile': GCS_PATH_FORMAT % (gcs_bucket, OUTPUT_FILENAME),
        'header': output_file_header
    },
    template=dataflow_template,
    gcp_conn_id=dataflow_conn_id,
    dag=dag)
download_file.set_downstream(process_elements)

upload_to_sftp = gcs_to_sftp_operator.GCSToSFTPOperator(
    task_id='upload_to_sftp',
    gcs_hook=gcs_hook,
    ssh_hook=ssh_hook,
    gcs_bucket=gcs_bucket,
    gcs_filename=OUTPUT_FILENAME,
    sftp_destination_path='/input.csv',
    gcp_conn_id=sa360_conn_id,
    header=output_file_header,
    dag=dag)
process_elements.set_downstream(upload_to_sftp)
