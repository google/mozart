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
import json

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

# Read configuration from Airflow variables
sa360_conn_id = 'google_cloud_default'
ssh_conn_id = 'sa360_sftp'
agency_id = models.Variable.get('mozart/sa360_agency_id')
advertisers = json.loads(models.Variable.get('mozart/sa360_advertisers'))
gcs_bucket = models.Variable.get('mozart/gcs_bucket')
start_date = datetime.datetime.strptime(
    models.Variable.get('mozart/start_date'), '%Y-%m-%d')
lookback_days = int(models.Variable.get('mozart/lookback_days'))
gcp_project = models.Variable.get('mozart/gcp_project')
gcp_zone = models.Variable.get('mozart/gcp_zone')
dataflow_staging = models.Variable.get('mozart/dataflow_staging')
dataflow_template = models.Variable.get('mozart/dataflow_template')
input_custom_data_file = models.Variable.get('input_custom_data_file')
custom_data_column_names = models.Variable.get('custom_data_column_names')

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

# Hooks for connecting to GCS, SA360 API and SA360's sFTP endpoint.
sa360_reporting_hook = sa360_reporting_hook.SA360ReportingHook(
    sa360_report_conn_id=sa360_conn_id)
gcs_hook = gcs_hook.GoogleCloudStorageHook()

# SA360 request builder
request_builder = request_builder.SA360ReportRequestBuilder(
    agency_id, list(elem['advertiserId'] for elem in advertisers))

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

for advertiser in advertisers:
  advertiser_id = advertiser['advertiserId']
  sftp_conn_id = advertiser.get('sftpConnId', None)
  sftp_host = advertiser.get('sftpHost', None)
  sftp_port = advertiser.get('sftpPort', None)
  sftp_username = advertiser.get('sftpUsername', None)
  sftp_password = advertiser.get('sftpPassword', None)
  connection_hook = ssh_hook.SSHHook(
      ssh_conn_id=sftp_conn_id,
      remote_host=sftp_host,
      username=sftp_username,
      password=sftp_password,
      port=sftp_port)

  output_filename = 'output/report-%s-{{ run_id }}.csv' % advertiser_id

  process_elements = dataflow_operator.DataflowTemplateOperator(
      task_id='process_elements-%s' % advertiser_id,
      dataflow_default_options={
          'project': gcp_project,
          'zone': gcp_zone,
          'tempLocation': dataflow_staging,
      },
      parameters={
          'inputKeywordsFile':
              GCS_PATH_FORMAT % (gcs_bucket, REPORT_FILENAME),
          'outputKeywordsFile':
              GCS_PATH_FORMAT % (gcs_bucket, output_filename),
          'keywordColumnNames':
              output_file_header,
          'inputCustomDataFile':
              input_custom_data_file,
          'customDataColumnNames':
              custom_data_column_names,
          'advertiserId':
              advertiser_id
      },
      template=dataflow_template,
      gcp_conn_id=sa360_conn_id,
      dag=dag)
  download_file.set_downstream(process_elements)

  upload_to_sftp = gcs_to_sftp_operator.GCSToSFTPOperator(
      task_id='upload_to_sftp-%s' % advertiser_id,
      gcs_hook=gcs_hook,
      ssh_hook=connection_hook,
      gcs_bucket=gcs_bucket,
      gcs_filename=output_filename,
      sftp_destination_path='/input.csv',
      gcp_conn_id=sa360_conn_id,
      header='Row Type,Action,Status,' + output_file_header,
      dag=dag)
  process_elements.set_downstream(upload_to_sftp)
