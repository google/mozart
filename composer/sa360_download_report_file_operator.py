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
"""Operator for downloading SA360 reports.

"""

import logging
import os
import tempfile
from airflow import models

logger = logging.getLogger(__name__)


class ReportNotReadyError(RuntimeError):
  """Error thrown when a Report is not ready on SA360.
  """
  pass


class SA360DownloadReportFileOperator(models.BaseOperator):
  """Operator to download a SA360 report.

  This Operator downloads a SA360 API into BigQuery. The operator takes the
  report ID from the *report_id* XCom variable.

  Report should be ready when execution gets to this operator. You may make use
  of SA360ReportFileAvailableSensor to make sure report is ready. A
  ReportNotReadyException will be raised on execution if report is found not to
  be ready.
  """

  template_fields = ['_filename']

  def __init__(self, sa360_reporting_hook, gcs_hook, gcs_bucket,
               filename, write_header, *args, **kwargs):
    """Constructor.

    Args:
      sa360_reporting_hook: Airflow hook for creating SA360 Reporting API
        service.
      gcs_hook: Airflow Hook for connecting to Google Cloud Storage (GCS)
        (e.g.: airflow.contrib.hooks.gcs_hook.GoogleCloudStorageHook).
      gcs_bucket: Name of the GCS bucket where the files downloaded from SA360
        API will be stored.
      filename: Name of the output file in GCS (templated).
      write_header: Whether output file should contain headers.
      *args: Other arguments for use with airflow.models.BaseOperator.
      **kwargs: Other arguments for use with airflow.models.BaseOperator.
    """
    super(SA360DownloadReportFileOperator, self).__init__(*args, **kwargs)
    self._service = None
    self._sa360_reporting_hook = sa360_reporting_hook
    self._gcs_hook = gcs_hook
    self._gcs_bucket = gcs_bucket
    self._filename = filename
    self._write_header = write_header

  def execute(self, context):
    """Execute operator.

    This method is invoked by Airflow to execute the task.

    Args:
      context: Airflow context.

    Raises:
      ReportNotReadyError: If report is not ready yet.
      ValueError: If no report_id is provided via XCom.
    """
    report_id = context['task_instance'].xcom_pull(
        task_ids=None, key='report_id')
    if not report_id:
      raise ValueError('No report_id found in XCom')
    if self._service is None:
      self._service = self._sa360_reporting_hook.get_service()

    # Get report files
    request = self._service.reports().get(reportId=report_id)
    response = request.execute()

    if not response['isReportReady']:
      raise ReportNotReadyError('Report %s is not ready' % (report_id))

    no_of_files = len(response['files'])

    temp_filename = None
    try:
      # Creating temp file. Creting it with 'delete=False' because we need to
      # close it and keep the contents stored for use by gcs_hook.upload().
      with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_filename = temp_file.name
        for i in xrange(no_of_files):
          request = self._service.reports().getFile(
              reportId=report_id, reportFragment=i)
          response = request.execute()
          # Skip header if this is not the first fragment or if headers are
          # disabled
          if (not self._write_header) or i > 0:
            splitted_response = response.split('\n', 1)
            if len(splitted_response) > 1:
              response = splitted_response[1]
            else:
              response = ''
          temp_file.writelines(response)
          logger.info('Report fragment %d written to file: %s', i,
                      temp_filename)
      self._gcs_hook.upload(self._gcs_bucket, self._filename, temp_filename)
    finally:
      if temp_filename:
        os.unlink(temp_filename)
