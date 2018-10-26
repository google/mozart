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
"""Operator to upload file from Google Cloud Storage into an sFTP.
"""

import contextlib
import StringIO
import logging
import os

from airflow import models


logger = logging.getLogger(__name__)


class GCSToSFTPOperator(models.BaseOperator):
  """Operator to upload a GCS file to an sFTP location.
  """

  template_fields = ['_gcs_filename']

  def __init__(self, gcs_hook, ssh_hook, gcs_bucket, gcs_filename,
               sftp_destination_path, header=None, **kwargs):
    """Constructor.

    Args:
      gcs_hook: GCS Hook for connecting to Google Cloud Storage.
      ssh_hook: sFTP Hook for connecting to sFTP server.
      gcs_bucket: GCS bucket where source file is stored.
      gcs_filename: File name in GCS of source file.
      sftp_destination_path: Destination path in the sFTP server.
      header: Optional header to be added to the uploaded file. Defaults to None
      **kwargs: Other arguments for use with airflow.models.BaseOperator.
    """
    super(GCSToSFTPOperator, self).__init__(**kwargs)
    self._gcs_hook = gcs_hook
    self._gcs_bucket = gcs_bucket
    self._gcs_filename = gcs_filename
    self._ssh_hook = ssh_hook
    self._sftp_destination_path = sftp_destination_path
    self._header = header

  def execute(self, context):
    """Execute operator.

    This method is invoked by Airflow to execute the task.

    Args:
      context: Airflow context.

    Raises:
      ValueError: If provided GCS URI cannot be parsed for bucket and filename.
    """
    file_data = self._gcs_hook.download(self._gcs_bucket,
                                        self._gcs_filename)
    with contextlib.closing(StringIO.StringIO()) as file_fd:
      if self._header:
        file_fd.write('%s\n' % self._header)
      file_fd.write(file_data)
      file_fd.seek(0)
      with self._ssh_hook.get_conn() as ssh_client:
        sftp_client = ssh_client.open_sftp()
        sftp_client.putfo(file_fd, self._sftp_destination_path)
        logger.debug('File [gs://%s/%s] uploaded to sFTP destination: %s',
                     self._gcs_bucket, self._gcs_filename,
                     self._sftp_destination_path)


