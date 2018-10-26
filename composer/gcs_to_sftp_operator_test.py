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
"""Tests for common.gcs_to_sftp_operator."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import contextlib
import mock

from google3.corp.gtech.ads.doubleclick.composer.common import test_utils
from google3.testing.pybase import googletest

GCSToSFTPOperator = test_utils.import_with_mock_dependencies(  # Mock dependency injection pylint: disable=invalid-name
    'google3.corp.gtech.ads.doubleclick.composer.'
    'common.gcs_to_sftp_operator.GCSToSFTPOperator', [{
        'name': 'airflow.models.BaseOperator',
        'mock': mock.MagicMock
    }])

GCS_BUCKET = 'test_gcs_bucket'
GCS_FILENAME = 'test_gcs_filename'
SFTP_DESTINATION = 'test_sftp_destination'
MOCK_FILE_NAME = 'mock_filename'
CONTEXT_TASK_INSTANCE = 'task_instance'
FILE_CONTENTS = u'Test first line áéíóú\nTest second line\n'
FILE_HEADER = u'Headeráéíóú'


class TestException(Exception):
  pass


class GcsToSftpOperatorTest(googletest.TestCase):

  def putfo_mock(self, file_fd, destination):
    self.sftp_filesystem[destination] = file_fd.read()

  def setUp(self):
    self.gcs_hook = mock.MagicMock()
    self.gcs_hook.download.return_value = FILE_CONTENTS

    self.ssh_hook = mock.MagicMock()
    self.ssh_hook.get_conn().__enter__(
        ).open_sftp().putfo.side_effect = self.putfo_mock
    self.operator = GCSToSFTPOperator(self.gcs_hook, self.ssh_hook, GCS_BUCKET,
                                      GCS_FILENAME, SFTP_DESTINATION)
    self.context = {CONTEXT_TASK_INSTANCE: mock.MagicMock()}

    self.sftp_filesystem = {}

  def test_gcs_download(self):
    self.operator.execute(self.context)
    self.gcs_hook.download.assert_called_once_with(GCS_BUCKET, GCS_FILENAME)

  def test_contents(self):
    self.operator.execute(self.context)
    self.assertEqual(self.sftp_filesystem[SFTP_DESTINATION], FILE_CONTENTS)

  def test_contents_header(self):
    self.operator = GCSToSFTPOperator(self.gcs_hook, self.ssh_hook, GCS_BUCKET,
                                      GCS_FILENAME, SFTP_DESTINATION,
                                      FILE_HEADER)
    self.operator.execute(self.context)
    self.assertEqual(self.sftp_filesystem[SFTP_DESTINATION], '%s\n%s' % (
        FILE_HEADER, FILE_CONTENTS))


if __name__ == '__main__':
  googletest.main()
