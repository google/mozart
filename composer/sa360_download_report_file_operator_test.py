# coding=utf-8
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
"""Tests for sa360_download_report_file_operator."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import contextlib
import StringIO

import mock

from google3.corp.gtech.ads.doubleclick.composer.common import test_utils
from google3.testing.pybase import googletest

FILENAME = 'test_output_filename'
TEMP_FILENAME = 'test_temp_filename'
GCS_BUCKET = 'test_gcs_bucket'
CONTEXT_TASK_INSTANCE = 'task_instance'
REPORT_ID = 'test_report_id'
REPORT_FILE_IDS = ['file_id1']
REPORT_HEADER = u'This is a test report. áéíóú\n'
REPORT_CONTENTS_FRAGMENT_1 = u'First fragment contents\n'
REPORT_CONTENTS_FRAGMENT_2 = u'Second fragment contents\n'
REPORT_CONTENTS = ['%s%s' % (REPORT_HEADER, REPORT_CONTENTS_FRAGMENT_1),
                   '%s%s' % (REPORT_HEADER, REPORT_CONTENTS_FRAGMENT_2)]
GCS_URI = 'gs://%s/%s'

dependencies = [{'name': 'airflow.models.BaseOperator', 'mock': mock.MagicMock}]

SA360DownloadReportFileOperator = test_utils.import_with_mock_dependencies(  # Mock dependency injection pylint: disable=invalid-name
    'google3.corp.gtech.ads.doubleclick.composer.'
    'sa360.sa360_download_report_file_operator.SA360DownloadReportFileOperator',
    dependencies)

ReportNotReadyError = test_utils.import_with_mock_dependencies(  # Mock dependency injection pylint: disable=invalid-name
    'google3.corp.gtech.ads.doubleclick.composer.'
    'sa360.sa360_download_report_file_operator.ReportNotReadyError',
    dependencies)


class TestException(Exception):
  pass


class Sa360DownloadReportFileOperatorTest(googletest.TestCase):

  def create_stringio_file(self, *unused_args, **unused_kwargs):
    file_mock = StringIO.StringIO()
    file_mock.name = TEMP_FILENAME
    # We don't want the StringIO file to be closed since we can't open it again
    # for reading like a normal file
    file_mock.close = mock.MagicMock()
    self.files[file_mock.name] = file_mock
    return contextlib.closing(file_mock)

  def create_read_only_stringio_file(self, *unused_args, **unused_kwargs):
    file_mock = mock.MagicMock()
    file_mock.name = TEMP_FILENAME
    file_mock.writelines.side_effect = OSError
    return contextlib.closing(file_mock)

  def setUp(self):
    self.report_ready = True
    self.files = {}
    self.gcs_file_contents = {}
    self.sa360_hook = mock.MagicMock()

    def get_report(reportId):  # Must use same name as SA360 API method pylint: disable=invalid-name
      if reportId != REPORT_ID:
        raise TestException('Report not found')
      request = mock.MagicMock()
      request.execute.return_value = {
          'files': REPORT_FILE_IDS,
          'isReportReady': self.report_ready
      }
      return request

    def get_report_file(reportId, reportFragment):  # Must use same name as SA360 API method pylint: disable=invalid-name
      if reportId != REPORT_ID:
        raise TestException('Report not found')
      request = mock.MagicMock()
      request.execute.return_value = REPORT_CONTENTS[reportFragment]
      return request

    self.sa360_hook.get_service().reports().get = mock.MagicMock(
        side_effect=get_report)
    self.sa360_hook.get_service().reports().getFile = mock.MagicMock(
        side_effect=get_report_file)

    self.gcs_hook = mock.MagicMock()

    def gcs_upload(gcs_bucket, target_filename, source_filename):
      self.gcs_file_contents[
          GCS_URI % (gcs_bucket,
                     target_filename)] = self.files[source_filename].getvalue()

    self.gcs_hook.upload = mock.MagicMock(side_effect=gcs_upload)

    self.context = {CONTEXT_TASK_INSTANCE: mock.MagicMock()}
    self.context[CONTEXT_TASK_INSTANCE].xcom_pull.return_value = REPORT_ID

    self.operator = SA360DownloadReportFileOperator(
        self.sa360_hook, self.gcs_hook, GCS_BUCKET, FILENAME, True)

  def test_report_contents(self):
    with mock.patch(
        'tempfile.NamedTemporaryFile',
        mock.MagicMock(
            side_effect=self.create_stringio_file)), mock.patch('os.unlink'):
      self.operator.execute(self.context)
      self.assertEqual(self.gcs_file_contents[GCS_URI % (GCS_BUCKET, FILENAME)],
                       REPORT_CONTENTS[0])

  def test_report_contents_multifragment(self):

    def get_report(reportId):  # Must use same name as SA360 API method pylint: disable=invalid-name
      if reportId != REPORT_ID:
        raise TestException('Report not found')
      request = mock.MagicMock()
      request.execute.return_value = {
          'files': ['file_id1', 'file_id2'],
          'isReportReady': self.report_ready
      }
      return request

    self.sa360_hook.get_service().reports().get = mock.MagicMock(
        side_effect=get_report)
    with mock.patch(
        'tempfile.NamedTemporaryFile',
        mock.MagicMock(
            side_effect=self.create_stringio_file)), mock.patch('os.unlink'):
      self.operator.execute(self.context)
      self.assertEqual(
          self.gcs_file_contents[GCS_URI % (GCS_BUCKET, FILENAME)],
          u'%s%s%s' % (REPORT_HEADER, REPORT_CONTENTS_FRAGMENT_1,
                       REPORT_CONTENTS_FRAGMENT_2))

  def test_no_report_id(self):
    with mock.patch(
        'tempfile.NamedTemporaryFile',
        mock.MagicMock(
            side_effect=self.create_stringio_file)), mock.patch('os.unlink'):
      self.context[CONTEXT_TASK_INSTANCE].xcom_pull.return_value = None
      with self.assertRaises(ValueError):
        self.operator.execute(self.context)

  def test_report_not_found(self):
    self.context[CONTEXT_TASK_INSTANCE].xcom_pull.return_value = 'invalid_id'
    with mock.patch(
        'tempfile.NamedTemporaryFile',
        mock.MagicMock(
            side_effect=self.create_stringio_file)), mock.patch('os.unlink'):
      with self.assertRaises(TestException):
        self.operator.execute(self.context)

  def test_report_not_finished(self):
    self.report_ready = False
    with mock.patch(
        'tempfile.NamedTemporaryFile',
        mock.MagicMock(
            side_effect=self.create_stringio_file)), mock.patch('os.unlink'):
      with self.assertRaises(ReportNotReadyError):
        self.operator.execute(self.context)

  def test_cannot_open_temp_file(self):
    with mock.patch(
        'tempfile.NamedTemporaryFile',
        mock.MagicMock(side_effect=OSError)), mock.patch('os.unlink'):
      with self.assertRaises(OSError):
        self.operator.execute(self.context)

  def test_cannot_write_to_temp_file(self):
    with mock.patch(
        'tempfile.NamedTemporaryFile',
        mock.MagicMock(side_effect=self.create_read_only_stringio_file)
    ), mock.patch('os.unlink'):
      with self.assertRaises(OSError):
        self.operator.execute(self.context)

  def test_cannot_upload_to_gcs(self):
    self.gcs_hook.upload = mock.MagicMock(side_effect=TestException)
    with mock.patch(
        'tempfile.NamedTemporaryFile',
        mock.MagicMock(
            side_effect=self.create_stringio_file)), mock.patch('os.unlink'):
      with self.assertRaises(TestException):
        self.operator.execute(self.context)

  def test_one_temp_file(self):
    with mock.patch(
        'tempfile.NamedTemporaryFile',
        mock.MagicMock(
            side_effect=self.create_stringio_file)), mock.patch('os.unlink'):
      self.operator.execute(self.context)
      self.assertEqual(len(self.files), 1)

  def test_temp_file_deleted(self):
    with mock.patch(
        'tempfile.NamedTemporaryFile',
        mock.MagicMock(side_effect=self.create_stringio_file)), mock.patch(
            'os.unlink') as unlink_mock:
      self.operator.execute(self.context)
      unlink_mock.assert_called_with(TEMP_FILENAME)

  def test_temp_file_deleted_on_gcs_exception(self):
    self.gcs_hook.upload = mock.MagicMock(side_effect=TestException)
    with mock.patch(
        'tempfile.NamedTemporaryFile',
        mock.MagicMock(side_effect=self.create_stringio_file)), mock.patch(
            'os.unlink') as unlink_mock:
      try:
        self.operator.execute(self.context)
      except TestException:
        pass
      unlink_mock.assert_called_with(TEMP_FILENAME)

  def test_report_contents_no_header(self):
    self.operator = SA360DownloadReportFileOperator(
        self.sa360_hook, self.gcs_hook, GCS_BUCKET, FILENAME, False)
    with mock.patch(
        'tempfile.NamedTemporaryFile',
        mock.MagicMock(
            side_effect=self.create_stringio_file)), mock.patch('os.unlink'):
      self.operator.execute(self.context)
      self.assertEqual(self.gcs_file_contents[GCS_URI % (GCS_BUCKET, FILENAME)],
                       REPORT_CONTENTS_FRAGMENT_1)

  def test_report_contents_multifragment_no_header(self):
    self.operator = SA360DownloadReportFileOperator(
        self.sa360_hook, self.gcs_hook, GCS_BUCKET, FILENAME, False)

    def get_report(reportId):  # Must use same name as SA360 API method pylint: disable=invalid-name
      if reportId != REPORT_ID:
        raise TestException('Report not found')
      request = mock.MagicMock()
      request.execute.return_value = {
          'files': ['file_id1', 'file_id2'],
          'isReportReady': self.report_ready
      }
      return request

    self.sa360_hook.get_service().reports().get = mock.MagicMock(
        side_effect=get_report)
    with mock.patch(
        'tempfile.NamedTemporaryFile',
        mock.MagicMock(
            side_effect=self.create_stringio_file)), mock.patch('os.unlink'):
      self.operator.execute(self.context)
      self.assertEqual(
          self.gcs_file_contents[GCS_URI % (GCS_BUCKET, FILENAME)],
          u'%s%s' % (REPORT_CONTENTS_FRAGMENT_1, REPORT_CONTENTS_FRAGMENT_2))


if __name__ == '__main__':
  googletest.main()
