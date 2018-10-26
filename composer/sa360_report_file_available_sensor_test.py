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
"""Tests for sa360_report_file_available_sensor."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import mock
from google3.corp.gtech.ads.doubleclick.composer.common import test_utils
from google3.testing.pybase import googletest

sensors_mock = mock.MagicMock
sensors_mock.BaseSensorOperator = mock.MagicMock

SA360ReportFileAvailableSensor = test_utils.import_with_mock_dependencies(  # Mock dependency injection pylint: disable=invalid-name
    'google3.corp.gtech.ads.doubleclick.composer.'
    'sa360.sa360_report_file_available_sensor.SA360ReportFileAvailableSensor',
    [{'name': 'airflow.operators.sensors',
      'mock': sensors_mock}
    ])


REPORT_ID = 'test_report_id'
XCOM_REPORT_ID_KEY = 'report_id'
CONTEXT_TASK_INSTANCE = 'task_instance'
RESPONSE_IS_READY_KEY = 'isReportReady'
RESPONSE_READY = {RESPONSE_IS_READY_KEY: True}
RESPONSE_NOT_READY = {RESPONSE_IS_READY_KEY: False}


class TestException(Exception):
  pass


class Sa360ReportFileAvailableSensorTest(googletest.TestCase):

  def setUp(self):
    self.hook = mock.MagicMock()
    self.context = {CONTEXT_TASK_INSTANCE: mock.MagicMock()}
    self.context[CONTEXT_TASK_INSTANCE].xcom_pull.return_value = REPORT_ID
    self.sensor = SA360ReportFileAvailableSensor(self.hook)

  def test_report_ready(self):
    self.hook.get_service().reports().get().execute(
        ).__getitem__.side_effect = RESPONSE_READY.__getitem__
    self.assertTrue(self.sensor.poke(self.context))

  def test_report_not_ready(self):
    self.hook.get_service().reports().get().execute(
        ).__getitem__.side_effect = RESPONSE_NOT_READY.__getitem__
    self.assertFalse(self.sensor.poke(self.context))

  def test_correct_xcom_pull(self):
    self.sensor.poke(self.context)
    self.context[CONTEXT_TASK_INSTANCE].xcom_pull.assert_called_once_with(
        task_ids=None, key=XCOM_REPORT_ID_KEY)

  def test_no_report_id(self):
    self.context[CONTEXT_TASK_INSTANCE].xcom_pull.return_value = None
    with self.assertRaises(KeyError):
      self.sensor.poke(self.context)

  def test_exception_on_execute(self):
    self.hook.get_service().reports().get().execute.side_effect = TestException
    with self.assertRaises(TestException):
      self.sensor.poke(self.context)

  def test_exception_on_get_service(self):
    self.hook.get_service.side_effect = TestException
    with self.assertRaises(TestException):
      self.sensor.poke(self.context)


if __name__ == '__main__':
  googletest.main()
