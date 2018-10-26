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
"""Tests for sa360_create_report_operator."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import datetime

import mock

from google3.corp.gtech.ads.doubleclick.composer.common import test_utils
from google3.testing.pybase import googletest

SA360CreateReportOperator = test_utils.import_with_mock_dependencies(  # Mock dependency injection pylint: disable=invalid-name
    'google3.corp.gtech.ads.doubleclick.composer.'
    'sa360.sa360_create_report_operator.SA360CreateReportOperator',
    [{'name': 'airflow.models.BaseOperator',
      'mock': mock.MagicMock}
    ])


class TestException(Exception):
  pass


class TestRequestBuilder(object):

  def build(self, start_date, end_date):
    result = {}
    result['startDate'] = start_date.isoformat()
    result['endDate'] = end_date.isoformat()
    return result


class Sa360CreateReportOperatorTest(googletest.TestCase):

  REQUEST_BODY = 'Test request body'
  XCOM_VARIABLE = 'report_id'
  RESPONSE_DICT = {'id': 'response_report_id'}
  CONTEXT_TASK_INSTANCE = 'task_instance'
  ID = 'id'
  LOOKBACK_DAYS = 10

  def create_and_execute_default(self):
    self.operator = SA360CreateReportOperator(
        self.hook, self.request_builder, self.LOOKBACK_DAYS)
    self.operator.execute(self.context)

  def setUp(self):
    self.hook = mock.MagicMock()
    self.context = {self.CONTEXT_TASK_INSTANCE: mock.MagicMock()}
    self.hook.get_service().reports().request().execute(
        ).__getitem__.side_effect = self.RESPONSE_DICT.__getitem__
    self.hook.reset_mock()
    self.request_builder = TestRequestBuilder()
    self.expected_start_date = datetime.date.today() - datetime.timedelta(
        self.LOOKBACK_DAYS + 1)
    self.expected_end_date = datetime.date.today() - datetime.timedelta(1)

  def tearDown(self):
    pass

  def test_request_body(self):
    self.create_and_execute_default()
    expected_body = {
        'startDate': self.expected_start_date.isoformat(),
        'endDate': self.expected_end_date.isoformat(),
    }
    self.hook.get_service().reports(
        ).request.assert_called_once_with(body=expected_body)

  def test_request_executed(self):
    self.create_and_execute_default()
    self.hook.get_service().reports(
        ).request().execute.assert_called_once_with()

  def test_id_pushed_to_xcom(self):
    self.create_and_execute_default()
    self.context[self.CONTEXT_TASK_INSTANCE].xcom_push.assert_called_with(
        self.XCOM_VARIABLE, self.RESPONSE_DICT[self.ID])

  def test_exception_on_get_service(self):
    self.hook.get_service = mock.MagicMock(side_effect=TestException)
    self.operator = SA360CreateReportOperator(
        self.hook, self.request_builder, self.LOOKBACK_DAYS)
    with self.assertRaises(TestException):
      self.operator.execute(self.context)

  def test_exception_on_request_execution(self):
    self.hook.get_service().reports().request().execute = mock.MagicMock(
        side_effect=TestException)
    self.operator = SA360CreateReportOperator(
        self.hook, self.request_builder, self.LOOKBACK_DAYS)
    with self.assertRaises(TestException):
      self.operator.execute(self.context)

  def test_exception_on_request_builder(self):
    self.request_builder.build = mock.MagicMock(
        side_effect=TestException)
    self.operator = SA360CreateReportOperator(
        self.hook, self.request_builder, self.LOOKBACK_DAYS)
    with self.assertRaises(TestException):
      self.operator.execute(self.context)

if __name__ == '__main__':
  googletest.main()
