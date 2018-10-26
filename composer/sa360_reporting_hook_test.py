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
"""Tests for sa360_reporting_hook."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import mock
from google3.corp.gtech.ads.doubleclick.composer.common import test_utils
from google3.testing.pybase import googletest


HTTP_AUTHORIZED = 'authorized_http'

cloud_base_hook_mock = mock.MagicMock
cloud_base_hook_mock._authorize = mock.MagicMock(return_value=HTTP_AUTHORIZED)

gcp_api_base_hook = mock.MagicMock
gcp_api_base_hook.GoogleCloudBaseHook = cloud_base_hook_mock


def mock_build_method(api_name, api_version, http):
  return (api_name, api_version, http)

discovery_mock = mock.MagicMock
discovery_mock.build = mock.MagicMock(side_effect=mock_build_method)

SA360ReportingHook = test_utils.import_with_mock_dependencies(  # Mock dependency injection pylint: disable=invalid-name
    'google3.corp.gtech.ads.doubleclick.composer'
    '.sa360.sa360_reporting_hook.SA360ReportingHook',
    [{'name': 'airflow.contrib.hooks.gcp_api_base_hook',
      'mock': gcp_api_base_hook},
     {'name': 'apiclient.discovery',
      'mock': discovery_mock}
    ])


class TestException(Exception):
  pass


class Sa360ReportingHookTest(googletest.TestCase):

  API_VERSION = 'v2'
  API_NAME = 'doubleclicksearch'
  CONNECTION_ID = 'sa360_report_default'

  def setUp(self):
    self.hook = SA360ReportingHook()

  def test_get_service(self):
    service = self.hook.get_service()
    self.assertEqual((self.API_NAME,
                      self.API_VERSION, HTTP_AUTHORIZED), service)

  def test_custom_api_name(self):
    self.hook = SA360ReportingHook(api_name='custom_api_name')
    service = self.hook.get_service()
    self.assertEqual(('custom_api_name',
                      self.API_VERSION, HTTP_AUTHORIZED), service)

  def test_custom_api_version(self):
    self.hook = SA360ReportingHook(api_version='custom_api_version')
    service = self.hook.get_service()
    self.assertEqual((self.API_NAME,
                      'custom_api_version', HTTP_AUTHORIZED), service)

  def test_exception_on_authorize(self):
    # Check that exceptions are properly propagated should they be raised.
    self.hook._authorize = mock.MagicMock(side_effect=TestException)
    with self.assertRaises(TestException):
      self.hook.get_service()

  def test_exception_on_build(self):
    # Check that exceptions are properly propagated should they be raised.
    with mock.patch('google3.corp.gtech.ads.doubleclick.composer.sa360'
                    '.sa360_reporting_hook.discovery.build') as build_mock:
      build_mock.side_effect = TestException
      with self.assertRaises(TestException):
        self.hook.get_service()


if __name__ == '__main__':
  googletest.main()
