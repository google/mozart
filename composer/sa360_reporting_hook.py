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
"""Airflow hook for accesing SA360 Reporting API.

This hook provides access to SA Reporting API. It makes use of
GoogleCloudBaseHook to provide an authenticated api client.
"""

from airflow.contrib.hooks import gcp_api_base_hook
from apiclient import discovery


class SA360ReportingHook(gcp_api_base_hook.GoogleCloudBaseHook):
  """Airflow Hook to connect to SA360 Reporting API.

  This hook utilizes GoogleCloudBaseHook connection to generate the service
  object to SA360 reporting API.

  Once the Hook is instantiated you can invoke get_service() to obtain a
  service object that you can use to invoke the API.
  """

  def __init__(self,
               sa360_report_conn_id='sa360_report_default',
               api_name='doubleclicksearch',
               api_version='v2'):
    """Constructor.

    Args:
      sa360_report_conn_id: Airflow connection ID to be used for accesing
        SA360 Reporting API and authenticating requests. Default is
        sa360_report_default
      api_name: SA360 API name to be used. Default is doubleclicksearch.
      api_version: SA360 API Version. Default is v2.
    """
    super(SA360ReportingHook, self).__init__(
        gcp_conn_id=sa360_report_conn_id)

    self.api_name = api_name
    self.api_version = api_version

  def get_service(self):
    """Get API service object.

    This is called by Airflow whenever an API client service object is needed.
    Returned service object is already authenticated using the Airflow
    connection data provided in the sa360_report_conn_id constructor parameter.

    Returns:
      Google API client service object.
    """
    http_authorized = self._authorize()
    return discovery.build(self.api_name,
                           self.api_version, http=http_authorized)
