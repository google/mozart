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
"""Sensor to check if SA360 report generation has finished.
"""

import logging
from airflow.operators import sensors

logger = logging.getLogger(__name__)


class SA360ReportFileAvailableSensor(sensors.BaseSensorOperator):
  """Sensor to check if SA360 report generation has finished.

  This sensor calls SA360 API to check if a SA360 report is ready to download.
  It makes use of SA360ReportingHook to access the SA360 API. It takes the
  SA360 report ID from the *report_id* XCom variable.
  """

  def __init__(self, sa360_reporting_hook, *args, **kwargs):
    """Constructor.

    Args:
      sa360_reporting_hook: Airflow hook for creating SA360 Reporting API
        service.
      *args: Other arguments for use with airflow.models.BaseOperator.
      **kwargs: Other arguments for use with airflow.models.BaseOperator.
    """
    super(SA360ReportFileAvailableSensor, self).__init__(*args, **kwargs)
    self._sa360_reporting_hook = sa360_reporting_hook
    self._service = None

  def poke(self, context):
    """Check for report ready.

    This method is invoked by Airflow to trigger the sensor. It checks whether
    the report is ready.

    Args:
      context: Airflow context.

    Returns:
      True if report is ready. False otherwise.

    Raises:
      KeyError: If *report_id* is not found in XCom
    """
    if self._service is None:
      self._service = self._sa360_reporting_hook.get_service()

    # Pull report_id from xcom.
    report_id = context['task_instance'].xcom_pull(
        task_ids=None, key='report_id')
    report_ready = False
    if report_id is None:
      raise KeyError("Report ID key not found in XCom: 'report_id'")
    request = self._service.reports().get(reportId=report_id)
    response = request.execute()
    logger.debug('Report poll response: %s', response)
    if response['isReportReady']:
      report_ready = True
    return report_ready
