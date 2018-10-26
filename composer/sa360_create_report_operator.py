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
"""Operator to create (request) a new SA360 Report.
"""

import datetime
import logging

from airflow import models


logger = logging.getLogger(__name__)


class SA360CreateReportOperator(models.BaseOperator):
  """Operator to request a new SA360 asynchronous report.

  This Operator requests a new report to the SA360 API. The newly requested
  report ID is then stored in a 'report_id' xcom so that it can be retrieved by
  a subsequent task.
  """

  def __init__(self, sa360_reporting_hook, request_builder, lookback_days,
               *args, **kwargs):
    """Constructor.

    Args:
      sa360_reporting_hook: Airflow hook to create the SA360 Reporting API
        service.
      request_builder: Request builder. Must be an object with a
        *build(start_date, end_date)* method.
      lookback_days: Number of days into the past for the report. Report will
        start at yesterday - lookback_days.
      *args: Other arguments for use with airflow.models.BaseOperator.
      **kwargs: Other arguments for use with airflow.models.BaseOperator.
    """
    super(SA360CreateReportOperator, self).__init__(*args, **kwargs)
    self._service = None
    self._request_builder = request_builder
    self._sa360_reporting_hook = sa360_reporting_hook
    self._lookback_days = lookback_days

  def execute(self, context):
    """Execute operator.

    This method is invoked by Airflow to execute the task.

    Args:
      context: Airflow context.
    """
    if self._service is None:
      self._service = self._sa360_reporting_hook.get_service()

    # start at yesterday - lookback_days and end at yesterday
    start_date = datetime.date.today() - datetime.timedelta(
        self._lookback_days + 1)
    end_date = datetime.date.today() - datetime.timedelta(1)

    request = self._service.reports().request(
        body=self._request_builder.build(start_date, end_date))
    response = request.execute()
    logger.info('Successfully created report')
    logger.debug('Create report response: %s', response)

    # Store report ID in xcom so that it can be retrieve by another task.
    context['task_instance'].xcom_push('report_id', response['id'])
