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
"""SA360 report request builder.

Helper for building SA360 report request bodies for SA360 API.
"""

ATTR_SCOPE = 'reportScope'
DEFAULT_COLUMNS = [{
    'columnName': 'advertiser'
}, {
    'columnName': 'account'
}, {
    'columnName': 'campaign'
}, {
    'columnName': 'keywordText'
}, {
    'columnName': 'keywordMatchType'
}, {
    'columnName': 'keywordMaxCpc'
}, {
    'columnName': 'clicks'
}, {
    'columnName': 'keywordId'
}]

# Equivalence between column names in the API and in the UI/Bulksheet
COLUMNS_DICT = {
    'advertiser': 'Advertiser',
    'account': 'Account',
    'campaign': 'Campaign',
    'keywordText': 'Keyword',
    'keywordMatchType': 'Match type',
    'keywordMaxCpc': 'Keyword max CPC',
    'clicks': 'Clicks',
    'keywordId': 'Keyword ID',
}


class SA360ReportRequestBuilder(object):
  """Class for building SA360 report requests.

  """

  def __init__(self, agency_id, advertiser_id=None, columns=None,
               filter_display_stats=True):
    """Constructor.

    Args:
      agency_id: ID of the agency for report scope.
      advertiser_id: Defaults to None. Advertiser ID for report scope. Report
        will be scoped to agency if this parameter is None.
      columns: JSON descriptor for the report columns. Defaults to basic set of
        columns.
      filter_display_stats: Whether Display Stats should be filtered out from
        the report (optional. Default: True).
    """
    self._agency_id = agency_id
    self._advertiser_id = advertiser_id
    self._columns = columns or DEFAULT_COLUMNS
    self._filter_display_stats = filter_display_stats

  def build(self, start_date, end_date):
    """Generate request body for keyword report.

    Args:
      start_date: datetime.date for report start date.
      end_date: datetime.date for report end date.

    Returns:
      Report request in JSON dict format.
    """
    request_body = {
        'downloadFormat': 'csv',
        'maxRowsPerFile': 100000000,
        'reportType': 'keyword',
        'statisticsCurrency': 'USD',
        ATTR_SCOPE: {
            'agencyId': self._agency_id
        },
        'columns': self._columns,
        'timeRange': {
            'startDate': start_date.isoformat(),
            'endDate': end_date.isoformat()
        }
    }
    if self._advertiser_id:
      request_body[ATTR_SCOPE]['advertiserId'] = self._advertiser_id
    if self._filter_display_stats:
      request_body['filters'] = [{
          'column': {
              'columnName': 'keywordText'
          },
          'operator': 'notEquals',
          'values': ['Display Network Stats']
      }]
    return request_body

  def get_headers(self, ui_names=True):
    """Get report headers.

    This method returns a list of strings with the headers for the generated
    report.

    Args:
      ui_names: Whether headers should follow UI naming (True) or API naming
        (False). Defaults to UI (True).

    Returns:
      List of strings with this report's headers.

    Raise:
      ValueError:
        If any column descriptor in self._columns does not contain exactly one
          element.
    """
    headers = []
    for column in self._columns:
      if len(column) != 1:
        raise ValueError('Unexpected number of values in column descriptor: %d'
                         % len(column))
      api_name = column.values()[0]
      headers.append(COLUMNS_DICT[api_name] if ui_names else api_name)
    return headers
