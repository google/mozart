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
"""Tests for sa360_report_request_builder."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import datetime

from google3.testing.pybase import googletest
from google3.corp.gtech.ads.doubleclick.mozart.composer import sa360_report_request_builder as request_builder

AGENCY_ID = 'test_agency_id'
ADVERTISER_ID = 'test_advertiser_id'
YEAR = 2018
MONTH = 3
DAY = 27
DATE_FORMAT = '%04d-%02d-%02d'


class Sa360ReportRequestBuilderTest(googletest.TestCase):

  def setUp(self):
    self.expected_body = {
        'downloadFormat':
            'csv',
        'maxRowsPerFile':
            100000000,
        'reportType':
            'keyword',
        'statisticsCurrency':
            'USD',
        'reportScope': {
            'agencyId': AGENCY_ID
        },
        'columns': [{
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
        }, {
            'columnName': 'advertiserId'
        }],
        'filters': [{
            'column': {
                'columnName': 'keywordId'
            },
            'operator': 'notEquals',
            'values': ['0']
        },
                    {
                        'column': {
                            'columnName': 'keywordText'
                        },
                        'operator': 'notEquals',
                        'values': ['Display Network Stats']
                    }],
        'timeRange': {}
    }

  def test_no_advertiser(self):
    builder = request_builder.SA360ReportRequestBuilder(AGENCY_ID)
    start_date = datetime.date(YEAR, MONTH, DAY)
    end_date = datetime.date(YEAR, MONTH + 1, DAY)
    body = builder.build(start_date, end_date)
    self.expected_body['timeRange']['startDate'] = DATE_FORMAT % (YEAR, MONTH,
                                                                  DAY)
    self.expected_body['timeRange']['endDate'] = DATE_FORMAT % (YEAR, MONTH + 1,
                                                                DAY)
    self.assertEqual(body, self.expected_body)

  def test_advertiser(self):
    builder = request_builder.SA360ReportRequestBuilder(AGENCY_ID,
                                                        [ADVERTISER_ID])
    start_date = datetime.date(YEAR, MONTH, DAY)
    end_date = datetime.date(YEAR, MONTH + 1, DAY)
    body = builder.build(start_date, end_date)
    self.expected_body['timeRange']['startDate'] = DATE_FORMAT % (YEAR, MONTH,
                                                                  DAY)
    self.expected_body['timeRange']['endDate'] = DATE_FORMAT % (YEAR, MONTH + 1,
                                                                DAY)
    self.expected_body['reportScope']['advertiserId'] = ADVERTISER_ID
    self.assertEqual(body, self.expected_body)

  def test_advertiser_list(self):
    advertiser_id_list = ['adv1', 'adv2', 'adv3']
    builder = request_builder.SA360ReportRequestBuilder(AGENCY_ID,
                                                        advertiser_id_list)
    start_date = datetime.date(YEAR, MONTH, DAY)
    end_date = datetime.date(YEAR, MONTH + 1, DAY)
    body = builder.build(start_date, end_date)
    self.expected_body['timeRange']['startDate'] = DATE_FORMAT % (YEAR, MONTH,
                                                                  DAY)
    self.expected_body['timeRange']['endDate'] = DATE_FORMAT % (YEAR, MONTH + 1,
                                                                DAY)
    self.expected_body['filters'].insert(
        1, {
            'column': {
                'columnName': 'advertiserId'
            },
            'operator': 'in',
            'values': advertiser_id_list
        })
    self.assertEqual(body, self.expected_body)

  def test_not_valid_start_date(self):
    builder = request_builder.SA360ReportRequestBuilder(AGENCY_ID)
    start_date = 'not_valid'
    end_date = datetime.date(YEAR, MONTH + 1, DAY)
    with self.assertRaises(AttributeError):
      builder.build(start_date, end_date)

  def test_not_valid_end_date(self):
    builder = request_builder.SA360ReportRequestBuilder(AGENCY_ID)
    start_date = datetime.date(YEAR, MONTH, DAY)
    end_date = 'not_valid'
    with self.assertRaises(AttributeError):
      builder.build(start_date, end_date)

  def test_custom_columns(self):
    custom_columns = [{'columnName': 'columnA'}, {'columnName': 'columnB'}]
    builder = request_builder.SA360ReportRequestBuilder(
        AGENCY_ID, columns=custom_columns)
    start_date = datetime.date(YEAR, MONTH, DAY)
    end_date = datetime.date(YEAR, MONTH + 1, DAY)
    body = builder.build(start_date, end_date)
    self.expected_body['timeRange']['startDate'] = DATE_FORMAT % (YEAR, MONTH,
                                                                  DAY)
    self.expected_body['timeRange']['endDate'] = DATE_FORMAT % (YEAR, MONTH + 1,
                                                                DAY)
    self.expected_body['columns'] = custom_columns
    self.assertEqual(body, self.expected_body)

  def test_get_headers_api_names_custom_columns(self):
    custom_columns = [{'columnName': 'columnA'}, {'columnName': 'columnB'}]
    builder = request_builder.SA360ReportRequestBuilder(
        AGENCY_ID, columns=custom_columns)
    headers = builder.get_headers(ui_names=False)
    self.assertEqual(headers, ['columnA', 'columnB'])

  def test_get_headers(self):
    builder = request_builder.SA360ReportRequestBuilder(AGENCY_ID)
    self.assertEqual(builder.get_headers(), [
        'Advertiser',
        'Account',
        'Campaign',
        'Keyword',
        'Match type',
        'Keyword max CPC',
        'Clicks',
        'Keyword ID',
        'Advertiser ID',
    ])

  def test_get_headers_api_names(self):
    builder = request_builder.SA360ReportRequestBuilder(AGENCY_ID)
    self.assertEqual(
        builder.get_headers(ui_names=False), [
            'advertiser', 'account', 'campaign', 'keywordText',
            'keywordMatchType', 'keywordMaxCpc', 'clicks', 'keywordId',
            'advertiserId'
        ])


if __name__ == '__main__':
  googletest.main()
