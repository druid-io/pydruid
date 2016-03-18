# -*- coding: UTF-8 -*-
#
# Copyright 2016 Metamarkets Group Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import pytest

from pydruid.query import QueryBuilder, Query
import csv

try:
    import pandas
    from pandas.util.testing import assert_frame_equal
except ImportError:
    pandas = None

from six import PY3
from pydruid.utils import aggregators
from pydruid.utils import postaggregator
from pydruid.utils import filters
from pydruid.utils import having


def create_query_with_results():
    query = Query({}, 'timeseries')
    query.result = [
        {'result': {'value1': 1, 'value2': '㬓'}, 'timestamp': '2015-01-01T00:00:00.000-05:00'},
        {'result': {'value1': 2, 'value2': '㬓'}, 'timestamp': '2015-01-02T00:00:00.000-05:00'}
    ]
    return query


EXPECTED_RESULTS_PANDAS = [{
            'timestamp': '2015-01-01T00:00:00.000-05:00',
            'value1': 1,
            'value2': '㬓',
        }, {
            'timestamp': '2015-01-02T00:00:00.000-05:00',
            'value1': 2,
            'value2': '㬓',
        }]


def expected_results_csv_reader():
    # csv.DictReader does not perform promotion to int64
    expected_results = []
    for element in EXPECTED_RESULTS_PANDAS:
        modified_elem = element.copy()
        modified_elem.update({'value1': str(modified_elem['value1'])})
        expected_results.append(modified_elem)
    return expected_results


class TestQueryBuilder:
    def test_build_query(self):
        # given
        expected_query_dict = {
            'queryType': None,
            'dataSource': 'things',
            'aggregations': [{'fieldName': 'thing', 'name': 'count', 'type': 'count'}],
            'postAggregations': [{
                'fields': [{
                    'fieldName': 'sum', 'type': 'fieldAccess',
                }, {
                    'fieldName': 'count', 'type': 'fieldAccess',
                }],
                'fn': '/',
                'name': 'avg',
                'type': 'arithmetic',
            }],
            'pagingSpec': {'pagingIdentifies': {}, 'threshold': 1},
            'filter': {'dimension': 'one', 'type': 'selector', 'value': 1},
            'having': {'aggregation': 'sum', 'type': 'greaterThan', 'value': 1},
            'new_key': 'value',
        }

        builder = QueryBuilder()

        # when
        query = builder.build_query(None, {
            'datasource': 'things',
            'aggregations': {
                'count': aggregators.count('thing'),
            },
            'post_aggregations': {
                'avg': (postaggregator.Field('sum') /
                        postaggregator.Field('count')),
            },
            'paging_spec': {
                'pagingIdentifies': {},
                'threshold': 1,
            },
            'filter': filters.Dimension('one') == 1,
            'having': having.Aggregation('sum') > 1,
            'new_key': 'value',
        })

        # then
        assert query.query_dict == expected_query_dict

    def test_validate_query(self):
        # given
        builder = QueryBuilder()

        # when
        builder.validate_query(None, ['validkey'], {'validkey': 'value'})

        # then
        pytest.raises(ValueError, builder.validate_query, *[None, ['validkey'], {'invalidkey': 'value'}])


class TestQuery:
    def test_export_tsv(self, tmpdir):
        query = create_query_with_results()
        file_path = tmpdir.join('out.tsv')
        query.export_tsv(str(file_path))

        with open(str(file_path)) as tsv_file:
            reader = csv.DictReader(tsv_file, delimiter="\t")
            actual = [line for line in reader]
            assert actual == expected_results_csv_reader()

    @pytest.mark.skipif(pandas is None, reason="requires pandas")
    def test_export_pandas(self):
        query = create_query_with_results()
        df = query.export_pandas()
        expected_df = pandas.DataFrame(EXPECTED_RESULTS_PANDAS)
        assert_frame_equal(df, expected_df)

    def test_query_acts_as_a_wrapper_for_raw_result(self):
        # given
        query = create_query_with_results()

        # then
        assert len(query) == 2
        assert isinstance(query[0], dict)
        assert isinstance(query[1], dict)
