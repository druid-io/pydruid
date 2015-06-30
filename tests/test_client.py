# -*- coding: UTF-8 -*-

import os
import pytest
import pandas
from pandas.util.testing import assert_frame_equal
from six import PY3
from pydruid.client import PyDruid
from pydruid.utils.aggregators import *
from pydruid.utils.postaggregator import *
from pydruid.utils.filters import *
from pydruid.utils.having import *

def create_client():
    return PyDruid('http://localhost:8083', 'druid/v2/')

def create_client_with_results():
    client = create_client()
    client.query_type = 'timeseries'
    client.result = [
        {'result': {'value1': 1, 'value2': '㬓'}, 'timestamp': '2015-01-01T00:00:00.000-05:00'},
        {'result': {'value1': 2, 'value2': '㬓'}, 'timestamp': '2015-01-02T00:00:00.000-05:00'}
    ]
    return client

def line_ending():
    if PY3:
        return os.linesep
    return "\r\n"

class TestClient:
    def test_build_query(self):
        client = create_client()
        assert client.query_dict == None

        client.build_query({
            'datasource': 'things',
            'aggregations': {
                'count': count('thing'),
            },
            'post_aggregations': {
                'avg': Field('sum') / Field('count'),
            },
            'paging_spec': {
                'pagingIdentifies': {},
                'threshold': 1,
            },
            'filter': Dimension('one') == 1,
            'having': Aggregation('sum') > 1,
            'new_key': 'value',
        })
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
        assert client.query_dict == expected_query_dict

    def test_validate_query(self):
        client = create_client()
        client.validate_query(['validkey'], {'validkey': 'value'})
        pytest.raises(ValueError, client.validate_query, *[['validkey'], {'invalidkey': 'value'}])

    def test_export_tsv(self, tmpdir):
        client = create_client_with_results()
        file_path = tmpdir.join('out.tsv')
        client.export_tsv(str(file_path))
        assert file_path.read() == "value2\tvalue1\ttimestamp" + line_ending() + "㬓\t1\t2015-01-01T00:00:00.000-05:00" + line_ending() + "㬓\t2\t2015-01-02T00:00:00.000-05:00" + line_ending()

    def test_export_pandas(self):
        client = create_client_with_results()
        df = client.export_pandas()
        expected_df = pandas.DataFrame([{
            'timestamp': '2015-01-01T00:00:00.000-05:00',
            'value1': 1,
            'value2': '㬓',
        }, {
            'timestamp': '2015-01-02T00:00:00.000-05:00',
            'value1': 2,
            'value2': '㬓',
        }])
        assert_frame_equal(df, expected_df)

