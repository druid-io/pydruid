# -*- coding: UTF-8 -*-
import pytest
from mock import patch, Mock
from six.moves import urllib

from pydruid.client import PyDruid
from pydruid.utils.aggregators import doublesum
from pydruid.utils.filters import Dimension


def create_client():
    return PyDruid("http://localhost:8083", "druid/v2/")


class TestPyDruid:
    @patch('pydruid.client.urllib.request.urlopen')
    def test_druid_returns_error(self, mock_urlopen):
        # given
        ex = urllib.error.HTTPError(None, 500, "Druid error", None, None)
        mock_urlopen.side_effect = ex
        client = create_client()

        # when / then
        with pytest.raises(IOError):
            client.topn(
                    datasource="testdatasource",
                    granularity="all",
                    intervals="2015-12-29/pt1h",
                    aggregations={"count": doublesum("count")},
                    dimension="user_name",
                    metric="count",
                    filter=Dimension("user_lang") == "en",
                    threshold=1,
                    context={"timeout": 1000})

    @patch('pydruid.client.urllib.request.urlopen')
    def test_druid_returns_results(self, mock_urlopen):
        # given
        response = Mock()
        response.read.return_value = """
            [ {
  "timestamp" : "2015-12-30T14:14:49.000Z",
  "result" : [ {
    "dimension" : "aaaa",
    "metric" : 100
  } ]
            } ]
        """.encode("utf-8")
        mock_urlopen.return_value = response
        client = create_client()

        # when
        top = client.topn(
                datasource="testdatasource",
                granularity="all",
                intervals="2015-12-29/pt1h",
                aggregations={"count": doublesum("count")},
                dimension="user_name",
                metric="count",
                filter=Dimension("user_lang") == "en",
                threshold=1,
                context={"timeout": 1000})

        # then
        assert top is not None
        assert len(top.result) == 1
        assert len(top.result[0]['result']) == 1

    @patch('pydruid.client.urllib.request.urlopen')
    def test_client_allows_to_export_last_query(self, mock_urlopen):
        # given
        response = Mock()
        response.read.return_value = """
            [ {
  "timestamp" : "2015-12-30T14:14:49.000Z",
  "result" : [ {
    "dimension" : "aaaa",
    "metric" : 100
  } ]
            } ]
        """.encode("utf-8")
        mock_urlopen.return_value = response
        client = create_client()
        client.topn(
                datasource="testdatasource",
                granularity="all",
                intervals="2015-12-29/pt1h",
                aggregations={"count": doublesum("count")},
                dimension="user_name",
                metric="count",
                filter=Dimension("user_lang") == "en",
                threshold=1,
                context={"timeout": 1000})

        # when / then
        # assert that last_query.export_tsv method was called (it should throw an exception, given empty path)
        with pytest.raises(TypeError):
            client.export_tsv(None)
