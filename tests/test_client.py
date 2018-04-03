# -*- coding: UTF-8 -*-
import textwrap

import pytest
from mock import patch, Mock
from six.moves import urllib

from pydruid.client import PyDruid
from pydruid.query import Query
from pydruid.utils.aggregators import doublesum
from pydruid.utils.filters import Dimension


def create_client():
    return PyDruid("http://localhost:8083", "druid/v2/")


def create_blank_query():
    return Query({}, 'none')


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
    def test_druid_returns_html_error(self, mock_urlopen):
        # given
        message = textwrap.dedent("""
            <html>
            <head>
            <meta http-equiv="Content-Type" content="text/html;charset=ISO-8859-1"/>
            <title>Error 500 </title>
            </head>
            <body>
            <h2>HTTP ERROR: 500</h2>
            <p>Problem accessing /druid/v2/. Reason:
            <pre>    javax.servlet.ServletException: java.lang.OutOfMemoryError: GC overhead limit exceeded</pre></p>
            <hr /><a href="http://eclipse.org/jetty">Powered by Jetty:// 9.3.19.v20170502</a><hr/>
            </body>
            </html>
        """).strip()
        ex = urllib.error.HTTPError(None, 500, message, None, None)
        mock_urlopen.side_effect = ex
        client = create_client()

        # when / then
        with pytest.raises(IOError) as e:
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

        assert str(e.value) == textwrap.dedent("""
            HTTP Error 500: <html>
            <head>
            <meta http-equiv="Content-Type" content="text/html;charset=ISO-8859-1"/>
            <title>Error 500 </title>
            </head>
            <body>
            <h2>HTTP ERROR: 500</h2>
            <p>Problem accessing /druid/v2/. Reason:
            <pre>    javax.servlet.ServletException: java.lang.OutOfMemoryError: GC overhead limit exceeded</pre></p>
            <hr /><a href="http://eclipse.org/jetty">Powered by Jetty:// 9.3.19.v20170502</a><hr/>
            </body>
            </html> 
             Druid Error: javax.servlet.ServletException: java.lang.OutOfMemoryError: GC overhead limit exceeded 
             Query is: {
                "aggregations": [
                    {
                        "fieldName": "count",
                        "name": "count",
                        "type": "doubleSum"
                    }
                ],
                "context": {
                    "timeout": 1000
                },
                "dataSource": "testdatasource",
                "dimension": "user_name",
                "filter": {
                    "dimension": "user_lang",
                    "type": "selector",
                    "value": "en"
                },
                "granularity": "all",
                "intervals": "2015-12-29/pt1h",
                "metric": "count",
                "queryType": "topN",
                "threshold": 1
            }
        """).strip()

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

    @patch('pydruid.client.urllib.request.urlopen')
    def test_client_auth_creds(self, mock_urlopen):
        client = create_client()
        query = create_blank_query()
        client.set_basic_auth_credentials('myUsername', 'myPassword')
        headers, _, _ = client._prepare_url_headers_and_body(query)
        assert headers['Authorization'] == "Basic bXlVc2VybmFtZTpteVBhc3N3b3Jk"
