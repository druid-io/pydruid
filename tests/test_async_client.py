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

import pytest

from mock import Mock
from pydruid.utils.aggregators import doublesum
from pydruid.utils.filters import Dimension

try:
    import tornado
    import tornado.ioloop
    import tornado.web
    from tornado.testing import AsyncHTTPTestCase
    from tornado.httpclient import HTTPError
    from pydruid.async_client import AsyncPyDruid
except ImportError:
    tornado = Mock()
    AsyncHTTPTestCase = object


class FailureHandler(tornado.web.RequestHandler):
    def post(self):
        raise HTTPError(500, "Druid error", response="Druid error")


class SuccessHandler(tornado.web.RequestHandler):
    def post(self):
        self.write("""
            [ {
  "timestamp" : "2015-12-30T14:14:49.000Z",
  "result" : [ {
    "dimension" : "aaaa",
    "metric" : 100
  } ]
            } ]
            """)


class TestAsyncPyDruid(AsyncHTTPTestCase):
    def get_app(self):
        return tornado.web.Application([
            (r"/druid/v2/fail_request", FailureHandler),
            (r"/druid/v2/return_results", SuccessHandler)
        ])

    @tornado.testing.gen_test
    def test_druid_returns_error(self):
        # given
        client = AsyncPyDruid("http://localhost:%s" % (self.get_http_port(), ),
                              "druid/v2/fail_request")

        # when / then
        with pytest.raises(IOError):
            yield client.topn(
                    datasource="testdatasource",
                    granularity="all",
                    intervals="2015-12-29/pt1h",
                    aggregations={"count": doublesum("count")},
                    dimension="user_name",
                    metric="count",
                    filter=Dimension("user_lang") == "en",
                    threshold=1,
                    context={"timeout": 1000})

    @tornado.testing.gen_test
    def test_druid_returns_results(self):
        # given
        client = AsyncPyDruid("http://localhost:%s" % (self.get_http_port(), ),
                              "druid/v2/return_results")

        # when
        top = yield client.topn(
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
        self.assertIsNotNone(top)
        self.assertEqual(len(top.result), 1)
        self.assertEqual(len(top.result[0]['result']), 1)

    @tornado.testing.gen_test
    def test_client_allows_to_export_last_query(self):
        # given
        client = AsyncPyDruid("http://localhost:%s" % (self.get_http_port(), ),
                              "druid/v2/return_results")
        yield client.topn(
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
