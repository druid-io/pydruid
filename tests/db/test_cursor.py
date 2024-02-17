# -*- coding: utf-8 -*-

import unittest
from collections import namedtuple
from io import BytesIO
from unittest.mock import patch

from requests.models import Response

from pydruid.db.api import (
    apply_dynamic_parameters,
    apply_parameters,
    Cursor,
    dynamic_parameter,
    dynamic_placeholder,
)
from pydruid.db.exceptions import OperationalError


class CursorTestSuite(unittest.TestCase):
    @patch("requests.post")
    def test_execute(self, requests_post_mock):
        response = Response()
        response.status_code = 200
        response.raw = BytesIO(
            b'[{"name": "alice"}, {"name": "bob"}, {"name": "charlie"}]'
        )
        requests_post_mock.return_value = response
        Row = namedtuple("Row", ["name"])

        cursor = Cursor("http://example.com/")
        cursor.execute("SELECT * FROM table")
        result = cursor.fetchall()
        expected = [Row(name="alice"), Row(name="bob"), Row(name="charlie")]
        self.assertEqual(result, expected)

    @patch("requests.post")
    def test_execute_empty_result(self, requests_post_mock):
        response = Response()
        response.status_code = 200
        response.raw = BytesIO(b"[]")
        requests_post_mock.return_value = response

        cursor = Cursor("http://example.com/")
        cursor.execute("SELECT * FROM table")
        result = cursor.fetchall()
        expected = []
        self.assertEqual(result, expected)

    @patch("requests.post")
    def test_context(self, requests_post_mock):
        response = Response()
        response.status_code = 200
        response.raw = BytesIO(b"[]")
        requests_post_mock.return_value = response

        url = "http://example.com/"
        query = "SELECT * FROM table"
        context = {"source": "unittest"}

        cursor = Cursor(url, user=None, password=None, context=context)
        cursor.execute(query)

        requests_post_mock.assert_called_with(
            "http://example.com/",
            auth=None,
            stream=True,
            headers={"Content-Type": "application/json"},
            json={"query": query, "context": context, "header": False},
            verify=True,
            cert=None,
            proxies=None,
        )

    @patch("requests.post")
    def test_header_false(self, requests_post_mock):
        response = Response()
        response.status_code = 200
        response.raw = BytesIO(b'[{"name": "alice"}]')
        requests_post_mock.return_value = response
        Row = namedtuple("Row", ["name"])

        url = "http://example.com/"
        query = "SELECT * FROM table"

        cursor = Cursor(url, header=False)
        cursor.execute(query)
        result = cursor.fetchall()
        self.assertEqual(result, [Row(name="alice")])

        self.assertEqual(
            cursor.description, [("name", 1, None, None, None, None, True)]
        )

    @patch("requests.post")
    def test_header_true(self, requests_post_mock):
        response = Response()
        response.status_code = 200
        response.raw = BytesIO(b'[{"name": null}, {"name": "alice"}]')
        requests_post_mock.return_value = response
        Row = namedtuple("Row", ["name"])

        url = "http://example.com/"
        query = "SELECT * FROM table"

        cursor = Cursor(url, header=True)
        cursor.execute(query)
        result = cursor.fetchall()
        self.assertEqual(result, [Row(name="alice")])
        self.assertEqual(cursor.description, [("name", None)])

    @patch("requests.post")
    def test_names_with_underscores(self, requests_post_mock):
        response = Response()
        response.status_code = 200
        response.raw = BytesIO(b'[{"_name": null}, {"_name": "alice"}]')
        requests_post_mock.return_value = response
        Row = namedtuple("Row", ["_name"], rename=True)

        url = "http://example.com/"
        query = "SELECT * FROM table"

        cursor = Cursor(url, header=True)
        cursor.execute(query)
        result = cursor.fetchall()
        self.assertEqual(result, [Row(_0="alice")])
        self.assertEqual(cursor.description, [("_name", None)])

    def test_apply_parameters(self):
        self.assertEqual(
            apply_parameters('SELECT 100 AS "100%"', None), 'SELECT 100 AS "100%"'
        )

        self.assertEqual(
            apply_parameters('SELECT 100 AS "100%"', {}), 'SELECT 100 AS "100%"'
        )

        self.assertEqual(
            apply_parameters('SELECT %(key)s AS "100%%"', {"key": 100}),
            'SELECT 100 AS "100%"',
        )

        self.assertEqual(apply_parameters("SELECT %(key)s", {"key": "*"}), "SELECT *")

        self.assertEqual(
            apply_parameters("SELECT %(key)s", {"key": "bar"}), "SELECT 'bar'"
        )

        self.assertEqual(
            apply_parameters("SELECT %(key)s", {"key": True}), "SELECT TRUE"
        )

        self.assertEqual(
            apply_parameters("SELECT %(key)s", {"key": False}), "SELECT FALSE"
        )

    @patch("requests.post")
    def test_execute_with_dynamic_parameters(self, requests_post_mock):
        response = Response()
        response.status_code = 200
        response.raw = BytesIO(b"[]")
        requests_post_mock.return_value = response

        url = "http://example.com/"
        parameters = {"name": "Druid"}
        query = "SELECT * FROM table where name = %(name)s"

        cursor = Cursor(url, user=None, password=None, dynamic_parameters=True)
        cursor.execute(query, parameters)

        assert_query = "SELECT * FROM table where name = ?"
        assert_params = [{"value": "Druid", "type": "VARCHAR"}]

        requests_post_mock.assert_called_with(
            "http://example.com/",
            auth=None,
            stream=True,
            headers={"Content-Type": "application/json"},
            json={
                "query": assert_query,
                "parameters": assert_params,
                "context": {},
                "header": False,
            },
            verify=True,
            cert=None,
            proxies=None,
        )

    def test_apply_dynamic_parameters(self):

        parameters = {
            "start_dt": "2015-09-12 00:00:00",
            "end_dt": "2015-09-13 00:00:00",
            "channels": ("#en.wikipedia", "#es.wikipedia"),
            "pages": ["Apache Druid", "Apache%"],
            "added_gt": 10,
        }

        operation = """
            SELECT
                channel,
                page,
                SUM(added)
            FROM wikipedia
            WHERE
                __time BETWEEN TIMESTAMP %(start_dt)s AND TIMESTAMP %(end_dt)s
            AND
                channel IN (%(channels)s)
            AND
                page IN (%(pages)s)
            GROUP BY channel, page
            ORDER BY SUM(added) DESC
            HAVING SUM(added) >= %(added_gt)s
        """

        r_op, r_params = apply_dynamic_parameters(operation, parameters)

        assert_op = """
            SELECT
                channel,
                page,
                SUM(added)
            FROM wikipedia
            WHERE
                __time BETWEEN TIMESTAMP ? AND TIMESTAMP ?
            AND
                channel IN (?, ?)
            AND
                page IN (?, ?)
            GROUP BY channel, page
            ORDER BY SUM(added) DESC
            HAVING SUM(added) >= ?
        """

        self.assertEqual(r_op, assert_op)

        self.assertEqual(
            (r_params[0]["value"], r_params[0]["type"]),
            (parameters["start_dt"], "VARCHAR"),
        )

        self.assertEqual(
            (r_params[1]["value"], r_params[1]["type"]),
            (parameters["end_dt"], "VARCHAR"),
        )

        self.assertEqual(
            (r_params[2]["value"], r_params[2]["type"]),
            (parameters["channels"][0], "VARCHAR"),
        )

        self.assertEqual(
            (r_params[3]["value"], r_params[3]["type"]),
            (parameters["channels"][1], "VARCHAR"),
        )

        self.assertEqual(
            (r_params[4]["value"], r_params[4]["type"]),
            (parameters["pages"][0], "VARCHAR"),
        )
        self.assertEqual(
            (r_params[5]["value"], r_params[5]["type"]),
            (parameters["pages"][1], "VARCHAR"),
        )

        self.assertEqual(
            (r_params[6]["value"], r_params[6]["type"]),
            (parameters["added_gt"], "INTEGER"),
        )

    def test_apply_dynamic_parameters_out_of_order(self):

        parameters = {
            "channels": ("#en.wikipedia", "#es.wikipedia"),
            "added_gt": 10,
            "start_dt": "2015-09-12 00:00:00",
        }

        operation = """
            SELECT
                channel,
                page,
                SUM(added)
            FROM wikipedia
            WHERE
                __time >= TIMESTAMP %(start_dt)s
            AND
                channel IN (%(channels)s)
            GROUP BY channel, page
            ORDER BY SUM(added) DESC
            HAVING SUM(added) >= %(added_gt)s
        """

        r_op, r_params = apply_dynamic_parameters(operation, parameters)

        assert_op = """
            SELECT
                channel,
                page,
                SUM(added)
            FROM wikipedia
            WHERE
                __time >= TIMESTAMP ?
            AND
                channel IN (?, ?)
            GROUP BY channel, page
            ORDER BY SUM(added) DESC
            HAVING SUM(added) >= ?
        """

        self.assertEqual(r_op, assert_op)

        self.assertEqual(
            (r_params[0]["value"], r_params[0]["type"]),
            (parameters["start_dt"], "VARCHAR"),
        )

        self.assertEqual(
            (r_params[1]["value"], r_params[1]["type"]),
            (parameters["channels"][0], "VARCHAR"),
        )

        self.assertEqual(
            (r_params[2]["value"], r_params[2]["type"]),
            (parameters["channels"][1], "VARCHAR"),
        )

        self.assertEqual(
            (r_params[3]["value"], r_params[3]["type"]),
            (parameters["added_gt"], "INTEGER"),
        )

    def test_apply_dynamic_parameters_no_params(self):
        self.assertEqual(
            apply_dynamic_parameters('SELECT 100 AS "100%"', None),
            ('SELECT 100 AS "100%"', None),
        )

    def test_apply_dynamic_parameters_no_match_error(self):

        with self.assertRaises(OperationalError) as cm:
            apply_dynamic_parameters("SELECT %(name)s", {"age": 15})

        self.assertEqual("Parameters and placeholders do not match", str(cm.exception))

    def test_dynamic_parameter(self):
        self.assertEqual(dynamic_parameter("str"), {"value": "str", "type": "VARCHAR"})

        self.assertEqual(dynamic_parameter(5), {"value": 5, "type": "INTEGER"})

        self.assertEqual(dynamic_parameter(5.5), {"value": 5.5, "type": "FLOAT"})

        self.assertEqual(dynamic_parameter(True), {"value": True, "type": "BOOLEAN"})

    def test_dynamic_placeholder(self):
        self.assertEqual(dynamic_placeholder(("a", "b")), "?, ?")

        self.assertEqual(dynamic_placeholder(["a", "b"]), "?, ?")

        self.assertEqual(dynamic_placeholder("a"), "?")


if __name__ == "__main__":
    unittest.main()
