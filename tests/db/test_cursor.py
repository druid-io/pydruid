# -*- coding: utf-8 -*-

import unittest
from collections import namedtuple
from io import BytesIO
from unittest.mock import ANY, patch

import requests
from requests.models import Response
from requests.auth import HTTPBasicAuth

from pydruid.db.api import BearerAuth, apply_parameters, Cursor, connect


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

    # Generated by CodiumAI
    # When `user` is not None, `HTTPBasicAuth` is used for authentication.
    @patch("requests.post")
    def test_user_not_none_http_basic_auth(self, mock_post):
        from unittest.mock import patch

        response = Response()
        response.raw = BytesIO(b"[]")
        response.status_code = 200
        mock_post.return_value = response

        user = "test_user"
        password = "test_password"
        url = "http://example.com/"
        query = "SELECT * FROM table"

        cursor = Cursor(url, user=user, password=password)
        cursor.execute(query)

        mock_post.assert_called_with(
            url,
            stream=True,
            headers={"Content-Type": "application/json"},
            json={"query": query, "context": cursor.context, "header": cursor.header,},
            auth=requests.auth.HTTPBasicAuth(user, password),
            verify=cursor.ssl_verify_cert,
            cert=cursor.ssl_client_cert,
            proxies=cursor.proxies,
        )

    # When `user` is None and `jwt` is not None, `auth` is not None.
    @patch("requests.post")
    def test_user_none_jwt_not_none_auth_not_none(self, mock_post):
        response = Response()
        response.raw = BytesIO(b"[]")
        response.status_code = 200
        mock_post.return_value = response

        jwt = "test_jwt"
        url = "http://example.com/"
        query = "SELECT * FROM table"

        cursor = Cursor(url, jwt=jwt)
        cursor.execute(query)

        mock_post.assert_called_with(
            url,
            stream=True,
            headers={"Content-Type": "application/json"},
            json={"query": query, "context": cursor.context, "header": cursor.header,},
            auth=ANY,
            verify=cursor.ssl_verify_cert,
            cert=cursor.ssl_client_cert,
            proxies=cursor.proxies,
        )

        last_call = mock_post.call_args
        auth_arg = last_call.kwargs["auth"]

        self.assertIsInstance(auth_arg, BearerAuth)
        self.assertEqual(auth_arg.token, jwt)

    # Test that no authentication is used when both `user` and `jwt` are None.
    @patch("requests.post")
    def test_no_authentication_used(self, requests_post_mock):
        response = Response()
        response.status_code = 200
        response.raw = BytesIO(b'{"result": "success"}')
        requests_post_mock.return_value = response

        conn = connect(user=None, jwt=None)
        curs = conn.cursor()

        # Perform some operation that requires authentication
        curs.execute("SELECT * FROM table")

        # Assert that no authentication was used
        requests_post_mock.assert_called_with(
            ANY,
            stream=True,
            headers=ANY,
            json=ANY,
            auth=None,
            verify=ANY,
            cert=ANY,
            proxies=ANY,
        )

    # The test verifies that when `user` is not None and `jwt` is not None, `HttpBasicAuth` is used for authentication.
    @patch("requests.post")
    def test_basic_auth_used_for_authentication_when_both_provided(
        self, requests_post_mock
    ):
        response = Response()
        response.status_code = 200
        response.raw = BytesIO(b'{"result": "success"}')
        requests_post_mock.return_value = response

        url = "http://example.com/"
        user = "test_user"
        password = "test_password"
        jwt = "test_jwt"

        cursor = Cursor(url, user=user, password=password, jwt=jwt)
        cursor.execute("SELECT * FROM table")

        requests_post_mock.assert_called_with(
            url,
            stream=True,
            headers={"Content-Type": "application/json"},
            json={"query": "SELECT * FROM table", "context": {}, "header": False},
            auth=ANY,
            verify=True,
            cert=None,
            proxies=None,
        )

        last_call = requests_post_mock.call_args
        auth_arg = last_call.kwargs["auth"]

        self.assertIsInstance(auth_arg, HTTPBasicAuth)
        self.assertEqual(auth_arg.username, user)
        self.assertEqual(auth_arg.password, password)

    # When `ssl_verify_cert` is False, SSL certificate is not verified.
    @patch("requests.post")
    def test_ssl_certificate_verification_disabled(self, requests_post_mock):
        response = Response()
        response.status_code = 200
        response.raw = BytesIO(b"[]")
        requests_post_mock.return_value = response
        user = "test_user"
        password = "test_password"

        url = "http://example.com/"
        query = "SELECT * FROM table"

        cursor = Cursor(
            url, user=user, password=password, header=True, ssl_verify_cert=False
        )
        cursor.execute(query)

        requests_post_mock.assert_called_with(
            url,
            stream=True,
            headers={"Content-Type": "application/json"},
            json={"query": "SELECT * FROM table", "context": {}, "header": True},
            auth=ANY,
            verify=False,
            cert=None,
            proxies=None,
        )

    # When `user` is not None and `password` is None, `HTTPBasicAuth` is used with empty password.
    @patch("requests.post")
    @patch("requests.auth.HTTPBasicAuth")
    def test_http_basic_auth_with_empty_user(
        self, http_basic_auth_mock, requests_post_mock
    ):
        response = Response()
        response.status_code = 200
        response.raw = BytesIO(b'[{"_name": null}, {"_name": "alice"}]')
        requests_post_mock.return_value = response

        url = "http://example.com/"
        user = "user"
        password = None
        jwt = None

        conn = connect(user=user, password=password, jwt=jwt)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM table")

        http_basic_auth_mock.assert_called_with(user, None)

        requests_post_mock.assert_called_with(
            ANY,
            stream=True,
            headers={"Content-Type": "application/json"},
            json={"query": "SELECT * FROM table", "context": {}, "header": False},
            auth=http_basic_auth_mock.return_value,
            verify=True,
            cert=None,
            proxies=None,
        )

    # Test SSL client certificate authentication when `ssl_client_cert` is not None.
    @patch("requests.post")
    def test_ssl_client_cert_authentication_with_patch_imported(
        self, requests_post_mock
    ):
        response = Response()
        response.status_code = 200
        response.raw = BytesIO(b'[]')
        requests_post_mock.return_value = response
        Row = namedtuple("Row", ["_name"], rename=True)

        url = "http://example.com/"
        query = "SELECT * FROM table"

        cursor = Cursor(url, header=True, ssl_client_cert="path/to/cert")
        cursor.execute(query)
        requests_post_mock.assert_called_with(
            ANY,
            stream=True,
            headers={"Content-Type": "application/json"},
            json={"query": "SELECT * FROM table", "context": {}, "header": False},
            auth=ANY,
            verify=True,
            cert="path/to/cert",
            proxies=None,
        )


if __name__ == "__main__":
    unittest.main()
