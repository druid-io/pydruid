# -*- coding: utf-8 -*-

import pytest
import unittest

from pydruid.db.api import Connection, Cursor
from pydruid.db.exceptions import Error

# Generated by CodiumAI
class TestConnection:

    # Returns a new Cursor object using the connection.

    def test_returns_new_cursor_object(self):
        conn = Connection(
            host="localhost", port=8082, user="admin", password="password"
        )
        cursor = conn.cursor()
        assert isinstance(cursor, Cursor)

    # Raises an exception if the connection is closed.
    def test_raises_exception_if_connection_closed(self):
        conn = Connection(
            host="localhost", port=8082, user="admin", password="password"
        )
        conn.close()
        with pytest.raises(Error):
            conn.cursor()

    # Appends the new cursor to the list of cursors.
    def test_appends_new_cursor(self):
        conn = Connection(
            host="localhost", port=8082, user="admin", password="password"
        )
        cursor = conn.cursor()
        assert cursor in conn.cursors

    # Returns the new cursor.
    def test_returns_new_cursor(self):
        conn = Connection(
            host="localhost", port=8082, user="admin", password="password"
        )
        cursor = conn.cursor()
        assert isinstance(cursor, Cursor)

    # The new cursor has the same url, user, password, context, header, ssl_verify_cert, ssl_client_cert, proxies, and jwt as the connection.
    def test_new_cursor_has_same_attributes_as_connection(self):
        conn = Connection(
            host="localhost", port=8082, user="admin", password="password"
        )
        cursor = conn.cursor()
        assert cursor.url == conn.url
        assert cursor.user == conn.user
        assert cursor.password == conn.password
        assert cursor.context == conn.context
        assert cursor.header == conn.header
        assert cursor.ssl_verify_cert == conn.ssl_verify_cert
        assert cursor.ssl_client_cert == conn.ssl_client_cert
        assert cursor.proxies == conn.proxies
        assert cursor.jwt == conn.jwt


if __name__ == "__main__":
    unittest.main()
