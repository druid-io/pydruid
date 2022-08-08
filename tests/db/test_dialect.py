# -*- coding: utf-8 -*-

import unittest
from unittest.mock import patch

from pydruid.db.sqlalchemy import DruidDialect


class DruidDialectTestSuite(unittest.TestCase):
    dialect = DruidDialect()

    @patch("pydruid.db.api.Connection")
    def test_do_ping_success(self, connection_mock):
        connection_mock.execute.return_value = [1]

        result = self.dialect.do_ping(connection_mock)

        # asserts the ping executes with a raw string
        connection_mock.execute.assert_called_once_with("SELECT 1")
        self.assertTrue(result)

    @patch("pydruid.db.api.Connection")
    def test_do_ping_with_exception(self, connection_mock):
        connection_mock.execute.side_effect = Exception("Something's wrong :(")

        result = self.dialect.do_ping(connection_mock)

        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
