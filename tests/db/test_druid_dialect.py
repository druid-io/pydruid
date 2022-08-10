# -*- coding: utf-8 -*-

import unittest
import warnings
from unittest.mock import patch

from sqlalchemy import exc, types

from pydruid.db.sqlalchemy import DruidDialect


def anonymous_object(**kwargs):
    return type("Object", (), kwargs)


class DruidDialectTestSuite(unittest.TestCase):
    dialect = DruidDialect()

    @patch("pydruid.db.api.Connection")
    def test_get_columns_type_mappings(self, connection_mock):
        # fmt: off
        connection_mock.execute.return_value = [
            anonymous_object(COLUMN_NAME="BigInteger1", JDBC_TYPE=-6, IS_NULLABLE="YES", COLUMN_DEFAULT=""),
            anonymous_object(COLUMN_NAME="BigInteger2", JDBC_TYPE=-5, IS_NULLABLE="YES", COLUMN_DEFAULT=""),
            anonymous_object(COLUMN_NAME="BigInteger3", JDBC_TYPE=4, IS_NULLABLE="YES", COLUMN_DEFAULT=""),
            anonymous_object(COLUMN_NAME="BigInteger4", JDBC_TYPE=5, IS_NULLABLE="YES", COLUMN_DEFAULT=""),
            anonymous_object(COLUMN_NAME="String1", JDBC_TYPE=1, IS_NULLABLE="NO", COLUMN_DEFAULT="default_string"),
            anonymous_object(COLUMN_NAME="String2", JDBC_TYPE=12, IS_NULLABLE="YES", COLUMN_DEFAULT=""),
            anonymous_object(COLUMN_NAME="Float1", JDBC_TYPE=3, IS_NULLABLE="NO", COLUMN_DEFAULT=1.23),
            anonymous_object(COLUMN_NAME="Float2", JDBC_TYPE=6, IS_NULLABLE="YES", COLUMN_DEFAULT=""),
            anonymous_object(COLUMN_NAME="Float3", JDBC_TYPE=7, IS_NULLABLE="YES", COLUMN_DEFAULT=""),
            anonymous_object(COLUMN_NAME="Float4", JDBC_TYPE=8, IS_NULLABLE="YES", COLUMN_DEFAULT=""),
            anonymous_object(COLUMN_NAME="Boolean", JDBC_TYPE=16, IS_NULLABLE="YES", COLUMN_DEFAULT=""),
            anonymous_object(COLUMN_NAME="DATE", JDBC_TYPE=91, IS_NULLABLE="YES", COLUMN_DEFAULT=""),
            anonymous_object(COLUMN_NAME="TIMESTAMP", JDBC_TYPE=93, IS_NULLABLE="YES", COLUMN_DEFAULT=""),
            anonymous_object(COLUMN_NAME="BLOB", JDBC_TYPE=1111, IS_NULLABLE="YES", COLUMN_DEFAULT=""),
        ]
        # fmt: on
        result = self.dialect.get_columns(connection_mock, "table_name")

        # fmt: off
        expected = [
            {"name": "BigInteger1", "type": types.BigInteger, "nullable": True, "default": None},
            {"name": "BigInteger2", "type": types.BigInteger, "nullable": True, "default": None},
            {"name": "BigInteger3", "type": types.BigInteger, "nullable": True, "default": None},
            {"name": "BigInteger4", "type": types.BigInteger, "nullable": True, "default": None},
            {"name": "String1", "type": types.String, "nullable": False, "default": "default_string"},
            {"name": "String2", "type": types.String, "nullable": True, "default": None},
            {"name": "Float1", "type": types.Float, "nullable": False, "default": "1.23"},
            {"name": "Float2", "type": types.Float, "nullable": True, "default": None},
            {"name": "Float3", "type": types.Float, "nullable": True, "default": None},
            {"name": "Float4", "type": types.Float, "nullable": True, "default": None},
            {"name": "Boolean", "type": types.Boolean, "nullable": True, "default": None},
            {"name": "DATE", "type": types.DATE, "nullable": True, "default": None},
            {"name": "TIMESTAMP", "type": types.TIMESTAMP, "nullable": True, "default": None},
            {"name": "BLOB", "type": types.BLOB, "nullable": True, "default": None},
        ]
        # fmt: on

        self.assertListEqual(expected, result)

    @patch("pydruid.db.api.Connection")
    def test_get_columns_type_mappings_with_unknown_type(self, connection_mock):
        connection_mock.execute.return_value = [
            anonymous_object(
                COLUMN_NAME="UnknownType",
                JDBC_TYPE=-42,
                IS_NULLABLE="YES",
                COLUMN_DEFAULT="",
            ),
        ]

        with warnings.catch_warnings():
            # avoid any noise due to our expected warn logs
            warnings.simplefilter("ignore", category=exc.SAWarning)
            result = self.dialect.get_columns(connection_mock, "table_name")

        expected = {
            "name": "UnknownType",
            "type": types.NullType,
            "nullable": True,
            "default": None,
        }
        self.assertEqual(1, len(result))
        self.assertEqual(expected, result[0])

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
