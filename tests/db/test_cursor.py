# -*- coding: utf-8 -*-

from collections import namedtuple
from mock import patch
import unittest

from requests.models import Response
from six import BytesIO

from pydruid.db.api import Cursor


class CursorTestSuite(unittest.TestCase):

    @patch('requests.post')
    def test_execute(self, requests_post_mock):
        response = Response()
        response.status_code = 200
        response.raw = BytesIO(b'[{"name": "alice"}, {"name": "bob"}, {"name": "charlie"}]')
        requests_post_mock.return_value = response
        Row = namedtuple('Row', ['name'])

        cursor = Cursor('http://example.com/')
        cursor.execute('SELECT * FROM table')
        result = cursor.fetchall()
        expected = [
            Row(name='alice'),
            Row(name='bob'),
            Row(name='charlie'),
        ]
        self.assertEquals(result, expected)

    @patch('requests.post')
    def test_execute_empty_result(self, requests_post_mock):
        response = Response()
        response.status_code = 200
        response.raw = BytesIO(b'[]')
        requests_post_mock.return_value = response

        cursor = Cursor('http://example.com/')
        cursor.execute('SELECT * FROM table')
        result = cursor.fetchall()
        expected = []
        self.assertEquals(result, expected)


if __name__ == '__main__':
    unittest.main()
