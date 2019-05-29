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

    @patch('requests.post')
    def test_context(self, requests_post_mock):
        response = Response()
        response.status_code = 200
        response.raw = BytesIO(b'[]')
        requests_post_mock.return_value = response

        url = 'http://example.com/'
        query = 'SELECT * FROM table'
        context = {'source': 'unittest'}

        cursor = Cursor(url, user=None, password=None, context=context)
        cursor.execute(query)

        requests_post_mock.assert_called_with(
            'http://example.com/',
            auth=None,
            stream=True,
            headers={'Content-Type': 'application/json'},
            json={'query': query, 'context': context, 'header': False},
        )

    @patch('requests.post')
    def test_header_false(self, requests_post_mock):
        response = Response()
        response.status_code = 200
        response.raw = BytesIO(b'[{"name": "alice"}]')
        requests_post_mock.return_value = response
        Row = namedtuple('Row', ['name'])

        url = 'http://example.com/'
        query = 'SELECT * FROM table'

        cursor = Cursor(url, header=False)
        cursor.execute(query)
        result = cursor.fetchall()
        self.assertEquals(result, [Row(name='alice')])

        self.assertEquals(
            cursor.description,
            [('name', 1, None, None, None, None, True)],
        )

    @patch('requests.post')
    def test_header_true(self, requests_post_mock):
        response = Response()
        response.status_code = 200
        response.raw = BytesIO(b'[{"name": null}, {"name": "alice"}]')
        requests_post_mock.return_value = response
        Row = namedtuple('Row', ['name'])

        url = 'http://example.com/'
        query = 'SELECT * FROM table'

        cursor = Cursor(url, header=True)
        cursor.execute(query)
        result = cursor.fetchall()
        self.assertEquals(result, [Row(name='alice')])
        self.assertEquals(cursor.description, [('name', None)])


if __name__ == '__main__':
    unittest.main()
