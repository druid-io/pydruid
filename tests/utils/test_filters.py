# -*- coding: UTF-8 -*-

import pytest

from pydruid.utils import filters


class TestDimension:

    def test_dimension(self):
        d = filters.Dimension('dim')
        actual = filters.Filter.build_filter(d == 'val')
        expected = {'type': 'selector', 'dimension': 'dim', 'value': 'val'}
        assert actual == expected


class TestFilter:

    def test_selector_filter(self):
        actual = filters.Filter.build_filter(
            filters.Filter(dimension='dim', value='val'))
        expected = {'type': 'selector', 'dimension': 'dim', 'value': 'val'}
        assert actual == expected

    def test_javascript_filter(self):
        actual = filters.Filter.build_filter(
            filters.Filter(type='javascript', dimension='dim', function='function(x){return true}'))
        expected = {'type': 'javascript', 'dimension': 'dim', 'function': 'function(x){return true}'}
        assert actual == expected

    def test_and_filter(self):
        f1 = filters.Filter(dimension='dim1', value='val1')
        f2 = filters.Filter(dimension='dim2', value='val2')
        f3 = filters.Filter(dimension='dim3', value='val3')
        actual = filters.Filter.build_filter(f1 & f2 & f3)
        expected = {
            'type': 'and',
            'fields': [
                {'type': 'selector', 'dimension': 'dim1', 'value': 'val1'},
                {'type': 'selector', 'dimension': 'dim2', 'value': 'val2'},
                {'type': 'selector', 'dimension': 'dim3', 'value': 'val3'}
            ]
        }
        assert actual == expected

    def test_or_filter(self):
        f1 = filters.Filter(dimension='dim1', value='val1')
        f2 = filters.Filter(dimension='dim2', value='val2')
        f3 = filters.Filter(dimension='dim3', value='val3')
        f4 = filters.Filter(dimension='dim4', value='val4')
        f5 = filters.Filter(dimension='dim5', value='val5')
        actual = filters.Filter.build_filter(f1 | f2 | f3 | f4 | f5)
        expected = {
            'type': 'or',
            'fields': [
                {'type': 'selector', 'dimension': 'dim1', 'value': 'val1'},
                {'type': 'selector', 'dimension': 'dim2', 'value': 'val2'},
                {'type': 'selector', 'dimension': 'dim3', 'value': 'val3'},
                {'type': 'selector', 'dimension': 'dim4', 'value': 'val4'},
                {'type': 'selector', 'dimension': 'dim5', 'value': 'val5'}
            ]
        }
        assert actual == expected

    def test_not_filter(self):
        f = filters.Filter(dimension='dim', value='val')
        actual = filters.Filter.build_filter(~f)
        expected = {
            'type': 'not',
            'field': {'type': 'selector', 'dimension': 'dim', 'value': 'val'}
        }
        assert actual == expected

    def test_not_and_filter(self):
        f1 = filters.Filter(dimension='dim1', value='val1')
        f2 = filters.Filter(dimension='dim2', value='val2')
        actual = filters.Filter.build_filter(~f1 & f2)
        expected = {
            'type': 'and',
            'fields': [
                {
                    'type': 'not',
                    'field': {'type': 'selector', 'dimension': 'dim1', 'value': 'val1'}
                },
                {'type': 'selector', 'dimension': 'dim2', 'value': 'val2'}
            ]
        }
        assert actual == expected

    def test_invalid_filter(self):
        with pytest.raises(NotImplementedError):
            filters.Filter(type='invalid', dimension='dim', value='val')
