# -*- coding: UTF-8 -*-

import pytest

from pydruid.utils import filters


class TestDimension:

    def test_dimension(self):
        d = filters.Dimension('dim')
        actual = filters.Filter.build_filter(d == 'val')
        expected = {'type': 'selector', 'dimension': 'dim', 'value': 'val'}
        assert actual == expected

    def test_ne_dimension(self):
        d = filters.Dimension('dim')
        actual = filters.Filter.build_filter(d != 'val')
        expected = {'field': {'dimension': 'dim', 'type': 'selector', 'value': 'val'},
                    'type': 'not'}
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
        actual = filters.Filter.build_filter(f1 & f2)
        expected = {
            'type': 'and',
            'fields': [
                {'type': 'selector', 'dimension': 'dim1', 'value': 'val1'},
                {'type': 'selector', 'dimension': 'dim2', 'value': 'val2'}
            ]
        }
        assert actual == expected

    def test_and_filter_multiple(self):
        f1 = filters.Filter(dimension='dim1', value='val1')
        f2 = filters.Filter(dimension='dim2', value='val2')
        f3 = filters.Filter(dimension='dim3', value='val3')
        filter = filters.Filter(type='and', fields=[f1, f2, f3])
        actual = filters.Filter.build_filter(filter)
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
        actual = filters.Filter.build_filter(f1 | f2)
        expected = {
            'type': 'or',
            'fields': [
                {'type': 'selector', 'dimension': 'dim1', 'value': 'val1'},
                {'type': 'selector', 'dimension': 'dim2', 'value': 'val2'}
            ]
        }
        assert actual == expected

    def test_nested_mix_filter(self):
        f1 = filters.Filter(dimension='dim1', value='val1')
        f2 = filters.Filter(dimension='dim2', value='val2')
        f3 = filters.Filter(dimension='dim3', value='val3')
        f4 = filters.Filter(dimension='dim4', value='val4')
        f5 = filters.Filter(dimension='dim5', value='val5')
        f6 = filters.Filter(dimension='dim6', value='val6')
        f7 = filters.Filter(dimension='dim7', value='val7')
        f8 = filters.Filter(dimension='dim8', value='val8')
        actual = filters.Filter.build_filter(f1 & ~f2 & f3 & (f4 | ~f5 | f6 | (f7 & ~f8)))
        expected = {
            'fields': [{'dimension': 'dim1', 'type': 'selector', 'value': 'val1'},
                       {'field': {'dimension': 'dim2', 'type': 'selector', 'value': 'val2'},
                        'type': 'not'},
                       {'dimension': 'dim3', 'type': 'selector', 'value': 'val3'},
                       {'fields': [{'dimension': 'dim4', 'type': 'selector', 'value': 'val4'},
                                   {'field': {'dimension': 'dim5', 'type': 'selector',
                                              'value': 'val5'},
                                    'type': 'not'},
                                   {'dimension': 'dim6', 'type': 'selector', 'value': 'val6'},
                                   {'fields': [
                                       {'dimension': 'dim7', 'type': 'selector', 'value': 'val7'},
                                       {'field': {'dimension': 'dim8', 'type': 'selector',
                                                  'value': 'val8'},
                                        'type': 'not'}],
                                    'type': 'and'}],
                        'type': 'or'}],
            'type': 'and'
        }
        assert actual == expected

    def test_or_filter_multiple(self):
        f1 = filters.Filter(dimension='dim1', value='val1')
        f2 = filters.Filter(dimension='dim2', value='val2')
        f3 = filters.Filter(dimension='dim3', value='val3')
        filter = filters.Filter(type='or', fields=[f1, f2, f3])
        actual = filters.Filter.build_filter(filter)
        expected = {
            'type': 'or',
            'fields': [
                {'type': 'selector', 'dimension': 'dim1', 'value': 'val1'},
                {'type': 'selector', 'dimension': 'dim2', 'value': 'val2'},
                {'type': 'selector', 'dimension': 'dim3', 'value': 'val3'}
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

    def test_nested_not_or_filter(self):
        f1 = filters.Filter(dimension='dim1', value='val1')
        f2 = filters.Filter(dimension='dim2', value='val2')
        actual = filters.Filter.build_filter(~(f1 | f2))
        expected = {
            'type': 'not',
            'field': {'type': 'or',
                      'fields': [{'type': 'selector', 'dimension': 'dim1', 'value': 'val1'},
                                 {'type': 'selector', 'dimension': 'dim2', 'value': 'val2'}]}
        }
        assert actual == expected

    def test_invalid_filter(self):
        with pytest.raises(NotImplementedError):
            filters.Filter(type='invalid', dimension='dim', value='val')
