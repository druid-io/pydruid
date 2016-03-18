# -*- coding: UTF-8 -*-

from operator import itemgetter
from copy import deepcopy

from pydruid.utils import aggregators
from pydruid.utils import filters


class TestAggregators:

    def test_aggregators(self):
        aggs = [('longsum', 'longSum'), ('doublesum', 'doubleSum'),
                ('min', 'min'), ('max', 'max'), ('count', 'count'),
                ('hyperunique', 'hyperUnique')]
        aggs_funcs = [(getattr(aggregators, agg_name), agg_type)
                      for agg_name, agg_type in aggs]
        for f, agg_type in aggs_funcs:
            assert f('metric') == {'type': agg_type, 'fieldName': 'metric'}

    def test_filtered_aggregator(self):
        filter_ = filters.Filter(dimension='dim', value='val')
        aggs = [aggregators.count('metric1'),
                aggregators.longsum('metric2'),
                aggregators.doublesum('metric3'),
                aggregators.min('metric4'),
                aggregators.max('metric5'),
                aggregators.hyperunique('metric6'),
                aggregators.cardinality('dim1'),
                aggregators.cardinality(['dim1', 'dim2'], by_row=True)]
        for agg in aggs:
            expected = {
                'type': 'filtered',
                'filter': {
                    'type': 'selector',
                    'dimension': 'dim',
                    'value': 'val'
                },
                'aggregator': agg
            }
            actual = aggregators.filtered(filter_, agg)
            assert actual == expected

    def test_nested_filtered_aggregator(self):
        filter1 = filters.Filter(dimension='dim1', value='val')
        filter2 = filters.Filter(dimension='dim2', value='val')
        agg = aggregators.filtered(filter1,
                                   aggregators.filtered(filter2, aggregators.count('metric1')))
        actual = aggregators.build_aggregators({'agg_name': agg})
        # the innermost aggregation must have 'agg_name'
        expected = [{
            'type': 'filtered',
            'aggregator': {
                'type': 'filtered',
                'aggregator': {'fieldName': 'metric1', 'type': 'count', 'name': 'agg_name'},
                'filter': {'dimension': 'dim2', 'value': 'val', 'type': 'selector'}},
            'filter': {'dimension': 'dim1', 'value': 'val', 'type': 'selector'}
        }]
        assert expected == actual

    def test_build_aggregators(self):
        agg_input = {
            'agg1': aggregators.count('metric1'),
            'agg2': aggregators.longsum('metric2'),
            'agg3': aggregators.doublesum('metric3'),
            'agg4': aggregators.min('metric4'),
            'agg5': aggregators.max('metric5'),
            'agg6': aggregators.hyperunique('metric6'),
            'agg7': aggregators.cardinality('dim1'),
            'agg8': aggregators.cardinality(['dim1', 'dim2'], by_row=True)
        }
        built_agg = aggregators.build_aggregators(agg_input)
        expected = [
            {'name': 'agg1', 'type': 'count', 'fieldName': 'metric1'},
            {'name': 'agg2', 'type': 'longSum', 'fieldName': 'metric2'},
            {'name': 'agg3', 'type': 'doubleSum', 'fieldName': 'metric3'},
            {'name': 'agg4', 'type': 'min', 'fieldName': 'metric4'},
            {'name': 'agg5', 'type': 'max', 'fieldName': 'metric5'},
            {'name': 'agg6', 'type': 'hyperUnique', 'fieldName': 'metric6'},
            {'name': 'agg7', 'type': 'cardinality', 'fieldNames': ['dim1'], 'byRow': False},
            {'name': 'agg8', 'type': 'cardinality', 'fieldNames': ['dim1', 'dim2'], 'byRow': True},
        ]
        assert (sorted(built_agg, key=itemgetter('name')) ==
                sorted(expected, key=itemgetter('name')))

    def test_build_filtered_aggregator(self):
        filter_ = filters.Filter(dimension='dim', value='val')
        agg_input = {
            'agg1': aggregators.filtered(filter_,
                                         aggregators.count('metric1')),
            'agg2': aggregators.filtered(filter_,
                                         aggregators.longsum('metric2')),
            'agg3': aggregators.filtered(filter_,
                                         aggregators.doublesum('metric3')),
            'agg4': aggregators.filtered(filter_,
                                         aggregators.min('metric4')),
            'agg5': aggregators.filtered(filter_,
                                         aggregators.max('metric5')),
            'agg6': aggregators.filtered(filter_,
                                         aggregators.hyperunique('metric6')),
            'agg7': aggregators.filtered(filter_,
                                         aggregators.cardinality('dim1')),
            'agg8': aggregators.filtered(filter_,
                                         aggregators.cardinality(['dim1', 'dim2'], by_row=True)),
        }
        base = {
            'type': 'filtered',
            'filter': {
                'type': 'selector',
                'dimension': 'dim',
                'value': 'val'
            }
        }

        aggs = [
            {'name': 'agg1', 'type': 'count', 'fieldName': 'metric1'},
            {'name': 'agg2', 'type': 'longSum', 'fieldName': 'metric2'},
            {'name': 'agg3', 'type': 'doubleSum', 'fieldName': 'metric3'},
            {'name': 'agg4', 'type': 'min', 'fieldName': 'metric4'},
            {'name': 'agg5', 'type': 'max', 'fieldName': 'metric5'},
            {'name': 'agg6', 'type': 'hyperUnique', 'fieldName': 'metric6'},
            {'name': 'agg7', 'type': 'cardinality', 'fieldNames': ['dim1'], 'byRow': False},
            {'name': 'agg8', 'type': 'cardinality', 'fieldNames': ['dim1', 'dim2'], 'byRow': True},
        ]
        expected = []
        for agg in aggs:
            exp = deepcopy(base)
            exp.update({'aggregator': agg})
            expected.append(exp)

        built_agg = aggregators.build_aggregators(agg_input)
        expected = sorted(built_agg, key=lambda k: itemgetter('name')(
            itemgetter('aggregator')(k)))
        actual = sorted(expected, key=lambda k: itemgetter('name')(
            itemgetter('aggregator')(k)))
        assert expected == actual
