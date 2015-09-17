# -*- coding: UTF-8 -*-

from operator import itemgetter

from pydruid.utils import aggregators


class TestAggregators:

    def test_aggregators(self):
        aggs = [('longsum', 'longSum'), ('doublesum', 'doubleSum'),
                ('min', 'min'), ('max', 'max'), ('count', 'count'),
                ('hyperunique', 'hyperUnique')]
        aggs_funcs = [(getattr(aggregators, agg_name), agg_type)
                      for agg_name, agg_type in aggs]
        for f, agg_type in aggs_funcs:
            assert f('metric') == {'type': agg_type, 'fieldName': 'metric'}

    def test_build_aggregators(self):
        agg_input = {
            'agg1': aggregators.count('metric1'),
            'agg2': aggregators.longsum('metric2'),
            'agg3': aggregators.doublesum('metric3'),
            'agg4': aggregators.min('metric4'),
            'agg5': aggregators.max('metric5'),
            'agg6': aggregators.hyperunique('metric6')
        }
        built_agg = aggregators.build_aggregators(agg_input)
        expected = [
            {'name': 'agg1', 'type': 'count', 'fieldName': 'metric1'},
            {'name': 'agg2', 'type': 'longSum', 'fieldName': 'metric2'},
            {'name': 'agg3', 'type': 'doubleSum', 'fieldName': 'metric3'},
            {'name': 'agg4', 'type': 'min', 'fieldName': 'metric4'},
            {'name': 'agg5', 'type': 'max', 'fieldName': 'metric5'},
            {'name': 'agg6', 'type': 'hyperUnique', 'fieldName': 'metric6'},
        ]
        assert (sorted(built_agg, key=itemgetter('name')) ==
                sorted(expected, key=itemgetter('name')))
