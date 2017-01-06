# -*- coding: UTF-8 -*-

from operator import itemgetter
from copy import deepcopy

from pydruid.utils.postaggregator import *
from pydruid.utils import filters


class TestPostAggregators:

    def test_anything(self):
        assert True
    def test_build_thetapostaggregator(self):
        pagg_input = {
            'pag1': ThetaSketchEstimate(Theta('theta1')),
            'pag2': ThetaSketchEstimate(Theta('theta1') & Theta('theta2')),
            'pag3': ThetaSketchEstimate(Theta('theta1') | Theta('theta2')),
            'pag4': ThetaSketchEstimate(Theta('theta1') != Theta('theta2')),
            'pag5': ThetaSketchEstimate(
                (Theta('theta1') != Theta('theta2')) & Theta('theta3')),
        }
        built_agg = ThetaSketchEstimate.build_post_aggregators(pagg_input)
        expected = [
            {'name': 'pag1', 'type': 'thetaSketchEstimate', 'field': {'type': 'fieldAccess', 'fieldName': 'theta1'}},
            {'name': 'pag2', 'type': 'thetaSketchEstimate', 'field':
                {'type': 'thetaSketchSetOp', 'func': 'INTERSECT', 'name': 'theta1andtheta2', 'fields':
                    [
                        {'type': 'fieldAccess', 'fieldName': 'theta1'},
                        {'type': 'fieldAccess', 'fieldName': 'theta2'}
                    ]
                }
             },
            {'name': 'pag3', 'type': 'thetaSketchEstimate', 'field':
                {'type': 'thetaSketchSetOp', 'func': 'UNION', 'name': 'theta1ortheta2', 'fields':
                    [
                        {'type': 'fieldAccess', 'fieldName': 'theta1'},
                        {'type': 'fieldAccess', 'fieldName': 'theta2'}
                    ]
                 }
             },
            {'name': 'pag4', 'type': 'thetaSketchEstimate', 'field':
                {'type': 'thetaSketchSetOp', 'func': 'NOT', 'name': 'theta1nottheta2', 'fields':
                    [
                        {'type': 'fieldAccess', 'fieldName': 'theta1'},
                        {'type': 'fieldAccess', 'fieldName': 'theta2'}
                    ]
                 }
             },
            {'name': 'pag5', 'type': 'thetaSketchEstimate', 'field':
                {'type': 'thetaSketchSetOp', 'func': 'INTERSECT', 'name': 'theta1nottheta2andtheta3', 'fields':
                    [
                        {'type': 'thetaSketchSetOp', 'name': 'theta1nottheta2', 'func': 'NOT', 'fields':
                            [
                                {'fieldName': 'theta1', 'type': 'fieldAccess'},
                                {'fieldName': 'theta2', 'type': 'fieldAccess'}
                            ],
                        },
                        {'fieldName': 'theta3', 'type': 'fieldAccess'}
                    ],
                },
             }

        ]
        assert(sorted(built_agg, key=itemgetter('name')) ==
                sorted(expected, key=itemgetter('name')))