#
# Licensed to Metamarkets Group Inc. (Metamarkets) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. Metamarkets licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

from operator import itemgetter
from copy import deepcopy

from pydruid.utils.postaggregator import *
from pydruid.utils import filters


class TestPostAggregators:

    def test_anything(self):
        assert True
    def test_build_thetapostaggregator(self):
        pagg_input = {
            'pag1': ThetaSketchEstimate(ThetaSketch('theta1')),
            'pag2': ThetaSketchEstimate(ThetaSketch('theta1') & ThetaSketch('theta2')),
            'pag3': ThetaSketchEstimate(ThetaSketch('theta1') | ThetaSketch('theta2')),
            'pag4': ThetaSketchEstimate(ThetaSketch('theta1') != ThetaSketch('theta2')),
            'pag5': ThetaSketchEstimate(
                (ThetaSketch('theta1') != ThetaSketch('theta2')) & ThetaSketch('theta3')),
        }
        built_agg = ThetaSketchEstimate.build_post_aggregators(pagg_input)
        expected = [
            {'name': 'pag1', 'type': 'thetaSketchEstimate', 'field': {'type': 'fieldAccess', 'fieldName': 'theta1'}},
            {'name': 'pag2', 'type': 'thetaSketchEstimate', 'field':
                {'type': 'thetaSketchSetOp', 'func': 'INTERSECT', 'name': 'theta1_AND_theta2', 'fields':
                    [
                        {'type': 'fieldAccess', 'fieldName': 'theta1'},
                        {'type': 'fieldAccess', 'fieldName': 'theta2'}
                    ]
                }
             },
            {'name': 'pag3', 'type': 'thetaSketchEstimate', 'field':
                {'type': 'thetaSketchSetOp', 'func': 'UNION', 'name': 'theta1_OR_theta2', 'fields':
                    [
                        {'type': 'fieldAccess', 'fieldName': 'theta1'},
                        {'type': 'fieldAccess', 'fieldName': 'theta2'}
                    ]
                 }
             },
            {'name': 'pag4', 'type': 'thetaSketchEstimate', 'field':
                {'type': 'thetaSketchSetOp', 'func': 'NOT', 'name': 'theta1_NOT_theta2', 'fields':
                    [
                        {'type': 'fieldAccess', 'fieldName': 'theta1'},
                        {'type': 'fieldAccess', 'fieldName': 'theta2'}
                    ]
                 }
             },
            {'name': 'pag5', 'type': 'thetaSketchEstimate', 'field':
                {'type': 'thetaSketchSetOp', 'func': 'INTERSECT', 'name': 'theta1_NOT_theta2_AND_theta3', 'fields':
                    [
                        {'type': 'thetaSketchSetOp', 'name': 'theta1_NOT_theta2', 'func': 'NOT', 'fields':
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