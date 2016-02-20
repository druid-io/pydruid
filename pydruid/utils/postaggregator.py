#
# Copyright 2013 Metamarkets Group Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import division

import six


class Postaggregator:
    def __init__(self, fn, fields, name):
        self.post_aggregator = {'type': 'arithmetic',
                                'name': name,
                                'fn': fn,
                                'fields': fields}
        self.name = name

    def __mul__(self, other):
        return Postaggregator('*', self.fields(other),
                              self.name + 'mul' + other.name)

    def __sub__(self, other):
        return Postaggregator('-', self.fields(other),
                              self.name + 'sub' + other.name)

    def __add__(self, other):
        return Postaggregator('+', self.fields(other),
                              self.name + 'add' + other.name)

    def __div__(self, other):
        return Postaggregator('/', self.fields(other),
                              self.name + 'div' + other.name)

    def __truediv__(self, other):
        return self.__div__(other)

    def fields(self, other):
        return [self.post_aggregator, other.post_aggregator]

    @staticmethod
    def build_post_aggregators(postaggs):
        def rename_postagg(new_name, post_aggregator):
            post_aggregator['name'] = new_name
            return post_aggregator

        return [rename_postagg(new_name, postagg.post_aggregator)
                for (new_name, postagg) in six.iteritems(postaggs)]


class Field(Postaggregator):
    def __init__(self, name):
        Postaggregator.__init__(self, None, None, name)
        self.post_aggregator = {
            'type': 'fieldAccess', 'fieldName': name}


class Const(Postaggregator):
    def __init__(self, value, output_name=None):

        if output_name is None:
            name = 'const'
        else:
            name = output_name

        Postaggregator.__init__(self, None, None, name)
        self.post_aggregator = {
            'type': 'constant', 'name': name, 'value': value}


class HyperUniqueCardinality(Postaggregator):
    def __init__(self, name):
        Postaggregator.__init__(self, None, None, name)
        self.post_aggregator = {
            'type': 'hyperUniqueCardinality', 'fieldName': name}
