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
try:
    import simplejson as json
except ImportError:
    import json


class Filter:
    def __init__(self, **args):

        if 'type' not in args.keys():
            self.filter = {"filter": {"type": "selector",
                                      "dimension": args["dimension"],
                                      "value": args["value"]}}

        elif args["type"] == "javascript":
            self.filter = {"filter": {"type": "javascript",
                                      "dimension": args["dimension"],
                                      "function": args["function"]}}

        elif args["type"] == "and":
            self.filter = {"filter": {"type": "and",
                                      "fields": args["fields"]}}

        elif args["type"] == "or":
            self.filter = {"filter": {"type": "or",
                                      "fields": args["fields"]}}

        elif args["type"] == "not":
            self.filter = {"filter": {"type": "not",
                                      "field": args["field"]}}
        elif args["type"] == "regex":
            self.filter = {"filter": {"type": "regex",
                                      "dimension": args["dimension"],
                                      "pattern": args["pattern"]}}
        else:
            raise NotImplementedError(
                'Filter type: {0} does not exist'.format(args['type']))

    def show(self):
        print(json.dumps(self.filter, indent=4))

    def __and__(self, x):
        if self.filter['filter']['type'] == 'and':
            # if `self` is already `and`, don't create a new filter
            # but just append `x` to the filter fields.
            self.filter['filter']['fields'].append(x)
            return self
        return Filter(type="and", fields=[self, x])

    def __or__(self, x):
        if self.filter['filter']['type'] == 'or':
            # if `self` is already `or`, don't create a new filter
            # but just append `x` to the filter fields.
            self.filter['filter']['fields'].append(x)
            return self
        return Filter(type="or", fields=[self, x])

    def __invert__(self):
        return Filter(type="not", field=self)

    @staticmethod
    def build_filter(filter_obj):
        filter = filter_obj.filter['filter']
        if filter['type'] in ['and', 'or']:
            filter = filter.copy()  # make a copy so we don't overwrite `fields`
            filter['fields'] = [Filter.build_filter(f) for f in filter['fields']]
        elif filter['type'] in ['not']:
            filter['field'] = Filter.build_filter(filter['field'])
        return filter


class Dimension:
    def __init__(self, dim):
        self.dimension = dim

    def __eq__(self, other):
        return Filter(dimension=self.dimension, value=other)

    def __ne__(self, other):
        return ~Filter(dimension=self.dimension, value=other)


class JavaScript:
    def __init__(self, dim):
        self.dimension = dim

    def __eq__(self, func):
        return Filter(type='javascript', dimension=self.dimension, function=func)
