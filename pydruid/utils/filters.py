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
import simplejson as json


class Filter:
    def __init__(self, **args):

        if 'type' not in args.keys():
            self.filter = {"filter": {"type": "selector",
                                      "dimension": args["dimension"],
                                      "value": args["value"]}}

        elif args["type"] == "and":
            self.filter = {"filter": {"type": "and",
                                      "fields": args["fields"]}}

        elif args["type"] == "or":
            self.filter = {"filter": {"type": "or",
                                      "fields": args["fields"]}}

        elif args["type"] == "not":
            self.filter = {"filter": {"type": "not",
                                      "field": args["field"]}}
        else:
            raise NotImplemented(
                'Filter type: {0} does not exist'.format(args['type']))

    def show(self):
        print(json.dumps(self.filter, indent=4))

    def __and__(self, x):
        return Filter(type="and",
                      fields=[self.filter['filter'], x.filter['filter']])

    def __or__(self, x):
        return Filter(type="or",
                      fields=[self.filter['filter'], x.filter['filter']])

    def __invert__(self):
        return Filter(type="not", field=self.filter['filter'])

    @staticmethod
    def build_filter(filter_obj):
        return filter_obj.filter['filter']


class Dimension:
    def __init__(self, dim):
        self.dimension = dim

    def __eq__(self, other):
        return Filter(dimension=self.dimension, value=other)

