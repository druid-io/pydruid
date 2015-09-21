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
from six import iteritems

from .filters import Filter


def longsum(raw_metric):
    return {"type": "longSum", "fieldName": raw_metric}


def doublesum(raw_metric):
    return {"type": "doubleSum", "fieldName": raw_metric}


def min(raw_metric):
    return {"type": "min", "fieldName": raw_metric}


def max(raw_metric):
    return {"type": "max", "fieldName": raw_metric}


def count(raw_metric):
    return {"type": "count", "fieldName": raw_metric}


def hyperunique(raw_metric):
    return {"type": "hyperUnique", "fieldName": raw_metric}


def filtered(filter, agg):
    return {"type": "filtered",
            "filter": Filter.build_filter(filter),
            "aggregator": agg}


def build_aggregators(agg_input):
    return [_build_aggregator(name, kwargs)
            for (name, kwargs) in iteritems(agg_input)]


def _build_aggregator(name, kwargs):
    if kwargs["type"] == "filtered":
        kwargs["aggregator"]["name"] = name
    else:
        kwargs.update({"name": name})

    return kwargs
