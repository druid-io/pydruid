# -*- coding: UTF-8 -*-

import pytest

from pydruid.utils.filters import Filter
from pydruid.utils.having import Aggregation, Dimension, Having


class TestHaving:
    def test_equalTo_having(self):
        h1 = Having(type="equalTo", aggregation="sum", value=1)
        actual = Having.build_having(h1)
        expected = {"type": "equalTo", "aggregation": "sum", "value": 1}
        assert actual == expected

    def test_equalTo_having_Aggregation(self):
        h1 = Aggregation("sum") == 1
        actual = Having.build_having(h1)
        expected = {"type": "equalTo", "aggregation": "sum", "value": 1}
        assert actual == expected

    def test_lessThan_having(self):
        h1 = Having(type="lessThan", aggregation="sum", value=1)
        actual = Having.build_having(h1)
        expected = {"type": "lessThan", "aggregation": "sum", "value": 1}
        assert actual == expected

    def test_lessThan_having_Aggregation(self):
        h1 = Aggregation("sum") < 1
        actual = Having.build_having(h1)
        expected = {"type": "lessThan", "aggregation": "sum", "value": 1}
        assert actual == expected

    def test_greaterThan_having(self):
        h1 = Having(type="greaterThan", aggregation="sum", value=1)
        actual = Having.build_having(h1)
        expected = {"type": "greaterThan", "aggregation": "sum", "value": 1}
        assert actual == expected

    def test_greaterThan_having_Aggregation(self):
        h1 = Aggregation("sum") > 1
        actual = Having.build_having(h1)
        expected = {"type": "greaterThan", "aggregation": "sum", "value": 1}
        assert actual == expected

    def test_and_having(self):
        h1 = Aggregation("sum1") > 1
        h2 = Aggregation("sum2") > 2
        actual = Having.build_having(h1 & h2)
        expected = {
            "type": "and",
            "havingSpecs": [
                {"type": "greaterThan", "aggregation": "sum1", "value": 1},
                {"type": "greaterThan", "aggregation": "sum2", "value": 2},
            ],
        }
        assert actual == expected

    def test_or_having(self):
        h1 = Aggregation("sum1") > 1
        h2 = Aggregation("sum2") > 2
        actual = Having.build_having(h1 | h2)
        expected = {
            "type": "or",
            "havingSpecs": [
                {"type": "greaterThan", "aggregation": "sum1", "value": 1},
                {"type": "greaterThan", "aggregation": "sum2", "value": 2},
            ],
        }
        assert actual == expected

    def test_not_having(self):
        h1 = Aggregation("sum") > 1
        actual = Having.build_having(~h1)
        expected = {
            "type": "not",
            "havingSpec": {"type": "greaterThan", "aggregation": "sum", "value": 1},
        }
        assert actual == expected

    def test_dimSelector_having(self):
        h1 = Having(type="dimSelector", dimension="foo", value="bar")
        actual = Having.build_having(h1)
        expected = {"type": "dimSelector", "dimension": "foo", "value": "bar"}
        assert actual == expected

    def test_dimension(self):
        h1 = Dimension("foo") == "bar"
        actual = Having.build_having(h1)
        expected = {"type": "dimSelector", "dimension": "foo", "value": "bar"}
        assert actual == expected

    def test_query_filter_having(self):
        f1 = Filter(type="selector", dimension="foo", value="bar")
        query_filter = Filter.build_filter(f1)
        h1 = Having(type="filter", filter=query_filter)
        actual = Having.build_having(h1)
        expected = {
            "type": "filter",
            "filter": {"type": "selector", "dimension": "foo", "value": "bar"},
        }
        assert actual == expected

    def test_not_exists_having_type(self):
        with pytest.raises(NotImplementedError):
            Having(type="notExists")
