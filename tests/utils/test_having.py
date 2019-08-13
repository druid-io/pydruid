# -*- coding: UTF-8 -*-

from pydruid.utils import having, filters


class TestHaving:
    def test_gte_numeric_filter_having(self):
        having_gte = having.Having(type="greaterThan", aggregation="revenue", value=20)
        actual = having.Having.build_having(having_gte)
        expected = {"type": "greaterThan", "aggregation": "revenue", "value": 20}
        assert actual == expected

    def test_query_filter_having(self):
        selector_filter = filters.Filter(
            type="selector", dimension="name", value="druid"
        )
        query_filter = filters.Filter.build_filter(selector_filter)
        having_filter = having.Having(type="filter", filter=query_filter)
        actual = having.Having.build_having(having_filter)
        expected = {
            "type": "filter",
            "filter": {"type": "selector", "dimension": "name", "value": "druid"},
        }
        assert actual == expected
