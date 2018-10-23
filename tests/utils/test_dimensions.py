from pydruid.utils.dimensions import ListFilteredSpec
from pydruid.utils.dimensions import RegexFilteredSpec
from pydruid.utils.dimensions import RegexExtraction
from pydruid.utils.dimensions import PartialExtraction
from pydruid.utils.dimensions import JavascriptExtraction
from pydruid.utils.dimensions import TimeFormatExtraction
from pydruid.utils.dimensions import MapLookupExtraction
from pydruid.utils.dimensions import NamespaceLookupExtraction
from pydruid.utils.dimensions import DimensionSpec
from pydruid.utils.dimensions import build_dimension


class TestDimensionSpec(object):

    def test_default(self):
        dim_spec = DimensionSpec('dim', 'out')
        actual = dim_spec.build()
        expected = {'type': 'default', 'dimension': 'dim', 'outputName': 'out'}

        assert actual == expected

    def test_extraction_functions(self):
        js_func = 'function(x) {return x};'
        ext_fns = [
            (RegexExtraction(r'\w+'), {'type': 'regex', 'expr': '\\w+'}),
            (PartialExtraction(r'\w+'), {'type': 'partial', 'expr': '\\w+'}),
            (JavascriptExtraction(js_func), {
                'type': 'javascript',
                'function': js_func,
                'injective': False
            }),
            (MapLookupExtraction(TestMapLookupExtraction.mapping), {
                'type': 'lookup',
                'lookup': {
                    'type': 'map',
                    'map': TestMapLookupExtraction.mapping
                },
                'retainMissingValue': False,
                'replaceMissingValueWith': None,
                'injective': False
            })
        ]

        for ext_fn, expected_ext_fn in ext_fns:
            dim_spec = DimensionSpec('dim', 'out', extraction_function=ext_fn)
            actual = dim_spec.build()
            expected = {
                'type': 'extraction',
                'dimension': 'dim',
                'outputName': 'out',
                'extractionFn': expected_ext_fn
            }

            assert actual == expected

    def test_filter_specs(self):
        delegate_spec = DimensionSpec('dim', 'out').build()
        filter_specs = [
            (ListFilteredSpec(['val1', 'val2']), {
                'type': 'listFiltered',
                'delegate': delegate_spec,
                'values': ['val1', 'val2'],
            }),
            (ListFilteredSpec(['val1', 'val2'], is_whitelist=False), {
                'type': 'listFiltered',
                'delegate': delegate_spec,
                'values': ['val1', 'val2'],
                'isWhitelist': False,
            }),
            (RegexFilteredSpec(r'\w+'), {
                'type': 'regexFiltered',
                'delegate': delegate_spec,
                'pattern': '\\w+',
            })
        ]

        for filter_spec, expected_dim_spec in filter_specs:
            dim_spec = DimensionSpec('dim', 'out', filter_spec=filter_spec)
            actual = dim_spec.build()

            assert actual == expected_dim_spec

    def test_build_dimension(self):
        assert build_dimension('raw_dim') == 'raw_dim'

        dim_spec = DimensionSpec('dim', 'out')
        assert build_dimension(dim_spec) == dim_spec.build()


class TestListFilteredSpec(object):

    def test_list_filtered_spec(self):
        dim_spec = DimensionSpec('dim', 'out').build()
        list_filtered_spec = ListFilteredSpec(['val1', 'val2'])
        actual = list_filtered_spec.build(dim_spec)
        expected_dim_spec = {'type': 'default', 'dimension': 'dim', 'outputName': 'out'}
        expected = {
            'type': 'listFiltered',
            'delegate': expected_dim_spec,
            'values': ['val1', 'val2'],
        }

        assert actual == expected

    def test_list_filtered_spec_whitelist(self):
        dim_spec = DimensionSpec('dim', 'out').build()
        list_filtered_spec = ListFilteredSpec(['val1', 'val2'], is_whitelist=False)
        actual = list_filtered_spec.build(dim_spec)
        expected_dim_spec = {'type': 'default', 'dimension': 'dim', 'outputName': 'out'}
        expected = {
            'type': 'listFiltered',
            'delegate': expected_dim_spec,
            'values': ['val1', 'val2'],
            'isWhitelist': False,
        }

        assert actual == expected


class TestRegexFilteredSpec(object):

    def test_regex_filtered_spec(self):
        dim_spec = DimensionSpec('dim', 'out').build()
        regex_filtered_spec = RegexFilteredSpec(r'\w+')
        actual = regex_filtered_spec.build(dim_spec)
        expected_dim_spec = {'type': 'default', 'dimension': 'dim', 'outputName': 'out'}
        expected = {
            'type': 'regexFiltered',
            'delegate': expected_dim_spec,
            'pattern': '\\w+',
        }

        assert actual == expected


class TestRegexExtraction(object):

    def test_regex(self):
        ext_fn = RegexExtraction(r'\w+')
        actual = ext_fn.build()
        expected = {'type': 'regex', 'expr': '\\w+'}

        assert actual == expected


class TestPartialExtraction(object):

    def test_regex(self):
        ext_fn = PartialExtraction(r'\w+')
        actual = ext_fn.build()
        expected = {'type': 'partial', 'expr': '\\w+'}

        assert actual == expected


class TestJavascriptExtraction(object):

    def test_js_injective(self):
        js_func = 'function(x) {return x};'
        ext_fn = JavascriptExtraction(js_func, injective=True)
        actual = ext_fn.build()
        expected = {
            'type': 'javascript',
            'function': js_func,
            'injective': True
        }

        assert actual == expected

    def test_js_not_injective(self):
        js_func = 'function(x) {return x};'
        ext_fn = JavascriptExtraction(js_func)
        actual = ext_fn.build()
        expected = {
            'type': 'javascript',
            'function': js_func,
            'injective': False
        }

        assert actual == expected


class TestTimeFormatExtraction(object):

    def test_time_format_all_set(self):
        ext_fn = TimeFormatExtraction('EEEE', 'en-US', 'Europe/Berlin')
        actual = ext_fn.build()
        expected = {
            'type': 'timeFormat',
            'format': 'EEEE',
            'locale': 'en-US',
            'timeZone': 'Europe/Berlin'
        }

        assert actual == expected

    def test_time_format_no_timezone(self):
        ext_fn = TimeFormatExtraction('EEEE', 'en-US')
        actual = ext_fn.build()
        expected = {
            'type': 'timeFormat',
            'format': 'EEEE',
            'locale': 'en-US',
        }

        assert actual == expected

    def test_time_format_only_format(self):
        ext_fn = TimeFormatExtraction('EEEE')
        actual = ext_fn.build()
        expected = {
            'type': 'timeFormat',
            'format': 'EEEE',
        }

        assert actual == expected


class TestMapLookupExtraction(object):

    mapping = {
        'foo1': 'bar1',
        'foo2': 'bar2'
    }

    def test_map_default(self):
        ext_fn = MapLookupExtraction(self.mapping)
        actual = ext_fn.build()
        expected = {
            'type': 'lookup',
            'lookup': {
                'type': 'map',
                'map': self.mapping
            },
            'retainMissingValue': False,
            'replaceMissingValueWith': None,
            'injective': False
        }

        assert actual == expected

    def test_map_retain_missing(self):
        ext_fn = MapLookupExtraction(self.mapping, retain_missing_values=True)
        actual = ext_fn.build()
        expected = {
            'type': 'lookup',
            'lookup': {
                'type': 'map',
                'map': self.mapping
            },
            'retainMissingValue': True,
            'replaceMissingValueWith': None,
            'injective': False
        }

        assert actual == expected

    def test_map_replace_missing(self):
        ext_fn = MapLookupExtraction(self.mapping,
                                     replace_missing_values='replacer')
        actual = ext_fn.build()
        expected = {
            'type': 'lookup',
            'lookup': {
                'type': 'map',
                'map': self.mapping
            },
            'retainMissingValue': False,
            'replaceMissingValueWith': 'replacer',
            'injective': False
        }

        assert actual == expected

    def test_map_injective(self):
        ext_fn = MapLookupExtraction(self.mapping, injective=True)
        actual = ext_fn.build()
        expected = {
            'type': 'lookup',
            'lookup': {
                'type': 'map',
                'map': self.mapping
            },
            'retainMissingValue': False,
            'replaceMissingValueWith': None,
            'injective': True
        }

        assert actual == expected


class TestNamespaceLookupExtraction(object):

    def test_map_default(self):
        ext_fn = NamespaceLookupExtraction('foo_namespace')
        actual = ext_fn.build()
        expected = {
            'type': 'lookup',
            'lookup': {
                'type': 'namespace',
                'namespace': 'foo_namespace'
            },
            'retainMissingValue': False,
            'replaceMissingValueWith': None,
            'injective': False
        }

        assert actual == expected

    def test_map_retain_missing(self):
        ext_fn = NamespaceLookupExtraction('foo_namespace', retain_missing_values=True)
        actual = ext_fn.build()
        expected = {
            'type': 'lookup',
            'lookup': {
                'type': 'namespace',
                'namespace': 'foo_namespace'
            },
            'retainMissingValue': True,
            'replaceMissingValueWith': None,
            'injective': False
        }

        assert actual == expected

    def test_map_replace_missing(self):
        ext_fn = NamespaceLookupExtraction('foo_namespace',
                                     replace_missing_values='replacer')
        actual = ext_fn.build()
        expected = {
            'type': 'lookup',
            'lookup': {
                'type': 'namespace',
                'namespace': 'foo_namespace'
            },
            'retainMissingValue': False,
            'replaceMissingValueWith': 'replacer',
            'injective': False
        }

        assert actual == expected

    def test_map_injective(self):
        ext_fn = NamespaceLookupExtraction('foo_namespace', injective=True)
        actual = ext_fn.build()
        expected = {
            'type': 'lookup',
            'lookup': {
                'type': 'namespace',
                'namespace': 'foo_namespace'
            },
            'retainMissingValue': False,
            'replaceMissingValueWith': None,
            'injective': True
        }

        assert actual == expected
