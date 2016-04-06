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

    def test_build_dimension(self):
        assert build_dimension('raw_dim') == 'raw_dim'

        dim_spec = DimensionSpec('dim', 'out')
        assert build_dimension(dim_spec) == dim_spec.build()


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
