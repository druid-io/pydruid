from pydruid.utils.virtualcolumns import build_virtual_column, VirtualColumnSpec


class BuildVirtualColumnTC:
    def test_with_virtual_column_spec(self) -> None:
        virtual_col_spec = VirtualColumnSpec("foo", "bar + 3")
        assert build_virtual_column(virtual_col_spec) == virtual_col_spec.build()

    def test_with_dict(self) -> None:
        virtual_column = {
            "type": "expression",
            "name": "doubleVote",
            "expression": "vote * 2",
            "outputType": "LONG",
        }
        assert build_virtual_column(virtual_column) == virtual_column


class TestVirtualColumnSpec:
    def test_default(self) -> None:
        spec = VirtualColumnSpec("foo", "concat(bar + '123')")
        expected = {
            "type": "expression",
            "name": "foo",
            "expression": "concat(bar + '123')",
        }
        assert spec.build() == expected

    def test_output_type(self) -> None:
        spec = VirtualColumnSpec("foo", "bar * 3", "LONG")
        expected = {
            "type": "expression",
            "name": "foo",
            "expression": "bar * 3",
            "outputType": "LONG",
        }
        assert spec.build() == expected
