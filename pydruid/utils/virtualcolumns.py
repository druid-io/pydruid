from __future__ import annotations

from typing import Union


def build_virtual_column(virtual_column: Union[VirtualColumnSpec, dict]) -> dict:
    if isinstance(virtual_column, VirtualColumnSpec):
        return virtual_column.build()

    return virtual_column


class VirtualColumnSpec:
    def __init__(self, name: str, expression: str, output_type: str = None):
        self._name = name
        self._expression = expression
        self._output_type = output_type

    def build(self) -> dict:
        build = {
            "type": "expression",
            "name": self._name,
            "expression": self._expression,
        }
        if self._output_type is not None:
            build["outputType"] = self._output_type

        return build
