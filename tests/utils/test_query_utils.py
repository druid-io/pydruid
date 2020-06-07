# -*- coding: UTF-8 -*-

import os

from pydruid.utils import query_utils


def open_file(file_path):
    return open(file_path, "w", newline="", encoding="utf-8")


def line_ending():
    return os.linesep


class TestUnicodeWriter:
    def test_writerow(self, tmpdir):
        file_path = tmpdir.join("out.tsv")
        f = open_file(str(file_path))
        w = query_utils.UnicodeWriter(f)
        w.writerow(["value1", "㬓"])
        f.close()
        assert file_path.read() == "value1\t㬓" + line_ending()

    def test_writerows(self, tmpdir):
        file_path = tmpdir.join("out.tsv")
        f = open_file(str(file_path))
        w = query_utils.UnicodeWriter(f)
        w.writerows([["header1", "header2"], ["value1", "㬓"]])
        f.close()
        assert (
            file_path.read()
            == "header1\theader2" + line_ending() + "value1\t㬓" + line_ending()
        )
