"""
sqless split 函数的独立测试。

使用方式:
    pytest tests/test_split.py -v
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from sqless.server import split


class TestSplit:
    """split 函数是解析 API 路径参数的核心工具。
    默认分隔符为逗号(',', 非 space)，且可识别括号/引号内的分隔符。"""

    def test_basic_split_by_comma(self):
        """默认用逗号分隔。"""
        result = list(split("a,b,c"))
        assert result == ["a", "b", "c"]

    def test_split_with_quotes(self):
        """引号内的逗号不应作为分隔符。"""
        result = list(split('func,"hello,world",arg'))
        assert result == ["func", "hello,world", "arg"]

    def test_split_with_single_quotes(self):
        result = list(split("func,'hello,world',arg"))
        assert result == ["func", "hello,world", "arg"]

    def test_split_with_parentheses(self):
        """括号内的逗号不应作为分隔符。"""
        result = list(split("func,(1, 2),arg"))
        assert result == ["func", "(1, 2)", "arg"]

    def test_split_with_brackets(self):
        result = list(split("func,[1, 2],arg"))
        assert result == ["func", "[1, 2]", "arg"]

    def test_split_with_braces(self):
        result = list(split('func,{"key":"val"},arg'))
        assert result == ['func', '{"key":"val"}', "arg"]

    def test_split_with_escaped_comma(self):
        """反斜杠转义的逗号不应作为分隔符。"""
        result = list(split(r"hello\,world,end"))
        assert result == [r"hello\,world", "end"]

    def test_split_empty_string(self):
        result = list(split(""))
        assert result == []

    def test_split_single_token(self):
        result = list(split("alone"))
        assert result == ["alone"]

    def test_split_custom_separator(self):
        result = list(split("a b c", sep=" "))
        assert result == ["a", "b", "c"]

    def test_split_custom_separator_with_brackets(self):
        """自定义分隔符下仍应尊重括号。"""
        result = list(split("func (1, 2) arg", sep=" "))
        assert result == ["func", "(1, 2)", "arg"]

    def test_split_nested_brackets(self):
        result = list(split("func,(a, (b, c)),arg"))
        assert result == ["func", "(a, (b, c))", "arg"]

    def test_split_trailing_sep_ignored(self):
        result = list(split("a,b,"))
        assert result == ["a", "b"]

    def test_split_leading_sep_ignored(self):
        result = list(split(",a,b"))
        assert result == ["a", "b"]
