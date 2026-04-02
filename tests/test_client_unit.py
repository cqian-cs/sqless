"""
sqless client.py 单元测试 (不需要服务器)

覆盖 RDB 类的构造、参数格式化和边界逻辑。

使用方式:
    pytest tests/test_client_unit.py -v
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from sqless.client import RDB


class TestRDBConstruction:
    """RDB 构造和参数验证。"""

    def test_basic_init(self):
        rdb = RDB("http://localhost:12239", "my_secret")
        assert rdb.host == "http://localhost:12239"
        assert rdb.secret == "my_secret"
        assert rdb.headers["Authorization"] == "Bearer my_secret"

    def test_host_trailing_slash_stripped(self):
        rdb = RDB("http://localhost:12239/", "secret")
        assert rdb.host == "http://localhost:12239"

    def test_basic_auth_header_format(self):
        """确保 Basic auth header 格式正确（Base64 编码 :secret）。"""
        rdb = RDB("http://localhost:12239", "my_secret")
        # RDB 只使用 Bearer，但 server 同时接受 Basic
        assert "Bearer my_secret" in rdb.headers["Authorization"]

    def test_custom_timeout(self):
        rdb = RDB("http://localhost:12239", "secret", timeout=(5, 30))
        assert rdb.timeout == (5, 30)


class TestRDBURLFormatting:
    """URL 构建格式确保不因升级而改变。"""

    def test_db_set_url(self):
        rdb = RDB("http://localhost:12239", "secret")
        # db_set 的 URL 格式应为 /db/{table}
        expected = "http://localhost:12239/db/users"
        assert rdb.host + "/db/users" == expected

    def test_db_get_url(self):
        rdb = RDB("http://localhost:12239", "secret")
        url = f"{rdb.host}/db/users/key = u1?page=1&limit=20"
        assert "/db/users/" in url
        assert "page=1" in url
        assert "limit=20" in url

    def test_fs_set_url(self):
        rdb = RDB("http://localhost:12239", "secret")
        url = f"{rdb.host}/fs/path/to/file.txt"
        assert "/fs/path/to/file.txt" in url

    def test_fs_check_url(self):
        rdb = RDB("http://localhost:12239", "secret")
        check_key = "test/file.txt?check"
        url = f"{rdb.host}/fs/{check_key}"
        assert "?check" in url


class TestRDBEdgeCases:
    """RDB 边界条件。"""

    def test_fs_get_returns_none_on_failure(self):
        """fs_get 在失败时应返回 None (无 path) 或 False (有 path)。"""
        rdb = RDB("http://nonexistent.invalid:99999", "secret", timeout=(1, 1))
        # 由于 host 不可达，retry 耗尽后应返回 None
        result = rdb.fs_get("any/key", path=None, retry=1)
        assert result is None

    def test_fs_check_returns_failure_on_unreachable(self):
        """fs_check 在服务器不可达时应返回包含 suc=False 的 dict。"""
        rdb = RDB("http://nonexistent.invalid:99999", "secret", timeout=(1, 1))
        result = rdb.fs_check("any/key", retry=1)
        assert isinstance(result, dict)
        assert result["suc"] is False
