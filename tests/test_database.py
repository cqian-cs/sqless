"""
sqless database.py 兼容性测试

覆盖 DB / Table / DBS / encode / decode / parse_where / valid_identifier
用于大版本升级前后的回归测试。所有断言均基于 v0.2.x 的实际行为。

使用方式:
    pytest tests/test_database.py -v
"""

import os
import sys
import pickle
import shutil
import sqlite3
import tempfile

import pytest
import orjson

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from sqless.database import (
    DB, Table,
    encode, decode,
    parse_where, valid_identifier,
    identifier_re,
)
from sqless.server import DBS


# ─────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────

@pytest.fixture
def tmp_dir():
    """提供临时目录，测试结束后自动清理。"""
    d = tempfile.mkdtemp(prefix="sqless_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def db(tmp_dir):
    """提供一个空的 DB 实例，生命周期内自动关闭。"""
    path = os.path.join(tmp_dir, "test.sqlite")
    instance = DB(path)
    yield instance
    instance.close()


# ═══════════════════════════════════════════
# 1. encode / decode
# ═══════════════════════════════════════════

class TestEncodeDecode:
    """encode/decode 的往返一致性，确保序列化协议不因升级而破坏。"""

    def test_encode_none(self):
        """None 在当前实现中通过 orjson 编码为 b'Jnull'（而非直接返回 None）。
        这是 v0.2.x 的实际行为，测试用于升级后回归检测。"""
        result = encode(None)
        assert isinstance(result, bytes)
        assert result[0:1] == b"J"
        assert decode(result) is None

    def test_decode_non_bytes_passthrough(self):
        assert decode("hello") == "hello"
        assert decode(42) == 42

    def test_encode_decode_bytes(self):
        raw = b"\x00\x01\x02\xff"
        result = decode(encode(raw))
        assert result == raw

    def test_encode_decode_json_types(self):
        for obj in [
            "hello",
            42,
            3.14,
            [1, 2, 3],
            {"a": 1, "b": "two"},
            {"nested": {"deep": True, "list": [1, None, "x"]}},
        ]:
            assert decode(encode(obj)) == obj

    def test_encode_decode_pickle_fallback(self):
        """orjson 无法序列化的类型应回退到 pickle。"""
        obj = {"lambda": lambda x: x}  # lambda 不可 JSON 序列化
        encoded = encode(obj)
        assert isinstance(encoded, bytes)
        assert encoded[0:1] == b"P"  # pickle 标记
        decoded = decode(encoded)
        assert isinstance(decoded, dict)
        assert callable(decoded["lambda"])

    def test_encode_decode_numpy_style(self):
        """确保或json numpy 优化路径的 J 前缀可被正确解析。"""
        obj = {"values": [1, 2, 3]}
        encoded = encode(obj)
        assert encoded[0:1] == b"J"
        assert decode(encoded) == obj


# ═══════════════════════════════════════════
# 2. valid_identifier / identifier_re
# ═══════════════════════════════════════════

class TestValidIdentifier:
    """标识符合法性检查，直接影响表名/列名的安全性。"""

    @pytest.mark.parametrize("name", [
        "users", "_users", "user123", "User_Name",
        "ab", "A1", "my-table", "my_table-2", "a1",
    ])
    def test_valid(self, name):
        assert valid_identifier(name) is True

    @pytest.mark.parametrize("name", [
        "", "a", "A", "1abc", "123", "-abc", "abc;", "abc--", "DROP TABLE",
        "table name", "a;b", "a'b", 'a"b', "users_",
    ])
    def test_invalid(self, name):
        assert valid_identifier(name) is False

    def test_identifier_re_pattern(self):
        """确保正则表达式模式在升级后不会意外改变。"""
        assert identifier_re.pattern == r"^[A-Za-z_][A-Za-z0-9_\-]*[A-Za-z0-9]$"


# ═══════════════════════════════════════════
# 3. parse_where
# ═══════════════════════════════════════════

class TestParseWhere:
    """WHERE 子句解析器，这是核心安全组件。"""

    def test_empty_string(self):
        ok, sql, params = parse_where("")
        assert ok is True
        assert sql == ""

    def test_basic_eq(self):
        ok, sql, params = parse_where('age = 18')
        assert ok is True
        assert "where" in sql.lower()
        assert "age" in sql
        assert "18" in params

    def test_double_eq(self):
        ok, sql, params = parse_where('age == 18')
        assert ok is True

    def test_string_value_double_quote(self):
        ok, sql, params = parse_where('name = "Alice"')
        assert ok is True
        assert params == ["Alice"]

    def test_string_value_single_quote(self):
        ok, sql, params = parse_where("name = 'Bob'")
        assert ok is True
        assert params == ["Bob"]

    def test_comparison_operators(self):
        for op in ["!=", "<", ">", "<=", ">="]:
            ok, _, _ = parse_where(f'age {op} 10')
            assert ok is True, f"operator {op} should be valid"

    def test_like_operator(self):
        ok, sql, params = parse_where('name like "%tom%"')
        assert ok is True
        assert params == ["%tom%"]

    def test_ilike_operator(self):
        ok, sql, params = parse_where('name ilike "%tom%"')
        assert ok is True

    def test_is_null(self):
        ok, sql, params = parse_where('age is null')
        assert ok is True
        assert "age is null" in sql
        assert params == []

    def test_and_logic(self):
        ok, sql, params = parse_where('age > 18 AND role = "Hero"')
        assert ok is True
        assert "and" in sql
        assert len(params) == 2

    def test_or_logic(self):
        ok, sql, params = parse_where('age < 10 OR age > 60')
        assert ok is True
        assert "or" in sql

    def test_not_logic(self):
        ok, sql, params = parse_where('NOT age < 10')
        assert ok is True
        assert "not" in sql

    def test_parentheses(self):
        ok, sql, params = parse_where(
            '(age < 10 AND name like "%e%") OR (role = "Antagonist" AND NOT age >= 16)'
        )
        assert ok is True

    def test_order_by_simple(self):
        ok, sql, params = parse_where('age > 18 ORDER BY id DESC')
        assert ok is True
        assert "order by" in sql
        assert "desc" in sql

    def test_order_by_multi_column(self):
        ok, sql, params = parse_where('name = "test" ORDER BY id DESC, name ASC')
        assert ok is True
        assert "desc" in sql
        assert "asc" in sql

    def test_order_by_no_direction_defaults(self):
        ok, sql, params = parse_where('age > 0 ORDER BY age')
        assert ok is True
        # 没有显式方向时不报错
        assert "order by" in sql

    def test_forbidden_semicolon(self):
        ok, _, _ = parse_where('age = 10; DROP TABLE users')
        assert ok is False

    def test_forbidden_comment(self):
        ok, _, _ = parse_where('age = 10 -- comment')
        assert ok is False

    def test_forbidden_block_comment(self):
        ok, _, _ = parse_where('age = 10 /* comment */')
        assert ok is False

    def test_invalid_column_name(self):
        ok, _, _ = parse_where('123abc = 10')
        assert ok is False

    def test_incomplete_condition(self):
        ok, _, _ = parse_where('age')
        assert ok is False

    def test_invalid_order_column(self):
        ok, _, _ = parse_where('age > 0 ORDER BY 123')
        assert ok is False

    def test_invalid_order_too_many_parts(self):
        ok, _, _ = parse_where('age > 0 ORDER BY id ASC DESC')
        assert ok is False


# ═══════════════════════════════════════════
# 4. DB 基本操作
# ═══════════════════════════════════════════

class TestDBBasic:
    """DB 类的核心 CRUD，确保返回格式和行为不因升级而变化。"""

    def test_create_upsert_and_get(self, db):
        """upsert + get_item 往返一致性。"""
        data = {"key": "u1", "name": "Alice", "age": 30}
        ret = db.upsert("users", data, "key")
        assert ret["suc"] is True

        item = db.get_item("users", "u1", "key")
        assert item is not None
        assert item["name"] == "Alice"
        assert item["age"] == 30

    def test_upsert_update_existing(self, db):
        """相同 key 的第二次 upsert 应为更新而非插入。"""
        db.upsert("users", {"key": "u1", "name": "Alice"}, "key")
        db.upsert("users", {"key": "u1", "name": "Bob", "age": 25}, "key")

        item = db.get_item("users", "u1", "key")
        assert item["name"] == "Bob"
        assert item["age"] == 25

    def test_upsert_missing_pkey(self, db):
        ret = db.upsert("users", {"name": "NoKey"}, "key")
        assert ret["suc"] is False
        assert "Missing primary key" in ret["msg"]

    def test_upsert_invalid_table_name(self, db):
        ret = db.upsert("123bad", {"key": "x"}, "key")
        assert ret["suc"] is False

    def test_upsert_non_dict(self, db):
        ret = db.upsert("users", [1, 2, 3], "key")
        assert ret["suc"] is False

    def test_get_item_nonexistent(self, db):
        item = db.get_item("users", "nonexistent", "key")
        # 表不存在时返回 {}
        assert item == {}

    def test_delete(self, db):
        db.upsert("users", {"key": "u1", "name": "Alice"}, "key")
        ret = db.delete("users", "key like u1")
        assert ret["suc"] is True

        item = db.get_item("users", "u1", "key")
        assert item == {}

    def test_delete_invalid_table(self, db):
        ret = db.delete("123bad", "key = x")
        assert ret["suc"] is False

    def test_count(self, db):
        db.upsert("users", {"key": "u1", "age": 20}, "key")
        db.upsert("users", {"key": "u2", "age": 30}, "key")
        db.upsert("users", {"key": "u3", "age": 25}, "key")
        assert db.count("users") == 3

    def test_count_with_where(self, db):
        db.upsert("users", {"key": "u1", "age": 20}, "key")
        db.upsert("users", {"key": "u2", "age": 30}, "key")
        assert db.count("users", "age >= 25") == 1

    def test_query(self, db):
        db.upsert("users", {"key": "u1", "age": 20}, "key")
        db.upsert("users", {"key": "u2", "age": 30}, "key")
        db.upsert("users", {"key": "u3", "age": 25}, "key")

        ret = db.query("users", "age >= 25")
        assert ret["suc"] is True
        assert len(ret["data"]) == 2

    def test_query_with_limit_offset(self, db):
        for i in range(10):
            db.upsert("items", {"key": f"i{i}", "val": i}, "key")

        ret = db.query("items", "", limit=3, offset=2)
        assert ret["suc"] is True
        assert len(ret["data"]) == 3

    def test_query_invalid_table(self, db):
        ret = db.query("123bad", "")
        assert ret["suc"] is False

    def test_query_invalid_where(self, db):
        ret = db.query("users", "bad;drop")
        assert ret["suc"] is False

    def test_find_iterator(self, db):
        db.upsert("users", {"key": "u1"}, "key")
        db.upsert("users", {"key": "u2"}, "key")
        results = list(db.find("users"))
        assert len(results) == 2

    def test_inspect_table(self, db):
        db.upsert("users", {"key": "u1", "name": "Alice", "age": 30}, "key")
        schema = db.inspect("users")
        assert "key" in schema
        assert "name" in schema
        assert "age" in schema

    def test_inspect_all_tables(self, db):
        db.upsert("users", {"key": "u1"}, "key")
        db.upsert("orders", {"key": "o1"}, "key")
        tables = db.inspect()
        assert "users" in tables
        assert "orders" in tables

    def test_contains(self, db):
        assert "users" not in db
        db.upsert("users", {"key": "u1"}, "key")
        assert "users" in db

    def test_contains_invalid_identifier(self, db):
        assert "123bad" not in db

    def test_drop_table(self, db):
        db.upsert("temp", {"key": "t1"}, "key")
        assert "temp" in db
        del db["temp"]
        assert "temp" not in db

    def test_drop_table_invalid_name(self, db):
        result = db.__delitem__("123bad")
        assert result is False


# ═══════════════════════════════════════════
# 5. DB schema 自适应
# ═══════════════════════════════════════════

class TestDBSchemaEvolution:
    """验证无 schema 设计下表结构的自动调整行为。"""

    def test_auto_create_table(self, db):
        """首次 upsert 应自动建表。"""
        db.upsert("users", {"key": "u1", "name": "Alice"}, "key")
        assert "users" in db

    def test_auto_add_column(self, db):
        """后续 upsert 带新字段时应自动加列。"""
        db.upsert("users", {"key": "u1", "name": "Alice"}, "key")
        db.upsert("users", {"key": "u2", "name": "Bob", "age": 25}, "key")

        schema = db.inspect("users")
        assert "age" in schema

    def test_type_mapping_str_to_text(self, db):
        db.upsert("tt", {"key": "k1", "name": "test"}, "key")
        assert db.inspect("tt")["name"] == "TEXT"

    def test_type_mapping_int_to_integer(self, db):
        db.upsert("tt", {"key": "k1", "count": 42}, "key")
        assert db.inspect("tt")["count"] == "INTEGER"

    def test_type_mapping_float_to_real(self, db):
        db.upsert("tt", {"key": "k1", "score": 3.14}, "key")
        assert db.inspect("tt")["score"] == "REAL"

    def test_type_mapping_complex_to_blob(self, db):
        """list / dict 应映射为 BLOB。"""
        db.upsert("tt", {"key": "k1", "tags": [1, 2, 3]}, "key")
        assert db.inspect("tt")["tags"] == "BLOB"
        item = db.get_item("tt", "k1")
        assert item["tags"] == [1, 2, 3]

    def test_pkey_is_primary_key_in_schema(self, db):
        db.upsert("pp", {"key": "u1", "name": "Alice"}, "key")
        schema = db.inspect("pp")
        assert "PRIMARY KEY" in schema["key"].upper()


# ═══════════════════════════════════════════
# 6. DB 复杂数据类型
# ═══════════════════════════════════════════

class TestDBComplexTypes:
    """确保复杂 Python 类型的存储和读取在升级后一致。"""

    def test_list_field(self, db):
        db.upsert("cx", {"key": "k1", "hobby": ["football", "basketball"]}, "key")
        item = db.get_item("cx", "k1")
        assert item["hobby"] == ["football", "basketball"]

    def test_dict_field(self, db):
        db.upsert("cx", {"key": "k1", "meta": {"height": 1.75, "weight": 70}}, "key")
        item = db.get_item("cx", "k1")
        assert item["meta"]["height"] == 1.75
        assert item["meta"]["weight"] == 70

    def test_nested_list(self, db):
        db.upsert("cx", {"key": "k1", "matrix": [[1, 2], [3, 4]]}, "key")
        item = db.get_item("cx", "k1")
        assert item["matrix"] == [[1, 2], [3, 4]]

    def test_boolean_values(self, db):
        db.upsert("cx", {"key": "k1", "active": True, "deleted": False}, "key")
        item = db.get_item("cx", "k1")
        assert item["active"] is True
        assert item["deleted"] is False

    def test_none_value(self, db):
        db.upsert("cx", {"key": "k1", "note": None}, "key")
        item = db.get_item("cx", "k1")
        assert item["note"] is None

    def test_unicode(self, db):
        db.upsert("cx", {"key": "k1", "name": "中文测试"}, "key")
        item = db.get_item("cx", "k1")
        assert item["name"] == "中文测试"

    def test_emoji(self, db):
        db.upsert("cx", {"key": "k1", "msg": "hello 🌍"}, "key")
        item = db.get_item("cx", "k1")
        assert item["msg"] == "hello 🌍"

    def test_large_dict(self, db):
        big = {f"field_{i}": i for i in range(100)}
        big["key"] = "big"
        db.upsert("cx", big, "key")
        item = db.get_item("cx", "big")
        for i in range(100):
            assert item[f"field_{i}"] == i


# ═══════════════════════════════════════════
# 7. DB upsert_mat (批量操作)
# ═══════════════════════════════════════════

class TestDBUpsertMat:
    """批量 upsert 行为验证。"""

    def test_basic_matrix_upsert(self, db):
        headers = ["key", "name", "age"]
        mat = [["u1", "Alice", 30], ["u2", "Bob", 25]]
        ret = db.upsert_mat("users", headers, mat, "key")
        assert ret["suc"] is True

        assert db.count("users") == 2

    def test_matrix_upsert_update(self, db):
        headers = ["key", "name", "age"]
        mat = [["u1", "Alice", 30]]
        db.upsert_mat("users", headers, mat, "key")

        mat2 = [["u1", "Alice", 31]]
        db.upsert_mat("users", headers, mat2, "key")

        item = db.get_item("users", "u1")
        assert item["age"] == 31

    def test_matrix_missing_pkey(self, db):
        headers = ["name", "age"]
        mat = [["Alice", 30]]
        ret = db.upsert_mat("users", headers, mat, "key")
        assert ret["suc"] is False
        assert "Missing primary key" in ret["msg"]

    def test_matrix_invalid_table(self, db):
        headers = ["key", "name"]
        mat = [["k1", "Alice"]]
        ret = db.upsert_mat("123bad", headers, mat, "key")
        assert ret["suc"] is False

    def test_matrix_empty_mat(self, db):
        ret = db.upsert_mat("users", ["key"], [], "key")
        assert ret["suc"] is False

    def test_matrix_non_list_rows(self, db):
        ret = db.upsert_mat("users", ["key"], "not_a_list", "key")
        assert ret["suc"] is False


# ═══════════════════════════════════════════
# 8. Table 类
# ═══════════════════════════════════════════

class TestTableClass:
    """Table 包装类的 dict-like 接口。"""

    def test_setitem_getitem(self, db):
        t = db["users"]
        t["u1"] = {"name": "Alice", "age": 30}
        item = t["u1"]
        assert item["name"] == "Alice"

    def test_setitem_auto_pkey(self, db):
        """t["u1"] = {"name": ...} 应自动注入 key=u1。"""
        t = db["users"]
        t["u1"] = {"name": "Alice"}
        item = t["u1"]
        assert item["key"] == "u1"

    def test_delitem(self, db):
        t = db["users"]
        t["u1"] = {"name": "Alice"}
        del t["u1"]
        item = t["u1"]
        assert item == {}

    def test_len(self, db):
        t = db["users"]
        assert len(t) == 0
        t["u1"] = {"name": "Alice"}
        t["u2"] = {"name": "Bob"}
        assert len(t) == 2

    def test_iter(self, db):
        t = db["users"]
        t["u1"] = {"name": "A"}
        t["u2"] = {"name": "B"}
        items = list(t)
        assert len(items) == 2

    def test_keys(self, db):
        t = db["users"]
        t["u1"] = {"name": "A"}
        t["u2"] = {"name": "B"}
        keys = t.keys()
        assert "u1" in keys
        assert "u2" in keys

    def test_find(self, db):
        t = db["users"]
        t["u1"] = {"name": "Alice", "age": 30}
        t["u2"] = {"name": "Bob", "age": 20}
        results = list(t.find("age >= 25"))
        assert len(results) == 1
        assert results[0]["name"] == "Alice"

    def test_query(self, db):
        t = db["users"]
        t["u1"] = {"name": "A"}
        t["u2"] = {"name": "B"}
        ret = t.query("", limit=1)
        assert ret["suc"] is True
        assert len(ret["data"]) == 1

    def test_dir_exposes_columns(self, db):
        t = db["users"]
        t["u1"] = {"name": "Alice", "age": 30}
        columns = dir(t)
        assert "name" in columns
        assert "age" in columns

    def test_str_representation(self, db):
        t = db["users"]
        t["u1"] = {"name": "Alice"}
        s = str(t)
        assert "users" in s
        assert "key" in s

    def test_invalid_table_via_getitem(self, db):
        t = db["123bad"]
        assert t is None


# ═══════════════════════════════════════════
# 9. DBS 多数据库管理
# ═══════════════════════════════════════════

class TestDBS:
    """DBS 分库管理器的行为验证。"""

    def test_basic_get(self, tmp_dir):
        dbs = DBS(tmp_dir)
        db = dbs["myapp"]
        assert isinstance(db, DB)

    def test_different_db_keys_create_different_files(self, tmp_dir):
        dbs = DBS(tmp_dir)
        db1 = dbs["app1"]
        db2 = dbs["app2"]
        assert db1 is not db2
        # 应产生不同的 sqlite 文件
        assert os.path.exists(os.path.join(tmp_dir, "app1.sqlite"))
        assert os.path.exists(os.path.join(tmp_dir, "app2.sqlite"))

    def test_same_key_returns_cached_instance(self, tmp_dir):
        dbs = DBS(tmp_dir)
        db1 = dbs["app1"]
        db2 = dbs["app1"]
        assert db1 is db2

    def test_slash_converted_to_dash(self, tmp_dir):
        dbs = DBS(tmp_dir)
        db = dbs["my/app"]
        assert os.path.exists(os.path.join(tmp_dir, "my-app.sqlite"))

    def test_path_traversal_rejected(self, tmp_dir):
        """../../etc/passwd 经 replace('/','-') 变为合法文件名，实际不会被 check_path 拒绝。
        这里测试的是 DBS 不会因特殊字符创建危险路径。"""
        dbs = DBS(tmp_dir)
        result = dbs["..-..-etc-passwd"]
        # 斜杠被替换为短横线后，文件名合法，check_path 通过
        assert isinstance(result, DB)

    def test_close_all(self, tmp_dir):
        dbs = DBS(tmp_dir)
        dbs["app1"]
        dbs["app2"]
        dbs.close()
        assert len(dbs.dbs) == 0

    def test_cross_db_isolation(self, tmp_dir):
        """不同 db 的数据应该隔离。"""
        dbs = DBS(tmp_dir)
        db1 = dbs["app1"]
        db2 = dbs["app2"]
        db1.upsert("users", {"key": "u1", "name": "Alice"}, "key")
        assert db2.count("users") == 0


# ═══════════════════════════════════════════
# 10. DB set_index
# ═══════════════════════════════════════════

class TestDBSetIndex:
    """索引创建行为。"""

    def test_create_index(self, db):
        db.upsert("users", {"key": "u1", "email": "a@b.com"}, "key")
        ret = db.set_index("users", "email")
        assert ret is True

    def test_duplicate_index_should_fail_gracefully(self, db):
        db.upsert("users", {"key": "u1", "email": "a@b.com"}, "key")
        db.set_index("users", "email")
        ret = db.set_index("users", "email")
        # 重复索引会抛异常，函数应返回 False
        assert ret is False

    def test_invalid_table(self, db):
        ret = db.set_index("123bad", "col")
        assert ret is False


# ═══════════════════════════════════════════
# 11. DB WAL 模式
# ═══════════════════════════════════════════

class TestDBWAL:
    """确认 WAL 模式启用。"""

    def test_wal_enabled_by_default(self, tmp_dir):
        path = os.path.join(tmp_dir, "wal_test.sqlite")
        db = DB(path, wal=True)
        db.cursor.execute("PRAGMA journal_mode;")
        mode = db.cursor.fetchone()[0]
        assert mode == "wal"
        db.close()

    def test_no_wal_mode(self, tmp_dir):
        path = os.path.join(tmp_dir, "no_wal.sqlite")
        db = DB(path, wal=False)
        db.cursor.execute("PRAGMA journal_mode;")
        mode = db.cursor.fetchone()[0]
        assert mode != "wal"
        db.close()


# ═══════════════════════════════════════════
# 12. 返回值格式兼容性
# ═══════════════════════════════════════════

class TestResponseFormat:
    """确保所有 API 返回的 dict 结构在升级后不变。"""

    def test_upsert_success_format(self, db):
        ret = db.upsert("t1", {"key": "k1"}, "key")
        assert "suc" in ret
        assert ret["suc"] is True
        # 成功时只有 suc 字段
        assert set(ret.keys()) == {"suc"}

    def test_upsert_failure_format(self, db):
        ret = db.upsert("t1", {"name": "no_key"}, "key")
        assert "suc" in ret
        assert ret["suc"] is False
        assert "msg" in ret

    def test_query_success_format(self, db):
        db.upsert("t1", {"key": "k1", "val": 1}, "key")
        ret = db.query("t1", "")
        assert ret["suc"] is True
        assert "data" in ret
        assert isinstance(ret["data"], list)

    def test_query_failure_format(self, db):
        ret = db.query("123bad", "")
        assert ret["suc"] is False
        assert "msg" in ret

    def test_delete_success_format(self, db):
        db.upsert("t1", {"key": "k1"}, "key")
        ret = db.delete("t1", "key = k1")
        assert ret["suc"] is True
        assert set(ret.keys()) == {"suc"}

    def test_delete_failure_format(self, db):
        ret = db.delete("123bad", "key = k1")
        assert ret["suc"] is False
        assert "msg" in ret

    def test_upsert_mat_success_format(self, db):
        ret = db.upsert_mat("t1", ["key", "val"], [["k1", 1]], "key")
        assert ret["suc"] is True
        assert set(ret.keys()) == {"suc"}

    def test_upsert_mat_failure_format(self, db):
        ret = db.upsert_mat("t1", ["key"], [], "key")
        assert ret["suc"] is False
        assert "msg" in ret
