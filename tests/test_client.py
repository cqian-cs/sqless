"""
sqless client.py 兼容性测试

覆盖 RDB 远程客户端与真实 server 的集成测试。
使用 aiohttp.test_utils 启动内存服务器，RDB 通过实际 HTTP 连接访问。

使用方式:
    pytest tests/test_client.py -v
"""

import asyncio
import base64
import json
import os
import shutil
import sys
import tempfile
import time

import pytest
import orjson
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from sqless.database import DB, DBS
from sqless.server import check_path
from sqless.client import RDB


SECRET = "test_secret_client"


class ClientIntegrationTestCase(AioHTTPTestCase):
    """RDB 集成测试，启动真实 HTTP 服务器。"""

    async def get_application(self):
        self.tmp_dir = tempfile.mkdtemp(prefix="sqless_client_")
        self.path_base_db = os.path.join(self.tmp_dir, "db")
        self.path_base_fs = os.path.join(self.tmp_dir, "fs")
        self.path_base_www = os.path.join(self.tmp_dir, "www")
        for p in [self.path_base_db, self.path_base_fs, self.path_base_www]:
            os.makedirs(p, exist_ok=True)

        self.dbs = DBS(self.path_base_db)
        self.allowed_auth = [
            f'Bearer {SECRET}',
            f"Basic {base64.b64encode((':' + SECRET).encode()).decode()}",
        ]

        import re
        identifier_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-]*[A-Za-z0-9]$")

        async def auth_mw(app, handler):
            async def mw(request):
                auth = request.headers.get('Authorization')
                if auth in self.allowed_auth:
                    return await handler(request)
                return web.Response(status=401, text='Unauthorized')
            return mw

        async def cors_mw(app, handler):
            async def mw(request):
                if request.method == 'OPTIONS':
                    r = web.Response(status=204)
                    r.headers['Access-Control-Allow-Origin'] = '*'
                    r.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
                    r.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
                    return r
                r = await handler(request)
                r.headers['Access-Control-Allow-Origin'] = '*'
                return r
            return mw

        async def post_db(request):
            db_table = request.match_info['db_table']
            if request.content_type == 'application/json':
                data = await request.json()
            else:
                post = await request.post()
                data = dict(post)
            db_key, table = os.path.split(db_table.replace('-', '/'))
            db_key = db_key or 'default'
            if not identifier_re.fullmatch(table):
                return web.Response(body=orjson.dumps({'suc': False, 'data': 'invalid table name'}),
                                    content_type='application/json')
            db = self.dbs[db_key]
            if isinstance(db, tuple) and db[0] is False:
                return web.Response(body=orjson.dumps({'suc': False, 'data': db[1]}),
                                    content_type='application/json')
            if not isinstance(data, dict):
                return web.Response(body=orjson.dumps({'suc': False, 'data': 'invalid data type'}),
                                    content_type='application/json')
            ret = db.upsert(table, data, 'key')
            return web.Response(body=orjson.dumps(ret), content_type='application/json')

        async def get_db(request):
            db_table = request.match_info['db_table']
            db_key, table = os.path.split(db_table.replace('-', '/'))
            db_key = db_key or 'default'
            if not identifier_re.fullmatch(table):
                return web.Response(body=orjson.dumps({'suc': False, 'data': 'invalid table name'}),
                                    content_type='application/json')
            db = self.dbs[db_key]
            if isinstance(db, tuple) and db[0] is False:
                return web.Response(body=orjson.dumps({'suc': False, 'data': db[1]}),
                                    content_type='application/json')
            where = request.match_info['where']
            page = max(int(request.query.get('page', 1)), 1)
            limit = min(max(int(request.query.get('per_page', 20)), 0), 100)
            offset = (page - 1) * limit
            ret = db.query(table, where, limit, offset)
            if isinstance(ret, dict) and ret.get('suc') and limit > 1 and not offset:
                cnt = db.count(table, where)
                ret['count'] = cnt
                ret['max_page'], rest = divmod(ret['count'], limit)
                if rest:
                    ret['max_page'] += 1
            return web.Response(body=orjson.dumps(ret), content_type='application/json')

        async def get_fs(request):
            suc, path_file = check_path(
                f"{self.path_base_fs}/{request.match_info['path_file']}",
                self.path_base_fs
            )
            if suc and os.path.isfile(path_file):
                if request.query.get('check') is not None:
                    return web.Response(body=orjson.dumps({'suc': True}),
                                        content_type='application/json')
                return web.FileResponse(path_file)
            if request.query.get('check') is not None:
                return web.Response(body=orjson.dumps({'suc': False}),
                                    content_type='application/json')
            return web.Response(status=404)

        async def post_fs(request):
            suc, path_file = check_path(
                f"{self.path_base_fs}/{request.match_info['path_file']}",
                self.path_base_fs
            )
            if not suc:
                return web.Response(body=orjson.dumps({'suc': False, 'data': 'Unsafe path'}),
                                    content_type='application/json')
            folder = os.path.dirname(path_file)
            if folder and not os.path.exists(folder):
                os.makedirs(folder, exist_ok=True)
            reader = await request.multipart()
            field = await reader.next()
            if not field:
                return web.Response(body=orjson.dumps({'suc': False, 'data': 'No file uploaded'}),
                                    content_type='application/json')
            try:
                import aiofiles
                async with aiofiles.open(path_file, 'wb') as f:
                    while True:
                        chunk = await field.read_chunk()
                        if not chunk:
                            break
                        await f.write(chunk)
                try:
                    os.chmod(path_file, 0o644)
                except Exception:
                    pass
                return web.Response(body=orjson.dumps({'suc': True, 'data': 'File Saved'}),
                                    content_type='application/json')
            except Exception as e:
                return web.Response(body=orjson.dumps({'suc': False, 'data': str(e)}),
                                    content_type='application/json')

        app = web.Application(middlewares=[cors_mw, auth_mw])
        app.router.add_post('/db/{db_table}', post_db)
        app.router.add_get('/db/{db_table}/{where:.*}', get_db)
        app.router.add_get('/fs/{path_file:.*}', get_fs)
        app.router.add_post('/fs/{path_file:.*}', post_fs)

        return app

    async def tearDownAsync(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def _make_rdb(self):
        """创建指向测试服务器的 RDB 客户端。"""
        url = str(self.client.make_url("/")).rstrip("/")
        return RDB(url, SECRET, timeout=(5, 10))


# ═══════════════════════════════════════════
# 1. RDB 数据库操作
# ═══════════════════════════════════════════

class TestRDBDatabase(ClientIntegrationTestCase):
    """RDB 客户端的数据库操作。"""

    @unittest_run_loop
    async def test_db_set_success(self):
        rdb = self._make_rdb()
        ret = rdb.db_set("users", {"key": "u1", "name": "Alice", "age": 30})
        assert ret["suc"] is True

    @unittest_run_loop
    async def test_db_set_get_roundtrip(self):
        rdb = self._make_rdb()
        rdb.db_set("users", {"key": "u1", "name": "Alice", "age": 30})
        ret = rdb.db_get("users", "key = u1")
        assert ret["suc"] is True
        assert len(ret["data"]) == 1
        assert ret["data"][0]["name"] == "Alice"

    @unittest_run_loop
    async def test_db_set_update(self):
        rdb = self._make_rdb()
        rdb.db_set("users", {"key": "u1", "name": "Alice"})
        rdb.db_set("users", {"key": "u1", "name": "Bob", "age": 25})
        ret = rdb.db_get("users", "key = u1")
        assert ret["data"][0]["name"] == "Bob"
        assert ret["data"][0]["age"] == 25

    @unittest_run_loop
    async def test_db_get_empty_result(self):
        rdb = self._make_rdb()
        ret = rdb.db_get("nonexistent_table", "key = x")
        # 空结果格式
        assert ret["suc"] is True
        assert ret["data"] == []

    @unittest_run_loop
    async def test_db_get_with_pagination(self):
        rdb = self._make_rdb()
        for i in range(15):
            rdb.db_set("items", {"key": f"i{i}", "val": i})

        ret = rdb.db_get("items", "", page=1, limit=5)
        assert ret["suc"] is True
        assert len(ret["data"]) == 5
        assert "count" in ret
        assert ret["count"] == 15
        assert ret["max_page"] == 3

    @unittest_run_loop
    async def test_db_get_page2(self):
        rdb = self._make_rdb()
        for i in range(5):
            rdb.db_set("items", {"key": f"i{i}"})

        ret = rdb.db_get("items", "", page=2, limit=2)
        assert ret["suc"] is True
        assert len(ret["data"]) == 2

    @unittest_run_loop
    async def test_db_iter(self):
        rdb = self._make_rdb()
        for i in range(25):
            rdb.db_set("items", {"key": f"i{i}", "val": i})

        results = list(rdb.db_iter("items", ""))
        assert len(results) == 25

    @unittest_run_loop
    async def test_db_iter_empty(self):
        rdb = self._make_rdb()
        results = list(rdb.db_iter("nonexistent", ""))
        assert results == []


# ═══════════════════════════════════════════
# 2. RDB 文件存储操作
# ═══════════════════════════════════════════

class TestRDBFileStorage(ClientIntegrationTestCase):
    """RDB 客户端的文件存储操作。"""

    @unittest_run_loop
    async def test_fs_set_bytes(self):
        rdb = self._make_rdb()
        ret = rdb.fs_set("test/data.txt", b"Hello World")
        assert ret is True

    @unittest_run_loop
    async def test_fs_set_string(self):
        rdb = self._make_rdb()
        ret = rdb.fs_set("test/data.txt", "Hello String")
        assert ret is True

    @unittest_run_loop
    async def test_fs_set_dict(self):
        rdb = self._make_rdb()
        ret = rdb.fs_set("test/data.json", {"key": "value", "num": 42})
        assert ret is True

    @unittest_run_loop
    async def test_fs_set_and_get_roundtrip(self):
        rdb = self._make_rdb()
        rdb.fs_set("test/roundtrip.txt", b"roundtrip data")
        content = rdb.fs_get("test/roundtrip.txt")
        assert content == b"roundtrip data"

    @unittest_run_loop
    async def test_fs_get_nonexistent(self):
        rdb = self._make_rdb()
        content = rdb.fs_get("test/nonexistent.txt")
        assert content is None

    @unittest_run_loop
    async def test_fs_get_save_to_path(self, tmp_path):
        rdb = self._make_rdb()
        rdb.fs_set("test/file.txt", b"save to disk")
        target = str(tmp_path / "downloaded.txt")
        ret = rdb.fs_get("test/file.txt", path=target)
        assert ret is True
        with open(target, "rb") as f:
            assert f.read() == b"save to disk"

    @unittest_run_loop
    async def test_fs_get_skip_existing(self, tmp_path):
        rdb = self._make_rdb()
        # 先写入本地文件
        target = str(tmp_path / "existing.txt")
        with open(target, "w") as f:
            f.write("local content")

        # overwrite=False 时应跳过
        ret = rdb.fs_get("test/any.txt", path=target, overwrite=False)
        assert ret is True
        with open(target) as f:
            assert f.read() == "local content"

    @unittest_run_loop
    async def test_fs_check_exists(self):
        rdb = self._make_rdb()
        rdb.fs_set("test/check.txt", b"data")
        ret = rdb.fs_check("test/check.txt")
        assert ret["suc"] is True

    @unittest_run_loop
    async def test_fs_check_not_exists(self):
        rdb = self._make_rdb()
        ret = rdb.fs_check("test/nonexistent.txt")
        assert ret["suc"] is False

    @unittest_run_loop
    async def test_fs_set_file_path(self, tmp_path):
        """从本地文件路径上传。"""
        rdb = self._make_rdb()
        local_file = str(tmp_path / "upload.txt")
        with open(local_file, "w") as f:
            f.write("content from file")

        ret = rdb.fs_set("test/uploaded.txt", local_file)
        assert ret is True

    @unittest_run_loop
    async def test_fs_binary_integrity(self):
        """二进制数据往返完整性。"""
        rdb = self._make_rdb()
        binary_data = bytes(range(256)) * 10
        rdb.fs_set("test/binary.bin", binary_data)
        downloaded = rdb.fs_get("test/binary.bin")
        assert downloaded == binary_data
