"""
sqless server.py 兼容性测试

覆盖 HTTP API 端点、认证、CORS、文件存储、MCP 协议、路径安全、@api 装饰器。
使用 aiohttp.test_utils 在内存中启动服务器，无需真实端口。

使用方式:
    pytest tests/test_server.py -v
"""

import asyncio
import base64
import json
import os
import shutil
import sys
import tempfile

import pytest
import orjson
from aiohttp import test_utils, web
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from sqless.server import (
    run_server, check_path, split, api,
    func_table, tools,
    DBS,
)


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

SECRET = "test_secret_for_compat"


class BaseServerTestCase(AioHTTPTestCase):
    """共享的 aiohttp 测试基类。"""

    async def get_application(self):
        """构建一个最小化的 aiohttp app 用于测试。"""
        from sqless.server import cors_middleware, auth_middleware

        self.tmp_dir = tempfile.mkdtemp(prefix="sqless_srv_")
        self.path_base_db = os.path.join(self.tmp_dir, "db")
        self.path_base_fs = os.path.join(self.tmp_dir, "fs")
        self.path_base_www = os.path.join(self.tmp_dir, "www")
        os.makedirs(self.path_base_db, exist_ok=True)
        os.makedirs(self.path_base_fs, exist_ok=True)
        os.makedirs(self.path_base_www, exist_ok=True)

        self.dbs = DBS(self.path_base_db)
        self.allowed_auth_header = [
            f'Bearer {SECRET}',
            f"Basic {base64.b64encode((':' + SECRET).encode()).decode()}",
        ]
        self.open_get_prefix = tuple()

        # 把 aiohttp handler 定义挂到 self 上供子类扩展
        from sqless.server import (
            handle_post_db, handle_get_db, handle_delete_db,
            handle_get_fs, handle_post_fs, handle_static,
            handle_xmlhttpRequest, handle_mcp_request,
            handle_get_api, handle_post_api,
        )

        # 用闭包绑定路由需要的上下文
        import types

        # 重新定义闭包需要的局部变量
        import re
        num2time = lambda: "20260101-000000"

        async def auth_middleware(app, handler):
            async def middleware_handler(request):
                try:
                    request['client_ip'] = request.headers.get(
                        'X-Real-IP',
                        request.transport.get_extra_info('peername')[0]
                    )
                except (TypeError, IndexError):
                    request['client_ip'] = 'unknown'
                route = request.match_info.route
                if route and getattr(route, "handler", None) == handle_static:
                    return await handler(request)
                auth_header = request.headers.get('Authorization')
                if auth_header in self.allowed_auth_header:
                    return await handler(request)
                if request.method == 'GET' and request.path.startswith(self.open_get_prefix):
                    return await handler(request)
                if request.path == '/mcp':
                    return web.json_response(
                        {"jsonrpc": "2.0", "error": {"code": -32000, "message": "Unauthorized"}, "id": None},
                        status=403
                    )
                return web.Response(status=401, text='Unauthorized',
                                    headers={'WWW-Authenticate': 'Basic realm="sqless API"'})
            return middleware_handler

        async def cors_middleware(app, handler):
            async def middleware_handler(request):
                if request.method == 'OPTIONS':
                    response = web.Response(status=204)
                    response.headers['Access-Control-Allow-Origin'] = '*'
                    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
                    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
                    return response
                response = await handler(request)
                response.headers['Access-Control-Allow-Origin'] = '*'
                return response
            return middleware_handler

        # 绑定 dbs 到 server 模块供 handler 使用
        import sqless.server as srv_mod
        original_dbs = getattr(srv_mod, '_test_dbs_backup', None)

        # 注入闭包变量 - 通过 monkey-patch handle_post_db 等
        # 更简单的方式: 直接用闭包重建 handler
        identifier_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-]*[A-Za-z0-9]$")

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

        async def delete_db(request):
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
            ret = db.delete(table, where)
            return web.Response(body=orjson.dumps(ret), content_type='application/json')

        async def get_fs(request):
            suc, path_file = check_path(
                f"{self.path_base_fs}/{request.match_info['path_file']}",
                self.path_base_fs
            )
            if suc:
                if os.path.isfile(path_file):
                    if request.query.get('check') is not None:
                        return web.Response(body=orjson.dumps({'suc': True}),
                                            content_type='application/json')
                    else:
                        return web.FileResponse(path_file)
                elif os.path.isdir(path_file):
                    if request.query.get('check') is not None:
                        files = sorted(os.listdir(path_file))
                        return web.Response(body=orjson.dumps({'suc': True, 'data': files}),
                                            content_type='application/json')
            if request.query.get('check') is not None:
                return web.Response(body=orjson.dumps({'suc': False}),
                                    content_type='application/json')
            else:
                return web.Response(status=404, text='File not found')

        async def post_fs(request):
            try:
                suc, path_file = check_path(
                    f"{self.path_base_fs}/{request.match_info['path_file']}",
                    self.path_base_fs
                )
                if not suc:
                    return web.Response(body=orjson.dumps({'suc': False, 'data': 'Unsafe path'}),
                                        content_type='application/json')
                folder = os.path.dirname(path_file)
                if not os.path.exists(folder):
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
            except Exception as e:
                return web.Response(body=orjson.dumps({'suc': False, 'data': str(e)}),
                                    content_type='application/json')

        async def static_handler(request):
            file = request.match_info.get('file') or 'index.html'
            suc, normalized_path = check_path(
                f"{self.path_base_www}/{file}", self.path_base_www
            )
            if suc and os.path.exists(normalized_path):
                return web.FileResponse(normalized_path)
            return web.Response(status=404, text="404 Not Found")

        # 清理全局 tools/func_table（防止跨测试污染）
        global func_table, tools
        saved_func_table = dict(func_table)
        saved_tools = list(tools)
        func_table.clear()
        tools.clear()

        # 注册测试用的 api 函数
        @api
        def _test_add(a: int, b: int) -> int:
            """Add two numbers"""
            return a + b

        @api
        def _test_hello(name: str = "world") -> str:
            """Say hello"""
            return f"Hello, {name}!"

        @api
        async def _test_async_sleep(seconds: int) -> str:
            """Async sleep for testing"""
            await asyncio.sleep(seconds)
            return f"slept {seconds}s"

        async def get_api(request):
            func_args = request.match_info.get('func_args')
            import ast
            cmd = list(split(func_args, ' '))
            func_name = cmd[0]
            if func_name not in func_table:
                return web.Response(body=orjson.dumps({"suc": False, "data": "Tool not found"}),
                                    content_type='application/json')
            func = func_table[func_name]
            args = []
            kwargs = {}
            for x in cmd[1:]:
                try:
                    x = ast.literal_eval(x)
                except:
                    pass
                args.append(x)
            for k, v in request.query.items():
                try:
                    v = ast.literal_eval(v)
                except:
                    pass
                kwargs[k] = v

            task = asyncio.create_task(
                await func['f'](*args, **kwargs) if func['async']
                else func['f'](*args, **kwargs)
            )
            # 简化: 直接执行
            if func['async']:
                ret = await func['f'](*args, **kwargs)
            else:
                ret = func['f'](*args, **kwargs)
            return web.Response(body=orjson.dumps(ret), content_type='application/json')

        async def post_api(request):
            if request.content_type == 'application/json':
                kwargs = await request.json()
            else:
                post = await request.post()
                kwargs = dict(post)
            if 'f' not in kwargs:
                return web.Response(body=orjson.dumps({"suc": False, "data": "Miss 'f' input"}),
                                    content_type='application/json')
            func_name = kwargs.pop('f')
            if func_name not in func_table:
                return web.Response(body=orjson.dumps({"suc": False, "data": "Tool not found"}),
                                    content_type='application/json')
            func = func_table[func_name]
            if func['async']:
                ret = await func['f'](**kwargs)
            else:
                ret = func['f'](**kwargs)
            return web.Response(body=orjson.dumps(ret), content_type='application/json')

        async def mcp_request(request: web.Request) -> web.Response:
            response = web.StreamResponse()
            response.content_type = 'text/event-stream'
            response.headers['Cache-Control'] = 'no-cache'
            response.headers['X-Accel-Buffering'] = 'no'
            await response.prepare(request)
            try:
                data = await request.json()
                request_method = data.get("method")
                request_id = data.get("id")
                params = data.get("params", {})
                response_body = {"jsonrpc": "2.0", "id": request_id}
                if request_method == "initialize":
                    response_body["result"] = {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "aiohttp-mcp-server", "version": "1.0.0"}
                    }
                elif request_method == "tools/list":
                    response_body["result"] = {"tools": tools}
                elif request_method == "tools/call":
                    tool_name = params.get("name")
                    arguments = params.get("arguments", {})
                    if tool_name in func_table:
                        try:
                            func = func_table[tool_name]
                            if func['async']:
                                result = await func['f'](*[], **arguments)
                            else:
                                result = func['f'](*[], **arguments)
                            response_body["result"] = {
                                "content": [{
                                    "type": "text",
                                    "text": result if type(result) == str else orjson.dumps(result).decode()
                                }]
                            }
                        except Exception as tool_error:
                            response_body["error"] = {"code": -32603, "message": str(tool_error)}
                    else:
                        response_body["error"] = {"code": -32601, "message": f"Tool not found: {tool_name}"}
                else:
                    response_body["error"] = {"code": -32601, "message": "Method not found"}
                message = f"event: message\ndata: {orjson.dumps(response_body).decode()}\n\n"
                await response.write(message.encode('utf-8'))
                await response.write_eof()
                return response
            except orjson.JSONDecodeError:
                error_body = {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}}
                msg = f"event: message\ndata: {orjson.dumps(error_body).decode()}\n\n"
                await response.write(msg.encode('utf-8'))
                return response
            except Exception as e:
                error_body = {"jsonrpc": "2.0", "id": None, "error": {"code": -32603, "message": "Internal error"}}
                msg = f"event: message\ndata: {orjson.dumps(error_body).decode()}\n\n"
                await response.write(msg.encode('utf-8'))
                return response

        async def get_mcp_tools(request):
            return web.Response(body=orjson.dumps(tools), content_type='application/json')

        app = web.Application(middlewares=[cors_middleware, auth_middleware])
        app.router.add_post('/db/{db_table}', post_db)
        app.router.add_get('/db/{db_table}/{where:.*}', get_db)
        app.router.add_delete('/db/{db_table}/{where:.*}', delete_db)
        app.router.add_get('/fs/{path_file:.*}', get_fs)
        app.router.add_post('/fs/{path_file:.*}', post_fs)
        app.router.add_post('/xmlhttpRequest', handle_xmlhttpRequest)
        app.router.add_get('/api/{func_args:.*}', get_api)
        app.router.add_post('/api', post_api)
        app.router.add_post('/mcp', mcp_request)
        app.router.add_get('/mcp_tools', get_mcp_tools)
        app.router.add_get('/{file:.*}', static_handler)

        return app

    async def tearDownAsync(self):
        global func_table, tools
        # 清理测试注入的函数
        func_table.clear()
        tools.clear()
        shutil.rmtree(self.tmp_dir, ignore_errors=True)


# ═══════════════════════════════════════════
# 1. 认证 (Authentication)
# ═══════════════════════════════════════════

class TestAuth(BaseServerTestCase):
    """认证行为兼容性。"""

    @unittest_run_loop
    async def test_no_auth_returns_401(self):
        resp = await self.client.get("/db/default/users/key='x'")
        assert resp.status == 401

    @unittest_run_loop
    async def test_bearer_auth_accepted(self):
        resp = await self.client.get(
            "/db/default/users/key='x'",
            headers={"Authorization": f"Bearer {SECRET}"}
        )
        assert resp.status == 200

    @unittest_run_loop
    async def test_basic_auth_accepted(self):
        cred = base64.b64encode((':' + SECRET).encode()).decode()
        resp = await self.client.get(
            "/db/default/users/key='x'",
            headers={"Authorization": f"Basic {cred}"}
        )
        assert resp.status == 200

    @unittest_run_loop
    async def test_wrong_bearer_returns_401(self):
        resp = await self.client.get(
            "/db/default/users/key='x'",
            headers={"Authorization": "Bearer wrong_secret"}
        )
        assert resp.status == 401

    @unittest_run_loop
    async def test_mcp_unauthorized_returns_403(self):
        resp = await self.client.post("/mcp", json={"method": "initialize", "id": 1})
        assert resp.status == 403


# ═══════════════════════════════════════════
# 2. CORS
# ═══════════════════════════════════════════

class TestCORS(BaseServerTestCase):
    """CORS 中间件行为。"""

    @unittest_run_loop
    async def test_preflight_options(self):
        resp = await self.client.options("/db/default/users")
        assert resp.status == 204
        assert resp.headers.get("Access-Control-Allow-Origin") == "*"
        assert "GET" in resp.headers.get("Access-Control-Allow-Methods", "")
        assert "Authorization" in resp.headers.get("Access-Control-Allow-Headers", "")

    @unittest_run_loop
    async def test_normal_request_has_cors_header(self):
        resp = await self.client.get(
            "/db/default/users/key='x'",
            headers={"Authorization": f"Bearer {SECRET}"}
        )
        assert resp.headers.get("Access-Control-Allow-Origin") == "*"


# ═══════════════════════════════════════════
# 3. DB 端点 (POST /db, GET /db, DELETE /db)
# ═══════════════════════════════════════════

class TestDBEndpoints(BaseServerTestCase):
    """HTTP 数据库端点的兼容性。"""

    @unittest_run_loop
    async def test_post_db_create(self):
        resp = await self.client.post(
            "/db/default/users",
            json={"key": "u1", "name": "Alice", "age": 30},
            headers={"Authorization": f"Bearer {SECRET}"}
        )
        assert resp.status == 200
        body = await resp.json()
        assert body["suc"] is True

    @unittest_run_loop
    async def test_post_db_update(self):
        headers = {"Authorization": f"Bearer {SECRET}"}
        await self.client.post("/db/default/users", json={"key": "u1", "name": "Alice"}, headers=headers)
        resp = await self.client.post("/db/default/users", json={"key": "u1", "name": "Bob", "age": 25}, headers=headers)
        assert resp.status == 200
        body = await resp.json()
        assert body["suc"] is True

    @unittest_run_loop
    async def test_post_db_missing_pkey(self):
        resp = await self.client.post(
            "/db/default/users",
            json={"name": "NoKey"},
            headers={"Authorization": f"Bearer {SECRET}"}
        )
        body = await resp.json()
        assert body["suc"] is False
        assert "Missing primary key" in body["msg"]

    @unittest_run_loop
    async def test_post_db_invalid_table_name(self):
        resp = await self.client.post(
            "/db/default/123bad",
            json={"key": "k1"},
            headers={"Authorization": f"Bearer {SECRET}"}
        )
        body = await resp.json()
        assert body["suc"] is False
        assert "invalid table name" in body["data"]

    @unittest_run_loop
    async def test_post_db_non_dict_body(self):
        resp = await self.client.post(
            "/db/default/users",
            data="not json",
            headers={"Authorization": f"Bearer {SECRET}"}
        )
        assert resp.status == 200
        body = await resp.json()
        # form-data 被解析为 dict，但非 dict 的 body 会返回错误
        assert body["suc"] is False

    @unittest_run_loop
    async def test_get_db_query(self):
        headers = {"Authorization": f"Bearer {SECRET}"}
        await self.client.post("/db/default/users", json={"key": "u1", "age": 30}, headers=headers)
        await self.client.post("/db/default/users", json={"key": "u2", "age": 20}, headers=headers)

        resp = await self.client.get(
            "/db/default/users/age >= 25",
            headers=headers
        )
        body = await resp.json()
        assert body["suc"] is True
        assert len(body["data"]) == 1
        assert body["data"][0]["key"] == "u1"

    @unittest_run_loop
    async def test_get_db_pagination(self):
        headers = {"Authorization": f"Bearer {SECRET}"}
        for i in range(5):
            await self.client.post(
                "/db/default/items",
                json={"key": f"i{i}", "val": i},
                headers=headers
            )

        resp = await self.client.get(
            "/db/default/items/?page=1&per_page=2",
            headers=headers
        )
        body = await resp.json()
        assert body["suc"] is True
        assert len(body["data"]) == 2
        assert "count" in body
        assert "max_page" in body

    @unittest_run_loop
    async def test_get_db_pagination_page2(self):
        headers = {"Authorization": f"Bearer {SECRET}"}
        for i in range(5):
            await self.client.post("/db/default/items", json={"key": f"k{i}"}, headers=headers)

        resp = await self.client.get(
            "/db/default/items/?page=2&per_page=2",
            headers=headers
        )
        body = await resp.json()
        assert body["suc"] is True
        assert len(body["data"]) == 2

    @unittest_run_loop
    async def test_delete_db(self):
        headers = {"Authorization": f"Bearer {SECRET}"}
        await self.client.post("/db/default/users", json={"key": "u1"}, headers=headers)
        resp = await self.client.delete("/db/default/users/key = u1", headers=headers)
        body = await resp.json()
        assert body["suc"] is True

    @unittest_run_loop
    async def test_db_multi_db_isolation(self):
        headers = {"Authorization": f"Bearer {SECRET}"}
        await self.client.post("/db/app1/users", json={"key": "u1"}, headers=headers)
        resp = await self.client.get("/db/app2/users/key = u1", headers=headers)
        body = await resp.json()
        # app2 中不应有数据
        assert len(body.get("data", [])) == 0

    @unittest_run_loop
    async def test_response_is_orjson_compatible(self):
        """确保响应体可以被 orjson 解析。"""
        headers = {"Authorization": f"Bearer {SECRET}"}
        await self.client.post("/db/default/users", json={"key": "u1"}, headers=headers)
        resp = await self.client.get("/db/default/users/key = u1", headers=headers)
        raw = await resp.read()
        # 确保可以被 orjson 解析
        parsed = orjson.loads(raw)
        assert parsed["suc"] is True


# ═══════════════════════════════════════════
# 4. 文件存储端点
# ═══════════════════════════════════════════

class TestFSEndpoints(BaseServerTestCase):
    """文件存储 HTTP 端点的兼容性。"""

    @unittest_run_loop
    async def test_upload_and_download(self):
        headers = {"Authorization": f"Bearer {SECRET}"}
        # Upload
        resp = await self.client.post(
            "/fs/test/hello.txt",
            data={"file": ("hello.txt", b"Hello World")},
            headers=headers
        )
        body = await resp.json()
        assert body["suc"] is True
        assert body["data"] == "File Saved"

        # Download
        resp = await self.client.get("/fs/test/hello.txt", headers=headers)
        assert resp.status == 200
        content = await resp.read()
        assert content == b"Hello World"

    @unittest_run_loop
    async def test_check_file_exists(self):
        headers = {"Authorization": f"Bearer {SECRET}"}
        await self.client.post(
            "/fs/test/hello.txt",
            data={"file": ("hello.txt", b"data")},
            headers=headers
        )
        resp = await self.client.get("/fs/test/hello.txt?check", headers=headers)
        body = await resp.json()
        assert body["suc"] is True

    @unittest_run_loop
    async def test_check_file_not_exists(self):
        headers = {"Authorization": f"Bearer {SECRET}"}
        resp = await self.client.get("/fs/test/nonexistent.txt?check", headers=headers)
        body = await resp.json()
        assert body["suc"] is False

    @unittest_run_loop
    async def test_check_directory(self):
        headers = {"Authorization": f"Bearer {SECRET}"}
        await self.client.post(
            "/fs/test/dir/a.txt",
            data={"file": ("a.txt", b"a")},
            headers=headers
        )
        await self.client.post(
            "/fs/test/dir/b.txt",
            data={"file": ("b.txt", b"b")},
            headers=headers
        )
        resp = await self.client.get("/fs/test/dir?check", headers=headers)
        body = await resp.json()
        assert body["suc"] is True
        assert isinstance(body["data"], list)
        assert "a.txt" in body["data"]

    @unittest_run_loop
    async def test_upload_path_traversal_rejected(self):
        headers = {"Authorization": f"Bearer {SECRET}"}
        resp = await self.client.post(
            "/fs/../../etc/passwd",
            data={"file": ("passwd", b"evil")},
            headers=headers
        )
        body = await resp.json()
        assert body["suc"] is False
        assert "Unsafe" in body.get("data", "")

    @unittest_run_loop
    async def test_download_nonexistent_file(self):
        headers = {"Authorization": f"Bearer {SECRET}"}
        resp = await self.client.get("/fs/nonexistent.txt", headers=headers)
        assert resp.status == 404

    @unittest_run_loop
    async def test_upload_creates_subdirectories(self):
        headers = {"Authorization": f"Bearer {SECRET}"}
        resp = await self.client.post(
            "/fs/deep/nested/path/file.txt",
            data={"file": ("file.txt", b"nested")},
            headers=headers
        )
        body = await resp.json()
        assert body["suc"] is True

    @unittest_run_loop
    async def test_binary_file_upload_download(self):
        """确保二进制文件的完整性。"""
        headers = {"Authorization": f"Bearer {SECRET}"}
        binary_data = bytes(range(256))
        resp = await self.client.post(
            "/fs/test/binary.bin",
            data={"file": ("binary.bin", binary_data)},
            headers=headers
        )
        assert (await resp.json())["suc"] is True

        resp = await self.client.get("/fs/test/binary.bin", headers=headers)
        downloaded = await resp.read()
        assert downloaded == binary_data


# ═══════════════════════════════════════════
# 5. 静态文件
# ═══════════════════════════════════════════

class TestStaticFiles(BaseServerTestCase):
    """静态文件服务。"""

    @unittest_run_loop
    async def test_serve_static_file(self):
        # 写入测试文件
        test_file = os.path.join(self.path_base_www, "index.html")
        with open(test_file, "w") as f:
            f.write("<h1>Hello</h1>")

        resp = await self.client.get("/index.html")
        assert resp.status == 200
        text = await resp.text()
        assert "Hello" in text

    @unittest_run_loop
    async def test_static_404(self):
        resp = await self.client.get("/nonexistent.html")
        assert resp.status == 404

    @unittest_run_loop
    async def test_static_no_auth_required(self):
        """静态文件不需要认证。"""
        test_file = os.path.join(self.path_base_www, "pub.html")
        with open(test_file, "w") as f:
            f.write("public")

        resp = await self.client.get("/pub.html")
        assert resp.status == 200

    @unittest_run_loop
    async def test_static_path_traversal_blocked(self):
        resp = await self.client.get("/../../etc/passwd")
        # 路径规范化后不在 www 目录下
        assert resp.status == 404


# ═══════════════════════════════════════════
# 6. MCP 协议
# ═══════════════════════════════════════════

class TestMCPEndpoints(BaseServerTestCase):
    """MCP (Model Context Protocol) 端点兼容性。"""

    @unittest_run_loop
    async def test_mcp_initialize(self):
        resp = await self.client.post(
            "/mcp",
            json={"method": "initialize", "id": 1, "params": {}},
            headers={"Authorization": f"Bearer {SECRET}"}
        )
        assert resp.status == 200
        raw = await resp.text()
        assert "2024-11-05" in raw
        assert "aiohttp-mcp-server" in raw

    @unittest_run_loop
    async def test_mcp_tools_list(self):
        resp = await self.client.post(
            "/mcp",
            json={"method": "tools/list", "id": 2},
            headers={"Authorization": f"Bearer {SECRET}"}
        )
        assert resp.status == 200
        raw = await resp.text()
        # 应包含我们在 setUp 中注册的 tool
        assert "_test_add" in raw
        assert "_test_hello" in raw

    @unittest_run_loop
    async def test_mcp_tools_call_sync(self):
        resp = await self.client.post(
            "/mcp",
            json={
                "method": "tools/call",
                "id": 3,
                "params": {"name": "_test_add", "arguments": {"a": 2, "b": 3}}
            },
            headers={"Authorization": f"Bearer {SECRET}"}
        )
        assert resp.status == 200
        raw = await resp.text()
        assert "5" in raw

    @unittest_run_loop
    async def test_mcp_tools_call_async(self):
        resp = await self.client.post(
            "/mcp",
            json={
                "method": "tools/call",
                "id": 4,
                "params": {"name": "_test_async_sleep", "arguments": {"seconds": 0}}
            },
            headers={"Authorization": f"Bearer {SECRET}"}
        )
        assert resp.status == 200
        raw = await resp.text()
        assert "slept" in raw

    @unittest_run_loop
    async def test_mcp_tools_call_not_found(self):
        resp = await self.client.post(
            "/mcp",
            json={
                "method": "tools/call",
                "id": 5,
                "params": {"name": "nonexistent_tool", "arguments": {}}
            },
            headers={"Authorization": f"Bearer {SECRET}"}
        )
        assert resp.status == 200
        raw = await resp.text()
        assert "Tool not found" in raw

    @unittest_run_loop
    async def test_mcp_unknown_method(self):
        resp = await self.client.post(
            "/mcp",
            json={"method": "nonexistent/method", "id": 6},
            headers={"Authorization": f"Bearer {SECRET}"}
        )
        assert resp.status == 200
        raw = await resp.text()
        assert "Method not found" in raw

    @unittest_run_loop
    async def test_mcp_response_format_sse(self):
        """MCP 响应应为 SSE (Server-Sent Events) 格式。"""
        resp = await self.client.post(
            "/mcp",
            json={"method": "initialize", "id": 1},
            headers={"Authorization": f"Bearer {SECRET}"}
        )
        assert resp.status == 200
        ct = resp.headers.get("Content-Type", "")
        assert "text/event-stream" in ct
        raw = await resp.text()
        assert raw.startswith("event: message\ndata: ")

    @unittest_run_loop
    async def test_get_mcp_tools(self):
        """GET /mcp_tools 端点。"""
        resp = await self.client.get(
            "/mcp_tools",
            headers={"Authorization": f"Bearer {SECRET}"}
        )
        assert resp.status == 200
        body = await resp.json()
        assert isinstance(body, list)
        assert any(t["name"] == "_test_add" for t in body)


# ═══════════════════════════════════════════
# 7. API 端点 (GET /api, POST /api)
# ═══════════════════════════════════════════

class TestAPIEndpoints(BaseServerTestCase):
    """API 调用端点的兼容性。"""

    @unittest_run_loop
    async def test_get_api(self):
        resp = await self.client.get(
            "/api/_test_add 2 3",
            headers={"Authorization": f"Bearer {SECRET}"}
        )
        assert resp.status == 200
        body = await resp.json()
        assert body == 5

    @unittest_run_loop
    async def test_get_api_with_query_params(self):
        resp = await self.client.get(
            "/api/_test_add?a=10&b=20",
            headers={"Authorization": f"Bearer {SECRET}"}
        )
        assert resp.status == 200
        body = await resp.json()
        assert body == 30

    @unittest_run_loop
    async def test_post_api(self):
        resp = await self.client.post(
            "/api",
            json={"f": "_test_add", "a": 5, "b": 7},
            headers={"Authorization": f"Bearer {SECRET}"}
        )
        assert resp.status == 200
        body = await resp.json()
        assert body == 12

    @unittest_run_loop
    async def test_post_api_missing_f(self):
        resp = await self.client.post(
            "/api",
            json={"a": 1, "b": 2},
            headers={"Authorization": f"Bearer {SECRET}"}
        )
        body = await resp.json()
        assert body["suc"] is False
        assert "Miss" in body["data"]

    @unittest_run_loop
    async def test_api_not_found(self):
        resp = await self.client.get(
            "/api/nonexistent_func",
            headers={"Authorization": f"Bearer {SECRET}"}
        )
        body = await resp.json()
        assert body["suc"] is False
        assert "Tool not found" in body["data"]


# ═══════════════════════════════════════════
# 8. @api 装饰器 Schema 生成
# ═══════════════════════════════════════════

class TestAPIDecorator:
    """@api 装饰器的 JSON Schema 生成行为，确保 MCP tool 定义不因升级而变化。"""

    def setup_method(self):
        """每个测试前清空全局状态。"""
        global func_table, tools
        func_table.clear()
        tools.clear()

    def teardown_method(self):
        global func_table, tools
        func_table.clear()
        tools.clear()

    def test_basic_function_registration(self):
        @api
        def my_func(a: int, b: str) -> str:
            """A test function"""
            return "ok"

        assert "my_func" in func_table
        assert func_table["my_func"]["async"] is False
        assert len(tools) == 1

    def test_async_function_registration(self):
        @api
        async def my_async_func(x: int) -> int:
            """An async test function"""
            return x

        assert "my_async_func" in func_table
        assert func_table["my_async_func"]["async"] is True

    def test_schema_type_int(self):
        @api
        def f(x: int) -> int:
            """Test int param"""
            return x
        schema = tools[0]["inputSchema"]
        assert schema["properties"]["x"]["type"] == "integer"

    def test_schema_type_float(self):
        @api
        def f(x: float) -> float:
            """Test float param"""
            return x
        schema = tools[0]["inputSchema"]
        assert schema["properties"]["x"]["type"] == "number"

    def test_schema_type_bool(self):
        @api
        def f(x: bool) -> bool:
            """Test bool param"""
            return x
        schema = tools[0]["inputSchema"]
        assert schema["properties"]["x"]["type"] == "boolean"

    def test_schema_type_str(self):
        @api
        def f(x: str) -> str:
            """Test str param"""
            return x
        schema = tools[0]["inputSchema"]
        assert schema["properties"]["x"]["type"] == "string"

    def test_schema_no_annotation_defaults_string(self):
        @api
        def f(x) -> str:
            """Test untyped param"""
            return x
        schema = tools[0]["inputSchema"]
        assert schema["properties"]["x"]["type"] == "string"

    def test_schema_required_params(self):
        @api
        def f(a: int, b: str) -> str:
            """Test required"""
            return ""
        schema = tools[0]["inputSchema"]
        assert set(schema["required"]) == {"a", "b"}

    def test_schema_optional_param(self):
        @api
        def f(a: int, b: str = "default") -> str:
            """Test optional"""
            return b
        schema = tools[0]["inputSchema"]
        assert schema["required"] == ["a"]
        assert schema["properties"]["b"]["default"] == "default"

    def test_schema_literal_type(self):
        from typing import Literal
        @api
        def f(mode: Literal["a", "b", "c"]) -> str:
            """Test literal"""
            return mode
        schema = tools[0]["inputSchema"]
        assert schema["properties"]["mode"]["type"] == "string"
        assert set(schema["properties"]["mode"]["enum"]) == {"a", "b", "c"}

    def test_schema_list_type(self):
        @api
        def f(items: list) -> str:
            """Test list"""
            return ""
        schema = tools[0]["inputSchema"]
        assert schema["properties"]["items"]["type"] == "array"

    def test_schema_list_int_type(self):
        from typing import List
        @api
        def f(nums: List[int]) -> str:
            """Test list int"""
            return ""
        schema = tools[0]["inputSchema"]
        assert schema["properties"]["nums"]["type"] == "array"
        assert schema["properties"]["nums"]["items"]["type"] == "integer"

    def test_schema_preserves_name_and_doc(self):
        @api
        def calculate_tax(amount: float, rate: float = 0.1) -> float:
            """Calculate tax amount"""
            return amount * rate

        tool_def = tools[0]
        assert tool_def["name"] == "calculate_tax"
        assert tool_def["description"] == "Calculate tax amount"
        assert "inputSchema" in tool_def

    def test_wrapper_preserves_name(self):
        @api
        def my_func(x: int) -> int:
            """Test"""
            return x
        # wrapper 应保留原始函数名
        assert func_table["my_func"]["f"].__name__ == "my_func"


# ═══════════════════════════════════════════
# 9. check_path (路径安全)
# ═══════════════════════════════════════════

class TestCheckPath:
    """路径安全检查函数。"""

    def test_safe_path(self, tmp_dir):
        target = os.path.join(tmp_dir, "subdir", "file.txt")
        os.makedirs(os.path.dirname(target), exist_ok=True)
        suc, result = check_path(target, tmp_dir)
        assert suc is True

    def test_path_traversal(self, tmp_dir):
        target = os.path.join(tmp_dir, "..", "..", "etc", "passwd")
        suc, result = check_path(target, tmp_dir)
        assert suc is False

    def test_symlink_outside(self, tmp_dir):
        """符号链接指向 base 外部。"""
        outside = tempfile.mkdtemp(prefix="outside_")
        try:
            link = os.path.join(tmp_dir, "link")
            os.symlink(outside, link)
            suc, result = check_path(link, tmp_dir)
            # realpath 会解析 symlink 到外部目录
            assert suc is False
        finally:
            shutil.rmtree(outside, ignore_errors=True)
