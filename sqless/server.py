import os
from .database import DB

# ---------- Configuration (use env vars in production) ----------
DEFAULT_SECRET = os.environ.get("SQLESS_SECRET", None)

import inspect
from typing import get_origin, get_args, Literal, List
func_table = {}
tools = []
def api(func):
    """函数装饰器，用于将一个普通Python函数转换为MCP工具的JSON Schema定义。"""
    properties = {}
    required_params = []
    for name, param in inspect.signature(func).parameters.items():
        param_schema = {}
        py_type = param.annotation
        param_schema["type"] = {
            int:'integer',
            float:'number',
            bool:'boolean',
            str:'string',
            inspect._empty:'string'
        }.get(py_type)
        if not param_schema["type"]:
            origin = get_origin(py_type)
            args = get_args(py_type)
            if origin is Literal:
                param_schema["type"] = type(args[0]).__name__
                param_schema["enum"] = list(args)
            elif origin is list:
                param_schema["type"] = "array"
                item_type = args[0] if args else str
                if item_type is int: param_schema["items"] = {"type": "integer"}
                elif item_type is float: param_schema["items"] = {"type": "number"}
                elif item_type is bool: param_schema["items"] = {"type": "boolean"}
                else: param_schema["items"] = {"type": "string"}
            else:
                param_schema["type"] = "object"
        if param.default is not inspect._empty:
            param_schema["default"] = param.default
        else:
            required_params.append(name)
        properties[name] = param_schema
    input_schema = {
        "type": "object",
        "properties": properties,
    }
    if required_params:
        input_schema["required"] = required_params
    mcp_definition = {
        "name": func.__name__,
        "description": func.__doc__,
        "inputSchema": input_schema
    }
    tools.append(mcp_definition)
    func_table[func.__name__]={'f':func,'async':inspect.iscoroutinefunction(func)}
    def wrapper(*args,**kwargs):
        return func(*args,**kwargs)
    wrapper.__name__ = func.__name__
    wrapper.__doc__ = func.__doc__
    return wrapper


def check_path(path_file, path_base):
    normalized_path = os.path.realpath(path_file)
    try:
        if os.path.commonpath([path_base, normalized_path]) == path_base:
            return True, normalized_path
    except Exception as e:
        pass
    return False, f"unsafe path: {normalized_path}"

def split(s, sep=',', L="{[(\"'", R="}])\"'"):
    stack = []
    temp = ''
    esc = False
    for c in s:
        if c == '\\':
            esc = True
            temp += c
            continue
        if not esc and c in R and stack:
            if c == R[L.index(stack[-1])]:
                stack.pop()
        elif not esc and c in L:
            stack.append(c)
        elif c == sep and not stack:
            if temp:
                yield temp
            temp = ''
            continue
        temp += c
        esc = False
    if temp:
        yield temp


class DBS:
    def __init__(self,folder):
        self.folder = folder
        self.dbs = {}
    def __getitem__(self, db_key):
        db_key = db_key.replace('/', '-')
        if db_key not in self.dbs:
            suc, path_db = check_path(f"{self.folder}/{db_key}.sqlite", self.folder)
            if not suc:
                return False, path_db
            db = DB(path_db)
            self.dbs[db_key] = db
        return self.dbs[db_key]
    def close(self):
        for db_key in list(self.dbs.keys()):
            self.dbs[db_key].close()
            del self.dbs[db_key]
        

async def run_server(
    host='0.0.0.0',
    port=27018,
    secret=DEFAULT_SECRET,
    path_this = os.getcwd(),
    path_cfg = 'sqless_config.py',
):
    import re
    import base64
    import asyncio
    from aiohttp import web, ClientSession, FormData, ClientTimeout
    import orjson
    import aiofiles
    import ast
    import time
    import traceback
    path_src = os.path.dirname(os.path.abspath(__file__))
    num2time = lambda t=None, f="%Y%m%d-%H%M%S": time.strftime(f, time.localtime(int(t if t else time.time())))
    tspToday = lambda: int(time.time() // 86400 * 86400 - 8 * 3600)  # UTC+8 today midnight

    identifier_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-]*[A-Za-z0-9]$")
    if not secret:
        print("[ERROR] Please set SQLESS_SECRET environment variable or pass --secret <secret>")
        return
    
    path_cfg = os.path.abspath(path_cfg)
    if not os.path.exists(path_cfg):
        os.makedirs(os.path.dirname(path_cfg),exist_ok=True)
        with open(f"{path_src}/sqless_config.py",'r',encoding='utf-8') as f:
            txt = f.read()
        with open(path_cfg,'w',encoding='utf-8') as f:
            f.write(txt)
            f.write(f"""
# --- start sqless server ---
if __name__=='__main__':
    asyncio.run(sqless.run_server(
        host='{host}',
        port={port},
        secret='{secret}',
        path_this = path_this,
        path_cfg = '{os.path.split(path_cfg)[1]}',
    ))
""")
    cfg_name = os.path.splitext(os.path.split(path_cfg)[1])[0]
    import importlib, sys
    sys.path.append(path_this)
    cfg = importlib.import_module(cfg_name)
    path_base_db = cfg.path_base_db if hasattr(cfg,'path_base_db') else os.path.realpath(f"{path_this}/db")
    path_base_fs = cfg.path_base_fs if hasattr(cfg,'path_base_fs') else os.path.realpath(f"{path_this}/fs")
    path_base_www= cfg.path_base_www if hasattr(cfg,'path_base_www') else os.path.realpath(f"{path_this}/www")
    max_filesize = cfg.max_filesize if hasattr(cfg,'max_filesize') else 200 # MB
    open_get_prefix = tuple(cfg.open_get_prefix) if hasattr(cfg,'open_get_prefix') else tuple([])
    dbs = cfg.dbs if hasattr(cfg,'dbs') else DBS(path_base_db)
    print(f"path_base_db: {path_base_db}")
    print(f"path_base_fs: {path_base_fs}")
    print(f"path_base_www: {path_base_www}")
    print(f"open_get_prefix: {open_get_prefix}")

    allowed_auth_header = [
        f'Bearer {secret}',
        f"Basic {base64.b64encode((':'+secret).encode('utf-8')).decode('utf-8')}",
    ]
    async def cors_middleware(app, handler):
        async def middleware_handler(request):
        # 1. 检查是否是预检请求
            if request.method == 'OPTIONS':
                # 直接返回一个 204 No Content 响应，并附上通用的 CORS 头
                response = web.Response(status=204)
                response.headers['Access-Control-Allow-Origin'] = '*'
                response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
                response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
                return response

            # 2. 对于非 OPTIONS 请求，正常处理
            response = await handler(request)
            response.headers['Access-Control-Allow-Origin'] = '*'
            return response
        return middleware_handler
    async def auth_middleware(app, handler):
        async def middleware_handler(request):
            try:
                request['client_ip'] = request.headers.get('X-Real-IP', request.transport.get_extra_info('peername')[0])
            except (TypeError, IndexError):
                request['client_ip'] = 'unknown'
            route = request.match_info.route
            if route and getattr(route, "handler", None) == handle_static:
                return await handler(request)
            auth_header = request.headers.get('Authorization')
            if auth_header in allowed_auth_header:
                return await handler(request)
            if request.method == 'GET' and request.path.startswith(open_get_prefix):
                return await handler(request)
            if request.path == '/mcp':
                return web.json_response(
                    {"jsonrpc": "2.0", "error": {"code": -32000, "message": "Unauthorized: Invalid token"}, "id": None},
                    status=403
                )
            return web.Response(status=401,text='Unauthorized',headers={'WWW-Authenticate': 'Basic realm="sqless API"'})
        return middleware_handler
    
    async def handle_post_db(request):
        db_table = request.match_info['db_table']
        if request.content_type == 'application/json':
            data = await request.json()
        else:
            post = await request.post()
            data = dict(post)
        db_key, table = os.path.split(db_table.replace('-', '/'))
        db_key = db_key or 'default'
        if not identifier_re.fullmatch(table):
            return web.Response(body=orjson.dumps({'suc': False, 'data': 'invalid table name'}), content_type='application/json')
        #db = await get_db(db_key)
        db = dbs[db_key]
        if isinstance(db, tuple) and db[0] is False:
            return web.Response(body=orjson.dumps({'suc': False, 'data': db[1]}), content_type='application/json')
        print(f"[{num2time()}]{request['client_ip']}|POST {db_key}|{table}|{data}")
        if not isinstance(data, dict):
            return web.Response(body=orjson.dumps({'suc': False, 'data': 'invalid data type'}), content_type='application/json')
        ret = db.upsert(table, data, 'key')
        return web.Response(body=orjson.dumps(ret), content_type='application/json')

    async def handle_delete_db(request):
        db_table = request.match_info['db_table']
        db_key, table = os.path.split(db_table.replace('-', '/'))
        db_key = db_key or 'default'
        if not identifier_re.fullmatch(table):
            return web.Response(body=orjson.dumps({'suc': False, 'data': 'invalid table name'}), content_type='application/json')
        #db = await get_db(db_key)
        db = dbs[db_key]
        where = request.match_info['where']
        print(f"[{num2time()}]{request['client_ip']}|DELETE {db_key}|{table}|{where}")
        ret = db.delete(table, where)
        return web.Response(body=orjson.dumps(ret), content_type='application/json')

    async def handle_get_db(request):
        db_table = request.match_info['db_table']
        db_key, table = os.path.split(db_table.replace('-', '/'))
        db_key = db_key or 'default'
        if not identifier_re.fullmatch(table):
            return web.Response(body=orjson.dumps({'suc': False, 'data': 'invalid table name'}), content_type='application/json')
        #db = await get_db(db_key)
        db = dbs[db_key]
        where = request.match_info['where']
        page = max(int(request.query.get('page', 1)), 1)
        limit = min(max(int(request.query.get('per_page', 20)), 0), 100)
        offset = (page - 1) * limit
        print(f"[{num2time()}]{request['client_ip']}|GET {db_key}|{table}|{where}?page={page}&per_page={limit}")
        ret = db.query(table, where, limit, offset)
        if isinstance(ret, dict) and ret.get('suc') and limit > 1 and not offset:
            cnt = db.count(table, where)
            ret['count'] = cnt
            ret['max_page'], rest = divmod(ret['count'], limit)
            if rest:
                ret['max_page'] += 1
        return web.Response(body=orjson.dumps(ret), content_type='application/json')

    def natural_sort_key(s):
        return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', s)]

    async def handle_get_fs(request):
        suc, path_file = check_path(f"{path_base_fs}/{request.match_info['path_file']}", path_base_fs)
        if suc:
            if os.path.isfile(path_file):
                if request.query.get('check') is not None:
                    print(f"[{num2time()}]{request['client_ip']}|CHECK {path_file}")
                    return web.Response(body=orjson.dumps({'suc': True}), content_type='application/json')
                else:
                    print(f"[{num2time()}]{request['client_ip']}|DOWNLOAD {path_file}")
                    return web.FileResponse(path_file)
            elif os.path.isdir(path_file):
                if request.query.get('check') is not None:
                    print(f"[{num2time()}]{request['client_ip']}|CHECK {path_file}")
                    return web.Response(body=orjson.dumps({'suc': True, 'data':sorted(os.listdir(path_file),key=natural_sort_key)}), content_type='application/json')
        if request.query.get('check') is not None:
            return web.Response(body=orjson.dumps({'suc': False}), content_type='application/json')
        else:
            return web.Response(status=404, text='File not found')

    async def handle_post_fs(request):
        try:
            suc, path_file = check_path(f"{path_base_fs}/{request.match_info['path_file']}", path_base_fs)
            print(f"[{num2time()}]{request['client_ip']}|UPLOAD attempt {suc} {path_file}")
            if not suc:
                return web.Response(body=orjson.dumps({'suc': False, 'data': 'Unsafe path'}), content_type='application/json')
            folder = os.path.dirname(path_file)
            if not os.path.exists(folder):
                os.makedirs(folder, exist_ok=True)
            reader = await request.multipart()
            field = await reader.next()
            if not field:
                return web.Response(body=orjson.dumps({'suc': False, 'data': 'No file uploaded'}), content_type='application/json')
            # write file safely
            try:
                async with aiofiles.open(path_file, 'wb') as f:
                    while True:
                        chunk = await field.read_chunk()
                        if not chunk:
                            break
                        await f.write(chunk)
                # ensure uploaded file isn't executable
                try:
                    os.chmod(path_file, 0o644)
                except Exception:
                    pass
                return web.Response(body=orjson.dumps({'suc': True, 'data': 'File Saved'}), content_type='application/json')
            except Exception as e:
                return web.Response(body=orjson.dumps({'suc': False, 'data': str(e)}), content_type='application/json')
        except Exception as e:
            print(f"fs/post error: {e}\n{traceback.format_exc()}")
    async def handle_static(request):
        file = request.match_info.get('file') or 'index.html'
        suc, normalized_path = check_path(f"{path_base_www}/{file}", path_base_www)
        if suc and os.path.exists(normalized_path):
            return web.FileResponse(f"{path_base_www}/{file}")
        return web.Response(status=404, text="404 Not Found")

    async def handle_xmlhttpRequest(request):
        try:
            data = await request.json()
            method = data.get("method", "POST").upper()
            url = data.get("url")
            if not url:
                return web.Response(body=orjson.dumps({"suc": False, "text": "no url"}), content_type='application/json')
            headers = data.get("headers", {})
            payload = None
            if data.get('type') == 'form':
                payload = FormData()
                for k, v in data.get("data", {}).items():
                    payload.add_field(k, v)
                for f in data.get("files", []):
                    content = base64.b64decode(f["base64"])
                    payload.add_field(
                        name=f["field"],
                        value=content,
                        filename=f["filename"],
                        content_type=f["content_type"]
                    )
            else:
                payload = data.get('data')
            # enclose outgoing request with timeout
            timeout = ClientTimeout(total=15)
            async with ClientSession(timeout=timeout) as session:
                async with session.request(method, url, headers=headers, data=payload, allow_redirects=True) as resp:
                    text = await resp.text()
                    return web.Response(body=orjson.dumps({
                        "suc": True,
                        "status": resp.status,
                        "text": text,
                        "url": str(resp.url)
                    }), content_type='application/json')
        except Exception as e:
            return web.Response(body=orjson.dumps({"suc": False, "text": str(e)}), content_type='application/json')
    async def handle_mcp_request(request: web.Request) -> web.Response:
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
                        result = await call_once(func_table[tool_name], [], arguments)
                        response_body["result"] = {"content": [{"type": "text", "text": result if type(result)==str else orjson.dumps(result).decode()}]}
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

        except orjson.JSONDecodeError as e:
            error_body = {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}}
            msg = f"event: message\ndata: {orjson.dumps(error_body).decode()}\n\n"
            await response.write(msg.encode('utf-8'))
            return response

        except Exception as e:
            error_body = {"jsonrpc": "2.0", "id": data.get("id") if 'data' in locals() else None, "error": {"code": -32603, "message": "Internal error"}}
            msg = f"event: message\ndata: {orjson.dumps(error_body).decode()}\n\n"
            await response.write(msg.encode('utf-8'))
            return response


    
    async def call_once(func,args,kwargs):
        print(kwargs)
        try:
            if func['async']:
                ret = await func['f'](*args,**kwargs)
            else:
                ret = func['f'](*args,**kwargs)
        except Exception as e:
            ret = {'suc':False,'data':f"Tool exception: {e}"}
        return ret
    async def handle_get_api(request):
        func_args = request.match_info.get('func_args')
        cmd = list(split(func_args,' '))
        func_name = cmd[0]
        if func_name not in func_table:
            return web.Response(body=orjson.dumps({"suc": False, "data": "Tool not found"}), content_type='application/json')
        func = func_table[func_name]
        args = []
        kwargs = {}
        for x in cmd[1:]:
            try:x = ast.literal_eval(x)
            except: pass
            args.append(x)
        for k,v in request.query.items():
            try:v = ast.literal_eval(v)
            except: pass
            kwargs[k] = v
        info_params = ','.join([str(x) for x in args]+[f"{k}={v}" for k,v in kwargs.items()])
        print(f"[{num2time()}]{request['client_ip']}|CALL {'async ' if func['async'] else ''}{func_name}({info_params})")
        task = asyncio.create_task(call_once(func, args, kwargs))
        while not task.done():
            await asyncio.sleep(0.1)
            if request.transport is None or request.transport.is_closing():
                print(f"[{num2time()}]{request['client_ip']}|CANCEL {'async ' if func['async'] else ''}{func_name}({info_params})")
                task.cancel()
                return
        ret = await task
        return web.Response(body=orjson.dumps(ret), content_type='application/json')
    
    async def handle_post_api(request):
        if request.content_type == 'application/json':
            kwargs = await request.json()
        else:
            post = await request.post()
            kwargs = dict(post)
        print(kwargs)
        if 'f' not in kwargs:
            return web.Response(body=orjson.dumps({"suc": False, "data": "Miss 'f' input"}), content_type='application/json')
        func_name = kwargs.pop('f')
        if func_name not in func_table:
            return web.Response(body=orjson.dumps({"suc": False, "data": "Tool not found"}), content_type='application/json')
        func = func_table[func_name]
        info_params = ','.join([f"{k}={v}" for k,v in kwargs.items()])
        print(f"[{num2time()}]{request['client_ip']}|CALL {'async ' if func['async'] else ''}{func_name}({info_params})")
        task = asyncio.create_task(call_once(func, [], kwargs))
        while not task.done():
            await asyncio.sleep(0.1)
            if request.transport is None or request.transport.is_closing():
                print(f"[{num2time()}]{request['client_ip']}|CANCEL {'async ' if func['async'] else ''}{func_name}({info_params})")
                task.cancel()
                return
        ret = await task
        return web.Response(body=orjson.dumps(ret), content_type='application/json')


    app = web.Application(middlewares=[cors_middleware,auth_middleware], client_max_size=max_filesize * 1024 ** 2)
    app.router.add_post('/db/{db_table}', handle_post_db)
    app.router.add_get('/db/{db_table}/{where:.*}', handle_get_db)
    app.router.add_delete('/db/{db_table}/{where:.*}', handle_delete_db)
    app.router.add_get('/fs/{path_file:.*}', handle_get_fs)
    app.router.add_post('/fs/{path_file:.*}', handle_post_fs)
    app.router.add_post('/xmlhttpRequest', handle_xmlhttpRequest)
    app.router.add_get('/api/{func_args:.*}',handle_get_api)
    app.router.add_post('/api',handle_post_api)
    app.router.add_post('/mcp',handle_mcp_request)
    app.router.add_get('/mcp_tools',lambda r:web.Response(body=orjson.dumps(tools), content_type='application/json'))
    app.router.add_get('/{file:.*}', handle_static)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    print(f"Serving on http://{'127.0.0.1' if host == '0.0.0.0' else host}:{port}")
    print(f"Serving at {os.path.abspath(path_this)}")
    stop_event = asyncio.Event()
    try:
        # simplified loop, exit on Cancelled/Error
        while not stop_event.is_set():
            await asyncio.sleep(86400)
    except asyncio.CancelledError:
        pass
    finally:
        print("Cleaning up...")
        await runner.cleanup()

def main():
    import argparse
    import asyncio
    
    parser = argparse.ArgumentParser(description='Run the sqless server')
    parser.add_argument('--host', default='127.0.0.1', help='Host to bind to (default: 127.0.0.1)')
    parser.add_argument('--port', type=int, default=12239, help='Port to bind to (default: 12239)')
    parser.add_argument('--secret', default=DEFAULT_SECRET, help='Secret for authentication')
    parser.add_argument('--path', default=os.getcwd(), help=f'Base path for database and file storage (default: {os.getcwd()})')
    parser.add_argument('--cfg', type=str, default='sqless_config.py', help='Path to configuration file')
    args = parser.parse_args()
    
    asyncio.run(run_server(
        host=args.host,
        port=args.port,
        secret=args.secret,
        path_this=args.path,
        path_cfg = args.cfg
    ))

if __name__ == "__main__":
    main()