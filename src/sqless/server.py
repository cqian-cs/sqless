import os
from .database import DBS
import inspect
from typing import get_origin, get_args, Literal, List
func_table = {}
tools = []
def api(func):
    """函数装饰器，用于将一个普通Python函数转换为MCP工具的JSON Schema定义。"""
    if func.__name__ not in func_table:
        properties = {}
        required_params = []
        params = []
        for name, param in inspect.signature(func).parameters.items():
            params.append(name)
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
            "description": func.__doc__ or "",
            "inputSchema": input_schema
        }
        tools.append(mcp_definition)
        func_table[func.__name__]={'f':func,'async':inspect.iscoroutinefunction(func),'params':params}
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


async def run_server(
    path_app = 'sqless_app.py',
    reset = False,
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
    Now = lambda: time.strftime("%Y%m%d-%H%M%S")
    path_this = os.path.dirname(os.path.abspath(path_app))
    os.makedirs(path_this,exist_ok=True)
    # cold-start
    path_src = os.path.dirname(os.path.abspath(__file__))
    if reset:
        filename,ext = os.path.splitext(path_app)
        os.rename(path_app,f"{filename}_bk-{Now()}{ext}")
    if not os.path.exists(path_app):
        with open(path_app,'w',encoding='utf-8') as fo:
            with open(f"{path_src}/sqless_app_example.py",'r',encoding='utf-8') as fi:
                fo.write(fi.read())
    import importlib, sys
    sys.path.append(path_this)
    app_name = os.path.splitext(os.path.split(path_app)[1])[0]
    app = importlib.import_module(app_name)
    cfg = app.cfg if hasattr(app,'cfg') else {}
    host = cfg.get('host','127.0.0.1')
    port = cfg.get('port',12239)
    secret = cfg.get('secret','abcdefghijklmnopqrstuvwxyz')
    path_base_db = cfg.get('path_base_db',f"{path_this}/db")
    path_base_fs = cfg.get('path_base_fs',f"{path_this}/fs")
    path_base_www= cfg.get('path_base_www',f"{path_this}/www")
    max_filesize= cfg.get('max_filesize',200)
    open_get_prefix= tuple(cfg.get('open_get_prefix',[]))
    # Server middlewware
    dbs = app.dbs if hasattr(app,'dbs') else DBS(path_base_db)
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
            if request.method == 'OPTIONS':
                response = web.Response(status=204)
                response.headers['Access-Control-Allow-Origin'] = '*'
                response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
                response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
                return response
            response = await handler(request)
            if response is not None:
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
        if not isinstance(data, dict):
            return web.Response(body=orjson.dumps({'suc': False, 'data': 'invalid data type'}), content_type='application/json')
        key = data.get('key')
        if not key:
            return web.Response(body=orjson.dumps({'suc': False, 'data': 'missing key'}), content_type='application/json')
        table = dbs[db_table]
        if table is None:
            return web.Response(body=orjson.dumps({'suc': False, 'data': 'invalid db/table'}), content_type='application/json')
        print(f"[{Now()}]{request['client_ip']}|POST {db_table}|{data}")
        ret = table.upsert({key: data})
        return web.Response(body=orjson.dumps(ret), content_type='application/json')

    async def handle_delete_db(request):
        db_table = request.match_info['db_table']
        where = request.match_info['where']
        table = dbs[db_table]
        if table is None:
            return web.Response(body=orjson.dumps({'suc': False, 'data': 'invalid db/table'}), content_type='application/json')
        print(f"[{Now()}]{request['client_ip']}|DELETE {db_table}|{where}")
        ret = table.delete(where)
        return web.Response(body=orjson.dumps(ret), content_type='application/json')

    async def handle_get_db(request):
        db_table = request.match_info['db_table']
        where = request.match_info['where']
        table = dbs[db_table]
        if table is None:
            return web.Response(body=orjson.dumps({'suc': False, 'data': 'invalid db/table'}), content_type='application/json')
        page = max(int(request.query.get('page', 1)), 1)
        limit = min(max(int(request.query.get('per_page', 20)), 0), 100)
        offset = (page - 1) * limit
        print(f"[{Now()}]{request['client_ip']}|GET {db_table}|{where}?page={page}&per_page={limit}")
        items = list(table.iter(where=where, limit=limit, offset=offset))
        cnt = table.count(where) or 0
        ret = {'suc': True, 'data': items, 'count': cnt}
        if limit > 0:
            ret['max_page'], rest = divmod(cnt, limit)
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
                    print(f"[{Now()}]{request['client_ip']}|CHECK {path_file}")
                    return web.Response(body=orjson.dumps({'suc': True}), content_type='application/json')
                else:
                    print(f"[{Now()}]{request['client_ip']}|DOWNLOAD {path_file}")
                    return web.FileResponse(path_file)
            elif os.path.isdir(path_file):
                if request.query.get('check') is not None:
                    print(f"[{Now()}]{request['client_ip']}|CHECK {path_file}")
                    return web.Response(body=orjson.dumps({'suc': True, 'data':sorted(os.listdir(path_file),key=natural_sort_key)}), content_type='application/json')
        if request.query.get('check') is not None:
            return web.Response(body=orjson.dumps({'suc': False}), content_type='application/json')
        else:
            return web.Response(status=404, text='File not found')

    async def handle_post_fs(request):
        try:
            suc, path_file = check_path(f"{path_base_fs}/{request.match_info['path_file']}", path_base_fs)
            print(f"[{Now()}]{request['client_ip']}|UPLOAD attempt {suc} {path_file}")
            if not suc:
                return web.Response(body=orjson.dumps({'suc': False, 'data': 'Unsafe path'}), content_type='application/json')
            folder = os.path.dirname(path_file)
            if not os.path.exists(folder):
                os.makedirs(folder, exist_ok=True)
            reader = await request.multipart()
            field = await reader.next()
            if not field:
                return web.Response(body=orjson.dumps({'suc': False, 'data': 'No file uploaded'}), content_type='application/json')
            try:
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
                        result = await call_once(func_table[tool_name], arguments, request['client_ip'])
                        response_body["result"] = {"content": [{"type": "text", "text": result if type(result)==str else orjson.dumps(result).decode()}]}
                    except Exception as tool_error:
                        response_body["error"] = {"code": -32603, "message": str(tool_error)}
                else:
                    response_body["error"] = {"code": -32601, "message": f"Tool not found: {tool_name}"}
            elif request_method == "notifications/initialized":
                await response.write_eof()
                return response
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



    async def call_once(func,kwargs,clinet_ip,_serial_number=[0]):
        serial_number = _serial_number[0]
        _serial_number[0] += 1
        print(f"[{Now()}]{clinet_ip}|FUNC CALL[{serial_number}] {func['f'].__name__} {kwargs}")
        try:
            if func['async']:
                ret = await func['f'](**kwargs)
            else:
                ret = func['f'](**kwargs)
        except Exception as e:
            ret = {'suc':False,'data':f"Tool exception: {e}"}
            with open(f"{path_this}/sqless_exception.log",'a',encoding='utf-8') as f:
                f.write(f"[{Now()}]{clinet_ip}|FUNC CALL[{serial_number}] Exception ({e})\n{func['f'].__name__} {kwargs}\n")
                f.write(traceback.format_exc())
                f.write('\n')
        print(f"[{Now()}]{clinet_ip}|FUNC RETURN[{serial_number}] {ret}")
        return ret
    async def handle_get_api(request):
        func_args = request.match_info.get('func_args')
        cmd = list(split(func_args,' '))
        func_name = cmd[0]
        if func_name not in func_table:
            return web.Response(body=orjson.dumps({"suc": False, "data": "Tool not found"}), content_type='application/json')
        func = func_table[func_name]
        kwargs = {}
        for k,v in zip(func['params'],cmd[1:]):
            try:v = ast.literal_eval(v)
            except: pass
            kwargs[k] = v
        for k,v in request.query.items():
            try:v = ast.literal_eval(v)
            except: pass
            kwargs[k] = v
        task = asyncio.create_task(call_once(func, kwargs, request['client_ip']))
        while not task.done():
            await asyncio.sleep(0.1)
            if request.transport is None or request.transport.is_closing():
                print(f"[{Now()}]{request['client_ip']}|CANCEL {'async ' if func['async'] else ''}{func_name}")
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
        task = asyncio.create_task(call_once(func, kwargs, request['client_ip']))
        while not task.done():
            await asyncio.sleep(0.1)
            if request.transport is None or request.transport.is_closing():
                print(f"[{Now()}]{request['client_ip']}|CANCEL {'async ' if func['async'] else ''}{func_name}({info_params})")
                task.cancel()
                return
        ret = await task
        return web.Response(body=orjson.dumps(ret), content_type='application/json')


    web_app = web.Application(middlewares=[cors_middleware,auth_middleware], client_max_size=max_filesize * 1024 ** 2)
    web_app.router.add_post('/db/{db_table}', handle_post_db)
    web_app.router.add_get('/db/{db_table}/{where:.*}', handle_get_db)
    web_app.router.add_delete('/db/{db_table}/{where:.*}', handle_delete_db)
    web_app.router.add_get('/fs/{path_file:.*}', handle_get_fs)
    web_app.router.add_post('/fs/{path_file:.*}', handle_post_fs)
    web_app.router.add_post('/xmlhttpRequest', handle_xmlhttpRequest)
    web_app.router.add_get('/api/{func_args:.*}',handle_get_api)
    web_app.router.add_post('/api',handle_post_api)
    web_app.router.add_post('/mcp',handle_mcp_request)
    web_app.router.add_get('/mcp_tools',lambda r:web.Response(body=orjson.dumps(tools), content_type='application/json'))
    web_app.router.add_get('/{file:.*}', handle_static)
    if hasattr(app,'on_init'):
        if inspect.iscoroutinefunction(app.on_init):
            await app.on_init()
        else:
            app.on_init()
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    print(f"Serving on http://{'127.0.0.1' if host == '0.0.0.0' else host}:{port}")
    print(f"Serving at {os.path.abspath(path_this)}")
    print(f"Loaded {len(tools)} tools")
    for i,tool in enumerate(tools):
        print(f"  {i+1} {tool['name']} {tool['description'].split('\n')[0]}")
    stop_event = asyncio.Event()
    try:
        while not stop_event.is_set():
            await asyncio.sleep(86400)
    except asyncio.CancelledError:
        pass
    finally:
        print("Cleaning up...")
        await runner.cleanup()
    if hasattr(app,'on_close'):
        if inspect.iscoroutinefunction(app.on_close):
            await app.on_close()
        else:
            app.on_close()

def main():
    import argparse
    import asyncio
    
    parser = argparse.ArgumentParser(description='Run the sqless server')
    parser.add_argument('--app', type=str, default='sqless_app.py', help='Path to configuration file')
    parser.add_argument('--reset', action='store_true', default=False, help='Reset the custom code')
    args = parser.parse_args()

    asyncio.run(run_server(
        path_app=args.app,
        reset=args.reset,
    ))

if __name__ == "__main__":
    main()
