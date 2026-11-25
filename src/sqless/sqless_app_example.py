# pip install aiohttp aiofiles
import os
import asyncio
import sqless
import aiohttp
from aiohttp import web
import base64
import mimetypes
from urllib.parse import unquote_to_bytes

path_this :str = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")

cfg = {
    'host': '127.0.0.1',
    'port': 12239,
    'secret': 'abcdefghijklmnopqrstuvwxyz',
    'path_base_db': os.path.realpath(f"{path_this}/db"),
    'path_base_fs': os.path.realpath(f"{path_this}/fs"),
    'path_base_www': os.path.realpath(f"{path_this}/www"),
    'max_filesize': 200,  # Max file size (in MB) allowed
    'open_get_prefix': [
        '/fs/public',  # allow GET /fs/public to fetch f"{path_base_fs}/public" without secret
        '/api/hello',  # allow GET /api/hello without secret
    ],
    'open_post_prefix': [
        '/api/hello',  # allow POST /api/hello without secret
    ],
}
async def on_init(app):
    async def handler_custom(request:web.Request):
        return web.Response(text="Hello from sqless_app custom route")
    app.router.add_get('/custom',handler_custom)
    print("Enter on_init")

async def on_exit():
    print("Enter on_exit")

# --- Database Initialization ---

dbs = sqless.DBS(cfg['path_base_db'])

# --- MCP Tools / API Functions ---

@sqless.api
def hello() -> str:
    """A simple hello tool"""
    return "Hello from MCP!"

@sqless.api
def add(a: int, b: int) -> int:
    """A simple add tool"""
    return a + b

@sqless.api
def get_time(timezone: str) -> str:
    """Return current time in any timezone"""
    import datetime
    from zoneinfo import ZoneInfo
    tz = ZoneInfo(timezone)
    return datetime.datetime.now(tz).isoformat()

# Async demo with semaphore (limit concurrent invocations)
semaphore_sleep = asyncio.Semaphore(3)
@sqless.api
async def sleep(seconds: int) -> str:
    """Sleep for given seconds, demonstrating rate limiting"""
    import datetime
    t0 = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    async with semaphore_sleep:
        t1 = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        await asyncio.sleep(seconds)
        t2 = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return f"Slept for {seconds} seconds, arrive at {t0}, start at {t1}, finish at {t2}"


# There are two types of parameters in tool functions that are hidden from the client
# 1. `request` which is automatically passed in by the server.
# 2. Parameters starting with _, such as _cache, _buffer, etc.
@sqless.api
def me(request: web.Request) -> dict:
    """check client info"""
    ip = request['client_ip']
    role = 'admin' if request['admin'] else 'no_login'
    return {'ip':ip,'role':role}

# This demonstrates how tool functions receive attachment parameters
# 1. Via POST form: {f=read_file, url=file}
# 2. Via API/MCP call: 
#   - Method 1, encode the file as a URL: {'f': read_file, 'url': data:{mime};base64,{base}}
#   - Method 2, upload the file externally: {'f': read_file, 'url': http(s)://xxx}
#   - Method 3, upload via POST /fs/xxx and let the tool function read it: {'f': read_file, 'url': xxx}
@sqless.api
async def read_file(url: str) -> dict:
    """receive attachment"""
    file = None
    if type(url) is web.FileField:
        file = {'filename':url.filename,'content':url.file.read(),'content-type':url.content_type}
    if not file and type(file) is str:
        if url.startswith("data:"):
            header, _, payload = url.partition(",")
            meta = header[len("data:"):]
            is_base64 = meta.endswith(";base64")
            mime = meta[:-len(";base64")] if is_base64 else meta
            mime = mime or "text/plain"
            if is_base64:
                payload = payload.replace("-", "+").replace("_", "/")  # URL-safe 兼容
                padding = (-len(payload)) % 4
                raw = base64.b64decode(payload + "=" * padding)
            else:
                raw = unquote_to_bytes(payload)
            ext = mimetypes.guess_extension(mime) or ""
            file = {'filename':f"file{ext}",'content':raw,'content-type':mime}
        elif url.startswith(("http://", "https://")):
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                async with session.get(url) as resp:
                    resp.raise_for_status()
                    raw = await resp.read()
                    mime = resp.headers.get("Content-Type", "application/octet-stream").split(";")[0].strip()
                    filename = url.rsplit("/", 1)[-1].split("?")[0] or "file"
            file = {'filename':filename,'content':raw,'content-type':mime}
        elif os.path.exists(f"{cfg['path_base_fs']}/{url}"):
            with open(f"{cfg['path_base_fs']}/{url}", "rb") as f:
                raw = f.read()
            mime, _ = mimetypes.guess_type(url)
            mime = mime or "application/octet-stream"
            file = {'filename':os.path.basename(url),'content':raw,'content-type':mime}
    if not file:
        return {'suc':False,'data': f"Can not open {url}"}
    return {'suc':True,'data': f"{file['content'][:200]}"}



# --- Usage Notes ---
"""
MCP usage:
{
  "mcpServers": {
    "<mcp_name>": {
      "type": "http",
      "url": "http://127.0.0.1:12239/mcp",
      "headers": {
        "Authorization": "Bearer <secret>"
      }
    }
  }
}
Claude Code usage:
    claude mcp add --transport http <mcp_name> http://127.0.0.1:12239/mcp --header "Authorization: Bearer <secret>"

API usage:
    GET example:
        curl -H "Authorization: Bearer <secret>" \
            "http://127.0.0.1:12239/api/add?a=1&b=2"
    POST example:
        curl -H "Authorization: Bearer <secret>" \
             -H "Content-Type: application/json" \
             -d '{"f":"add","a":1,"b":2}' \
             http://127.0.0.1:12239/api

Browser usage:
    http://127.0.0.1:12239/api/add 1 2
        username: (empty)
        password: <secret>

Database API:
    Write:
        curl -H "Authorization: Bearer <secret>" \
             -H "Content-Type: application/json" \
             -d '{"key":"user_1","name":"Alice","age":30}' \
             http://127.0.0.1:12239/db/users
    Query:
        curl -H "Authorization: Bearer <secret>" \
             "http://127.0.0.1:12239/db/users/age%20>%2020?page=1&per_page=20"
    Delete:
        curl -X DELETE -H "Authorization: Bearer <secret>" \
             "http://127.0.0.1:12239/db/users/key%20=%20user_1"

File API:
    Upload:
        curl -H "Authorization: Bearer <secret>" -F "file=@local.txt" \
             http://127.0.0.1:12239/fs/data/local.txt
    Download:
        curl -H "Authorization: Bearer <secret>" \
             http://127.0.0.1:12239/fs/data/local.txt -o local.txt
    Check existence:
        curl -H "Authorization: Bearer <secret>" \
             http://127.0.0.1:12239/fs/data/local.txt?check
    List files:
        curl -H "Authorization: Bearer <secret>" \
             http://127.0.0.1:12239/fs/data/?check

Static site:
    Files under cfg['path_base_www'] are served at the root path:
        GET /          -> www/index.html
        GET /app.js    -> www/app.js

"""

if __name__=='__main__':
    asyncio.run(sqless.run_server(__file__))