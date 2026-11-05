# pip install aiohttp aiofiles
import os
import asyncio
import sqless
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
        '/fs/data',  # allow GET /fs/xxx to fetch f"{path_base_fs}/xxx" without secret
        '/api/hello' # allow GET /api/hello without secret
    ]
}
async def on_init():
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