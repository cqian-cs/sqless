# sqless

An async HTTP server for SQLite, FileStorage, WebPage, WebAPI and [Model Context Protocol (MCP)](https://modelcontextprotocol.io/docs/getting-started/intro) .

It is also an ORM for lazy people, similar to [dataset](https://github.com/pudo/dataset).

## Why sqless is special:
- **Schema free**. Auto adjusts SQLite schema to fit JSON inputs.
- **High performance**. Faster than many ORMs, see [performance test](#performance-test).
- **Minimal setup**. Just `pip install sqless` to run the server.
- **Multi-file sharding**. Easily store data across multiple SQLite files.
- **SQL-safe**. Uses semantic parsing, parameter binding, and identifier validation.



## Installation

```bash
pip install sqless
```

## Quick Start

### Running the server

```bash
sqless --host 127.0.0.1 --port 12239 --secret your-secret-key
```

This will create `www` directory in the current directory, which is used for WebPage.
You can access the `www/index.html` at `http://127.0.0.1:12239/index.html`

It will also creates `db` and `fs` directories in the current directory, when saving data by database API and file storage API.

### Using the database API

```python
import requests

# Set up the base URL and authentication
BASE_URL = "http://127.0.0.1:12239"
SECRET = "your-secret-key"
DB_TABLE = "users"

# Insert or update data
r = requests.post(
    f"{BASE_URL}/db/{DB_TABLE}",
    headers={"Authorization": f"Bearer {SECRET}"},
    json={"key": "U001", "name": "Tom", "age": 14}
)

# Query data
r = requests.get(
    f"{BASE_URL}/db/{DB_TABLE}/key = U001",
    headers={"Authorization": f"Bearer {SECRET}"}
)

# Fuzzy query
r = requests.get(
    f"{BASE_URL}/db/{DB_TABLE}/name like %om%?limit=10&page=1",
    headers={"Authorization": f"Bearer {SECRET}"}
)

# Value query
r = requests.get(
    f"{BASE_URL}/db/{DB_TABLE}/age > 10?limit=10&page=1",
    headers={"Authorization": f"Bearer {SECRET}"}
)

# Delete data
r = requests.delete(
    f"{BASE_URL}/db/{DB_TABLE}/key = U001",
    headers={"Authorization": f"Bearer {SECRET}"}
)
```
sqless does not limit you to one database.

You can access **many SQLite databases** by using a separator in the table name `DB_TABLE`.

Example:
```
/db/users              -> db/default.sqlite (table: users)
/db/mall-users         -> db/mall.sqlite    (table: users)
/db/east-mall-users    -> db/east/mall.sqlite (table: users)
```

### Using the FileStorage API
```python
import requests

# Upload a file to ./fs/example.txt
with open("example.txt", "rb") as f:
    r = requests.post(
        f"{BASE_URL}/fs/example.txt",
        headers={"Authorization": f"Bearer {SECRET}"},
        files={"file": f}
    )

# Check if a file exists
r = requests.get(
    f"{BASE_URL}/fs/example.txt?check=1",
    headers={"Authorization": f"Bearer {SECRET}"}
)

# Download a file
r = requests.get(
    f"{BASE_URL}/fs/example.txt",
    headers={"Authorization": f"Bearer {SECRET}"},
    stream=True
)
with open("downloaded_example.txt", "wb") as f:
    for chunk in r.iter_content(chunk_size=8192):
        f.write(chunk)
```

### Using the Proxy API
```python
import requests
import base64

payload = {
    "method": "POST",
    "url": "https://httpbin.org/post",
    "headers": {
        "User-Agent": "SQLESS-Client/1.0",
        "Authorization": "Bearer mytoken"
    },
    "type": "form",
    "data": {"foo": "bar"},
    "files": [
        {
            "field": "file1",
            "filename": "example.txt",
            "content_type": "text/plain",
            "base64": base64.b64encode(open("example.txt", "rb").read()).decode()
        }
    ]
}

r = requests.post(
    f"{BASE_URL}/xmlhttpRequest",
    headers={"Authorization": f"Bearer {SECRET}"},
    json=payload
)
print(r.json())
```

## Use sqless as an ORM

Example:
```python
import sqless
db = sqless.DB(path_db = "your_database.db")

# create/get the "users" table
users = db['users']
# upsert item
users['U0001'] = {"name": "Tom", 'age':12, 'sex':'M', 'hobby':["football", 'basketball'],'meta':{"height": 1.75, "weight": 70}}
# get item
print(users['U0001'])

# query data from the "users" table
r = db.query("users", 'age > 9')
if r['suc']:
    print(r['data']) # result list
else:
    print(r['msg']) # error message
```

## Use sqless as remote databases
```
# Server: 
#     sqless --host 0.0.0.0 --secret RANDOM_PASSWORD
#   optional:
#       --host 127.0.0.1  Host
#       --port 12239      Port
#       --path ./         Home folder
#
# Client:
#     ↓ ↓ ↓
import os
import sqless

# [1/7] connect to remote sqless server
rdb = sqless.RDB("http://127.0.0.1:12239","RANDOM_PASSWORD")

# [2/7] fs_set(key,path_or_data,retry=5): upload to remote
print(rdb.fs_set("demo/image.png","D:/1.png"))    # if path exists, upload file
print(rdb.fs_set("demo/1.txt","hello world"))     # if path not exist, upload data
print(rdb.fs_set("demo/10.txt",b'\x01\x02\x03'))  # data can be str, bytes, list, dict
print(rdb.fs_set("demo/2.txt",["hello", "world"]))
print(rdb.fs_set("demo/22.txt",{"a":1,"b":2}))


# [3/7] fs_get(key,path_or_none,overwrite=False,retry=3): download from remote
print(rdb.fs_get("demo/image.png","D:/2.png", overwrite=True)) # download and overwrite local file
print(rdb.fs_get("demo/image.png","D:/2.png")) # skip if local file exists
print(rdb.fs_get("demo/1.txt"))  # b'hello world'
print(rdb.fs_get("demo/2.txt"))  # b'["hello","world"]'
print(rdb.fs_get("demo/10.txt")) # b'\x01\x02\x03'
print(rdb.fs_get("demo/22.txt")) # b'{"a":1,"b":2}'

# [4/7] fs_check(key):
#   1. check if a file exists (without downloading).
print(rdb.fs_check("demo/image.png"))   # {'suc': True}
print(rdb.fs_check("demo/404.png"))     # {'suc': False}
#   2. list filenames in natural order.
print(rdb.fs_check("demo")) # {'suc': True, 'data': ['1.txt', '2.txt', '10.txt', '22.txt', 'image.png']}


# [5/7] db_set(db_table, data, retry=5): insert or update data using the 'key' field
print(rdb.db_set("demo-users", {'key':'U0001', 'name':'Tom', 'age':14, 'species':'Cat', 'role':'Protagonist'}))       # {'suc': True}
print(rdb.db_set("demo-users", {'key':'U0002', 'name':'Jerry', 'age':12, 'species':'Mouse', 'role':'Protagonist'}))   # {'suc': True}
print(rdb.db_set("demo-users", {'key':'U0003', 'name':'Spike', 'age':8, 'species':'Dog', 'role':'Supporting'}))       # {'suc': True}
print(rdb.db_set("demo-users", {'key':'U0004', 'name':'Tyke', 'age':6, 'species':'Dog', 'role':'Supporting'}))        # {'suc': True}
print(rdb.db_set("demo-users", {'key':'U0005', 'name':'Butch', 'age':15, 'species':'Cat', 'role':'Antagonist'}))      # {'suc': True}
print(rdb.db_set("demo-users", {'key':'U0006', 'name':'Tuffy', 'age':5, 'species':'Mouse', 'role':'Supporting'}))     # {'suc': True}
print(rdb.db_set("demo-users", {'key':'U0007', 'name':'Toodles', 'age':13, 'species':'Cat', 'role':'Supporting'}))    # {'suc': True}
print(rdb.db_set("demo-users", {'key':'U0008', 'name':'Nibbles', 'age':6, 'species':'Mouse', 'role':'Supporting'}))   # {'suc': True}
print(rdb.db_set("demo-users", {'key':'U0009', 'name':'Quacker', 'age':6, 'species':'Duck', 'role':'Supporting'}))    # {'suc': True}
print(rdb.db_set("demo-users", {'key':'U0010', 'name':'Lightning', 'age':16, 'species':'Cat', 'role':'Antagonist'}))  # {'suc': True}

# [6/7] db_get(db_table,where,page=1,limit=20): query one page of data
print(rdb.db_get("demo-users", '(age < 10 and name like "%e%") OR (role = "Antagonist" and not age >= 16) order by age desc, name asc'))
#{
#    'suc': True,
#    'data': [
#        {'key': 'U0005', 'name': 'Butch', 'age': 15, 'species': 'Cat', 'role': 'Antagonist'},
#        {'key': 'U0003', 'name': 'Spike', 'age': 8, 'species': 'Dog', 'role': 'Supporting'},
#        {'key': 'U0008', 'name': 'Nibbles', 'age': 6, 'species': 'Mouse', 'role': 'Supporting'},
#        {'key': 'U0009', 'name': 'Quacker', 'age': 6, 'species': 'Duck', 'role': 'Supporting'},
#        {'key': 'U0004', 'name': 'Tyke', 'age': 6, 'species': 'Dog', 'role': 'Supporting'}
#    ],
#    'count': 5,
#    'max_page': 1
#}

# [7/7] db_iter(db_table,where): iterate over all data
for user in rdb.db_iter("demo-users",'(age < 10 and name like "%e%") OR (role = "Antagonist" and not age >= 16) order by age desc, name asc'):
    print(user) # {'key': 'U0005', 'name': 'Butch', 'age': 15, 'species': 'Cat', 'role': 'Antagonist'}

for user in rdb.db_iter("demo-users",''): # An empty where='' returns all data
    print(user) # {'key': 'U0001', 'name': 'Tom', 'age': 14, 'species': 'Cat', 'role': 'Protagonist'}
```

## Use sqless as API and MCP Server

After running `sqless --secret RANDOM_PASSWORD`, it will create a `sqless_config.py` at the current directory.

You can modify the demo functions, wrap with `@sqless.api`, restart `sqless`, then use your functions in both MCP and API modes.

The MCP functions are automatically registered as API endpoints, providing dual functionality.

When running long tasks, if client connection closes, the task will be automatically canceled.

```python
@sqless.api
def add(a: int, b: int) -> int:
    """A simple add tool"""
    return a + b
```

- MCP usage:
    - URL: http://127.0.0.1:12239/mcp
    - Header: `Authorization`: `Bearer <secret>`

- Claude Code usage:
    - `claude mcp add --transport http <mcp_name> http://127.0.0.1:12239/mcp --header "Authorization: Bearer <secret>"`

- API usage:
    - GET example:
        ```
        curl -H "Authorization: Bearer <secret>" \
            "http://127.0.0.1:12239/api/add?a=1&b=2"
        ```
    - POST example:
        ```
        curl -X POST \
             -H "Authorization: Bearer <secret>" \
             -H "Content-Type: application/json" \
             -d '{"f":"add","a":1,"b":2}' \
             http://127.0.0.1:12239/api
        ```
- Browser (address bar) usage:
    - `http://127.0.0.1:12239/api/add 1 2`
    - username: (empty)
    - password: `<secret>`


## Performance Test

Run the benchmark script:

```bash
pip install dataset pony sqlalchemy prettytable
python3 ./benchmark/cmp_with_other_orms.py
```

Test machine: **AMD EPYC 7K62 (4 cores) @ 2.595GHz, Ubuntu 22.04.5 LTS x86_64**

Result: 
```
| name       | init (s)        | write (s)         | read (s)         |
| ---------- | --------------- | ----------------- | ---------------- |
| dataset    | 0.006 (↑94.69%) | 2.932 (↑43.84%)   | 21.421 (↑95.89%) |
| pony.orm   | 0.015 (↑97.85%) | 0.040 (↓4013.15%) | 11.617 (↑92.43%) |
| sqlalchemy | 0.009 (↑96.45%) | 5.357 (↑69.26%)   | 27.279 (↑96.78%) |
| sqless     | 0.000 (↑0.00%)  | 1.647 (↑0.00%)    | 0.879 (↑0.00%)   |
```


## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
