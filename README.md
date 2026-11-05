# sqless

[English](README.md) | [中文版](README_zh.md)

A **schema-flexible, zero-abstraction** SQLite interface supporting **Relational tables, JSON tables, Full-Text Search, and Semantic Search**.

![:@sqless](https://count.getloli.com/get/@sqless?theme=booru-lewd)

---

## Why sqless is special

* **Schema-free** – the table schema adjusts automatically to the input data; no need to define tables up front.
* **High-performance** – executes SQL directly, close to raw SQLite speed, faster than many ORMs.
* **Multi-file sharding** – easily spread data across multiple SQLite files.
* **SQL-safe** – semantic parsing, parameter binding, and identifier validation block injection at the source.
* **Multi-table type** – supports Relational, JSON, Full-Text Search (FTS), and Vector tables.

---

## ⚡ Performance Test

* **Unit:** Seconds (lower is better).
* **Percentages** indicate the performance difference compared to other libraries.
    * `↑`: How much faster `sqless` is than the library (i.e., how much slower the library is).
    * `↓`: How much slower `sqless` is than raw SQLite (i.e., the overhead of `sqless`).

| Name       | Init         | Write         | Update       | Read          | Category      |
| ---------- | ------------ | ------------- | ------------ | ------------- |---------------|
| sqlalchemy | 0.022 (↑12%) | 5.572 (↑94%)  | 3.218 (↑99%) | 58.345 (↑97%) | Static schema |
| dataset    | 0.030 (↑33%) | 10.337 (↑97%) | 1.290 (↑98%) | 46.209 (↑96%) | Flex schema   |
| pony       | 0.030 (↑34%) | 2.221 (↑84%)  | 0.679 (↑96%) | 10.283 (↑84%) | Static schema |
| **sqless** | **0.020**    | **0.359**     | **0.030**    | **1.676**     | **Flex schema** |
| raw sqlite | 0.013 (↓51%) | 0.260 (↓38%)  | 0.023 (↓32%) | 1.406 (↓19%)  | Static schema |

See [benchmark/cmp_with_other_orms.py](benchmark/cmp_with_other_orms.py) for details.

---

## 🚀 Quick Start

```python
import sqless

db = sqless.DB("your.db")
table = db['users']

table.upsert({
    'U1': {'name': 'Tom', 'age': 14},
    'U2': {'name': 'Jerry', 'age': 12},
})

for item in table.iter('age < 15'):
    print(item)

table['U1']['age'] = 15   # ❌ does NOT update database
table['U1'] = {'age':15}  # ✅ updates database
print(table['U1']) # {'key': 'U1', 'name': 'Tom', 'age': 15}
```

---

## 🧠 Table Types

Table type is determined by prefix:

| Type       | Prefix  | Features                          |
| ---------- | ------- | --------------------------------- |
| Relational | none    | low-flexible schema, partial updates   |
| JSON       | `json_` | high-flexible schema, full replacement |
| FTS        | `fts_`  | full-text search                  |
| Vector     | `vec_`  | semantic search                   |

---

### 🔹 Relational Table

```python
table = db['users']
table['U1'] = {'name': 'Tom', 'age': 14}
table['U1'] = {'age': 15}
print(table['U1']) # {'key': 'U1', 'name': 'Tom', 'age': 15}
```

* Partial updates: updating one field does not affect others.

See [examples/hello-sqless.py](examples/1-hello-sqless.py) for details.

---

### 🔹 JSON Table

```python
table = db['json_users']
table['U1'] = {'name': 'Tom', 'age': 14}
table['U1'] = {'age': 15}
print(table['U1']) # {'age': 15}
```

* Full replacement: writing new data overwrites all previous fields.

See [examples/json-table-example.py](examples/3-json-table-example.py) for details.

---

### 🔹 FTS Table (Full-Text Search)

```python
table = db['fts_docs']

table.upsert({
    "A": "SQLite supports full text search"
})

results = table.search('"SQLite"')
```

See [examples/fts-table-example.py](examples/4-fts-table-example.py) for details.

---

### 🔹 Vector Table (Semantic Search)

**Requirements**:

1. Install the extra dependency: `pip install sqless[vec]`
2. Register on the SiliconFlow platform (invite link: [https://cloud.siliconflow.cn/i/szt2CkYN](https://cloud.siliconflow.cn/i/szt2CkYN))
3. Set the API key created on SiliconFlow as the environment variable `SILICON_API_KEY` (uses the free BAAI/bge-m3 embedding model)

**Usage**:
```python
table = db['vec_docs']

table.upsert({
    "A": "Deep learning is a branch of machine learning",
    "B": "NLP stands for Natural Language Processing.",
    "C": "SQLite is a lightweight database",
})

results = table.search("large language model", k=2)
```

See [examples/vec-table-example.py](examples/5-vec-table-example.py) for details.

---

## 📁 Multi-file DB

```python
dbs = sqless.DBS(folder="db_folder")

table = dbs["app-Asia-users"]
table.upsert({...})
```

Mapping rule example:

```
app-Asia-users
↓
table `users` in db_folder/app-Asia.sqlite
```

See [examples/multi-files-sqless.py](examples/2-multi-files-sqless.py) for details.

---

## 🗂 DB Operations

```python
db = sqless.DB("your.db") # create the database file
table = db["users"]       # create / get a table
dir(db)                   # list all table names in the database
del db['users']           # drop a table
db.close()                # close the connection
```

See [examples/hello-sqless.py](examples/1-hello-sqless.py) for details.

---

## Safe WHERE Expressions

`sqless` supports **safe, parameterized WHERE parsing**:

* Logical operators: `AND`, `OR`, `NOT`
* Comparison operators: `=`, `==`, `!=`, `<`, `>`, `<=`, `>=`, `like`, `ilike`, `is`, `in`
* Parentheses: `(` and `)`
* Sorting: `ORDER BY <column> [ASC|DESC]`
* SQL injection safe: forbidden characters `; -- /* */` blocked
* Values automatically bound with `?` placeholders

### Example

```python
# Filter age in [10,12,14] or name like 'Tom%', order by age ascending
where_str = "age in [10,12,14] OR name like 'Tom%' order by age asc"
success, sql, params = parse_where(where_str)
print(sql)    # where age in (?,?,?) or name like ? order by age asc
print(params) # [10, 12, 14, 'Tom%']
```

**Notes**:

* `in` requires a list value: `[10,12,14]`
* `is null` works, e.g., `age is null`
* Column names must be valid identifiers (letters, digits, underscores, Chinese characters; cannot start with a digit)
* Complex SQL functions or subqueries are **not supported**

---

## 🧭 Use Cases

* Local database tools
* Data analysis scripts
* Lightweight backend services
* AI applications (RAG / semantic search)

---

## 🔹 Developing a Backend Service with sqless

sqless ships a simple, practical backend service framework.

### Functions as a Service, Four Access Methods

1. Install dependencies
```bash
pip install aiohttp aiofiles certifi
```

2. Write a server file `sqless_app.py`, turning functions into backend endpoints with `@sqless.api`.
```python
import asyncio
import sqless

cfg = {
    'host': '127.0.0.1',
    'port': 12239,
    'secret': 'SECRET',
}

@sqless.api
def add(a: int, b: int) -> int:
    """A simple add tool"""
    return a + b

if __name__=='__main__':
    asyncio.run(sqless.run_server(__file__))
```

3. Start the backend service
```bash
python sqless_app.py
```

4. Try computing 1+2 with the backend function

**Method 1: HTTP GET (command line)**
- Open `http://127.0.0.1:12239/api/add 1 2` in a browser
    - Username: (leave empty)
    - Password: SECRET
- With curl
```bash
curl -H "Authorization: Bearer SECRET" \
    "http://127.0.0.1:12239/api/add%201%202"
```

**Method 2: HTTP GET (query params)**
- Open `http://127.0.0.1:12239/api/add?a=1&b=2` in a browser
    - Username: (leave empty)
    - Password: SECRET
- With curl
```bash
curl -H "Authorization: Bearer SECRET" \
    "http://127.0.0.1:12239/api/add?a=1&b=2"
```

**Method 3: HTTP POST**
- With curl
```bash
curl -H "Authorization: Bearer SECRET" \
     -H "Content-Type: application/json" \
     -d '{"f":"add","a":1,"b":2}' \
     http://127.0.0.1:12239/api
```

**Method 4: MCP (let AI agents call your functions)**
- Claude Code
```bash
claude mcp add --transport http sqless http://127.0.0.1:12239/mcp --header "Authorization: Bearer SECRET"
```
- Generic MCP config
```
{
  "mcpServers": {
    "sqless": {
      "type": "http",
      "url": "http://127.0.0.1:12239/mcp",
      "headers": {
        "Authorization": "Bearer SECRET"
      }
    }
  }
}
```

### One Command, Start from Scratch

Run:
```bash
sqless
```
This generates a `sqless_app.py` template in the current directory and starts the service.

### Built-in Database, File Upload, Static Site, CORS

**Database API**

```bash
# Write
curl -H "Authorization: Bearer SECRET" \
     -H "Content-Type: application/json" \
     -d '{"key":"user_1","name":"Alice","age":30}' \
     http://127.0.0.1:12239/db/users

# Query
curl -H "Authorization: Bearer SECRET" \
     "http://127.0.0.1:12239/db/users/age%20>%2020?page=1&per_page=20"

# Delete
curl -X DELETE -H "Authorization: Bearer SECRET" \
     "http://127.0.0.1:12239/db/users/key%20=%20user_1"
```

**File API**

```
# Upload
curl -H "Authorization: Bearer SECRET" \
     -F "file=@local.txt" \
     http://127.0.0.1:12239/fs/data/local.txt

# Download
curl -H "Authorization: Bearer SECRET" \
     http://127.0.0.1:12239/fs/data/local.txt -o local.txt

# Check existence
curl -H "Authorization: Bearer SECRET" \
     http://127.0.0.1:12239/fs/data/local.txt?check

# List files
curl -H "Authorization: Bearer SECRET" \
     http://127.0.0.1:12239/fs/data/?check
```

**Static Site**

Files under the `cfg['path_base_www']` directory are served directly at the root path:
```
GET /             -> www/index.html
GET /app.js       -> www/app.js
```

**CORS**

The sqless server allows cross-origin access, and also helps static sites bypass other sites' CORS restrictions:

```javascript
// HTTP GET
r = await fetch('http://127.0.0.1:12239/xmlhttpRequest',{
    "method": "POST",
    "headers": {
        "Content-Type": "application/json",
        "Authorization": "Bearer SECRET"
    },
    "body":JSON.stringify(
        {
          "method": "GET",
          "url": "https://www.example.com",
        })
    }
)
console.log(await r.json())
// HTTP POST (JSON)
r = await fetch('http://127.0.0.1:12239/xmlhttpRequest',{
    "method": "POST",
    "headers": {
        "Content-Type": "application/json",
        "Authorization": "Bearer SECRET"
    },
    "body":JSON.stringify(
        {
          "method": "POST",
          "url": "https://api.example.com/data",
          "headers": {"Content-Type": "application/json"},
          "data": {"key": "value"}
        })
    }
)
console.log(await r.json())
// HTTP POST (Form)
r = await fetch('http://127.0.0.1:12239/xmlhttpRequest',{
    "method": "POST",
    "headers": {
        "Content-Type": "application/json",
        "Authorization": "Bearer SECRET"
    },
    "data":JSON.stringify(
        {
          "method": "POST",
          "url": "https://api.example.com/upload",
          "type": "form",
          "data": {"field1": "value1"},
          "files": [
            {
              "field": "file",
              "filename": "a.png",
              "content_type": "image/png",
              "base64": "<...>"
            }
          ]
        })
    }
)
console.log(await r.json())
```

**Other Features**
- If the client disconnects mid-execution of an API/MCP call, the task is cancelled automatically to avoid wasted work.
- On an API/MCP call failure, the error is written to `sqless_exception.log` for easier debugging.
- The GET (command-line) style automatically skips quotes and brackets when splitting arguments.
    - For example, `/api/add "hello world" " 123"` yields `hello world 123`
- The GET (command-line) style automatically parses Python dicts and lists.
    - For example, `/api/add [{'a': 1}] [0]` yields `[{"a":1},0]`
- The server auto-generates MCP tool descriptions from function docstrings, parameter defaults, and type annotations.
    - For example, the `add` function above produces: `{"name":"add","description":"A simple add tool","inputSchema":{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"integer"}},"required":["a","b"]}}`
- Like the FTS and vector tables, the backend service framework loads its modules dynamically only when used.
