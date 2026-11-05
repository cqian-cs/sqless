# sqless

[English](README_en.md) | [中文版](README.md)

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

sqless ships a small, practical backend framework: decorate a function with `@sqless.api` and it becomes both an HTTP endpoint and an MCP tool.

#### One function is one HTTP endpoint + one MCP tool

Create `sqless_app.py`:

```python
import asyncio
import sqless

cfg = {'host': '127.0.0.1', 'port': 12239, 'secret': 'SECRET'}

@sqless.api
def add(a: int, b: int) -> int:
    """A simple add tool"""
    return a + b

if __name__ == '__main__':
    asyncio.run(sqless.run_server(__file__))
```

Start the service:

```bash
pip install aiohttp aiofiles
python3 sqless_app.py
```

#### Four ways to call a remote function

```bash
# 1. GET, command-line style
curl -H "Authorization: Bearer SECRET" "http://127.0.0.1:12239/api/add%201%202"

# 2. GET, query params
curl -H "Authorization: Bearer SECRET" "http://127.0.0.1:12239/api/add?a=1&b=2"

# 3. POST, JSON
curl -H "Authorization: Bearer SECRET" -H "Content-Type: application/json" \
     -d '{"f":"add","a":1,"b":2}' http://127.0.0.1:12239/api

# 4. MCP (Claude Code)
claude mcp add --transport http sqless http://127.0.0.1:12239/mcp --header "Authorization: Bearer SECRET"
```

The first two GET forms can also be pasted straight into a browser — password `SECRET`, username empty:

- command-line style: `http://127.0.0.1:12239/api/add 1 2`
- query params: `http://127.0.0.1:12239/api/add?a=1&b=2`

### Start from one command

```bash
sqless
```

The command generates [sqless_app.py](src/sqless/sqless_app_example.py) in the current directory.

It contains config (`cfg`), init and exit hooks (`on_init` / `on_exit`), multi-database handles (`dbs`), four example functions (`hello`, `add`, `get_time`, `sleep`), and usage notes (MCP setup, API calling styles, the `/db` database API, `/fs` file upload and download, and the `www/` static site).
