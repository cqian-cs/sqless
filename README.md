# sqless

A **schema-flexible, zero-abstraction SQLite interface** supporting **Relational tables, JSON tables, Full-Text Search, and Semantic Search**.

---

## Why sqless is special

* **Schema-free** – automatically adjusts the table schema to fit the input data structure.
* **High-performance** – executes SQL directly, close to raw SQLite speed, faster than many ORMs.
* **Multi-file sharding** – store data across multiple SQLite files easily.
* **SQL-safe** – semantic parsing, parameter binding, and identifier validation prevent SQL injection.
* **Multi-table type** – supports Relational, JSON, Full-Text Search (FTS), and Vector tables.

---

## ⚡ Performance Test

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

See [hello-sqless.py](examples/1-hello-sqless.py) for details.

---

### 🔹 JSON Table

```python
table = db['json_users']
table['U1'] = {'name': 'Tom', 'age': 14}
table['U1'] = {'age': 15}
print(table['U1']) # {'age': 15}
```

* Full replacement: writing field A will overwrite old fields.

See [json-table-example.py](examples/3-json-table-example.py) for details.
---

### 🔹 FTS Table (Full-Text Search)

```python
table = db['fts_docs']

table.upsert({
    "A": "SQLite supports full text search"
})

results = table.search('"SQLite"')
```

See [fts-table-example.py](examples/4-fts-table-example.py) for details.

---

### 🔹 Vector Table (Semantic Search)

**Requirements**:

1. `pip install sqless[vec]`
2. Set `SILICON_API_KEY` environment variable (uses BAAI/bge-m3 embedding model).
3. Register for SiliconFlow free API if needed: [https://cloud.siliconflow.cn/i/szt2CkYN](https://cloud.siliconflow.cn/i/szt2CkYN)

**Usage**:
```python
table = db['vec_docs']

table.upsert({
    "A": "Deep learning is part of machine learning"
})

results = table.search("large language model", k=2)
```

See [vec-table-example.py](examples/5-vec-table-example.py) for details.

---

## 📁 Multi-file DB

```python
dbs = sqless.DBS(folder="db_folder")

table = dbs["app-Asia-users"]
table.upsert({...})
```

Mapping rule:

```
app-Asia-users
↓
app-Asia.sqlite / users
```

See [multi-files-sqless.py](examples/2-multi-files-sqless.py) for details.

---

## 🗂 DB Operations

```python
dir(db)
db.list_tables()
del db['users']
db.close()
```

See [hello-sqless.py](examples/1-hello-sqless.py) for details.

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
* Data analysis
* Lightweight services
* AI applications (RAG / semantic search)

---

## 🔄 Migration / Changes

**New in this version**:

* `vec_` table (semantic search)
* `fts_` table (full-text search)
* `json_` table (high-flexible schema)

Default table type remains Relational (low-flexible schema).

---

## 🔹 Legacy Server (Deprecated)

```bash
sqless --host 127.0.0.1 --port 12239 --secret xxx
```

* Auto creates directories: `www/`, `db/`, `fs/`
* Access via: `http://127.0.0.1:12239/index.html`

⚠️ Will be removed in future versions. Current version still compatible.

