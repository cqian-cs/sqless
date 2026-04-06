# sqless

[English Version](README.md) | [中文版](README_zh.md)

一个 **schema 灵活、零抽象** 的 SQLite 接口，支持 **关系表、JSON表、全文搜索和语义搜索**。

---

## sqless好在哪

* **无schema约束** – 表结构会根据你写入的数据自动调整，不用提前建表。
* **高性能** – 直接执行 SQL，性能接近原生 SQLite，比很多 ORM 都要快。
* **多文件分片** – 可以轻松把数据分散到多个 SQLite 文件中。
* **防SQL注入** – 通过语义解析、参数绑定和标识符校验，从源头避免注入风险。
* **多种表类型** – 支持关系表、JSON 表、全文搜索表（FTS）和向量表。

---

## ⚡ 性能对比

| 名称       | 初始化       | 写入         | 更新       | 读取          | 类型          |
| ---------- | ------------ | ------------- | ------------ | ------------- |---------------|
| sqlalchemy | 0.022 (↑12%) | 5.572 (↑94%)  | 3.218 (↑99%) | 58.345 (↑97%) | 固定 schema |
| dataset    | 0.030 (↑33%) | 10.337 (↑97%) | 1.290 (↑98%) | 46.209 (↑96%) | 灵活 schema   |
| pony       | 0.030 (↑34%) | 2.221 (↑84%)  | 0.679 (↑96%) | 10.283 (↑84%) | 固定 schema |
| **sqless** | **0.020**    | **0.359**     | **0.030**    | **1.676**     | **灵活 schema** |
| 原生 sqlite | 0.013 (↓51%) | 0.260 (↓38%)  | 0.023 (↓32%) | 1.406 (↓19%)  | 固定 schema |

详细测试代码见 [benchmark/cmp_with_other_orms.py](benchmark/cmp_with_other_orms.py)。

---

## 🚀 快速上手

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

table['U1']['age'] = 15   # ❌ 这样写不会更新数据库
table['U1'] = {'age':15}  # ✅ 这样才会更新数据库
print(table['U1']) # {'key': 'U1', 'name': 'Tom', 'age': 15}
```

---

## 🧠 表类型说明

表类型由表名前缀决定：

| 类型       | 前缀    | 特点                          |
| ---------- | ------- | --------------------------------- |
| 关系表 | 无      | schema 相对固定，支持部分更新   |
| JSON 表    | `json_` | schema 极其灵活，每次都是整体替换 |
| FTS 表        | `fts_`  | 全文搜索                  |
| 向量表     | `vec_`  | 语义搜索                   |

---

### 🔹 关系表

```python
table = db['users']
table['U1'] = {'name': 'Tom', 'age': 14}
table['U1'] = {'age': 15}
print(table['U1']) # {'key': 'U1', 'name': 'Tom', 'age': 15}
```

* 部分更新：只改某个字段，其他字段保留。

详见 [examples/hello-sqless.py](examples/1-hello-sqless.py)。

---

### 🔹 JSON 表

```python
table = db['json_users']
table['U1'] = {'name': 'Tom', 'age': 14}
table['U1'] = {'age': 15}
print(table['U1']) # {'age': 15}
```

* 整体替换：写入新数据时，旧数据全部被覆盖。

详见 [examples/json-table-example.py](examples/3-json-table-example.py)。

---

### 🔹 FTS 表（全文搜索）

```python
table = db['fts_docs']

table.upsert({
    "A": "SQLite 支持全文搜索"
})

results = table.search('"SQLite"')
```

详见 [examples/fts-table-example.py](examples/4-fts-table-example.py)。

---

### 🔹 向量表（语义搜索）

**准备工作**：

1. 安装扩展依赖：`pip install sqless[vec]`
2. 注册硅基流动平台（邀请链接：[https://cloud.siliconflow.cn/i/szt2CkYN](https://cloud.siliconflow.cn/i/szt2CkYN)）
3. 将硅基流动平台创建的API密钥，设置环境变量 `SILICON_API_KEY` （使用免费的 BAAI/bge-m3 向量模型）


**用法**：
```python
table = db['vec_docs']

table.upsert({
    "A": "深度学习是机器学习的一个分支",
    "B": "NLP stands for Natural Language Processing.",
    "C": "SQLite是轻量级的数据库",
})

results = table.search("大语言模型", k=2)
```

详见 [examples/vec-table-example.py](examples/5-vec-table-example.py)。

---

## 📁 多文件数据库

```python
dbs = sqless.DBS(folder="db_folder")

table = dbs["app-Asia-users"]
table.upsert({...})
```

映射规则示例：

```
app-Asia-users
↓
db_folder/app-Asia.sqlite 数据库中的 users 表
```

详见 [examples/multi-files-sqless.py](examples/2-multi-files-sqless.py)。

---

## 🗂 数据库常用操作

```python
db = sqless.DB("your.db") # 创建数据库文件
table = db["users"]       # 创建/获取表
dir(db)                   # 列出数据库中所有的表名
del db['users']           # 删除表
db.close()                # 关闭连接
```

详见 [examples/hello-sqless.py](examples/1-hello-sqless.py)。

---

## 安全的 WHERE 语句

`sqless` 支持**安全、参数化**的 WHERE 解析：

* 逻辑运算符：`AND`, `OR`, `NOT`
* 比较运算符：`=`, `==`, `!=`, `<`, `>`, `<=`, `>=`, `like`, `ilike`, `is`, `in`
* 括号：`(` 和 `)`
* 排序：`ORDER BY <列名> [ASC|DESC]`
* 防注入：禁止输入 `; -- /* */` 等危险字符
* 值会自动用 `?` 占位符绑定，避免拼接 SQL

### 示例

```python
# 筛选 age 在 [10,12,14] 中，或者 name 以 'Tom' 开头，结果按 age 升序排列
where_str = "age in [10,12,14] OR name like 'Tom%' order by age asc"
success, sql, params = parse_where(where_str)
print(sql)    # where age in (?,?,?) or name like ? order by age asc
print(params) # [10, 12, 14, 'Tom%']
```

**注意**：

* `in` 后面必须跟列表，例如 `[10,12,14]`
* `is null` 写法可用，例如 `age is null`
* 列名必须是合法的标识符（字母、数字、下划线、中文，且不能以数字开头）
* 不支持复杂的 SQL 函数或子查询

---

## 🧭 适合做什么

* 本地数据库小工具
* 数据分析脚本
* 轻量级后端服务
* AI 应用（RAG / 语义搜索）

---

## 🔄 版本变化

**本次版本新增**：

* `json_` 前缀的JSON表
* `fts_` 前缀的全文搜索表
* `vec_` 前缀的语义搜索表

---

## 🔹 旧版服务端（已废弃）

```bash
sqless --host 127.0.0.1 --port 12239 --secret xxx
```

* 会自动创建目录：`www/`、`db/`、`fs/`
* 访问地址：`http://127.0.0.1:12239/index.html`

⚠️ 未来版本会移除，当前版本暂时保留兼容。

