# sqless

[English](README.md) | [中文版](README_zh.md)

一个 **schema 灵活、零抽象** 的 SQLite 接口，支持 **关系表、JSON表、全文搜索和语义搜索**。

![:@sqless](https://count.getloli.com/get/@sqless?theme=booru-lewd)

---

## sqless好在哪

* **无schema约束** – 表结构会根据你写入的数据自动调整，不用提前建表。
* **高性能** – 直接执行 SQL，性能接近原生 SQLite，比很多 ORM 都要快。
* **多文件分片** – 可以轻松把数据分散到多个 SQLite 文件中。
* **防SQL注入** – 通过语义解析、参数绑定和标识符校验，从源头避免注入风险。
* **多种表类型** – 支持关系表、JSON 表、全文搜索表（FTS）和向量表。

---

## ⚡ 性能对比

* 单位：秒，数值越小越好。
* 百分比：表示相较于对比库的性能提升幅度。
    * `↑` 表示 sqless 比该库快多少（即该库比 sqless 慢多少）
    * `↓` 表示 sqless 比原生sqlite慢多少（即 sqless 的额外开销）

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

## 🔹 使用sqless开发后端服务

sqless内置一个简单务实的后端服务框架。

### 函数即服务，四种访问方式
1. 安装依赖
```bash
pip install aiohttp aiofiles certifi
```
2. 编写服务端文件`sqless_app.py`，用`@sqless.api`把函数变成后端接口。
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
3. 启动后端服务
```bash
python sqless_app.py
```
4. 尝试用后端函数计算1+2

**方式1：HTTP GET（命令行）**
- 浏览器访问`http://127.0.0.1:12239/api/add 1 2`
    - 用户名：（不填）
    - 密码：SECRET
- curl访问
```bash
curl -H "Authorization: Bearer SECRET" \
    "http://127.0.0.1:12239/api/add%201%202"
```

**方式2：HTTP GET（参数）**
- 浏览器访问`http://127.0.0.1:12239/api/add?a=1&b=2`
    - 用户名：（不填）
    - 密码：SECRET
- curl访问
```bash
curl -H "Authorization: Bearer SECRET" \
    "http://127.0.0.1:12239/api/add?a=1&b=2"
```

**方式3：HTTP POST**
- curl访问
```bash
curl -H "Authorization: Bearer SECRET" \
     -H "Content-Type: application/json" \
     -d '{"f":"add","a":1,"b":2}' \
     http://127.0.0.1:12239/api
```

**方式4：MCP（让AI智能体调用你的函数）**
- Claude Code
```bash
claude mcp add --transport http sqless http://127.0.0.1:12239/mcp --header "Authorization: Bearer SECRET"
```
- 通用MCP配置
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

### 一个命令，从零启动
运行命令：
```bash
sqless
```
即可在当前目录下生成`sqless_app.py`模板并启动服务。

### 自带数据库、文件上传、静态网站、跨域访问等功能

**数据库接口**

```bash
# 写入
curl -H "Authorization: Bearer SECRET" \
     -H "Content-Type: application/json" \
     -d '{"key":"user_1","name":"Alice","age":30}' \
     http://127.0.0.1:12239/db/users

# 查询
curl -H "Authorization: Bearer SECRET" \
     "http://127.0.0.1:12239/db/users/age%20>%2020?page=1&per_page=20"

# 删除
curl -X DELETE -H "Authorization: Bearer SECRET" \
     "http://127.0.0.1:12239/db/users/key%20=%20user_1"
```

**文件接口**

```
# 上传
curl -H "Authorization: Bearer SECRET" \
     -F "file=@local.txt" \
     http://127.0.0.1:12239/fs/data/local.txt

# 下载
curl -H "Authorization: Bearer SECRET" \
     http://127.0.0.1:12239/fs/data/local.txt -o local.txt

# 检查是否存在
curl -H "Authorization: Bearer SECRET" \
     http://127.0.0.1:12239/fs/data/local.txt?check

# 列出文件
curl -H "Authorization: Bearer SECRET" \
     http://127.0.0.1:12239/fs/data/?check
```

**静态网站**

`cfg['path_base_www']` 目录下的文件直接托管在根路径：
```
GET /             -> www/index.html
GET /app.js       -> www/app.js
```

**跨域访问**

sqless服务端允许被跨域访问，也帮助静态网站绕过其他网站的跨域限制：

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

**其他特性**
- API/MCP调用执行中途，如果客户端断开连接，则自动取消任务，减少空转。
- API/MCP调用失败时，自动把报错信息写到`sqless_exception.log`里，方便排障。
- GET（命令行）调用方式，在分隔参数时会自动跳过引号、括号。
    - 例如 `/api/add "hello world" " 123"` ，得到 `hello world 123`
- GET（命令行）调用方式，会自动解析Python字典、列表。
    - 例如 `/api/add [{'a': 1}] [0]` ，得到`[{"a":1},0]`
- 服务端会根据函数注释、参数默认值与类别标识，自动创建MCP工具描述。
    - 例如前面的add函数，自动生成的MCP工具描述是： `{"name":"add","description":"A simple add tool","inputSchema":{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"integer"}},"required":["a","b"]}}`
- 后端服务框架与全文搜索表、向量表一样，只有在使用时才会动态载入模块。
