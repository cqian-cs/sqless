# sqless iter(where, select) 技巧手册

## 签名

```python
table.iter(where='', select=None, limit=0, offset=0)
```

所有表类型共享同一签名，行为略有差异（见下方对照表）。

---

## 1. 基础组合矩阵

| # | 调用方式 | 作用 | 返回值类型 |
|---|---------|------|-----------|
| 1 | `iter()` | 遍历全部 | `dict` (所有字段) |
| 2 | `iter('条件')` | 过滤遍历 | `dict` |
| 3 | `iter(select=['col'])` | 只取一列 | **裸值** (非 dict) |
| 4 | `iter(select=['a','b'])` | 只取多列 | `dict` |
| 5 | `iter('条件', select=['col'])` | 过滤 + 只取一列 | 裸值 |
| 6 | `iter('条件', select=['a','b'])` | 过滤 + 只取多列 | `dict` |
| 7 | `iter('条件', limit=N)` | 过滤 + 分页 | `dict` |
| 8 | `iter('条件', limit=N, offset=M)` | 过滤 + 分页偏移 | `dict` |
| 9 | `iter('条件 order by col asc')` | 过滤 + 排序 | `dict` |

### 关键规则

- `select` 传字符串或单元素列表 → yield **裸值**（`decode(row[0])`）
- `select` 传多元素列表 → yield **dict**（`{col: val, ...}`）
- `select=None` → RelTable 返回 `SELECT *` 的完整 dict；Json/Fts/Vec 返回默认字段

---

## 2. where 表达式语法

### 比较运算符

```python
iter('age = 14')
iter('age == 14')
iter('age != 14')
iter('age > 10')
iter('age >= 10')
iter('age < 20')
iter('age <= 20')
```

### 模糊匹配

```python
iter('name like "Tom%"')      # 前缀匹配
iter('name like "%o%"')       # 包含匹配
iter('name ilike "tom%"')     # 大小写不敏感
```

### IN 列表

```python
iter('age in [10, 12, 14]')
iter('species in ["Cat", "Dog"]')
```

### NULL 判断

```python
iter('age is null')
```

### 逻辑组合 (AND / OR / NOT + 括号)

```python
iter('age < 10 OR age > 20')
iter('age > 10 AND species = "Cat"')
iter('NOT age < 10')
iter('(age < 10 OR age > 20) AND species = "Cat"')
```

### 排序 (ORDER BY)

```python
iter('age < 15 order by age asc')                    # 单列排序
iter('age < 15 order by age asc, name desc')          # 多列排序
iter('order by updated_at desc')                      # 仅排序，不过滤
```

### 复合条件示例

```python
iter('age in [10,12,14] OR name like "Tom%" order by age asc')
iter('species = "Cat" AND role != "Protagonist" order by age desc')
```

### 安全机制

- 禁止字符: `;` `--` `/*` `*/`
- 所有值自动参数化绑定 (`?`)
- 列名必须是合法标识符（字母/数字/下划线/中文，不能以数字开头）

---

## 3. select 技巧

### 单列 → 裸值

```python
# 只取 name 列，直接 yield str
for name in table.iter(select='name'):
    print(name)  # 'Tom', 'Jerry', ...

# 等价写法
for name in table.iter(select=['name']):
    print(name)
```

### 多列 → dict

```python
for item in table.iter(select=['name', 'age']):
    print(item)  # {'name': 'Tom', 'age': 14}
```

### 配合过滤

```python
# 取所有猫的名字
for name in table.iter('species = "Cat"', select='name'):
    print(name)

# 取年龄大于10的 name 和 age
for item in table.iter('age > 10', select=['name', 'age']):
    print(item['name'], item['age'])
```

### 快捷方法（内部都调用 iter）

```python
table.keys()    # iter('', select=['key'])        → yield 裸值
table.values()  # iter('')                         → yield dict (RelTable) / yield 裸值 (JsonTable)
table.items()   # iter('')                         → yield (key, dict) / (key, data)
```

---

## 4. 各表类型差异对照

| 特性 | RelTable | JsonTable | FtsTable | VecTable |
|------|----------|-----------|----------|----------|
| 前缀 | 无 | `json_` | `fts_` | `vec_` |
| `select=None` 默认字段 | `*` (所有列) | `key,data,updated_at` | `key,data` | `key,data` |
| `select` 可选值 | 任意列名 | `key,data,updated_at` | `key,data,text,updated_at` | `key,data,vector,updated_at` |
| 特殊 JOIN 列 | 无 | 无 | `text` (自动 JOIN FTS 表) | `vector` (自动 JOIN vec 表) |
| 值解码 | `decode()` 处理 bytes | 原生 JSON | 原生 JSON | `vector` 自动 bytes→list |
| 更新模式 | 部分更新 | 整体替换 | 整体替换 | 整体替换 |

### FtsTable 特有

```python
# select 包含 'text' 时自动 JOIN FTS 虚拟表
for item in fts_table.iter(select=['key', 'text']):
    print(item)  # {'key': 'A', 'text': 'SQLite supports full text search'}
```

### VecTable 特有

```python
# select 包含 'vector' 时自动 JOIN vec 表，并转换为 list[float]
for item in vec_table.iter(select=['key', 'vector']):
    print(item)  # {'key': 'A', 'vector': [0.12, -0.34, ...]}
```

---

## 5. 实战模式

### 分页遍历

```python
page_size = 100
for offset in range(0, table.count(), page_size):
    for item in table.iter(limit=page_size, offset=offset):
        process(item)
```

### 按条件批量提取单列

```python
ages = list(table.iter('species = "Cat"', select='age'))
# ages = [14, 15, 13, 16]
```

### 构建索引映射

```python
name_to_key = {
    item['name']: item['key']
    for item in table.iter(select=['key', 'name'])
}
```

### 排序取 Top N

```python
top_5_oldest = list(table.iter('order by age desc', limit=5))
```

### 条件删除前预览

```python
to_delete = list(table.iter('age < 6', select='key'))
if to_delete:
    table.delete('age < 6')
```

### 判断是否存在

```python
exists = table.count('name = "Tom"') > 0
# 或
first = table.get_one('name = "Tom"')
```

### 列表推导式快速取值

```python
# 所有猫的名字
cat_names = [name for name in table.iter('species = "Cat"', select='name')]

# 年龄求和（Python 侧）
total_age = sum(age for age in table.iter(select='age'))
```

---

## 6. 常见陷阱

| 陷阱 | 说明 | 正确做法 |
|------|------|---------|
| `table['U1']['age'] = 15` | 只改了内存 dict，不写库 | `table['U1'] = {'age': 15}` |
| `iter(select=['age'])` 返回 dict? | 单列返回**裸值**，不是 dict | 需要 dict 时至少选 2 列 |
| `iter()` 后修改数据 | iter 是生成器，惰性求值 | 先 `list(iter(...))` 缓存 |
| `delete('')` | 空字符串被拒绝 | 用 `db.drop_table()` 清空 |
| `where` 中用 `%o%` | 需要 `like "%o%"` 带引号 | `'name like "%o%"'` |
