'''
@File        :util_dbs.py
@Description :三合一数据库（JSON数据库+向量数据库+FTS5全文搜索数据库）

用法：
    import util_dbs
    dbs = util_dbs.DBS('folder_dbs') # 用来存放数据库的文件夹

JSON数据库：
    market_users = dbs['market-users']
    # 增加数据
    ret = market_users.upsert({
        'user_001':{'name':'Tom'  ,'age':12},
        'user_002':{'name':'Jerry','age':8},
        'user_003':{'name':'Spike','age':14},
    })
    ret['suc'] # 是否成功
    ret['data'] # update {integer} items
    # 修改数据
    market_users.upsert({
        'user_001':{'age':13}
    })
    # 字典操作
    market_users['user_001'] = {'age':13}
    data = market_users['user_001']
    for key,value in market_users.items():
        print(key,value)
    keys = list(market_user.keys())
    values = list(market_user.values())
    # 获取详细
    item = market_users.get_item('user_001')
    item['key'] # 主键
    item['data'] # 数据，空值None
    item['updated_at'] # 更新时间，例如2026-02-12 15:18:58，空值''
    # 批量获取
    ret = market_users.get_items(['user_001','user_002','user_003'])
    if ret['suc']:
        for item in ret['data']:
            ...
    # 查询数量
    count = market_users.count() 
    count = market_users.count("age < 12") 
    # 检索数据
    for item in market_users.iter(
        where = 'age < 12',
        select = ['key','data','updated_at'],
        offset = 0,
        limit = 0
    ):
        ...

向量数据库用法：
    # (1/2) 在创建数据表时，表名前缀vec_
    market_goods = dbs['market-vec_goods']
    # (2/2) 向量搜索
    ret = market_goods.search("床上喝的东西")
    if ret['suc']:
        for items in ret['data']:
            item['key']  # 主键
            item['data'] # 数据
            item['updated_at'] # 更新时间
            item['vector']     # 1024维向量
            item['distance']   # 语义距离

全文数据库用法：
    # (1/2) 在创建数据表时，表名前缀fts_
    market_comments = dbs['market-fts_goods']
    # (2/2) 全文搜索
    ret = market_comments.search('"纸" AND ("可乐" OR "洗洁精") ')
    if ret['suc']:
        for items in ret['data']:
            item['key']  # 主键
            item['data'] # 数据
            item['updated_at'] # 更新时间
            item['score']      # BM25得分
        

@Date        :2026/02/12 21:35:02
@Author      :QianCheng
@Version     :1.0
'''

# pip install sqlite-vec scikit-learn numpy aiohttp orjson tqdm
import aiohttp
import orjson
import asyncio
import os
from typing import Union, List, Literal
import re
import sqlite3
import sqlite_vec
import tqdm
import ast
import struct
import pickle
_session = None
async def init_session():
    global _session
    if not _session or _session.closed:
        connector = aiohttp.TCPConnector(
            limit=200,             # 全局最大连接数
            limit_per_host=10,     # 单域名最大连接数 (防止单点拖垮)
            force_close=False,     # 保持 Keep-Alive 复用
            enable_cleanup_closed=True,  # 自动清理已关闭的连接
            ttl_dns_cache=300
        )
        _session = aiohttp.ClientSession(connector=connector)

async def close_session():
    global _session
    if _session and not _session.closed:
        await _session.close()

async def fetch_json(options,custom):
    global _session
    try:
        async with _session.request(**options) as response:
            content = await response.text()
    except Exception as e:
        return {'suc':False,'data':f"Network Error: {str(e)}",**custom}
    try:
        data = orjson.loads(content)
    except Exception as e:
        return {'suc':False,'data':f"JSON Error: {str(e)}",**custom}
    return {'suc':True,'data':data,**custom}


async def embedding(txts: Union[str,List[str]]):
    if type(txts)==str:
        txts = [txts]
    limit = 16002 # 8192 Token
    results = [None for _ in range(len(txts))]
    i_lens = [(i,len(x)) for i,x in enumerate(txts)]
    for i,l in i_lens:
        if l > limit:
            return {'suc':False,'data':f'index {i} ({txts[i][:20]}) exceed size limit.'}
    i_lens.sort(key=lambda x:x[1],reverse=True)
    batches_of_indices = []
    batch_lengths = []
    for orig_idx, length in i_lens:
        placed = False
        for i in range(len(batches_of_indices)):
            if batch_lengths[i] + length <= limit:
                batches_of_indices[i].append(orig_idx)
                batch_lengths[i] += length
                placed = True
                break
        if not placed:
            batches_of_indices.append([orig_idx])
            batch_lengths.append(length)
    with tqdm.tqdm(total = len(batches_of_indices),desc='Embedding') as pbar:
        for future in asyncio.as_completed([
            fetch_json(
                options = {
                    "url":"https://api.siliconflow.cn/v1/embeddings",
                    "method":"POST",
                    "headers":{
                        "Authorization": f"Bearer {os.getenv('SILICON_API_KEY')}",
                        "Content-Type": "application/json"
                    },
                    "json":{
                        "model": "BAAI/bge-m3",
                        "input": [txts[i] for i in batches_of_indice]
                    }
                },
                custom = {'batches_of_indice':batches_of_indice}
            )
            for batches_of_indice in batches_of_indices
        ]):
            ret = await future
            if not ret['suc']:
                return ret
            embeddings = [x['embedding'] for x in ret['data']['data']]
            for i, embedding in zip(ret['batches_of_indice'],embeddings):
                results[i] = embedding
            pbar.update(1)
    return {'suc':True,'data':results}



#
#
#if __name__=='__main__':
#    async def main():
#        await init_session()
#        ret = await embedding([
#            '你好',
#            '世界'*8000,
#            '世界'*8000,
#            '你好',
#        ])
#        if ret['suc']:
#            for embed in ret['data']:
#                print(len(embed),embed[:10])
#        else:
#            print(ret)
#        await close_session()
#    asyncio.run(main())
#



sqlite3.register_converter("JSON", lambda b: orjson.loads(b))

def parse_val(val):
    try:
        return ast.literal_eval(val)
    except:
        return val

def parse_col(col):
    if col in ['key','update_at']:
        return col
    return f"data->>'$.{col}'"

type_map = {
    str: 'TEXT',
    int: 'INTEGER',
    float: 'REAL',
    bool: 'INTEGER',
}

value_map = {
    str: None,
    int: None,
    float: None,
    bool: None,
}

try:
    import numpy as np
    for _t in [np.int8, np.int16, np.int32, np.int64, np.uint8, np.uint16, np.uint32, np.uint64]:
        type_map[_t] = 'INTEGER'
        value_map[_t] = int
    for _t in [np.float16, np.float32, np.float64]:
        type_map[_t] = 'REAL'
        value_map[_t] = float
except ImportError:
    pass

def encode(obj):
    if obj is None:
        return None
    if isinstance(obj, bytes):
        return b'B' + obj
    try:
        return b'J' + orjson.dumps(obj, option=orjson.OPT_SERIALIZE_NUMPY)
    except:
        return b'P' + pickle.dumps(obj)

def decode(binary):
    if type(binary) != bytes:
        return binary
    if binary[0] == ord('J'):
        return orjson.loads(binary[1:])
    if binary[0] == ord('P'):
        return pickle.loads(binary[1:])
    return binary[1:]

def parse_where(where_str, use_json_path=True):
    """
    Parse safe WHERE expressions into (sql, params) with AND/OR/NOT, parentheses, and ORDER BY.

    -------------------------------------------------------------------------
    Syntax Guide for Developers:

    1. Basic condition:
       col operator value
       - col: column name (must match [A-Za-z_][A-Za-z0-9_]*)
       - operator: one of =, ==, !=, <, >, <=, >=, like, ilike, is
       - value: string (quoted with single ' or double ") or numeric literal
         - Example: age >= 18
         - Example: name = "Alice Bob"
         - Example: role is null

    2. Logical operators:
       - AND, OR, NOT (case-insensitive)
       - NOT applies to the condition immediately following it
       - Examples:
         - age >= 18 AND role = "Hero"
         - NOT age < 10

    3. Parentheses:
       - Use () to group expressions and control precedence
       - Examples:
         - (age < 10 AND name like "%e%") OR (role = "Antagonist" AND NOT age >= 16)

    4. ORDER BY clause (optional):
       - Use at the end of the expression: ORDER BY col1 [ASC|DESC], col2 [ASC|DESC], ...
       - Column names must be valid identifiers
       - ASC/DESC is optional; default ordering depends on DB
       - Example:
         - ORDER BY id DESC, name ASC

    5. Safety rules:
       - Forbidden characters: ; -- /* */
       - Only valid identifiers allowed as column names
       - String literals must be quoted
       - Function calls or subqueries are NOT allowed

    6. Return:
       - (True, sql_string, params_list) on success
       - (False, error_message, []) on parse error

    -------------------------------------------------------------------------
    Example usage:

    expr = '(age < 10 AND name like "%e%") OR (role = "Antagonist" AND NOT age >= 16) ORDER BY id DESC, name ASC'
    ok, sql, params = parse_where(expr)
    print(ok)     # True
    print(sql)    # where ( age < ? and name like ? ) or ( role = ? and not age >= ? ) order by id desc, name asc
    print(params) # ['10', '%e%', 'Antagonist', '16']
    -------------------------------------------------------------------------
    """
    allowed_ops = {'=', '==', '!=', '<', '>', '<=', '>=', 'like', 'ilike', 'is', 'in'}

    def valid_identifier(s):
        return re.match(r'^[A-Za-z_\u2E80-\u9FFF][A-Za-z0-9_\u2E80-\u9FFF]*$', s) is not None

    if not where_str:
        return True, '', []

    s = where_str.strip()

    # reject dangerous chars
    if any(x in s for x in (';', '--', '/*', '*/')):
        return False, 'contains forbidden characters', []

    # separate ORDER BY if present
    m_order = re.search(r'\border\s+by\b', s, re.IGNORECASE)
    if m_order:
        where_part = s[:m_order.start()].strip()
        order_part = s[m_order.end():].strip()
    else:
        where_part = s
        order_part = ''

    # tokenize where_part
    token_pattern = r"""
        (\() |               # open parenthesis
        (\)) |               # close parenthesis
        ("[^"]*") |          # double-quoted string
        ('[^']*') |          # single-quoted string
        (\bAND\b|\bOR\b|\bNOT\b) |   # logical operators
        (<=|>=|!=|==|=|<|>|like|ilike|is|in) | # comparison operators
        ([^\s()]+)           # identifiers / values
    """
    tokens = [t for t in re.findall(token_pattern, where_part, re.IGNORECASE | re.VERBOSE)]
    tokens = [next(filter(None, t)) for t in tokens]  # flatten

    sql_parts = []
    params = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]

        # parentheses
        if tok in ('(', ')'):
            sql_parts.append(tok)
            i += 1
            continue

        # logical operators
        if tok.upper() in ('AND', 'OR', 'NOT'):
            sql_parts.append(tok.lower())
            i += 1
            continue

        # expect: col op val
        if i + 2 >= len(tokens):
            return False, f"Invalid condition near: {tok}", []

        _col, op, _val = tokens[i], tokens[i+1].lower(), tokens[i+2]

        if not valid_identifier(_col):
            return False, f"Invalid column name: {_col}", []
        
        col = parse_col(_col) if use_json_path else _col

        if op not in allowed_ops:
            return False, f"Operator not allowed: {op}", []

        # handle NULL
        if _val.lower() == 'null' and op == 'is':
            sql_parts.append(f"{col} is null")
        else:
            val = parse_val(_val)
            if type(val) == list:
                sql_parts.append(f"{col} {op} ({','.join(['?']*len(val))})")
                params+=val
            else:
                sql_parts.append(f"{col} {op} ?")
                params.append(val)

        i += 3

    sql = "where " + " ".join(sql_parts)

    # handle ORDER BY (simple)
    if order_part:
        order_cols = []
        for part in order_part.split(','):
            items = part.strip().split()
            if not items:
                continue
            _col = items[0]
            colname = parse_col(_col) if use_json_path else _col
            if not valid_identifier(_col):
                return False, f"Invalid order column: {_col}", []
            direction = ''
            if len(items) == 2 and items[1].lower() in ('asc', 'desc'):
                direction = f" {items[1].lower()}"
            elif len(items) > 2:
                return False, f"Invalid order clause: {part}", []
            order_cols.append(f"{colname}{direction}")
        if order_cols:
            sql += " order by " + ", ".join(order_cols)

    return True, sql, params



bytes2vec = lambda b: list(struct.unpack(f"<{len(b)//4}f", b))
vec2bytes = lambda v: struct.pack(f"<{len(v)}f", *v)

async def async_dict_to_vec(key_texts:dict,to_bytes=False):
    await init_session()
    keys = [x for x in key_texts.keys()]
    values = [x for x in key_texts.values()]
    ret = await embedding(values)
    await close_session()
    if not ret['suc']:
        print(f"""DB_ERROR|Embedding Error: {ret['data']}""")
        return {'suc':False,'data':f"Embedding Error: {ret['data']}"}
    if to_bytes:
        return {'suc':True,'data':{key:vec2bytes(embed) for key,embed in zip(keys,ret['data'])}}
    return {'suc':True,'data':{key:embed for key,embed in zip(keys,ret['data'])}}


def dict_to_vec(key_texts:dict,to_bytes=False):
    return asyncio.run(async_dict_to_vec(key_texts,to_bytes))

identifier_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-]*[A-Za-z0-9]$")
def valid_identifier(name: str) -> bool:
    return bool(name and identifier_re.fullmatch(name))

path_this = os.path.dirname(os.path.abspath(__file__))

class DB:
    def __init__(self,path_db=f"{path_this}/util_db_vec.sqlite",wal = True):
        self.path = os.path.realpath(path_db)
        os.makedirs(os.path.dirname(self.path),exist_ok=True)
        self.conn = sqlite3.connect(self.path,detect_types=sqlite3.PARSE_DECLTYPES)
        self.conn.enable_load_extension(True)
        self.tables = {}
        sqlite_vec.load(self.conn)
        if wal:
            # Enable WAL (Write-Ahead Logging)
            cursor = self.conn.cursor()
            cursor.execute("PRAGMA journal_mode = WAL;")
            self.conn.commit()
    def __del__(self):
        self.close()
    def __str__(self):
        return f"DB({self.path})"
    def __repr__(self):
        return f"DB({self.path})"
    def __dir__(self):
        return self.list_tables()
    def __contains__(self,table_name):
        suc,ret = self.check_table_exists(table_name)
        return suc and ret
    def __getitem__(self,table_name):
        return self.get_table(table_name)
    def close(self):
        if self.conn:
            self.conn.close()
            print(f"DB({self.path}) closed.")
    def drop_table(self,table_name):
        if not valid_identifier(table_name):
            print(f"DB_ERROR| Illegal table name: {table_name}")
            return False
        try:
            cursor = self.conn.cursor()
            sql = f"DROP TABLE IF EXISTS {table_name};"
            cursor.execute(sql)
            sql = f"DROP TABLE IF EXISTS {table_name}_vec;"
            cursor.execute(sql)
            sql = f"DROP TABLE IF EXISTS {table_name}_fts;"
            cursor.execute(sql)
            self.conn.commit()
        except Exception as e:
            print(f"DB_ERROR|{e}({sql})")
            return False
    def get_table(self,table_name,table_type:Literal['Table','VecTable','FtsTable','RelTable']='Table'):
        if not table_name in self.tables:
            if table_type == 'VecTable':
                new_table = VecTable(self,table_name)
            elif table_type == 'FtsTable':
                new_table = FtsTable(self,table_name)
            elif table_type == 'RelTable':
                new_table = RelTable(self,table_name)
            else:
                new_table = Table(self,table_name)
            suc,msg = new_table.ensure_table()
            if not suc:
                print(msg)
                return None
            self.tables[table_name] = new_table
        return self.tables[table_name]
    
    def check_table_exists(self,table_name):
        sql = f"SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        values = (table_name,)
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql,values)
            return True, cursor.fetchone() is not None
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
            return False, f"DB_ERROR|{e}({sql}){values}"
    
    def check_table_type(self,table_name):
        suc, ret = self.check_table_exists(table_name)
        if not suc:
            return suc, ret
        if not ret:
            return True, "Not Exist"
        suc, ret = self.check_table_exists(f"{table_name}_vec")
        if not suc:
            return suc, ret
        if ret:
            return True, "VecTable"
        suc, ret = self.check_table_exists(f"{table_name}_fts")
        if not suc:
            return suc, ret
        if ret:
            return True, "FtsTable"
        # Distinguish Table vs RelTable by schema
        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = {row[1]: row[2] for row in cursor.fetchall()}
        if 'data' in columns and columns.get('data') == 'JSON':
            return True, "Table"
        return True, "RelTable"

    def list_tables(self):
        sql = """
            SELECT 
                m.name,
            FROM sqlite_master m
            WHERE m.type = 'table';
        """
        values = None
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql,values)
            return [name for name in cursor.fetchall() if name!='sqlite_sequence' and not name.endswith('_vec') and not name.endswith('_fts')]
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
            return []
        

#===========================================================================
# 普通数据表 for 不可读数据
#===========================================================================


class Table:
    def __init__(self,db,table_name):
        self.db = db
        self.name = table_name
    def ensure_table(self):
        table = self.name
        suc,ret = self.db.check_table_type(table)
        if not suc:
            return suc,ret
        if ret == 'Table':
            return True, f"exist"
        if ret != 'Not Exist':
            return False,f"Failed to create Table({table}), already exist {ret}({table})"
        try:
            cursor = self.db.conn.cursor()
            sql = f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    key TEXT PRIMARY KEY,
                    data JSON,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
            """
            cursor.execute(sql)
            self.db.conn.commit()
            return True,'ok'
        except Exception as e:
            return False,f"Ensuring fields error: {e}({sql})"
    def __str__(self):
        return f"Table({self.db.path})[{self.name}]"
    def __repr__(self):
        return f"Table({self.db.path})[{self.name}]"
    def __dir__(self):
        return ['key','data','updated_at']
    def __contains__(self,key):
        return self.get(key) != None
    def __getitem__(self,key):
        ret = self.get_item(key)
        return ret['data']
    def __setitem__(self,key,data):
        return self.upsert({key:data})
    def __len__(self):
        return self.count()
    def pre_upsert(self,key_values:dict):
        key_old_values = self.get_items(list(key_values.keys()))
        processed_key_values = {}
        for key,new_value in key_values.items():
            old_value = key_old_values[key]['data']
            if type(old_value) == type(new_value) and old_value == new_value:
                continue
            if type(old_value) == type(new_value) == dict:
                no_diff = True
                for k,v in new_value.items():
                    if v != old_value.get(k):
                        no_diff = False
                        old_value[k] = v
                    if v == None:
                        old_value.pop(k,None)
                if no_diff:
                    continue
                new_value = old_value
            processed_key_values[key] = new_value
        return processed_key_values

    def upsert(self,key_values:dict):
        table = self.name
        processed_key_values = self.pre_upsert(key_values)
        if len(processed_key_values) == 0:
            return {'suc': True,'data':f"update 0 items."}
        key_texts = {k:orjson.dumps(v).decode('utf-8') for k,v in processed_key_values.items()}
        try:
            cursor = self.db.conn.cursor()
            #cursor.execute("BEGIN")
            sql = f"INSERT OR REPLACE INTO {table}(key,data) VALUES (?, ?)"
            values = [(k,v) for k,v in key_texts.items()]
            cursor.executemany(sql,values)
            self.db.conn.commit()
            return {'suc': True,'data':f"update {len(processed_key_values)} items."}
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
            return {'suc': False, 'data': str(e), 'debug': sql}
    
    def get(self,key,default=None):
        ret = self.get_item(key)
        return ret['data'] or default
    
    def get_item(self,key):
        table = self.name
        sql = f"SELECT key,data,updated_at FROM {table} WHERE key = ?;"
        values = (key,)
        try:
            cursor = self.db.conn.cursor()
            cursor.execute(sql,values)
            row = cursor.fetchone()
            if row:
                return {'key':row[0],'data':row[1],'updated_at':row[2]}
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
        return {'key':key,'data':None,'updated_at':None}
    def get_items(self,keys):
        items = {k:{'key':k,'data':None,'updated_at':None} for k in keys}
        table = self.name
        sql = f"SELECT key,data,updated_at FROM {table} WHERE key IN ({','.join(['?']*len(keys))});"
        values = keys
        try:
            cursor = self.db.conn.cursor()
            cursor.execute(sql,values)
            for row in cursor:
                items[row[0]] = {'key':row[0],'data':row[1],'updated_at':row[2]}
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
        return items
    def count(self,where=''):
        suc, sql_where, values = parse_where(where)
        if not suc:
            print(f"DB_ERROR| Illegal where: {sql_where}")
            return None
        table = self.name
        sql = f"SELECT count(*) FROM {table} {sql_where};"
        try:
            cursor = self.db.conn.cursor()
            cursor.execute(sql,values)
            row = cursor.fetchone()
            return row[0] if row else 0
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
            return None
    
    def iter(self,
             where='',
             select:List[Literal['key','data','updated_at']]=None,
             limit=0,offset=0
        ):
        if not select:
            select = ['key','data','updated_at']
        if type(select) == str:
            select = [select]
        suc, sql_where, values = parse_where(where)
        if not suc:
            print(f"DB_ERROR| Illegal where: {sql_where}")
            return None
        if limit > 0:
            sql_where += f" LIMIT {limit}"
            if offset > 0:
                sql_where += f" OFFSET {offset}"
        table = self.name
        fields = ','.join(select)
        sql = f"SELECT {fields} FROM {table} {sql_where};"
        try:
            cursor = self.db.conn.cursor()
            cursor.execute(sql,values)
            for row in cursor:
                yield {k:v for k,v in zip(select,row)} if len(select)>1 else row[0]
        except Exception as e:
            print(f"DB_ERROR|{e}({sql},{values})")
            return None
    def get_one(self,where,default=None):
        items = list(self.iter(where=where,select=['key','data','updated_at'],limit=1))
        if items:
            return items[-1]
        return default
    def keys(self):
        for x in self.iter('',['key']):
            yield x
    def values(self):
        for x in self.iter('',['data']):
            yield x
    def items(self):
        for x in self.iter('',['key','data']):
            yield x['key'],x['data']

#===========================================================================
# 向量数据库 for 摘要数据
#===========================================================================

class VecTable(Table):
    def __init__(self,db,table_name):
        super().__init__(db, table_name)

    def ensure_table(self):
        table = self.name
        suc,ret = self.db.check_table_type(table)
        if not suc:
            return suc,ret
        if ret == 'VecTable':
            return True, f"exist"
        if ret != 'Not Exist':
            return False,f"Failed to create VecTable({table}), already exist {ret}({table})"
        try:
            cursor = self.db.conn.cursor()
            sql = f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    key TEXT PRIMARY KEY,
                    data JSON,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
            """
            cursor.execute(sql)
            sql = f"""
                CREATE VIRTUAL TABLE IF NOT EXISTS {table}_vec 
                USING vec0(
                    vector FLOAT[1024]
                );
            """
            cursor.execute(sql)
            self.db.conn.commit()
            return True,'ok'
        except Exception as e:
            return False,f"Ensuring fields error: {e}({sql})"
    def __str__(self):
        return f"VecTable({self.db.path})[{self.name}]"
    def __repr__(self):
        return f"VecTable({self.db.path})[{self.name}]"
    def __dir__(self):
        return ['key','data','vector','updated_at']
    def _upsert(self,key_texts:dict,key_vecs:dict):
        table = self.name
        key_rowid = {}
        try:
            cursor = self.db.conn.cursor()
            #cursor.execute("BEGIN")
            sql = f"INSERT OR REPLACE INTO {table}(key,data) VALUES (?, ?)"
            values = [(k,v) for k,v in key_texts.items()]
            cursor.executemany(sql,values)
            sql = f"SELECT rowid, key FROM {table} WHERE key in ({','.join(['?']*len(key_texts))})" 
            values = tuple(key_texts.keys())
            cursor.execute(sql,values)
            for row in cursor.fetchall():
                key_rowid[row[1]] = row[0]
            sql = f"DELETE FROM {table}_vec WHERE rowid = ?"
            values = [(rowid,) for rowid in key_rowid.values()]
            cursor.executemany(sql,values)
            sql = f"INSERT INTO {table}_vec(rowid,vector) VALUES (?, ?)"
            values = [(key_rowid.get(k),v) for k,v in key_vecs.items()]
            cursor.executemany(sql,values)
            self.db.conn.commit()
            return {'suc': True,'data':f"update {len(key_vecs)} items."}
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
            return {'suc': False, 'data': str(e), 'debug': sql}
    def upsert(self,key_values:dict):
        processed_key_values = self.pre_upsert(key_values)
        if len(processed_key_values) == 0:
            return {'suc': True,'data':f"update 0 items."}
        key_texts = {k:orjson.dumps(v).decode('utf-8') for k,v in processed_key_values.items()}
        ret = dict_to_vec(key_texts,to_bytes=True)
        if not ret['suc']:
            return ret
        key_vecs = ret['data']
        return self._upsert(key_texts,key_vecs)
    async def async_upsert(self,key_values:dict):
        processed_key_values = self.pre_upsert(key_values)
        if len(processed_key_values) == 0:
            return {'suc': True,'data':f"update 0 items."}
        key_texts = {k:orjson.dumps(v).decode('utf-8') for k,v in processed_key_values.items()}
        ret = await async_dict_to_vec(key_texts,to_bytes=True)
        if not ret['suc']:
            return ret
        key_vecs = ret['data']
        return self._upsert(key_texts,key_vecs)

    def get_vectors(self,keys,raw=False):
        items = {k:None for k in keys}
        table = self.name
        sql = f"SELECT key,vector FROM {table}_vec v JOIN {table} t ON t.rowid = v.rowid WHERE key IN ({','.join(['?']*len(keys))});"
        values = keys
        try:
            cursor = self.db.conn.cursor()
            cursor.execute(sql,values)
            for row in cursor:
                items[row[0]] = row[1] if raw else bytes2vec(row[1])
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
        return items
    def get_graph(self,keys,k=2,max_hop=2,state={}):
        current_hop = state.get('current_hop') or 0
        nodes = state.get('nodes') or {}
        nodes_key_vectors = state.get('nodes_key_vectors') or {}
        edges = state.get('edges') or {}
        key_vectors = [(k,v) for k,v in self.get_vectors(keys,raw=True).items() if v]
        table = self.name
        values = None
        try:
            cursor = self.db.conn.cursor()
            sql = f"CREATE TEMP TABLE IF NOT EXISTS query_vectors (key TEXT, vector BLOB);"
            cursor.execute(sql)
            sql = f"DELETE FROM query_vectors;"
            cursor.execute(sql)
            sql = "INSERT INTO query_vectors (key, vector) VALUES (?, ?);"
            values = key_vectors
            cursor.executemany(sql,values)
            sql = f"""
                SELECT
                    q.key,
                    t.key,
                    t.data,
                    v.distance,
                    v.vector
                FROM query_vectors q
                JOIN {table}_vec v ON v.vector MATCH q.vector AND v.k = ?
                JOIN {table} t ON t.rowid = v.rowid
                ORDER BY v.distance
            """
            values = (k+1,)
            cursor.execute(sql, values)
            for row in cursor:
                src = row[0]
                dst = row[1]
                data = row[2]
                weight = row[3]
                vector = bytes2vec(row[4])
                if dst not in nodes:
                    nodes[dst] = {'x':0,'y':0,'z':0,'data':data,'c':current_hop+1}
                    nodes_key_vectors[dst] = vector
                if src != dst:
                    edges[(src,dst)]=weight
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
            return {'suc':False,'data':f"DB_ERROR|{e}({sql}){values}"}
        if current_hop < max_hop:
            self.get_graph(list(nodes.keys()),k,max_hop,{
                'current_hop':current_hop+1,
                'nodes':nodes,
                'nodes_key_vectors': nodes_key_vectors,
                'edges':edges
            })
        if current_hop == 0:
            if len(nodes)<3:
                for key,(x,y,z) in zip(nodes.keys(),[(0,0,0),(0,1,0),(1,0,0)]):
                    nodes[key]['x'] = x
                    nodes[key]['y'] = y
                    nodes[key]['z'] = z
            else:
                key_xyz = PCA_of_key_vectors(nodes_key_vectors.items())
                for key,(x,y,z) in key_xyz.items():
                    nodes[key]['x'] = x
                    nodes[key]['y'] = y
                    nodes[key]['z'] = z
            for key in keys:
                if key in nodes:
                    nodes[key]['c'] = 0
            edges = [[src,dst,weight] for (src,dst),weight in edges.items()]
        return {'suc':True,'data':{'nodes':nodes,'edges':edges}}
    def search(self,query,k=5):
        table = self.name
        sql = f"""
            SELECT 
                key, 
                data,
                updated_at,
                vector,
                distance
            FROM {table}_vec v
            JOIN {table} t ON t.rowid = v.rowid
            WHERE vector MATCH ? AND k = ?
            ORDER BY distance
        """
        ret = dict_to_vec({'key':orjson.dumps(query).decode()},to_bytes=True)
        if not ret['suc']:
            return ret
        values = (ret['data']['key'],k)
        try:
            cursor = self.db.conn.cursor()
            cursor.execute(sql,values)
            results = [
                {'key':row[0],'data':row[1],'updated_at':row[2],'vector':bytes2vec(row[3]),'distance':row[4]}
                for row in cursor
            ]
            return {'suc':True,'data':results}
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
            return {'suc':False,'data':f"DB_ERROR|{e}({sql}){values}"}
    def iter(self,
             where='',
             select:List[Literal['key','data','vector','updated_at']]=None,
             limit=0,offset=0
        ):
        if not select:
            select = ['key','data']
        if type(select) == str:
            select = [select]
        suc, sql_where, values = parse_where(where)
        if not suc:
            print(f"DB_ERROR| Illegal where: {sql_where}")
            return None
        if limit > 0:
            sql_where += f" LIMIT {limit}"
            if offset > 0:
                sql_where += f" OFFSET {offset}"
        table = self.name
        i_vec = None
        if 'vector' not in select:
            fields = ','.join(select)
            sql = f"SELECT {fields} FROM {table} {sql_where};"
        else:
            i_vec = select.index('vector')
            fields = ','.join(select)
            sql = f"SELECT {fields} FROM {table} t JOIN {table}_vec v ON t.rowid = v.rowid {sql_where};"
        try:
            cursor = self.db.conn.cursor()
            cursor.execute(sql,values)
            for row in cursor:
                if i_vec != None:
                    row = list(row)
                    row[i_vec] = bytes2vec(row[i_vec])
                yield {k:v for k,v in zip(select,row)} if len(select)>1 else row[0]
        except Exception as e:
            print(f"DB_ERROR|{e}({sql},{values})")
            return None
    def vectors(self):
        for x in self.iter('',['key','vector']):
            yield x['key'],x['vector']
    
#===========================================================================
# 全文搜索数据表 for 长文数据
#===========================================================================
anyword_re = re.compile(r'[\u2E80-\u9FFF]|[A-Za-z0-9_]+|[^\sA-Za-z0-9_\u2E80-\u9FFF]')
def text_to_fts(text):
    return ' '.join(anyword_re.findall(text))
class FtsTable(Table):
    def __init__(self,db,table_name):
        super().__init__(db, table_name)
        
    def ensure_table(self):
        table = self.name
        suc,ret = self.db.check_table_type(table)
        if not suc:
            return suc,ret
        if ret == 'FtsTable':
            return True, f"exist"
        if ret != 'Not Exist':
            return False,f"Failed to create FtsTable({table}), already exist {ret}({table})"
        try:
            cursor = self.db.conn.cursor()
            sql = f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    key TEXT PRIMARY KEY,
                    data JSON,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
            """
            cursor.execute(sql)
            sql = f"""
                CREATE VIRTUAL TABLE IF NOT EXISTS {table}_fts 
                USING fts5(
                    text
                );
            """
            cursor.execute(sql)
            self.db.conn.commit()
            return True,'ok'
        except Exception as e:
            return False,f"Ensuring fields error: {e}({sql})"
    def __str__(self):
        return f"FtsTable({self.db.path})[{self.name}]"
    def __repr__(self):
        return f"FtsTable({self.db.path})[{self.name}]"
    def upsert(self,key_values:dict):
        table = self.name
        processed_key_values = self.pre_upsert(key_values)
        if len(processed_key_values) == 0:
            return {'suc': True,'data':f"update 0 items."}
        key_texts = {k:orjson.dumps(v).decode('utf-8') for k,v in processed_key_values.items()}
        key_fts = {k:text_to_fts(v) for k,v in key_texts.items()}
        key_rowid = {}
        try:
            cursor = self.db.conn.cursor()
            #cursor.execute("BEGIN")
            sql = f"INSERT OR REPLACE INTO {table}(key,data) VALUES (?, ?)"
            values = [(k,v) for k,v in key_texts.items()]
            cursor.executemany(sql,values)
            sql = f"SELECT rowid, key FROM {table} WHERE key in ({','.join(['?']*len(key_texts))})" 
            values = tuple(key_texts.keys())
            cursor.execute(sql,values)
            for row in cursor.fetchall():
                key_rowid[row[1]] = row[0]
            sql = f"DELETE FROM {table}_fts WHERE rowid = ?"
            values = [(rowid,) for rowid in key_rowid.values()]
            cursor.executemany(sql,values)
            sql = f"INSERT INTO {table}_fts(rowid,text) VALUES (?, ?)"
            values = [(key_rowid.get(k),v) for k,v in key_fts.items()]
            cursor.executemany(sql,values)
            self.db.conn.commit()
            return {'suc': True,'data':f"update {len(processed_key_values)} items."}
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
            return {'suc': False, 'data': str(e), 'debug': sql}
    def get_texts(self,keys):
        items = {k:None for k in keys}
        table = self.name
        sql = f"SELECT key,text FROM {table}_fts v JOIN {table} t ON t.rowid = v.rowid WHERE key IN ({','.join(['?']*len(keys))});"
        values = keys
        try:
            cursor = self.db.conn.cursor()
            cursor.execute(sql,values)
            for row in cursor:
                items[row[0]] = row[1]
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
        return items
    def search(self,query):
        table = self.name
        sql = f"""
            SELECT 
                key, 
                data,
                updated_at,
                bm25({table}_fts)
            FROM {table}_fts v
            JOIN {table} t ON t.rowid = v.rowid
            WHERE text MATCH ?
            ORDER BY bm25({table}_fts)
        """
        values = (text_to_fts(query),)
        try:
            cursor = self.db.conn.cursor()
            cursor.execute(sql,values)
            results = [
                {'key':row[0],'data':row[1],'updated_at':row[2],'score':row[3]}
                for row in cursor
            ]
            return {'suc':True,'data':results}
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
            return {'suc':False,'data':f"DB_ERROR|{e}({sql}){values}"}
    def iter(self,
             where='',
             select:List[Literal['key','data','text','updated_at']]=None,
             limit=0,offset=0
        ):
        if not select:
            select = ['key','data']
        if type(select) == str:
            select = [select]
        suc, sql_where, values = parse_where(where)
        if not suc:
            print(f"DB_ERROR| Illegal where: {sql_where}")
            return None
        if limit > 0:
            sql_where += f" LIMIT {limit}"
            if offset > 0:
                sql_where += f" OFFSET {offset}"
        table = self.name
        if 'text' not in select:
            fields = ','.join(select)
            sql = f"SELECT {fields} FROM {table} {sql_where};"
        else:
            fields = ','.join(select)
            sql = f"SELECT {fields} FROM {table} t JOIN {table}_fts v ON t.rowid = v.rowid {sql_where};"
        try:
            cursor = self.db.conn.cursor()
            cursor.execute(sql,values)
            for row in cursor:
                yield {k:v for k,v in zip(select,row)} if len(select)>1 else row[0]
        except Exception as e:
            print(f"DB_ERROR|{e}({sql},{values})")
            return None
    def texts(self):
        for x in self.iter('',['key','text']):
            yield x['key'],x['text']


#===========================================================================
# 关系型数据表 for 高性能结构化数据
#===========================================================================

class RelTable:
    def __init__(self, db, table_name):
        self.db = db
        self.name = table_name
        self.cursor = db.conn.cursor()
        self._col_names = None

    def ensure_table(self):
        table = self.name
        suc, ret = self.db.check_table_type(table)
        if not suc:
            return suc, ret
        if ret == 'RelTable':
            return True, "exist"
        if ret != 'Not Exist':
            return False, f"Failed to create RelTable({table}), already exist {ret}({table})"
        try:
            cursor = self.db.conn.cursor()
            sql = f"CREATE TABLE IF NOT EXISTS {table} (key TEXT PRIMARY KEY);"
            cursor.execute(sql)
            self.db.conn.commit()
            return True, 'ok'
        except Exception as e:
            return False, f"Error: {e}({sql})"

    def __str__(self):
        return f"RelTable({self.db.path})[{self.name}]"
    def __repr__(self):
        return f"RelTable({self.db.path})[{self.name}]"
    def __dir__(self):
        return list(self._get_columns().keys())
    def __contains__(self, key):
        return self.get_item(key) is not None
    def __getitem__(self, key):
        return self.get_item(key)
    def __setitem__(self, key, data):
        return self.upsert({key: data})
    def __len__(self):
        return self.count()

    def _get_columns(self):
        cursor = self.db.conn.cursor()
        cursor.execute(f"PRAGMA table_info({self.name})")
        return {row[1]: row[2] for row in cursor.fetchall()}

    def _get_column_names(self):
        if not hasattr(self, '_col_names') or self._col_names is None:
            cursor = self.db.conn.cursor()
            cursor.execute(f"SELECT * FROM {self.name} LIMIT 0")
            self._col_names = [desc[0] for desc in cursor.description]
        return self._col_names

    def _ensure_fields(self, data: dict):
        columns = self._get_columns()
        new_cols = {}
        for k, v in data.items():
            if k not in columns:
                if v is not None:
                    new_cols[k] = type_map.get(type(v), 'BLOB')
                else:
                    new_cols[k] = 'TEXT'
        if new_cols:
            try:
                cursor = self.db.conn.cursor()
                for col_name, col_type in new_cols.items():
                    cursor.execute(f"ALTER TABLE {self.name} ADD COLUMN {col_name} {col_type};")
                self.db.conn.commit()
                self._col_names = None  # invalidate cache
            except Exception as e:
                return False, f"Error adding columns: {e}"
        return True, 'ok'

    def _transform_value(self, v):
        if v is None:
            return None
        L = value_map.get(type(v))
        if L is not None:
            return L(v)
        if type(v) in value_map:
            return v
        return encode(v)

    def upsert(self, key_values: dict):
        if not key_values:
            return {'suc': True, 'data': "update 0 items."}
        # Normalize: ensure all values are dicts
        normalized = {}
        for key, val in key_values.items():
            if isinstance(val, dict):
                normalized[key] = val
            else:
                normalized[key] = {'value': val}
        # Group by column set for correct partial updates
        groups = {}
        for key, val in normalized.items():
            cols = tuple(sorted(val.keys()))
            if cols not in groups:
                groups[cols] = []
            groups[cols].append((key, val))
        total = 0
        cursor = self.db.conn.cursor()
        try:
            for cols, items in groups.items():
                # Ensure columns exist — collect sample for type inference
                sample = {c: None for c in cols}
                for _, val in items:
                    for c in cols:
                        if val.get(c) is not None and sample[c] is None:
                            sample[c] = val[c]
                suc, msg = self._ensure_fields(sample)
                if not suc:
                    return {'suc': False, 'data': msg}
                # Build SQL
                headers = ['key'] + list(cols)
                keys_sql = ','.join(headers)
                pins = ','.join(['?'] * len(headers))
                updates = ", ".join([f"{c}=excluded.{c}" for c in cols])
                sql = f"""INSERT INTO {self.name} ({keys_sql}) VALUES ({pins})
                          ON CONFLICT(key) DO UPDATE SET {updates};"""
                # Prepare values
                values_mat = []
                for key, val in items:
                    row = [key] + [self._transform_value(val.get(c)) for c in cols]
                    values_mat.append(row)
                cursor.executemany(sql, values_mat)
                total += len(items)
            self.db.conn.commit()
            return {'suc': True, 'data': f"update {total} items."}
        except Exception as e:
            print(f"DB_ERROR|{e}")
            return {'suc': False, 'data': str(e)}

    def get_item(self, key):
        table = self.name
        sql = f"SELECT * FROM {table} WHERE key = ?;"
        values = (key,)
        try:
            self.cursor.execute(sql, values)
            row = self.cursor.fetchone()
            if row:
                col_names = self._get_column_names()
                return dict(zip(col_names, row))
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
        return None

    def get_items(self, keys):
        items = {k: None for k in keys}
        if not keys:
            return items
        table = self.name
        sql = f"SELECT * FROM {table} WHERE key IN ({','.join(['?']*len(keys))});"
        values = keys
        try:
            cursor = self.db.conn.cursor()
            cursor.execute(sql, values)
            columns = [desc[0] for desc in cursor.description]
            for row in cursor:
                item = {k: decode(v) for k, v in zip(columns, row)}
                items[item['key']] = item
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
        return items

    def get(self, key, default=None):
        ret = self.get_item(key)
        return ret if ret is not None else default

    def count(self, where=''):
        suc, sql_where, values = parse_where(where, use_json_path=False)
        if not suc:
            print(f"DB_ERROR| Illegal where: {sql_where}")
            return None
        table = self.name
        sql = f"SELECT count(*) FROM {table} {sql_where};"
        try:
            self.cursor.execute(sql, values)
            row = self.cursor.fetchone()
            return row[0] if row else 0
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
            return None

    def iter(self, where='', select=None, limit=0, offset=0):
        table = self.name
        if select is None:
            fields = '*'
            single = False
        else:
            if isinstance(select, str):
                select = [select]
            fields = ','.join(select)
            single = len(select) == 1
        suc, sql_where, values = parse_where(where, use_json_path=False)
        if not suc:
            print(f"DB_ERROR| Illegal where: {sql_where}")
            return None
        if limit > 0:
            sql_where += f" LIMIT {limit}"
            if offset > 0:
                sql_where += f" OFFSET {offset}"
        sql = f"SELECT {fields} FROM {table} {sql_where};"
        try:
            cursor = self.db.conn.cursor()
            cursor.execute(sql, values)
            columns = [desc[0] for desc in cursor.description]
            for row in cursor:
                if single:
                    yield decode(row[0])
                else:
                    yield {k: decode(v) for k, v in zip(columns, row)}
        except Exception as e:
            print(f"DB_ERROR|{e}({sql},{values})")
            return None

    def get_one(self, where, default=None):
        items = list(self.iter(where=where, limit=1))
        if items:
            return items[-1]
        return default

    def keys(self):
        for x in self.iter('', select=['key']):
            yield x

    def values(self):
        for x in self.iter(''):
            yield x

    def items(self):
        for x in self.iter(''):
            yield x['key'], {k: v for k, v in x.items() if k != 'key'}

    def set_index(self, field):
        if not valid_identifier(field):
            print(f"DB_ERROR| Illegal identifier: {field}")
            return False
        sql = f'CREATE INDEX IF NOT EXISTS idx_{self.name}_{field} ON {self.name}({field});'
        try:
            cursor = self.db.conn.cursor()
            cursor.execute(sql)
            self.db.conn.commit()
            return True
        except Exception as e:
            print(f"DB_ERROR|{e}({sql})")
            return False

    def delete(self, where=''):
        suc, sql_where, values = parse_where(where, use_json_path=False)
        if not suc:
            print(f"DB_ERROR| Illegal where: {sql_where}")
            return {'suc': False, 'msg': f"Illegal where: {sql_where}"}
        sql = f"DELETE FROM {self.name} {sql_where};"
        try:
            cursor = self.db.conn.cursor()
            cursor.execute(sql, values)
            self.db.conn.commit()
            return {'suc': True}
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
            return {'suc': False, 'msg': str(e)}

    def inspect(self):
        return self._get_columns()


class DBS:
    def __init__(self,folder):
        self.folder = os.path.realpath(folder)
        self.dbs = {}

    
    def __getitem__(self, db_table):
        return self.get_table(db_table)
    
    def get_db(self,db_key):
        db_key = db_key.replace('/', '-')
        if db_key not in self.dbs:
            path_base = self.folder
            path_file = os.path.realpath(f"{self.folder}/{db_key}.sqlite")
            if not os.path.commonpath([path_base, path_file]) == path_base:
                print(f"DB_ERROR|Unsafe Database Path")
                return None
            db = DB(path_file,wal=True)
            self.dbs[db_key] = db
        return self.dbs[db_key]
    
    def get_table(self,db_table):
        db_key,table_name = os.path.split(db_table.replace('-','/'))
        db = self.get_db(db_key)
        if db == None:
            return None
        if table_name.startswith('vec_'):
            table = db.get_table(table_name[4:],'VecTable')
        elif table_name.startswith('fts_'):
            table = db.get_table(table_name[4:],'FtsTable')
        elif table_name.startswith('rel_'):
            table = db.get_table(table_name[4:],'RelTable')
        else:
            table = db.get_table(table_name,'Table')
        if table == None:
            return None
        return table
    
    def close(self):
        for db_key in list(self.dbs.keys()):
            del self.dbs[db_key]

def PCA_of_key_vectors(key_vectors):
    import numpy as np
    from sklearn.decomposition import PCA
    keys = []
    vectors = []
    for key,vector in key_vectors:
        keys.append(key)
        vectors.append(vector)
    data = np.array(vectors)
    pca = PCA(n_components=3)  # 固定3维
    reduced_data = pca.fit_transform(data)
    return {k:v.tolist() for k,v in zip(keys,reduced_data)}




if __name__ == "__main__":

    db = DBS('db')
    data = {
        "深度学习": "深度学习是机器学习的一个子领域",
        "数据库教程": "SQLite是轻量级的数据库，支持全文搜索",
        "Python编程": "Python是一种广泛使用的编程语言，适合数据分析",
        "test": "greet world!",
        "人工智能": "人工智能正在改变世界，包括自然语言处理技术",
        "NLP技术": "NLP stands for Natural Language Processing.",
        "Deep Learning": "Deep learning is a subset of machine learning.",
        "Tom": {'uid':1,'name':'Tom','age':12,'介绍':'Tom是一只猫'},
        "Jerry": {'uid':2,'name':'Jerry','age':7,'介绍':'Jerry是一只老鼠'},
        "Spike": {'uid':3,'name':'Spike','age':13,'介绍':'Spike是一只狗'},
    }
    print("== ==================== ==")
    print("==     测试 VecTable     ==")
    print("== ==================== ==")
    docs = db['test_DB-vec_docs']
    ret = docs.upsert(data)
    print(ret)
    print("== iter keys ==")
    for key in docs.keys():
        print(key)
    print("== iter values ==")
    for value in docs.values():
        print(value)
    print("== iter vectors ==")
    for key,vectors in docs.vectors():
        print(key,vectors[:5])
    print("== iter sel ==")
    for r in docs.iter('name == Tom'):
        print(r)
    print("== iter sel ==")
    for r in docs.iter('uid in [2,3]'):
        print(r)
    print("== search ==")
    ret = docs.search("猫和老鼠")
    if ret['suc']:
        for item in ret['data']:
            print(item['key'])
            print(item['data'])
            print(item['updated_at'])
            print(item['vector'][:3])
            print(item['distance'])
            print('---')
    else:
        print(ret['data'])

    print("== ==================== ==")
    print("==     测试 FtsTable     ==")
    print("== ==================== ==")
    docs = db['test_DB-fts_docs2']
    ret = docs.upsert(data)
    print(ret)
    print("== iter keys ==")
    for key in docs.keys():
        print(key)
    print("== iter values ==")
    for value in docs.values():
        print(value)
    print("== iter texts ==")
    for key,text in docs.texts():
        print(key,text[:30])
    print("== iter sel ==")
    for r in docs.iter('name == Tom'):
        print(r)
    print("== iter sel ==")
    for r in docs.iter('uid in [2,3]'):
        print(r)
    print("== search ==")
    ret = docs.search('"数据" AND (数分 OR 库)')
    if ret['suc']:
        for item in ret['data']:
            print(item['key'])
            print(item['data'])
            print(item['updated_at'])
            print(item['score'])
            print('---')
    else:
        print(ret['data'])

    print("== ==================== ==")
    print("==     测试 Table     ==")
    print("== ==================== ==")
    docs = db['test_DB-docs3']
    ret = docs.upsert(data)
    print(ret)
    print("== iter keys ==")
    for key in docs.keys():
        print(key)
    print("== iter values ==")
    for value in docs.values():
        print(value)

    print("== iter sel ==")
    for r in docs.iter('name == Tom'):
        print(r)
    print("== iter sel ==")
    for r in docs.iter('uid in [2,3]'):
        print(r)

    print("== ==================== ==")
    print("==     测试 RelTable     ==")
    print("== ==================== ==")
    users = db['test_DB-rel_users']
    ret = users.upsert({
        'U001': {'name': 'Tom', 'age': 12, 'tags': ['cat', 'pet']},
        'U002': {'name': 'Jerry', 'age': 7},
        'U003': {'name': 'Spike', 'age': 13},
    })
    print("upsert:", ret)
    # 部分更新（只改 age，不影响 name）
    ret = users.upsert({'U001': {'age': 13}})
    print("partial upsert:", ret)
    # dict 操作
    print("get_item:", users.get_item('U001'))
    print("getitem:", users['U001'])
    print("contains U001:", 'U001' in users)
    print("contains N/A:", 'N/A' in users)
    print("len:", len(users))
    print("inspect:", users.inspect())
    # 迭代
    print("== iter keys ==")
    for key in users.keys():
        print(key)
    print("== iter values ==")
    for val in users.values():
        print(val)
    print("== iter items ==")
    for key, val in users.items():
        print(key, val)
    print("== iter where ==")
    for r in users.iter('age > 10'):
        print(r)
    print("== iter select ==")
    for r in users.iter(select=['name', 'age']):
        print(r)
    print("== count ==")
    print(users.count())
    print(users.count('age > 10'))
    print("== get_one ==")
    print(users.get_one('name == "Tom"'))
    # 索引
    print("set_index:", users.set_index('age'))
    # delete
    print("delete:", users.delete('name == "Spike"'))
    print("count after delete:", len(users))
    db.close()
