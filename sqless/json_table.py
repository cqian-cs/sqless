from typing import List, Literal

from .database import parse_where,retry_on_db_lock
import orjson

BATCH_SIZE_WRITE = 100_000
BATCH_SIZE_READ = 1000

class JsonTable:
    def __init__(self, db, table_name):
        self.db = db
        self.name = table_name

    def ensure_table(self):
        table = self.name
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
            return True, 'ok'
        except Exception as e:
            return False, f"Ensuring fields error: {e}({sql})"

    def __str__(self):
        return f"{self.__class__.__name__}({self.db.path})[{self.name}]"

    def __repr__(self):
        return f"{self.__class__.__name__}({self.db.path})[{self.name}]"

    def __dir__(self):
        return ['key', 'data', 'updated_at']

    def __contains__(self, key):
        return self.get(key) is not None

    def __getitem__(self, key):
        ret = self.get_item(key)
        return ret['data']

    def __setitem__(self, key, data):
        return self.upsert({key: data})

    def __len__(self):
        return self.count()

    def __iter__(self):
        return self.iter()
    
    def __delitem__(self, key):
        return self.delete(f'key = "{key}"')

    def pre_upsert(self, key_values: dict):
        key_old_values = self.get_items(list(key_values.keys()))
        processed_key_values = {}
        for key, new_value in key_values.items():
            old_value = key_old_values[key]['data']
            if type(old_value) == type(new_value) and old_value == new_value:
                continue
            if type(old_value) == type(new_value) == dict:
                no_diff = True
                for k, v in new_value.items():
                    if v != old_value.get(k):
                        no_diff = False
                        old_value[k] = v
                    if v is None:
                        old_value.pop(k, None)
                if no_diff:
                    continue
                new_value = old_value
            processed_key_values[key] = new_value
        return processed_key_values

    @retry_on_db_lock(max_retries=6, base_delay=0.05)
    def upsert(self, key_values: dict, is_delta=False):
        table = self.name
        processed_key_values = self.pre_upsert(key_values) if is_delta else key_values
        processed_key_values = key_values
        if len(processed_key_values) == 0:
            return {'suc': True, 'data': "update 0 items."}
        key_texts = {k: orjson.dumps(v).decode('utf-8') for k, v in processed_key_values.items()}
        try:
            cursor = self.db.conn.cursor()
            sql = f"INSERT OR REPLACE INTO {table}(key,data) VALUES (?, ?)"
            values = [(k, v) for k, v in key_texts.items()]
            cursor.executemany(sql, values)
            self.db.conn.commit()
            return {'suc': True, 'data': f"update {len(processed_key_values)} items."}
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
            return {'suc': False, 'data': str(e), 'debug': sql}

    def get(self, key, default=None):
        ret = self.get_item(key)
        return ret['data'] or default

    def get_item(self, key):
        table = self.name
        sql = f"SELECT key,data,updated_at FROM {table} WHERE key = ?;"
        values = (key,)
        try:
            cursor = self.db.conn.cursor()
            cursor.execute(sql, values)
            row = cursor.fetchone()
            if row:
                return {'key': row[0], 'data': row[1], 'updated_at': row[2]}
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
        return {'key': key, 'data': None, 'updated_at': None}

    def get_items(self, keys):
        if len(keys) > BATCH_SIZE_READ:
            return self._get_items_tempjoin(keys)
        items = {k: {'key': k, 'data': None, 'updated_at': None} for k in keys}
        table = self.name
        sql = f"SELECT key,data,updated_at FROM {table} WHERE key IN ({','.join(['?'] * len(keys))});"
        values = keys
        try:
            cursor = self.db.conn.cursor()
            cursor.execute(sql, values)
            for row in cursor:
                items[row[0]] = {'key': row[0], 'data': row[1], 'updated_at': row[2]}
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
        return items

    @retry_on_db_lock(max_retries=6, base_delay=0.05)
    def _get_items_tempjoin(self,keys):
        cursor = self.db.conn.cursor()
        items = {k: {'key': k, 'data': None, 'updated_at': None} for k in keys}
        cursor.execute("CREATE TEMP TABLE tmp_get_item_keys(key TEXT PRIMARY KEY)")
        for i in range(0, len(keys), BATCH_SIZE_WRITE):
            batch_keys = keys[i:i+BATCH_SIZE_WRITE]
            cursor.executemany("INSERT INTO tmp_get_item_keys(key) VALUES (?)", [(k,) for k in batch_keys])
        sql = f"SELECT t.key, t.data, t.updated_at FROM {self.name} t JOIN tmp_get_item_keys k ON t.key = k.key"
        cursor.execute(sql)
        for row in cursor.fetchall():
            items[row[0]] = {'key': row[0], 'data': row[1], 'updated_at': row[2]}
        cursor.execute("DROP TABLE tmp_get_item_keys")
        return items
    def count(self, where=''):
        suc, sql_where, values = parse_where(where)
        if not suc:
            print(f"DB_ERROR| Illegal where: {sql_where}")
            return None
        table = self.name
        sql = f"SELECT count(*) FROM {table} {sql_where};"
        try:
            cursor = self.db.conn.cursor()
            cursor.execute(sql, values)
            row = cursor.fetchone()
            return row[0] if row else 0
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
            return None

    def iter(self,
             where='',
             select: List[Literal['key', 'data', 'updated_at']] = None,
             limit=0, offset=0
             ):
        if not select:
            select = ['key', 'data', 'updated_at']
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
            cursor.execute(sql, values)
            for row in cursor:
                yield {k: v for k, v in zip(select, row)} if len(select) > 1 else row[0]
        except Exception as e:
            print(f"DB_ERROR|{e}({sql},{values})")
            return None

    def get_one(self, where, default=None):
        items = list(self.iter(where=where, select=['key', 'data', 'updated_at'], limit=1))
        if items:
            return items[-1]
        return default

    def delete(self, where=''):
        if not where:
            return {'suc': False, 'msg': f"Not specified where. To delete all data, use db.drop_table({self.name}) instead."}
        suc, sql_where, values = parse_where(where)
        if not suc:
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

    def keys(self):
        for x in self.iter('', ['key']):
            yield x

    def values(self):
        for x in self.iter('', ['data']):
            yield x

    def items(self):
        for x in self.iter('', ['key', 'data']):
            yield x['key'], x['data']
