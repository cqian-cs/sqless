from typing import List, Literal
import orjson,pickle
from .database import parse_where, valid_identifier, retry_on_db_lock

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

if 'np' in globals():
    try:
        np = globals()['np']
        for _t in [np.int8, np.int16, np.int32, np.int64, np.uint8, np.uint16, np.uint32, np.uint64]:
            type_map[_t] = 'INTEGER'
            value_map[_t] = int
        for _t in [np.float16, np.float32, np.float64]:
            type_map[_t] = 'REAL'
            value_map[_t] = float
    except Exception:
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


class RelTable:
    def __init__(self, db, table_name):
        self.db = db
        self.name = table_name
        self.cursor = db.conn.cursor()
        self._col_names = None

    def ensure_table(self):
        table = self.name
        try:
            cursor = self.db.conn.cursor()
            sql = f"CREATE TABLE IF NOT EXISTS {table} (key TEXT PRIMARY KEY) WITHOUT ROWID;"
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
    def __iter__(self):
        return self.iter()
    
    def __delitem__(self, key):
        return self.delete(f'key = "{key}"')
    
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
                self._col_names = None
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

    @retry_on_db_lock(max_retries=6, base_delay=0.05)
    def upsert(self, key_values: dict):
        if not key_values:
            return {'suc': True, 'data': "update 0 items."}
        normalized = {}
        for key, val in key_values.items():
            if isinstance(val, dict):
                normalized[key] = val
            else:
                normalized[key] = {'value': val}
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
                sample = {c: None for c in cols}
                for _, val in items:
                    for c in cols:
                        if val.get(c) is not None and sample[c] is None:
                            sample[c] = val[c]
                suc, msg = self._ensure_fields(sample)
                if not suc:
                    return {'suc': False, 'data': msg}
                headers = ['key'] + list(cols)
                keys_sql = ','.join(headers)
                pins = ','.join(['?'] * len(headers))
                updates = ", ".join([f"{c}=excluded.{c}" for c in cols])
                sql = f"""INSERT INTO {self.name} ({keys_sql}) VALUES ({pins})
                          ON CONFLICT(key) DO UPDATE SET {updates};"""
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
                return {k: decode(v) for k, v in zip(col_names, row)}
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
        return None

    def get_items(self, keys):
        items = {k: None for k in keys}
        if not keys:
            return items
        table = self.name
        sql = f"SELECT * FROM {table} WHERE key IN ({','.join(['?'] * len(keys))});"
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

    def iter(self, where='', select=None, limit=0, offset=0, mat_mode=False):
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
            if mat_mode and not single:
                yield columns
                for row in cursor:
                    yield [decode(v) for v in row]
            else:
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
        if not where:
            return {'suc': False, 'msg': f"Not specified where. To delete all data, use db.drop_table({self.name}) instead."}
        suc, sql_where, values = parse_where(where, use_json_path=False)
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

    def inspect(self):
        return self._get_columns()
