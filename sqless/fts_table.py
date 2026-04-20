import orjson
import re
from typing import List, Literal

from .database import parse_where,retry_on_db_lock
from .json_table import JsonTable

anyword_re = re.compile(r'[\u2E80-\u9FFF]|[A-Za-z0-9_]+|[^\sA-Za-z0-9_\u2E80-\u9FFF]')


def text_to_fts(text):
    return ' '.join(anyword_re.findall(text))


class FtsTable(JsonTable):
    def __init__(self, db, table_name):
        super().__init__(db, table_name)

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
            sql = f"""
                CREATE VIRTUAL TABLE IF NOT EXISTS {table}_fts
                USING fts5(
                    text
                );
            """
            cursor.execute(sql)
            self.db.conn.commit()
            return True, 'ok'
        except Exception as e:
            return False, f"Ensuring fields error: {e}({sql})"
    
    @retry_on_db_lock(max_retries=6, base_delay=0.05)
    def upsert(self, key_values: dict, is_delta=False):
        table = self.name
        processed_key_values = self.pre_upsert(key_values) if is_delta else key_values
        if len(processed_key_values) == 0:
            return {'suc': True, 'data': "update 0 items."}
        key_texts = {k: orjson.dumps(v).decode('utf-8') for k, v in processed_key_values.items()}
        key_fts = {k: text_to_fts(v) for k, v in key_texts.items()}
        key_rowid = {}
        try:
            cursor = self.db.conn.cursor()
            sql = f"INSERT OR REPLACE INTO {table}(key,data) VALUES (?, ?)"
            values = [(k, v) for k, v in key_texts.items()]
            cursor.executemany(sql, values)
            sql = f"SELECT rowid, key FROM {table} WHERE key in ({','.join(['?'] * len(key_texts))})"
            values = tuple(key_texts.keys())
            cursor.execute(sql, values)
            for row in cursor.fetchall():
                key_rowid[row[1]] = row[0]
            sql = f"DELETE FROM {table}_fts WHERE rowid = ?"
            values = [(rowid,) for rowid in key_rowid.values()]
            cursor.executemany(sql, values)
            sql = f"INSERT INTO {table}_fts(rowid,text) VALUES (?, ?)"
            values = [(key_rowid.get(k), v) for k, v in key_fts.items()]
            cursor.executemany(sql, values)
            self.db.conn.commit()
            return {'suc': True, 'data': f"update {len(processed_key_values)} items."}
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
            return {'suc': False, 'data': str(e), 'debug': sql}

    def get_texts(self, keys):
        items = {k: None for k in keys}
        table = self.name
        sql = f"SELECT key,text FROM {table}_fts v JOIN {table} t ON t.rowid = v.rowid WHERE key IN ({','.join(['?'] * len(keys))});"
        values = keys
        try:
            cursor = self.db.conn.cursor()
            cursor.execute(sql, values)
            for row in cursor:
                items[row[0]] = row[1]
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
        return items

    def search(self, query):
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
            cursor.execute(sql, values)
            results = [
                {'key': row[0], 'data': row[1], 'updated_at': row[2], 'score': row[3]}
                for row in cursor
            ]
            return {'suc': True, 'data': results}
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
            return {'suc': False, 'data': f"DB_ERROR|{e}({sql}){values}"}

    def iter(self,
             where='',
             select: List[Literal['key', 'data', 'text', 'updated_at']] = None,
             limit=0, offset=0
             ):
        if not select:
            select = ['key', 'data']
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
            cursor.execute(sql, values)
            for row in cursor:
                yield {k: v for k, v in zip(select, row)} if len(select) > 1 else row[0]
        except Exception as e:
            print(f"DB_ERROR|{e}({sql},{values})")
            return None

    def texts(self):
        for x in self.iter('', ['key', 'text']):
            yield x['key'], x['text']
