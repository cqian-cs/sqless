import orjson
import os
import re
import sqlite3
import ast

# Optional: sqlite_vec (needed to load extension in DB.__init__)
_sqlite_vec_available = False
try:
    import sqlite_vec
    _sqlite_vec_available = True
except ImportError:
    pass

sqlite3.register_converter("JSON", lambda b: orjson.loads(b))

def parse_val(val):
    try:
        return ast.literal_eval(val)
    except:
        return val

def parse_col(col):
    if col in ['key', 'updated_at']:
        return col
    return f"data->>'$.{col}'"




def parse_where(where_str, use_json_path=True):
    """
    Parse safe WHERE expressions into (sql, params) with AND/OR/NOT, parentheses, and ORDER BY.

    Returns:
        (True, sql_string, params_list) on success
        (False, error_message, []) on parse error
    """
    allowed_ops = {'=', '==', '!=', '<', '>', '<=', '>=', 'like', 'ilike', 'is', 'in'}

    def valid_identifier(s):
        return re.match(r'^[A-Za-z_\u2E80-\u9FFF][A-Za-z0-9_\u2E80-\u9FFF]*$', s) is not None

    if not where_str:
        return True, '', []

    s = where_str.strip()

    if any(x in s for x in (';', '--', '/*', '*/')):
        return False, 'contains forbidden characters', []

    m_order = re.search(r'\border\s+by\b', s, re.IGNORECASE)
    if m_order:
        where_part = s[:m_order.start()].strip()
        order_part = s[m_order.end():].strip()
    else:
        where_part = s
        order_part = ''

    token_pattern = r"""
        (\() |
        (\)) |
        ("[^"]*") |
        ('[^']*') |
        (\bAND\b|\bOR\b|\bNOT\b) |
        (<=|>=|!=|==|=|<|>|like|ilike|is|in) |
        ([^\s()]+)
    """
    tokens = [t for t in re.findall(token_pattern, where_part, re.IGNORECASE | re.VERBOSE)]
    tokens = [next(filter(None, t)) for t in tokens]

    sql_parts = []
    params = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]

        if tok in ('(', ')'):
            sql_parts.append(tok)
            i += 1
            continue

        if tok.upper() in ('AND', 'OR', 'NOT'):
            sql_parts.append(tok.lower())
            i += 1
            continue

        if i + 2 >= len(tokens):
            return False, f"Invalid condition near: {tok}", []

        _col, op, _val = tokens[i], tokens[i + 1].lower(), tokens[i + 2]

        if not valid_identifier(_col):
            return False, f"Invalid column name: {_col}", []

        col = parse_col(_col) if use_json_path else _col

        if op not in allowed_ops:
            return False, f"Operator not allowed: {op}", []

        if _val.lower() == 'null' and op == 'is':
            sql_parts.append(f"{col} is null")
        else:
            val = parse_val(_val)
            if type(val) == list:
                sql_parts.append(f"{col} {op} ({','.join(['?'] * len(val))})")
                params += val
            else:
                sql_parts.append(f"{col} {op} ?")
                params.append(val)

        i += 3

    sql = "where " + " ".join(sql_parts)

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




identifier_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-]*[A-Za-z0-9]$")

def valid_identifier(name: str) -> bool:
    return bool(name and identifier_re.fullmatch(name))

path_this = os.path.dirname(os.path.abspath(__file__))

TABLE_NAME_SUFFIXES=[
    '_fts',
    '_fts_config',
    '_fts_content',
    '_fts_data',
    '_fts_docsize',
    '_fts_idx',
    '_vec',
    '_vec_chunks',
    '_vec_info',
    '_vec_rowids',
    '_vec_vector_chunks00',
]

class DB:
    def __init__(self, path_db=f"{path_this}/util_db_vec.sqlite", wal=True):
        self.path = os.path.realpath(path_db)
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self.conn = sqlite3.connect(self.path, detect_types=sqlite3.PARSE_DECLTYPES)
        self.tables = {}
        if _sqlite_vec_available:
            self.conn.enable_load_extension(True)
            sqlite_vec.load(self.conn)
        if wal:
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
        return self.list_tables(is_hidden=True)

    def __contains__(self, table_name):
        suc, ret = self.check_table_exists(table_name)
        return suc and ret

    def __getitem__(self, table_name):
        return self.get_table(table_name)
    def __delitem__(self, key):
        return self.drop_table(key)
    def close(self):
        if self.conn:
            self.conn.close()

    def drop_table(self, table_name):
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

    def get_table(self, table_name):
        if table_name not in self.tables:
            if table_name.startswith('json_'):
                from .json_table import JsonTable
                new_table = JsonTable(self, table_name)
            elif table_name.startswith('fts_'):
                from .fts_table import FtsTable
                new_table = FtsTable(self, table_name)
            elif table_name.startswith('vec_'):
                from .vec_table import VecTable
                new_table = VecTable(self, table_name)
            else:
                from .rel_table import RelTable
                new_table = RelTable(self, table_name)
            suc, msg = new_table.ensure_table()
            if not suc:
                print(msg)
                return None
            self.tables[table_name] = new_table
        return self.tables[table_name]

    def check_table_exists(self, table_name):
        sql = f"SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        values = (table_name,)
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql, values)
            return True, cursor.fetchone() is not None
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
            return False, f"DB_ERROR|{e}({sql}){values}"

    def list_tables(self,is_hidden=False):
        sql = """
            SELECT m.name
            FROM sqlite_master m
            WHERE m.type = 'table';
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            if is_hidden:
                return [name[0] for name in cursor.fetchall()
                        if name[0] != 'sqlite_sequence'
                        and not any(name[0].endswith(x) for x in TABLE_NAME_SUFFIXES)]
            else:
                return [name[0] for name in cursor.fetchall()
                        if name[0] != 'sqlite_sequence']
        except Exception as e:
            print(f"DB_ERROR|{e}({sql})")
            return []


#===========================================================================
# Multi-Database Manager
#===========================================================================

class DBS:
    def __init__(self, folder):
        self.folder = os.path.realpath(folder)
        self.dbs = {}

    def __getitem__(self, db_table):
        return self.get_table(db_table)

    def get_db(self, db_key):
        db_key = db_key.replace('/', '-')
        if db_key not in self.dbs:
            path_base = self.folder
            path_file = os.path.realpath(f"{self.folder}/{db_key}.sqlite")
            if not os.path.commonpath([path_base, path_file]) == path_base:
                print("DB_ERROR|Unsafe Database Path")
                return None
            db = DB(path_file, wal=True)
            self.dbs[db_key] = db
        return self.dbs[db_key]

    def get_table(self, db_table):
        db_key, table_name = os.path.split(db_table.replace('-', '/'))
        db = self.get_db(db_key)
        if db is None:
            return None
        return db.get_table(table_name)

    def close(self):
        for db_key in list(self.dbs.keys()):
            del self.dbs[db_key]

