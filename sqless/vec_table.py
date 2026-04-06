"""
Requires extras: pip install sqless[vec]
"""

import orjson
import os
import struct
from typing import Union, List
import sqlite_vec
import numpy as np
import aiohttp
import asyncio
from .database import parse_where
from .json_table import JsonTable


bytes2vec = lambda b: list(struct.unpack(f"<{len(b) // 4}f", b))
vec2bytes = lambda v: struct.pack(f"<{len(v)}f", *v)

async def fetch_json(options, custom):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
        try:
            async with session.request(**options) as response:
                content = await response.text()
        except Exception as e:
            return {'suc': False, 'data': f"Network Error: {str(e)}", **custom}
        try:
            data = orjson.loads(content)
        except Exception as e:
            return {'suc': False, 'data': f"JSON Error: {str(e)}", **custom}
        return {'suc': True, 'data': data, **custom}


async def embedding(txts: Union[str, List[str]]):
    if type(txts) == str:
        txts = [txts]
    limit = 16002
    results = [None for _ in range(len(txts))]
    i_lens = [(i, len(x)) for i, x in enumerate(txts)]
    for i, l in i_lens:
        if l > limit:
            return {'suc': False, 'data': f'index {i} ({txts[i][:20]}) exceed size limit.'}
    i_lens.sort(key=lambda x: x[1], reverse=True)
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
    for future in asyncio.as_completed([
        fetch_json(
            options={
                "url": "https://api.siliconflow.cn/v1/embeddings",
                "method": "POST",
                "headers": {
                    "Authorization": f"Bearer {os.getenv('SILICON_API_KEY')}",
                    "Content-Type": "application/json"
                },
                "json": {
                    "model": "BAAI/bge-m3",
                    "input": [txts[i] for i in batches_of_indice]
                }
            },
            custom={'batches_of_indice': batches_of_indice}
        )
        for batches_of_indice in batches_of_indices
    ]):
        ret = await future
        if not ret['suc']:
            return ret
        embeddings = [x['embedding'] for x in ret['data']['data']]
        for i, embedding_val in zip(ret['batches_of_indice'], embeddings):
            results[i] = embedding_val
    return {'suc': True, 'data': results}


async def async_dict_to_vec(key_texts: dict, to_bytes=False):
    ret = await embedding(list(key_texts.values()))
    if not ret['suc']:
        return ret
    if to_bytes:
        return {'suc': True, 'data': {k: vec2bytes(v) for k, v in zip(key_texts.keys(), ret['data'])}}
    return {'suc': True, 'data': {k: v for k, v in zip(key_texts.keys(), ret['data'])}}


def dict_to_vec(key_texts: dict, to_bytes=False):
    ret = asyncio.run(embedding(list(key_texts.values())))
    if not ret['suc']:
        return ret
    if to_bytes:
        return {'suc': True, 'data': {k: vec2bytes(v) for k, v in zip(key_texts.keys(), ret['data'])}}
    return {'suc': True, 'data': {k: v for k, v in zip(key_texts.keys(), ret['data'])}}


def UMAP_of_key_vectors(key_vectors,is_3d=True):
    import umap # pip install umap-learn
    keys = []
    vectors = []
    for key, vector in key_vectors:
        keys.append(key)
        vectors.append(vector)
    data = np.array(vectors)
    pca = umap.UMAP(
        n_components=3 if is_3d else 2,
        n_neighbors=15,
        min_dist=0.1,         
        metric='cosine',
        random_state=42
    )
    reduced_data = pca.fit_transform(data)
    print(reduced_data)
    return {k: v.tolist() for k, v in zip(keys, reduced_data)}


class VecTable(JsonTable):
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
                CREATE VIRTUAL TABLE IF NOT EXISTS {table}_vec
                USING vec0(
                    vector FLOAT[1024]
                );
            """
            cursor.execute(sql)
            self.db.conn.commit()
            return True, 'ok'
        except Exception as e:
            return False, f"Ensuring fields error: {e}({sql})"

    def __dir__(self):
        return ['key', 'data', 'vector', 'updated_at']

    def _upsert(self, key_texts: dict, key_vecs: dict):
        table = self.name
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
            sql = f"DELETE FROM {table}_vec WHERE rowid = ?"
            values = [(rowid,) for rowid in key_rowid.values()]
            cursor.executemany(sql, values)
            sql = f"INSERT INTO {table}_vec(rowid,vector) VALUES (?, ?)"
            values = [(key_rowid.get(k), v) for k, v in key_vecs.items()]
            cursor.executemany(sql, values)
            self.db.conn.commit()
            return {'suc': True, 'data': f"update {len(key_vecs)} items."}
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
            return {'suc': False, 'data': str(e), 'debug': sql}

    def upsert(self, key_values: dict, is_delta=False):
        processed_key_values = self.pre_upsert(key_values) if is_delta else key_values
        if len(processed_key_values) == 0:
            return {'suc': True, 'data': "update 0 items."}
        key_texts = {k: orjson.dumps(v).decode('utf-8') for k, v in processed_key_values.items()}
        ret = dict_to_vec(key_texts, to_bytes=True)
        if not ret['suc']:
            return ret
        key_vecs = ret['data']
        return self._upsert(key_texts, key_vecs)

    async def async_upsert(self, key_values: dict, is_delta=False):
        processed_key_values = self.pre_upsert(key_values) if is_delta else key_values
        if len(processed_key_values) == 0:
            return {'suc': True, 'data': "update 0 items."}
        key_texts = {k: orjson.dumps(v).decode('utf-8') for k, v in processed_key_values.items()}
        ret = await async_dict_to_vec(key_texts, to_bytes=True)
        if not ret['suc']:
            return ret
        key_vecs = ret['data']
        return self._upsert(key_texts, key_vecs)

    def get_vectors(self, keys, raw=False):
        items = {k: None for k in keys}
        table = self.name
        sql = f"SELECT key,vector FROM {table}_vec v JOIN {table} t ON t.rowid = v.rowid WHERE key IN ({','.join(['?'] * len(keys))});"
        values = keys
        try:
            cursor = self.db.conn.cursor()
            cursor.execute(sql, values)
            for row in cursor:
                items[row[0]] = row[1] if raw else bytes2vec(row[1])
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
        return items

    def get_graph(self, keys, k=2, max_hop=2, is_3d=False, state={}):
        current_hop = state.get('current_hop') or 0
        nodes = state.get('nodes') or {}
        nodes_key_vectors = state.get('nodes_key_vectors') or {}
        edges = state.get('edges') or {}
        key_vectors = [(k, v) for k, v in self.get_vectors(keys, raw=True).items() if v]
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
            cursor.executemany(sql, values)
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
            values = (k + 1,)
            cursor.execute(sql, values)
            for row in cursor:
                src = row[0]
                dst = row[1]
                data = row[2]
                weight = row[3]
                vector = bytes2vec(row[4])
                if dst not in nodes:
                    if is_3d:
                        nodes[dst] = {'x': 0, 'y': 0, 'z': 0, 'data': data, 'c': current_hop + 1}
                    else:
                        nodes[dst] = {'x': 0, 'y': 0, 'data': data, 'c': current_hop + 1}
                    nodes_key_vectors[dst] = vector
                if src != dst:
                    edges[(src, dst)] = weight
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
            return {'suc': False, 'data': f"DB_ERROR|{e}({sql}){values}"}
        if current_hop < max_hop:
            self.get_graph(list(nodes.keys()), k, max_hop, is_3d, {
                'current_hop': current_hop + 1,
                'nodes': nodes,
                'nodes_key_vectors': nodes_key_vectors,
                'edges': edges
            })
        if current_hop == 0:
            if len(nodes) < 3:
                if is_3d:
                    for key, (x, y, z) in zip(nodes.keys(), [(0, 0, 0), (0, 1, 0), (1, 0, 0)]):
                        nodes[key]['x'] = x
                        nodes[key]['y'] = y
                        nodes[key]['z'] = z
                else:
                    for key, (x, y) in zip(nodes.keys(), [(0, 0), (1, 0)]):
                        nodes[key]['x'] = x
                        nodes[key]['y'] = y
            else:
                if is_3d:
                    key_xyz = UMAP_of_key_vectors(nodes_key_vectors.items(),is_3d)
                    for key, (x, y, z) in key_xyz.items():
                        nodes[key]['x'] = x
                        nodes[key]['y'] = y
                        nodes[key]['z'] = z
                else:
                    key_xy = UMAP_of_key_vectors(nodes_key_vectors.items(),is_3d)
                    for key, (x, y) in key_xy.items():
                        nodes[key]['x'] = x
                        nodes[key]['y'] = y
            for key in keys:
                if key in nodes:
                    nodes[key]['c'] = 0
            edges = [[src, dst, weight] for (src, dst), weight in edges.items()]
        return {'suc': True, 'data': {'nodes': nodes, 'edges': edges}}

    def search(self, query, k=5):
        table = self.name
        sql = f"""
            SELECT
                key,
                data,
                updated_at,
                distance
            FROM {table}_vec v
            JOIN {table} t ON t.rowid = v.rowid
            WHERE vector MATCH ? AND k = ?
            ORDER BY distance
        """
        ret = dict_to_vec({'key': orjson.dumps(query).decode()}, to_bytes=True)
        if not ret['suc']:
            return ret
        values = (ret['data']['key'], k)
        try:
            cursor = self.db.conn.cursor()
            cursor.execute(sql, values)
            results = [
                {'key': row[0], 'data': row[1], 'updated_at': row[2],  'distance': row[3]}
                for row in cursor
            ]
            return {'suc': True, 'data': results}
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
            return {'suc': False, 'data': f"DB_ERROR|{e}({sql}){values}"}

    def iter(self,
             where='',
             select: List[str] = None,
             limit=0, offset=0
             ):
        from typing import Literal
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
            cursor.execute(sql, values)
            for row in cursor:
                if i_vec is not None:
                    row = list(row)
                    row[i_vec] = bytes2vec(row[i_vec])
                yield {k: v for k, v in zip(select, row)} if len(select) > 1 else row[0]
        except Exception as e:
            print(f"DB_ERROR|{e}({sql},{values})")
            return None

    def vectors(self):
        for x in self.iter('', ['key', 'vector']):
            yield x['key'], x['vector']
