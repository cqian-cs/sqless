import os
import time
import random
import sqlite3
import numpy as np
from contextlib import contextmanager
path_this = os.path.dirname(os.path.abspath(__file__))
DB_PATH = f"{path_this}/bench.db"

N_ROWS = 100_000
N_READS = 200_000
HOT_RATIO = 0.2
ZIPF_A = 1.5


# =========================
# utils
# =========================
def now():
    return time.time()

@contextmanager
def timer(name, result):
    t0 = now()
    yield
    result[name] = now() - t0

def reset_sqlite():
    for f in [DB_PATH, DB_PATH+"-wal", DB_PATH+"-shm"]:
        if os.path.exists(f):
            os.remove(f)


# =========================
# data
# =========================
def generate_data():
    np.random.seed(42)
    random.seed(42)
    keys = [f"C{i}" for i in range(N_ROWS)]

    zipf_ids = np.random.zipf(ZIPF_A, size=N_READS) % N_ROWS
    hot_ids = np.random.randint(0, int(N_ROWS*HOT_RATIO), size=N_READS//2)

    read_ids = np.concatenate([zipf_ids, hot_ids]) % N_ROWS
    random.shuffle(read_ids)

    reads = [f"C{i}" for i in read_ids]
    return keys, reads


# =========================
# sqlite raw (baseline)
# =========================
def test_sqlite_raw(keys, reads):
    reset_sqlite()
    result = {}

    with timer("init", result):
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE cats(name TEXT, update_time INTEGER)")
        conn.execute("CREATE INDEX idx_name ON cats(name)")
        conn.commit()

    with timer("write", result):
        conn.execute("BEGIN")
        for k in keys:
            conn.execute("INSERT INTO cats VALUES (?,?)", (k, int(time.time())))
        conn.commit()

    with timer("update", result):
        conn.execute("BEGIN")
        for k in keys[:len(keys)//10]:
            conn.execute("UPDATE cats SET update_time=? WHERE name=?", (int(time.time()), k))
        conn.commit()

    with timer("read", result):
        for k in reads:
            conn.execute("SELECT * FROM cats WHERE name=?", (k,)).fetchone()

    conn.close()
    return result


# =========================
# dataset
# =========================
def test_dataset(keys, reads):
    import dataset
    reset_sqlite()
    result = {}

    with timer("init", result):
        db = dataset.connect(f"sqlite:///{DB_PATH}")
        db.query("PRAGMA journal_mode=WAL")
        table = db["cats"]
        table.insert({"name": "C0", "update_time": int(time.time())})
        db.query("CREATE INDEX idx_name ON cats(name)")

    with timer("write", result):
        with db as tx:
            for k in keys:
                table.insert(dict(name=k, update_time=int(time.time())))

    with timer("update", result):
        with db as tx:
            for k in keys[:len(keys)//10]:
                table.update(dict(name=k, update_time=int(time.time())), ["name"])

    with timer("read", result):
        for k in reads:
            table.find_one(name=k)
    db.close()
    return result


# =========================
# pony ORM
# =========================
def test_pony(keys, reads):
    from pony import orm
    reset_sqlite()
    result = {}

    with timer("init", result):
        db = orm.Database()

        class Cat(db.Entity):
            name = orm.Required(str, index=True)
            update_time = orm.Required(int)

        db.bind("sqlite", DB_PATH, create_db=True)
        db.generate_mapping(create_tables=True)

        import sqlite3
        sqlite3.connect(DB_PATH).execute("PRAGMA journal_mode=WAL")

    with timer("write", result):
        with orm.db_session:
            for k in keys:
                Cat(name=k, update_time=int(time.time()))

    with timer("update", result):
        with orm.db_session:
            for k in keys[:len(keys)//10]:
                c = Cat.get(name=k)
                if c:
                    c.update_time = int(time.time())

    with timer("read", result):
        with orm.db_session:
            for k in reads:
                Cat.get(name=k)
    db.disconnect()
    return result


# =========================
# SQLAlchemy
# =========================
def test_sqlalchemy(keys, reads):
    from sqlalchemy import create_engine, Column, Integer, String
    from sqlalchemy.orm import sessionmaker, declarative_base

    reset_sqlite()
    result = {}

    with timer("init", result):
        engine = create_engine(f"sqlite:///{DB_PATH}")
        with engine.connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")

        Base = declarative_base()

        class Cat(Base):
            __tablename__ = "cats"
            id = Column(Integer, primary_key=True)
            name = Column(String, index=True)
            update_time = Column(Integer)

        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)

    with timer("write", result):
        with Session() as s:
            for k in keys:
                s.add(Cat(name=k, update_time=int(time.time())))
            s.commit()

    with timer("update", result):
        with Session() as s:
            for k in keys[:len(keys)//10]:
                c = s.query(Cat).filter_by(name=k).first()
                if c:
                    c.update_time = int(time.time())
            s.commit()

    with timer("read", result):
        with Session() as s:
            for k in reads:
                s.query(Cat).filter_by(name=k).first()
    engine.dispose()
    return result


# =========================
# sqless（你的系统）
# =========================
def test_sqless(keys, reads):
    import sqless

    reset_sqlite()
    result = {}

    with timer("init", result):
        db = sqless.DB(DB_PATH,wal=True)
        table = db["cats"]

    with timer("write", result):
        kv = {k: {"update_time": int(time.time())} for k in keys}
        table.upsert(kv)

    with timer("update", result):
        kv = {k: {"update_time": int(time.time())} for k in keys[:len(keys)//10]}
        table.upsert(kv)

    with timer("read", result):
        for k in reads:
            table.get_item(k)

    db.close()
    return result


# =========================
# show
# =========================
def show(results):
    metrics = ["init", "write", "update", "read"]
    names = list(results.keys())
    base = 'sqless'
    print("\n=== Benchmark ===")
    print(f"{'name':<16}", *[f"{m:<16}" for m in metrics])
    for name in names:
        row = []
        for m in metrics:
            v = results[name][m]
            base_v = results[base][m]
            r = (v - base_v) / v if v else 0
            arrow = "↑" if r > 0 else "↓"
            row.append(f"{v:.3f}({arrow}{abs(r):.1%})")
        print(f"{name:<16}", *[f"{m:<16}" for m in row])


# =========================
# main
# =========================
import json

def json_load(path_json):
    try:
        with open(path_json,'r',encoding='utf-8') as f:
            return json.loads(f.read())
    except Exception as e:
        return None

def json_save(path_json,data):
    os.makedirs(os.path.dirname(path_json),exist_ok=True)
    with open(path_json,'w',encoding='utf-8') as f:
        f.write(json.dumps(data,ensure_ascii=False,indent=2))

if __name__ == "__main__":
    keys, reads = generate_data()

    results = json_load(f"{path_this}/results.json") or {}
    if "sqlite_raw" not in results:
        results["sqlite_raw"] = test_sqlite_raw(keys, reads)
        json_save(f"{path_this}/results.json",results)
    if "dataset" not in results:
        results["dataset"] = test_dataset(keys, reads)
        json_save(f"{path_this}/results.json",results)
    if "pony" not in results:
        results["pony"] = test_pony(keys, reads)
        json_save(f"{path_this}/results.json",results)
    if "sqlalchemy" not in results:
        results["sqlalchemy"] = test_sqlalchemy(keys, reads)
        json_save(f"{path_this}/results.json",results)
    if "sqless" not in results:
        results["sqless"] = test_sqless(keys, reads)
        json_save(f"{path_this}/results.json",results)
    show(results)