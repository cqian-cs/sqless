import os
import orjson
import random
import time
path_this = os.path.dirname(os.path.abspath(__file__))

def load_json(path_json,default=None):
    if os.path.exists(path_json):
        with open(path_json,'rb') as f:
            return orjson.loads(f.read())
    return default

def save_json(path_json,data):
    with open(path_json,'wb') as f:
        f.write(orjson.dumps(data))

def show_mat(mat,sortby=None,reversesort=False):
    import prettytable
    table = prettytable.PrettyTable()
    table.field_names = mat[0]
    for arr in mat[1:]:
        table.add_row(arr)
    text = table.get_string(sortby=sortby,reversesort=reversesort)
    return text

def show_results_old():
    results = load_json(f"{path_this}/results.json")
    print(results)
    mat = [['name','init','write','read']]
    base = 'sqless'
    algorithms = ['dataset','pony.orm','sqlalchemy','sqless']
    for algorithm in algorithms:
        new_arr=[algorithm]
        for metric in ['init','set','get']:
            value = results[algorithm][metric]
            me_value = results[base][metric]
            improve = (value-me_value)/value
            new_arr.append(f"{value:.3f} ({improve:+.2%})".replace('+','↑').replace('-','↓'))
        mat.append(new_arr)
    print(show_mat(mat))
        

def generate_data(overwrite=False):
    path_data = f"{path_this}/test_data.json"
    data = load_json(path_data,None)
    if data and not overwrite:
        return data
    data={}
    data['Cats'] = [{
        'name': f"C{i}",
        'birthday': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(random.randint(1670000000,1674296539))),
        'create_time': int(time.time()),
        'update_time': int(time.time())
    } for i in range(1000)]
    data['equals'] =[f"C{random.randint(0,1000)}" for _ in range(100000)]    
    save_json(path_data,data)
    return data

def test_dataset():
    data = generate_data()
    import dataset # pip install dataset
    name_this = 'dataset'
    path_db = f'{path_this}/test_{name_this}.db'
    result = {}
    for file in [path_db,f"{path_db}-shm",f"{path_db}-wal"]:
        if os.path.exists(file):
            os.remove(file)
    
    t0=time.time()
    db = dataset.connect(f'sqlite:///{path_db}')
    db.query('PRAGMA journal_mode = WAL')
    table = db['Cats']
    result['init']=time.time()-t0
    
    t0=time.time()
    for cat in data['Cats']:
        table.insert(cat)
    result['set']=time.time()-t0

    t0=time.time()
    for cat in data['Cats']:
        cat['update_time'] = int(time.time())
        table.update(cat, ['name'])
    result['update']=time.time()-t0

    t0=time.time()
    for name in data['equals']:
        table.find_one(name=name)
    result['get'] = time.time()-t0
    
    results = load_json(f"{path_this}/results.json",default={})
    results[name_this]=result
    save_json(f"{path_this}/results.json",results)




def test_ponyorm():
    data = generate_data()
    from pony import orm # pip install pony
    name_this = 'pony.orm'
    path_db = f'{path_this}/test_{name_this}.db'
    result = {}
    for file in [path_db,f"{path_db}-shm",f"{path_db}-wal"]:
        if os.path.exists(file):
            os.remove(file)
    
    t0=time.time()
    db = orm.Database()
    class Cat(db.Entity):
        name = orm.Required(str)
        birthday = orm.Required(str)
        create_time = orm.Required(int,default=int(time.time()))
        update_time = orm.Required(int,default=int(time.time()))
    
    db.bind('sqlite', path_db, create_db=True)
    db.generate_mapping(create_tables=True)
    import sqlite3 as _sqlite3
    _sqlite3.connect(path_db).execute('PRAGMA journal_mode = WAL')
    result['init']=time.time()-t0
    
    t0=time.time()
    with orm.db_session:
        for cat in data['Cats']:
            Cat(**cat)
    result['set']=time.time()-t0

    t0=time.time()
    with orm.db_session:
        for cat in data['Cats']:
            c = Cat.get(name=cat['name'])
            if c: c.update_time = int(time.time())
    result['update']=time.time()-t0

    t0=time.time()
    with orm.db_session:
        for name in data['equals']:
            Cat.get(name=name)
    result['get'] = time.time()-t0
    
    results = load_json(f"{path_this}/results.json",default={})
    results[name_this]=result
    save_json(f"{path_this}/results.json",results)


def test_sqlalchemy():
    data = generate_data()
    # pip install sqlalchemy
    from sqlalchemy import create_engine,text
    from sqlalchemy.orm import sessionmaker,declarative_base
    from sqlalchemy import Column,Integer,String,Float,Boolean
    name_this = 'sqlalchemy'
    path_db = f'{path_this}/test_{name_this}.db'
    result = {}
    for file in [path_db,f"{path_db}-shm",f"{path_db}-wal"]:
        if os.path.exists(file):
            os.remove(file)
    
    t0=time.time()
    engine=create_engine(f'sqlite:///{path_db}')
    with engine.connect() as conn:
        conn.execute(text("PRAGMA journal_mode = WAL"))
    Session = sessionmaker(bind=engine)
    Base = declarative_base()
    class Cat(Base):
        __tablename__='cats'
        id=Column(Integer,primary_key=True,autoincrement=True, nullable=True,comment='ID')
        name=Column(String(15),unique=False,nullable=False,index=False,comment='姓名')
        birthday=Column(String(20),unique=False,nullable=True,index=False,comment='出生时间')
        create_time = Column(Integer,default=int(time.time()),comment='创建时间')
        update_time = Column(Integer,default=int(time.time()),comment='更新时间')
    Cat.__table__.create(engine)
    result['init']=time.time()-t0
    
    t0=time.time()
    with Session() as session:
        for cat in data['Cats']:
            session.add(Cat(**cat))
            session.commit()
    result['set']=time.time()-t0
    
    t0=time.time()
    with Session() as session:
        for cat in data['Cats']:
            c = session.query(Cat).filter(Cat.name==cat['name']).one_or_none()
            if c: c.update_time = int(time.time())
            session.commit()
    result['update']=time.time()-t0

    t0=time.time()
    with Session() as session:
        for name in data['equals']:
            cat = session.query(Cat).filter(Cat.name==name).one_or_none()
    result['get'] = time.time()-t0

    results = load_json(f"{path_this}/results.json",default={})
    results[name_this]=result
    save_json(f"{path_this}/results.json",results)






def test_util_dbs_reltable():
    data = generate_data()
    import util_dbs
    name_this = 'util_dbs.RelTable'
    path_folder = f'{path_this}/test_util_dbs_rel'
    result = {}
    import shutil
    if os.path.exists(path_folder):
        shutil.rmtree(path_folder)

    t0=time.time()
    dbs = util_dbs.DBS(path_folder)
    table = dbs['bench-rel_cats']
    result['init']=time.time()-t0

    t0=time.time()
    key_values = {f"C{cat['name'][1:]}": cat for cat in data['Cats']}
    table.upsert(key_values)
    result['set']=time.time()-t0

    t0=time.time()
    for cat in data['Cats']:
        key = f"C{cat['name'][1:]}"
        table.upsert({key: {'update_time': int(time.time())}})
    result['update']=time.time()-t0

    t0=time.time()
    for name in data['equals']:
        table.get_item(name)
    result['get'] = time.time()-t0

    dbs.close()
    results = load_json(f"{path_this}/results.json",default={})
    results[name_this]=result
    save_json(f"{path_this}/results.json",results)

def test_util_dbs_table():
    data = generate_data()
    import util_dbs
    name_this = 'util_dbs.Table'
    path_folder = f'{path_this}/test_util_dbs_json'
    result = {}
    import shutil
    if os.path.exists(path_folder):
        shutil.rmtree(path_folder)

    t0=time.time()
    dbs = util_dbs.DBS(path_folder)
    table = dbs['bench-cats']
    result['init']=time.time()-t0

    t0=time.time()
    key_values = {f"C{cat['name'][1:]}": cat for cat in data['Cats']}
    table.upsert(key_values)
    result['set']=time.time()-t0

    t0=time.time()
    for cat in data['Cats']:
        key = f"C{cat['name'][1:]}"
        table.upsert({key: {'update_time': int(time.time())}})
    result['update']=time.time()-t0

    t0=time.time()
    for name in data['equals']:
        table.get_item(name)
    result['get'] = time.time()-t0

    dbs.close()
    results = load_json(f"{path_this}/results.json",default={})
    results[name_this]=result
    save_json(f"{path_this}/results.json",results)

def test_database():
    data = generate_data()
    import database
    name_this = 'database.DB'
    path_db = f'{path_this}/test_database.db'
    result = {}
    for file in [path_db,f"{path_db}-shm",f"{path_db}-wal"]:
        if os.path.exists(file):
            os.remove(file)

    t0=time.time()
    db = database.DB(path_db)
    result['init']=time.time()-t0

    t0=time.time()
    headers = ['key','name','birthday','create_time','update_time']
    mat = []
    for cat in data['Cats']:
        key = f"C{cat['name'][1:]}"
        mat.append([key, cat['name'], cat['birthday'], cat['create_time'], cat['update_time']])
    db.upsert_mat('Cats', headers, mat, pkey='key')
    result['set']=time.time()-t0

    t0=time.time()
    for cat in data['Cats']:
        key = f"C{cat['name'][1:]}"
        db.upsert('Cats', {'key': key, 'update_time': int(time.time())}, pkey='key')
    result['update']=time.time()-t0

    t0=time.time()
    for name in data['equals']:
        db.get_item('Cats', name, pkey='key')
    result['get'] = time.time()-t0

    db.close()
    results = load_json(f"{path_this}/results.json",default={})
    results[name_this]=result
    save_json(f"{path_this}/results.json",results)

def show_results():
    results = load_json(f"{path_this}/results.json")
    if not results:
        print("No results found.")
        return
    mat = [['name','init','write','update','read']]
    base = 'database.DB'
    algorithms = ['dataset','pony.orm','sqlalchemy','database.DB','util_dbs.RelTable','util_dbs.Table']
    for algorithm in algorithms:
        if algorithm not in results:
            continue
        if not all(m in results[algorithm] for m in ['init','set','update','get']):
            continue
        new_arr=[algorithm]
        for metric in ['init','set','update','get']:
            value = results[algorithm][metric]
            me_value = results[base][metric]
            improve = (value-me_value)/value
            new_arr.append(f"{value:.3f} ({improve:+.2%})".replace('+','↑').replace('-','↓'))
        mat.append(new_arr)
    print(show_mat(mat))


if __name__=='__main__':
    test_dataset()
    test_ponyorm()
    test_sqlalchemy()
    test_database()
    test_util_dbs_reltable()
    test_util_dbs_table()
    show_results()
