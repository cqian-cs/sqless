print("""--- 1 Init DB and Table ---""")
import sqless
db = sqless.DB(path_db = "examples/your_database_folders/your_database.db")
table = db['users'] # create a table if not exists


print("""--- 2 Upsert Records ---""")
print("""--- 2.1 upsert records by table.upsert(key-values) ---""")
# we use upsert method to create/update bulk records to the table
ret = table.upsert({
    'U0001':{'name':'Tom', 'age':14, 'species':'Cat', 'role':'Protagonist'},
    'U0002':{'name':'Jerry', 'age':12, 'species':'Mouse', 'role':'Protagonist'},
    'U0003':{'name':'Spike', 'age':8, 'species':'Dog', 'role':'Supporting'},
    'U0004':{'name':'Tyke', 'age':6, 'species':'Dog', 'role':'Supporting'},
    'U0005':{'name':'Butch', 'age':15, 'species':'Cat', 'role':'Antagonist'},
    'U0006':{'name':'Tuffy', 'age':5, 'species':'Mouse', 'role':'Supporting'},
    'U0007':{'name':'Toodles', 'age':13, 'species':'Cat', 'role':'Supporting'},
    'U0008':{'name':'Nibbles', 'age':6, 'species':'Mouse', 'role':'Supporting'},
    'U0009':{'name':'Quacker', 'age':6, 'species':'Duck', 'role':'Supporting'},
    'U0010':{'name':'Lightning', 'age':16, 'species':'Cat', 'role':'Antagonist'}
})
print(ret) # {'suc': True, 'data': 'update 10 items.'}
print("""--- 2.2 upsert records by table[key] ---""")
# Every record has a "key" field (TEXT primary key), for pythonic Dict operation.
table['U0001']['age']=15    # × Not modify the record in database
table['U0001']={'age':15}   # √ Modify the record in database
print(table['U0001']) # {'key': 'U0001', 'age': 15, 'name': 'Tom', 'role': 'Protagonist', 'species': 'Cat'}


print("""--- 3 Get Records ---""")
print("""--- 3.1 get records by table.get_items(keys) ---""")
ret = table.get_items(['U0004', 'U0006', 'U0008','Uxxxx']) # return a dict
print(ret['U0006']) # {'key': 'U0006', 'age': 5, 'name': 'Tuffy', 'role': 'Supporting', 'species': 'Mouse'}
print(ret['Uxxxx']) # None (not found key=Uxxxx in database)
print("""--- 3.2 get records by table.iter(filter) ---""")
for item in table.iter('name like %o% or age < 8 order by age asc, name desc'):
    print(item['key'], item['name'], item['age'])
print('---')
for item in table.iter('age in [5,15]'):
    print(item['key'], item['name'], item['age'])
print("""--- 3.3 get records by iterator ---""")
for item in table:
    print(item['key'], item['name'], item['age'])



print("""--- 4 Count Records ---""")
print("""--- 4.1 Count Records by len(table) ---""")
print('total_count', len(table)) # 10
print("""--- 4.2 Count Records by table.count(filter) ---""")
print('filtered_count', table.count('name like %o% or age < 8')) # 6


print("""--- 5 Delete Records ---""")
print("""--- 5.1 Delete Records by del(table[key]) ---""")
del table['U0001']
print("Delete by key, remaining",[item['key'] for item in table]) # ['U0002', 'U0003', 'U0004', 'U0005', 'U0006', 'U0007', 'U0008', 'U0009', 'U0010']
print("""--- 5.2 Delete Records by table.delete(filter) ---""")
table.delete('name like %o% or age < 8')
print("Delete by filter, remaining",[item['key'] for item in table]) # ['U0002', 'U0003', 'U0005', 'U0010']


print("""--- 6 DB operations ---""")
print("""--- 6.1 create table by db[name] ---""")
json_events = db['json_events'] # prefix with "json_", create JSON Table
fts_docs    = db['fts_docs']    # prefix with "fts_", create Full-Text-Search Table
vec_memory  = db['vec_memory']  # prefix with "vec_", create Vector Table
finance     = db['finance']     # without above prefixes, create Relational Table
print("""--- 6.2 list visualble tables by dir(db) ---""")
print(dir(db)) # ['finance', 'fts_docs', 'json_events', 'users', 'vec_memory']
print("""--- 6.3 list hidden tables by db.list_tables() ---""")
print(db.list_tables()) # ['json_events', 'fts_docs', 'fts_docs_fts', 'fts_docs_fts_data', 'fts_docs_fts_idx', 'fts_docs_fts_content', 'fts_docs_fts_docsize', 'fts_docs_fts_config', 'vec_memory', 'vec_memory_vec', 'vec_memory_vec_info', 'vec_memory_vec_chunks', 'vec_memory_vec_rowids', 'vec_memory_vec_vector_chunks00', 'finance', 'users']
print("""--- 6.4 drop tables by del db[name] ---""")
del db['json_events']
del db['fts_docs']
del db['vec_memory']
del db['finance']
print(db.list_tables()) # ['users']
print("""--- 6.5 close db by db.close() ---""")
db.close()