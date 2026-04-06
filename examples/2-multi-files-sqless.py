print("""--- 1 Init DBS ---""")
import sqless
dbs = sqless.DBS(folder='examples/your_database_folders')


print("""--- 2 Get Table instance by dbs[<db_name>-<table_name>] ---""")
table = dbs[f'my_app-Asia-users']
table.upsert({
    'U0001':{'name':'Tom', 'age':14, 'species':'Cat', 'role':'Protagonist'},
    'U0002':{'name':'Jerry', 'age':12, 'species':'Mouse', 'role':'Protagonist'},
    'U0003':{'name':'Spike', 'age':8, 'species':'Dog', 'role':'Supporting'},
    'U0004':{'name':'Tyke', 'age':6, 'species':'Dog', 'role':'Supporting'},
    'U0005':{'name':'Butch', 'age':15, 'species':'Cat', 'role':'Antagonist'},
}) # upsert to Table(users) at DB(examples/your_database_folders/my_app-Asia.sqlite)
dbs[f'my_app-Europe-users'].upsert({
    'U0001':{'name':'Tuffy', 'age':5, 'species':'Mouse', 'role':'Supporting'},
    'U0002':{'name':'Toodles', 'age':13, 'species':'Cat', 'role':'Supporting'},
    'U0003':{'name':'Nibbles', 'age':6, 'species':'Mouse', 'role':'Supporting'},
    'U0004':{'name':'Quacker', 'age':6, 'species':'Duck', 'role':'Supporting'},
    'U0005':{'name':'Lightning', 'age':16, 'species':'Cat', 'role':'Antagonist'}
}) # upsert to Table(users) at DB(examples/your_database_folders/my_app-Europe.sqlite)


print("""--- 3 Get DB instance by dbs.get_db(db_name) ---""")
db1 = dbs.get_db('my_app-Asia')
print([item['name'] for item in db1['users']]) # ['Tom', 'Jerry', 'Spike', 'Tyke', 'Butch']
db2 = dbs.get_db('my_app-Europe')
print([item['name'] for item in db2['users']]) # ['Tuffy', 'Toodles', 'Nibbles', 'Quacker', 'Lightning']
