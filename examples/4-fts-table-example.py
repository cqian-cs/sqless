import sqless
print("""--- 1 Init DB and Table ---""")
import sqless
db = sqless.DB(path_db = "examples/your_database_folders/your_database.db")
table = db['fts_excerpts'] # create a table if not exists, prefix with "fts_", create Full-Text-Search Table


print("""--- 2 Upsert Records by table.upsert(key-values) ---""")
table.upsert({
    "深度学习": "深度学习是机器学习的一个子领域",
    "数据库教程": "SQLite是轻量级的数据库，支持全文搜索",
    "Python编程": "Python是一种广泛使用的编程语言，适合数据分析",
    "人工智能": "人工智能正在改变世界，包括自然语言处理技术",
    "NLP技术": "NLP stands for Natural Language Processing.",
})

print("""--- 3 Full-Text-Search by table.search(pattern) ---""")
ret = table.search('"数据" AND (数分 OR 库)')
if ret['suc']:
    for item in ret['data']:
        print(item['key'])
        print(item['data'])
        print(item['updated_at'])
        print(item['score'])
        print('---')
else:
    print(ret['data'])