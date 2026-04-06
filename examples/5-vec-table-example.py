import sqless
def main():
    print("""--- 1 Init DB and Table ---""")
    db = sqless.DB(path_db = "examples/your_database_folders/your_database.db")
    table = db['vec_excerpts'] # create a table if not exists, prefix with "vec_", create Vector Table


    print("""--- 2 Upsert Records by table.upsert(key-values) ---""")
    table.upsert({
        "深度学习": "深度学习是机器学习的一个子领域",
        "数据库教程": "SQLite是轻量级的数据库，支持全文搜索",
        "Python编程": "Python是一种广泛使用的编程语言，适合数据分析",
        "人工智能": "人工智能正在改变世界，包括自然语言处理技术",
        "NLP技术": "NLP stands for Natural Language Processing.",
    })

    print("""--- 3 Semantic-Search by table.search(pattern) ---""")
    ret = table.search('大语言模型',k=2)
    print(ret)
    #{
    #    'suc': True, 
    #    'data': [
    #        {'key': '人工智能', 'data': '人工智能正在改变世界，包括自然语言处理技术', 'updated_at': '2026-04-06 11:43:14', 'distance': 0.9426205158233643}, 
    #        {'key': 'Python编程', 'data': 'Python是一种广泛使用的编程语言，适合数据分析', 'updated_at': '2026-04-06 11:43:14', 'distance': 0.958746075630188}
    #    ]
    #}
    print("""--- 4 get Semantic Graph by table.get_graph(keys) ---""")
    ret = table.get_graph(['深度学习','数据库教程','Python编程','人工智能'],k=2, max_hop=2)
    print(ret)
    #{
    #    'suc': True, 
    #    'data': {
    #        'nodes': {
    #            '深度学习': {'x': 10.734728813171387, 'y': 6.421365261077881, 'data': '深度学习是机器学习的一个子领域', 'c': 0}, 
    #            '数据库教程': {'x': 9.676085472106934, 'y': 6.993357181549072, 'data': 'SQLite是轻量级的数据库，支持全文搜索', 'c': 0}, 
    #            'Python编程': {'x': 10.402581214904785, 'y': 7.312066555023193, 'data': 'Python是一种广泛使用的编程语言，适合数据分析', 'c': 0}, 
    #            '人工智能': {'x': 10.278227806091309, 'y': 5.744253635406494, 'data': '人工智能正在改变世界，包括自然语言处理技术', 'c': 0}, 
    #            'NLP技术': {'x': 9.471141815185547, 'y': 5.413893699645996, 'data': 'NLP stands for Natural Language Processing.', 'c': 1}
    #        }, 
    #        'edges': [
    #            ['人工智能', 'NLP技术', 0.8777573704719543], 
    #            ['数据库教程', 'Python编程', 0.9278976321220398], 
    #            ['Python编程', '数据库教程', 0.9278976321220398], 
    #            ['Python编程', '人工智能', 0.941740095615387], 
    #            ['人工智能', 'Python编程', 0.941740095615387], 
    #            ['深度学习', 'Python编程', 0.966070294380188], 
    #            ['深度学习', '人工智能', 0.9701557159423828], 
    #            ['数据库教程', '深度学习', 0.9999065399169922], 
    #            ['NLP技术', '人工智能', 0.8777573704719543], 
    #            ['NLP技术', 'Python编程', 1.0011892318725586]
    #        ]
    #    }
    #}
if __name__=='__main__':
    main()