import pymongo


def create_connect_mongo(username, password, host, port, db, authenticate = True):
    mongo_uri = 'mongodb://' + host + ':' + str(port)
    client = pymongo.MongoClient(mongo_uri)
    db_cur = client[db]
    if authenticate:
        db_cur.authenticate(username, password)
    return db_cur


def connect_mongo(DB):
    if DB == 'candidate':
        db_cursor = create_connect_mongo('analytics', 'aN*lyt!cs@321', '172.22.65.157', '27018', 'sumoplus', True)
    elif DB == 'recruiter':
        db_cursor = create_connect_mongo('analytics', 'aN*lyt!cs@321', '172.22.65.59', '27017', 'recruiter_master', False)
    return db_cursor

if __name__ == '__main__':
    print 'Hi...'
