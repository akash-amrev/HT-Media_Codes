import MySQLdb


def create_connect_mysql(username, password, host, port, database, unix_socket = "/tmp/mysql.sock"):
    
    db = MySQLdb.connect(host = host, user = username, passwd=password, db = database, unix_socket = unix_socket, port = port)
    db.autocommit(True)
    #cursor = db.cursor(MySQLdb.cursors.DictCursor)
    #return cursor
    return db



def connect_mysql(DB):
    if DB == 'candidate':
        db_cursor = create_connect_mysql('Analytics', 'An@lytics', '172.22.65.170', 3306, 'sumoplus', "/tmp/mysql.sock")
    elif DB == 'recruiter':
        db_cursor = create_connect_mysql('analytics', 'pedRkCkV6y', '172.22.65.82', 3306, 'SumoPlus', "/tmp/mysql.sock")

    elif DB == 'coldcalling':
        db_cursor = create_connect_mysql('shinecrm_prod', 'Kgp28Za88CoU566', '172.22.65.63', 3306, 'shinecrm_prod', "/tmp/mysql.sock")
    return db_cursor

if __name__ == '__main__':
    print 'Hi...'
    cand_cur = connect_mysql('recruiter')
    query = """
    SELECT jobid, publisheddate, externalapply_check, DATE_SUB(CURDATE(), INTERVAL 60 DAY) AS Filter
    FROM `recruiter_job`
    WHERE (externalapply_check <> 0) AND (publisheddate IS NOT NULL) 
        AND (jobstatus IN (3,9)) 
        AND publisheddate >= DATE_SUB(CURDATE(), INTERVAL 60 DAY)
"""
    print cand_cur
    print query
    cand_cur.execute(query)
    print cand_cur.fetchone()
    
