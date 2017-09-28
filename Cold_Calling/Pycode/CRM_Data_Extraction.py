###############################################################################################################################
############-------- Program to Extract Last 2 Months Lead from CRM Database and saving it into CSV Format
###############################################################################################################################

import os, sys, csv, traceback, MySQLdb
#from DataConnections.MySQLConnect.MySQLConnect import MySQLConnect


def create_connect_mysql(username, password, host, port, database, unix_socket = "/tmp/mysql.sock"):
    
    db = MySQLdb.connect(host = host, user = username, passwd=password, db = database, unix_socket = unix_socket, port = port)
    db.autocommit(True)
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    return cursor


def connect_mysql(DB):
    if DB == 'candidate':
        db_cursor = create_connect_mysql('Analytics', 'An@lytics', '172.22.65.170', 3306, 'sumoplus', "/tmp/mysql.sock")
    elif DB == 'recruiter':
        db_cursor = create_connect_mysql('analytics', '@n@lytics', '172.16.66.64', 3306, 'SumoPlus', "/tmp/mysql.sock")
    elif DB == 'coldcalling':
        db_cursor = create_connect_mysql('shinecrm_prod', 'Kgp28Za88CoU566', '172.22.65.63', 3306, 'shinecrm_prod', "/tmp/mysql.sock")
    return db_cursor

if __name__ == '__main__':
    crm_curr = connect_mysql("coldcalling")
    
    # query = """SELECT username, paymentemail FROM (SELECT username, paymentemail, fileid FROM crm_leads WHERE leadinsertdate > DATE(DATE_SUB(NOW(), INTERVAL 55 DAY))) AS cl INNER JOIN crm_leadsfile AS clf ON cl.fileid=clf.id WHERE clf.source IN (1,5,6,7,8,15,19,26,42) and username != ''"""
    query = """SELECT username, paymentemail FROM (SELECT username, paymentemail, fileid FROM crm_leads WHERE leadinsertdate > DATE(DATE_SUB(NOW(), INTERVAL 61 DAY))) AS cl INNER JOIN crm_leadsfile AS clf ON cl.fileid=clf.id WHERE clf.source IN (1,5,6,7,8,15,19,26,42,48,32, 11, 9, 51, 47, 40, 16, 14) and username != ''"""

    crm_curr.execute(query)

    last_21days_cc_cand = crm_curr.fetchall()
    exclusion_list = []

    for cand in last_21days_cc_cand:
        try:
            exclusion_list.append(cand["username"].replace("-", '').replace(",", '').strip())
            exclusion_list.append(cand["paymentemail"].replace("-", '').replace(",", '').strip())
        except:
            pass
        
    exclusion_list = list(set(exclusion_list))
    print 'Unique Candidates called in last 55 days', len(exclusion_list)
    #fout = open("ata/CRM_last_55_days_exclusion_list.csv", 'wb')
    fout = open('/data/Projects/Cold_Calling/Output_Files/CRM_last_55_days_exclusion_list.csv', 'wb') 	
    writer = csv.writer(fout)
    for email in exclusion_list:
        writer.writerow([email, "1"])
    fout.close()
    
    
