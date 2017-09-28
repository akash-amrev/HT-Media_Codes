###############################################################################################################################
############-------- Program to Extract Last 2 Months Lead from CRM Database and saving it into CSV Format
###############################################################################################################################

import sys
sys.path.append('/data/Projects/JobAlerts/')

from DataConnections.MySQLConnect.MySQLConnect import MySQLConnect

if __name__ == '__main__':
    
    host="172.22.65.48"
    user="analytics"
    password="@n@L4t1cs"
    database="shinecpcrm"
    unix_socket="/tmp/mysql.sock"
    port = 3306
    
    query = """select mobile_number from leads_leaduser where last_action > DATE(DATE_SUB(NOW(), INTERVAL 61 DAY)) or dnd = 1"""

    data = mysql_conn.query(query)
    
    for row in data:
        print row
        sys.exit(0)
    
    
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
    
    
