import sys
sys.path.append('./../')
from DataConnections.MongoConnect.MongoConnect import MongoConnect
import csv
import os
import datetime
from datetime import timedelta


monconn_users_static = MongoConnect('candidate_applications', host = 'localhost', database = 'JobAlerts')

ifile = open('UserData.csv','rb')
reader = csv.reader(ifile)
reader.next()

ofile = open("User_Applications.csv","w")
writer = csv.writer(ofile)
writer.writerow(['User_Id','Job_Applied','Application_Date'])

candidate_id = []
'''for records in reader:
    candidate_id.append(str(records[0]).strip())'''
#print len(candidate_id)

for records in reader:
    data_user = monconn_users_static.loadFromTable({"fcu":str(records[0])})
    for records in data_user:
        try:
            User_Id = records.get('fcu','N/A')
            Job_Applied = records.get('fjj','N/A')
            Application_Date = records.get('ad','N/A')
            writer.writerow([User_Id,Job_Applied,Application_Date])
        except Exception as e:
            print e
            pass
    '''try:
        details = list(data_user)
        print details
        sys.exit(0)
        User_Id = details.get('fcu',"N/A")
        Job_Applied = details.get('fjj','N/A')
        Application_Date = details.get('ad','N/A')
        writer.writerow([User_Id,Job_Applied,Application_Date])
    except Exception as e:
        print e
        pass'''
    