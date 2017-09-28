
import sys
from _sqlite3 import Row
sys.path.append('./../')
from pprint import pprint
from DataConnections.MySQLConnect.MySQLConnect import MySQLConnect
from DataConnections.MongoConnect.MongoConnect import MongoConnect
import pdb
import csv
import time
from multiprocessing import Pool
import os
import datetime
from datetime import timedelta
from Features.JobAlert_Functions import *


monconn_users_static = MongoConnect('candidates_processed_4', host = 'localhost', database = 'JobAlerts')
'''
ifile = open('JAM_27_JAN_opens.csv','r')
reader = csv.reader(ifile)
reader.next()
count = 0

i = 0
for row in reader:
    i+=1
    if i%5000 == 0:
        print i
    #print row
'''
count = 0
#ofile = open("UserData.csv",'w')
ofile = open('Customer_Profile.csv','w')
writer = csv.writer(ofile , lineterminator = '\n')
writer.writerow(['Email_Id','User_Id','Gender','Industry','Job_Title','Experience','Salary','Functional_Area','Sub-FA','CellPhone'])

#ifile = open('boxx_leads.csv','r')
#ifile = open("email_id_cp.csv",'r')
ifile = open('customer.csv','r')
reader = csv.reader(ifile)
reader.next()

i = 0
user_id = []

for records in ifile:
    user_id.append(str(records).strip())
print len(user_id)
print user_id[1:10]


data_user = monconn_users_static.loadFromTable({"_id":{'$in':user_id}})
for records in data_user:
    try:
        user_id = records.get('_id')
        user_email = records.get('user_email')
        user_gender = records.get('user_gender')
        user_industry = records.get("user_industry",'N/A')
        user_jobtitle = records.get("user_jobtitle","N/A")
        user_exp = records.get("user_experience",'N/A')
        user_salary = records.get("user_ctc",'N/A')
        user_functional_area = records.get("user_functionalarea","N/A")
        user_subfa = records.get("user_functionalarea",'N/A')
        user_cellphone = records.get("user_phone",'N/A')
        writer.writerow([user_email,user_id,user_gender,user_industry,user_jobtitle,user_exp,user_salary,user_functional_area,user_subfa,user_cellphone])
    except:
        pass
    
print 'All Records written'
sys.exit(0)

for row in  reader:
    i+=1
    if i%10000 == 0:
        print i
    #data_user = monconn_users_static.loadFromTable({"user_email":row[0]})
    data_user = monconn_users_static.loadFromTable({"_id":row[0]})
    
    
    print row[0]
    if len(list(data_user)) > 0:
        print "Hello"

    try:
        details =  list(data_user)[0]
        user_email = details.get('user_email')
        user_id = details.get('_id')
        user_gender = details.get('user_gender')
        user_industry = details.get("user_industry",'N/A')
        user_jobtitle = details.get("user_jobtitle","N/A")
        user_exp = details.get("user_experience",'N/A')
        user_salary = details.get("user_ctc",'N/A')
        user_functional_area = details.get("user_functionalarea","N/A")
        user_subfa = details.get("user_functionalarea",'N/A')
        
        
        
        
        #application = details.get("application_count",0)
        #application_list = details.get("application_list",0)
        

        writer.writerow([user_email,user_id,user_gender,user_industry,user_jobtitle,user_exp,user_salary,user_functional_area,user_subfa])
        
    except Exception as e:
        print e
        pass
    #break
    
    
'''
for i in range(80):
    print i
    data_user = monconn_users_static.find({'p':i})
         
    for row in data_user :
        application_count =  row.get("application_count",0)
        jobtitle = cleanText(row.get("user_jobtitle",None))
        if application_count == 0:
            count +=1
            writer.writerow([jobtitle])
            #if jobtitle == None:
            #    continue
            #else:
                
                #writer.writerow([jobtitle.encode('utf-8').strip()])
        #   pass
            
        else:
            pass
        
print count
'''
'''
job_fa_dict = {}
ifile = open('JobsDataFinal.csv','r')
reader = csv.reader(ifile)
reader.next()
count = 0
for row in reader:
    try:
        job_fa_dict[int(float(row[0]))] = int(float(row[1]))
    except:
        count+=1
        
print count
subfa_fa_lookup = {}
ifile = open('fa_lookup.csv','r')
reader = csv.reader(ifile)
reader.next()
for row in reader:
    subfa_fa_lookup[int(float(row[0]))] = int(float(row[1]))
    

    
    
ofile = open('FA_to_FA.csv','w')
writer = csv.writer(ofile,lineterminator = '\n')
writer.writerow(['User_FA','Job_FA'])
    
monconn_users_static = MongoConnect('candidates_processed_3', host = 'localhost', database = 'JobAlerts').getCursor()
for i in range(80):
    print i
    data_user = monconn_users_static.find({'p':i})
    count = 0     
    for row in data_user :
        application_count =  row.get("application_count",0)
        application_list = row.get("application_list",[])
        subfa_id = row.get("user_functionalarea_id",0) 
        if application_count == 0:
            continue
        
        for jobid in application_list:
            try:
                job_fa = job_fa_dict[int(jobid)]
                user_fa = subfa_fa_lookup[int(subfa_id)]
                writer.writerow([user_fa,job_fa])
            except:
                pass
    
'''           
        
        