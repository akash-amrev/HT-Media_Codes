from _sqlite3 import Row
__author__="Ashish.Jain"
__date__ ="$Dec 14, 2015 15:45PM$"

'''
This module check the status of midouts
'''

#Include the root directory in the path
import sys
sys.path.append('./../')

#Include the root directory in the path
import sys
#sys.path.append('./../')
sys.path.append('/data/Projects/JobAlerts/')
from pprint import pprint
from DataConnections.MySQLConnect.MySQLConnect import MySQLConnect
from DataConnections.MongoConnect.MongoConnect import MongoConnect
from datetime import date
import pdb
import csv
import time
from multiprocessing import Pool
import os
import re
import datetime
from datetime import timedelta
from datetime import *
#from datetime import date, datetime, time
import calendar
import pandas as pd
import numpy as np
from random import sample

username = 'analytics'
password = 'aN*lyt!cs@321'


mongo_conn = MongoConnect('CandidateStatic', host = '172.22.65.88', port = 27018,database = 'sumoplus',username= username,password = password,authenticate = True).getCursor()
monconn_users_static = MongoConnect('candidates_processed_4', host = '172.22.66.198', database = 'JobAlerts').getCursor()
mon_conn_sub_fa = MongoConnect('LookupSubFunctionalArea', host = '172.22.65.88', port = 27018,database = 'sumoplus',username= username,password = password,authenticate = True).getCursor()

ifile = open('/data/Projects/Cold_Calling/Pycode/concentrix_leads_v1.csv','rb')
reader = csv.reader(ifile)

ofile = open('/data/Projects/Cold_Calling/Pycode/email_fa_mapping.csv','wb')
writer = csv.writer(ofile)
writer.writerow(['Email','Candidate_Name','City','Industy','Salary','Total_Experience','Functional_Area','Sub_Functional_Area'])

email_id = []

for records in reader:
    email_id.append(str(records[0]).strip())
print len(email_id)
print email_id[1:5]

data_user = monconn_users_static.find({'user_email':{'$in':email_id}})

sub_fa_lookup = mon_conn_sub_fa.find()
sub_fa = {}
for records in sub_fa_lookup:
    sub_fa[records['sfe']] = records['fe']

for records in data_user:
    try:
        email = records['user_email']
    except:
        email = ''
    try:
        user_sub_functionalarea = row.get("user_functionalarea","None").encode('ascii','ignore').decode('ascii')
        user_functionalarea = sub_fa[user_sub_functionalarea]
    except:
        user_sub_functionalarea = "None"
        user_functionalarea = "None"
    try:
        city = row.get('user_location','')
        city = str(city[0])     
        #sys.exit(0)
    except:
        city = 'missing'
    try:
        user_experience = row.get("user_experience","None").encode('ascii','ignore').decode('ascii')
        user_experience = float((re.findall("[-+]?\d+[\.]?\d*", user_experience.replace(',','').replace("-"," ")))[1])
        #print user_experience
        #sys.exit(0)
    except:
        user_experience = "" 
        try:
            user_industry = row.get("user_industry","None").encode('ascii','ignore').decode('ascii')
        except:
            user_industry  = "" 
        try:
            user_ctc = row.get("user_ctc","None").encode('ascii','ignore').decode('ascii')
        except:
            user_ctc = ""
    try:
        name = row.get('user_fullname','').encode('ascii','ignore').decode('ascii')
    except:
        name = ""
    #writer.writerow([email,name,user_functionalarea,user_sub_functionalarea,user_experience,user_industry,user_ctc,city])
    writer.writerow([email,name,city,user_industry,user_ctc,user_experience,user_functionalarea,user_sub_functionalarea]) 
ofile.close()

#Email','Candidate_Name','City','Industy','Salary','Total_Experience','Functional_Area','Sub_Functional_Area