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

ifile = open('/data/Projects/Cold_Calling/Pycode/Cold_Calling_File.csv','rb')
reader = csv.reader(ifile)
reader.next()

'''ofile = open('/data/Projects/Cold_Calling/Pycode/consolidated_cold_calling_files_corrected.csv','wb')
writer = csv.writer(ofile)
writer.writerow(['Email','Candidate_Name','Phone','City','Industry','Functional_Area','Salary','Total_Experience','Sub_FA'])'''

ofile_1 = open('/data/Projects/Cold_Calling/Pycode/corrected_numbers.csv','wb')
writer_1 = csv.writer(ofile_1)
writer_1.writerow(['Email','CellPhone'])

email_id = []

for records in reader:
    email_id.append(str(records[0]).strip())
print len(email_id)    
print email_id[1:5]
required_data = mongo_conn.find({'e':{'$in':email_id}},{'e':1,'cp':1})

count = 0
for rows in required_data:
    #print rows
    email = rows['e']
    cellphone = rows['cp']
    writer_1.writerow([email,cellphone])
    count = count +1
'''
    
    

df_1 = pd.read_csv('/data/Projects/Cold_Calling/Pycode/consolidated_cold_calling_files.csv')
df_2 = pd.read_csv('/data/Projects/Cold_Calling/Pycode/corrected_numbers.csv')

df_merge = pd.merge(df_1,df_2, left_on = 'Email',right_on = 'Email',how ='left')
print df_merge.count()

df_final = df_merge[['Email','Candidate_Name','CellPhone','City','Industry','Functional_Area','Salary','Total_Experience','Sub_FA']]
#print df_final.head(3)

df_final.to_csv('/data/Projects/Cold_Calling/Pycode/cold_calling_file_final.csv',index = False)
'''
    