#!/usr/bin/python
__author__="Ashish.Jain"
__date__ ="15 Mar 2017"


#########################################################################################################             
############-----------------  Summary of Script
#########################################################################################################
#--> Script to extract resume details of a candidate from 'ResumeParserDump' collection in 'miscellaneous'
#    database (172.22.65.88) and store it in 'candidate_data' collection in 'ResumeDump' database 
#    172.22.66.233
#--> Alerts check mailers implemented to inform in case of any failure



#########################################################################################################             
############-----------------Importing Python Libraries and Modules
#########################################################################################################
#Inbuilt Modules
import sys
sys.path.append('./..')
import os
import csv
import datetime  
from collections import defaultdict
from pprint import pprint
from Utils.Utils_1 import cleanToken
from bson.objectid import ObjectId
from datetime import datetime, timedelta

#Custom Modules
from DataConnections.MongoConnect.MongoConnect import MongoConnect  #Custom Module - /data/Projects/JobAlerts/DataConnections/MongoConnect/MongoConnect.py
from Notifier.Notifier import send_email                            #Custom Module - /data/Projects/JobAlerts/Notifier/Notifier.py



#########################################################################################################             
############-----------------Credentials for server
#########################################################################################################
send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Resume Dump Incremental - JAM",'Resume Details Dump Incremental Started!!\n Collection Name : candidate_data \n DB Name : ResumeDump')
username = 'analytics'
password = 'aN*lyt!cs@321' 
tableName = 'ResumeParserDump'
date1 = datetime.now() - timedelta(days=2)
print "Date : ", datetime.now()



#########################################################################################################             
############-----------------Try Except to provide alert in case of code failure
#########################################################################################################
try:
    #########################################################################################################             
    ############-----------------Creating a mongo connection to miscellaneous DB
    #########################################################################################################
    monconn_users = MongoConnect(tableName, host = '172.22.65.88', port = 27018, database = 'miscellaneous', username = username, password = password, authenticate = True)
    monconn_users_cur = monconn_users.getCursor()
    myCondition = {"cd":{'$gt':date1}}
    users = monconn_users.loadFromTable(myCondition)
    print "Number of recoreds : "+str(len(users))
    
    
    #########################################################################################################             
    ############-----------------Creating a mongo connection to resume dump DB Mongo(172.22.66.233)
    #########################################################################################################
    tableName = 'candidate_data'
    monconn_resume = MongoConnect(tableName, host = '172.22.66.198', database = 'ResumeDump')
    monconn_resume_cur = monconn_resume.getCursor()
    
    
    
    #########################################################################################################             
    ############-----------------Extracting the resume data and dumping in local Mongo
    #########################################################################################################
    i = 0
    for user in users:
        i+=1
        if i%1000 == 0:
            print "Records Processed : ",i
    
        skills_list = user.get('_skills',[])
        skills_extracted = []
        for skill_dict in skills_list:
            skills_extracted.append(skill_dict['nm'])
        skills = ','.join(skills_extracted)
        
        
        jobs_list = user.get('_jobs',[])
        if len(jobs_list) !=0:
            user_job = jobs_list[0].get('jt','')
        else:
            user_job = ''
    
        
        document = {
                    '_id': user['cid'],
                    'user_creation_date':user['cd'],
                    'user_jobtitle_resume':cleanToken(user_job),
                    'user_skills_resume':cleanToken(skills)
                    }
        
        monconn_resume.saveToTable(document)
    send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Resume Dump Incremental - JAM",'Resume Details Dump Incremental Completed!! \n Collection Name : candidate_data \n DB Name : ResumeDump')
    del(monconn_users)
    del(monconn_resume)
except:
    send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com','progressiveht@hindustantimes.com'],"Resume Dump Incremental - JAM FAILED",'Resume Details Dump Incremental Failed!! Call Akash (+91-8527716555) or Kanika (+91-9560649296) asap.')