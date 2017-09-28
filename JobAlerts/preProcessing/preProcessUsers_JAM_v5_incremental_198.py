#!/usr/bin/python
__author__="Ashish.Jain"
__date__ ="15 Feb 2017"


#################################################################################################################           
############-----------------  Summary of Script
#################################################################################################################
#--> Extract last 2 days candidate data from SOLR (172.22.65.28:8989)
#--> Process candidate details to bring them to desired format
#--> Extract user's resume details from 'candidate_data' collection in 'ResumeDump' Database (172.22.66.233)
#--> Extract applications data from 'candidate_applications' collection in 'JobAlerts' Database (172.22.66.233)
#--> Extract user's preferred details from 'CandidatePreferences' in 'sumoplus' Database (172.22.65.88)
#--> Dumping the final data in 'candidates_processed_4' collection in 'JobAlerts' Database (172.22.66.233)
#--> Alerts check mailers implemented to inform in case of any failure
#--> Chances of failure arise when SOLR crashes, contact Rajat for this issue .



#################################################################################################################            
############-----------------Importing Python Libraries and Modules
#################################################################################################################
#Inbuilt Modules
import sys
from _mysql import NULL
sys.path.append('./../')
import os
from datetime import datetime
import MySQLdb
import simplejson as json
from httplib2 import Http
import pymongo
import requests
import time
import datetime
from datetime import timedelta
from pymongo import Connection
import unicodedata
import MySQLdb
import sys, traceback
import bson
import re
import multiprocessing
import logging
from time import sleep
from pymongo.database import Database
from datetime import datetime, timedelta
import csv

#Custom Modules
from DataConnections.MySQLConnect.MySQLConnect import MySQLConnect  #Custom Module - /data/Projects/JobAlerts/DataConnections/MySQLConnect/MySQLConnect.py
from DataConnections.MongoConnect.MongoConnect import MongoConnect  #Custom Module - /data/Projects/JobAlerts/DataConnections/MongoConnect/MongoConnect.py
from Utils.Utils_1 import cleanToken                                #Custom Module - /data/Projects/JobAlerts/Utils/Utils_1.py
from Utils.HtmlCleaner import HTMLStripper                          #Custom Module - /data/Projects/JobAlerts/Utils/HtmlCleaner.py
from Features.LSI_common.MyBOW import MyBOW                         #Custom Module - /data/Projects/JobAlerts/Features/LSI_common/MyBOW.py
from Notifier.Notifier import send_email                            #Custom Module - /data/Projects/JobAlerts/Notifier/Notifier.py 
from Features.JobAlert_Functions import *                           #Custom Module - /data/Projects/JobAlerts/Features/JobAlert_Functions.py





#########################################################################################################             
############-----------------Function to return a Mongo Database
#########################################################################################################

def getMongoMaster():
        MONGO_HOST = 'localhost'
        MONGO_PORT = 27017
        connection = Connection(MONGO_HOST,MONGO_PORT)  #######---------- Leave blank for local DB
        #db=Database(connection,'JobAlerts')
        db=Database(connection,'JobAlerts')
        #db.authenticate('sumoplus','sumoplus')
        return db



#########################################################################################################             
############-----------------Try Except to provide alert in case of code failure
#########################################################################################################
try:
    
    send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Users Incremental - JAM",'Candidates Extraction from SOLR Started!!\n Collection Name : candidates_processed_4 \n DB Name : JobAlerts')
    
    #########################################################################################################             
    ############-----------------Loading the mappings for Bag of Words
    #########################################################################################################
    print 'Loading the mappings for bow'
    synMappingFileName = '../Features/rawData/LSI/Model_UnifiedTKE/unifiedtkelist.csv'
    keywordIdMappingFileName = '../Features/rawData/LSI/Model_UnifiedTKE/unifiedtkelist_numbered.csv' #This file is created
    mb = MyBOW(synMappingFileName, keywordIdMappingFileName)
    print 'Loading the mappings for bow...finished'
    
    
    
    #########################################################################################################             
    ############-----------------Finding the date 183 days before today's date
    #########################################################################################################
    date1 = datetime.now() - timedelta(days=183)
    print datetime.now()
    
    
    #########################################################################################################             
    ############-----------------Creating Mongo Connection for candidates database
    #########################################################################################################
    mongo_conn = getMongoMaster()
    collection = getattr(mongo_conn,"candidates_processed_4")
    collection.remove({'user_lastlogin' : {'$lt':str(date1)}})
    print "Candidates with last login less than 183 days removed"
    
    
    
    #########################################################################################################             
    ############-----------------Connecting to Mongo CandidateStatic and CandidatePreferences
    #########################################################################################################
    username = 'analytics'
    password = 'aN*lyt!cs@321' 
    #monconn_users_static = MongoConnect('CandidateStatic', host = '172.22.65.157', port = 27018, database = 'sumoplus', username = username, password = password, authenticate = True).getCursor()
    monconn_users_preferences = MongoConnect('CandidatePreferences', host = '172.22.65.88', port = 27018, database = 'sumoplus', username = username, password = password, authenticate = True)
    monconn_users_preferences_cur = monconn_users_preferences.getCursor()
    
    
    
    
    #########################################################################################################             
    ############-----------------Creating a Dictionary of Subfa to FA
    #########################################################################################################
    ifile = open('subfa_fa.csv','r')
    reader = csv.reader(ifile)
    reader.next()
    sub_fa_dict = {}
    for row in reader:
        sub_fa_dict[int(row[3])]= [row[4],int(row[1])]
    
    
    
    #########################################################################################################             
    ############-----------------Loading the FA lookup by passing filename to LoadFA_lookup function in 
    ############-----------------JobAlert_Functions script
    #########################################################################################################
    fa_lookup = LoadFA_lookup('/data/Projects/JobAlerts_Improvements/Data/fa_lookup.csv')
    
    
        
    
    #########################################################################################################             
    ############-----------------Creating SOLR query for incremental candidates
    ############-----------------Contact Rajat incase of any issues/change in query
    #########################################################################################################
    SOLR_URL_NEW = 'http://172.22.65.28:8989/solr/cda/select/?wt=json&rows=1000&q=*:*'
    #SOLR_URL_NEW = 'http://172.22.65.244:8983/solr/cda/select/?wt=json&rows=1000&q=*:*'
    #SOLR_URL_NEW = 'http://172.22.65.28:8983/solr/cda6m/select/?wt=json&rows=1000&q=*:*'
    query="&fq=sEAS:(1%203)&fq=sPSS:0&fq=sPBS:0&fq=sLH:[NOW/DAY-2DAY TO *]"
    
    
    
    #########################################################################################################             
    ############-----------------Getting the response data from SOLR 
    #########################################################################################################
    SOLR_URL_NEW=SOLR_URL_NEW +query
    fl ="id,sEm,sCP,sEx,sCTT,sGM,sCC,sCFA,sCFAID,sLM,sFLN,sCPV,sCLID,sASID,sHQL,sHES,sEV,sLH,sAS,sCL,sPSS,sPBS,sEAS,sS,sCIT,sRST"
    fls=fl
    fls="&fl="+fls
    SOLR_URL_NEW=SOLR_URL_NEW +fls
    response = requests.get(SOLR_URL_NEW)
    result_new = response.json()
    new_doc = result_new['response']['docs']
    solrcount=result_new['response']['numFound']
    count=int(solrcount)
    start=0
    querystart="&start="+str(start)
    SOLR_URL_NEW=SOLR_URL_NEW + querystart
    querycount=0
    insert=[]
    cnt=1000
    print count
    pidcounter=0
    
    
    #########################################################################################################             
    ############-----------------Creating the connection to resume dump database Mongo(172.22.66.233)
    #########################################################################################################
    tablename = "candidate_data"
    monconn_resume = MongoConnect(tablename, host='localhost', database='ResumeDump')
    monconn_resume_cur=monconn_resume.getCursor()
    
    
    #########################################################################################################             
    ############-----------------Creating the connection to application dump database Mongo(172.22.66.233)
    #########################################################################################################
    tablename = "candidate_applications"
    monconn_applies = MongoConnect(tablename, host='localhost', database='JobAlerts')
    monconn_applies_cur = monconn_applies.getCursor()
    
    
    #########################################################################################################             
    ############-----------------Loop to dump candidates in batch of 1000 
    #########################################################################################################
    user_id_list = []
    while(start < count):
            
        response = requests.get(SOLR_URL_NEW)
        result_new = response.json()
        new_doc = result_new['response']['docs']
        URL=SOLR_URL_NEW.split(querystart)
        start = start + cnt;
        querystart=""
        querystart="&start="+str(start)
        SOLR_URL_NEW=URL[0]+querystart
        
        for doc in new_doc:
            datadict={}
            if pidcounter >79:
                pidcounter=0
            
            try:    
                
    
                querycount=querycount+1
                try:
                    #########################################################################################################             
                    ############-----------------Getting the user details from SOLR 
                    #########################################################################################################
    
                    datadict["_id"]=(doc['id'])
                    datadict["user_ctc"]=doc.get('sAS',None)
                    datadict["user_experience"]=doc.get('sEx',None)              
                    datadict["user_id"]=(doc['id'])
                    datadict["user_email"]=(doc['sEm'])
                    datadict["user_location"]=[doc.get('sCL',None)]
                    datadict["user_lastlogin"]=doc.get('sLH',None)
                    datadict["user_phone"]=doc.get('sCP',None)
                    datadict["user_gender"]=doc.get('sGM',None)
                    datadict["user_current_company"]=doc.get('sCC',None)
                    datadict["user_functionalarea_id"]=doc.get('sCFAID',None)
                    datadict["user_lastmodified"]=doc.get('sLM',None)
                    datadict["user_fullname"]=doc.get('sFLN','Candidate')
                    datadict["user_phone_verified"]=doc.get('sCPV',None)
                    datadict["user_location_id"]=doc.get('sCLID',None)
                    datadict["user_ctc_id"]=doc.get('sASID',None)
                    datadict["user_highest_qual"]=doc.get('sHQL',None)
                    datadict["user_edu_special"]=doc.get('sHES',None)
                    datadict["user_email_verified"]=doc.get('sEV',None)
                    datadict["user_spam_status"]=doc.get('sPSS',None)
                    datadict["user_bounce_status"]=doc.get('sPBS',None)
                    datadict["user_email_alert_status"]=doc.get('sEAS',None)
                    datadict["user_functionalarea"]=doc.get('sCFA',None)
                    datadict["user_jobtitle"]=doc.get('sCTT',None)
                    
                    datadict["user_industry"]=doc.get('sCIT',None) 
                    datadict["user_profiletitle"]=doc.get('sRST',None)
                    
                    try:
                        datadict["user_edom"]=(doc['sEm']).split("@")[1]
                    except :
                        datadict["user_edom"]= None
                                
                    try:
                        datadict["user_skills"]=  ', '.join(doc['sS']) 
                    except:
                        datadict["user_skills"] = None
    
                    
    
                    #########################################################################################################             
                    ############-----------------Getting the resume data for a candidate
                    #########################################################################################################
                    Condition = {"_id":doc['id']}
                    resume_data = monconn_resume.loadFromTable(Condition)
                    if len(resume_data) != 0:
                        datadict['resume_jobtitle'] = resume_data[0]["user_jobtitle_resume"]
                        datadict['resume_skills'] =  resume_data[0]["user_skills_resume"]
                    else:                       
                        datadict['resume_jobtitle'] = None
                        datadict['resume_skills'] = None
    
                    
                    #########################################################################################################             
                    ############-----------------Creating the list of applied jobs for a candidate 
                    #########################################################################################################
                    Condition = {'fcu':doc['id']}
                    apply_data = monconn_applies.loadFromTable(Condition)
                    apply_data_list = list(apply_data)
                    application_list = []
                    if len(apply_data) == 0:
                        pass
                    else:
                        for row in apply_data_list:
                            application_list.append(row['fjj'])
                    application_list.sort()
                    datadict['application_list'] = application_list
                    datadict['application_count'] = len(application_list)
    
                    
                    #########################################################################################################             
                    ############-----------------Getting the preferred details 
                    #########################################################################################################
                    
                    if datadict["user_functionalarea_id"] in ["10053","0","-1","4401"] or datadict["user_functionalarea_id"]== None :
                        Condition = {'fcu':doc['id']}
                        pref_data = monconn_users_preferences.loadFromTable(Condition)
                        datadict['preferred_sub_faid'] = next((item['v'] for item in pref_data if item["t"] == 10), None)
                        try:
                            datadict['preferred_sub_fa'] = sub_fa_dict[int(datadict['preferred_sub_faid'])][0]
                            datadict['actual_faid'] = sub_fa_dict[int(datadict['preferred_sub_faid'])][1]
                            datadict['subject_status'] = 2
                        except Exception as e :
                            datadict['preferred_sub_faid'] = None
                            datadict['preferred_sub_fa'] = None
                            datadict['actual_faid'] = None
                            datadict['subject_status'] = 3
                    else:
                        datadict['subject_status'] = 1
                        datadict['preferred_sub_faid'] = None
                        datadict['preferred_sub_fa'] = None
                        datadict['actual_faid'] = fa_lookup[int(datadict["user_functionalarea_id"])]
    
                    
                except Exception as e :
                    print e
                    continue
                datadict["p"]=pidcounter
                datadict["s"]=False 
                user_id_list.append(doc['id'])
                #print len(insert)
                insert.append(datadict)                    
                pidcounter =pidcounter + 1
    
    
    
                #########################################################################################################             
                ############-----------------Inserting in mongo in batch of 10000
                #########################################################################################################
    
                if querycount%10000==0:
                    print "query count is " ,querycount
                    collection.remove({'user_id':{'$in':user_id_list}})
                    collection.insert(insert)
                    insert=[]
                    user_id_list = []
            except Exception as e:
                print e
                insert=[]
                user_id_list = []
    
    #########################################################################################################             
    ############-----------------Adding last batch of candidates 
    #########################################################################################################
    collection.remove({'user_id':{'$in':user_id_list}})        
    collection.insert(insert)
    send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Users Incremental - JAM",'Candidates Extraction from SOLR Completed!!\n Collection Name : candidates_processed_4 \n DB Name : JobAlerts \n Number of candidates: '+str(count))
    del(monconn_resume)
    del(monconn_applies)
except Exception as e:
    print e	
    send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com','progressiveht@hindustantimes.com'],"Users Incremental - JAM Failed URGENT","Call Akash (+91-8527716555) or Kanika (+91-9560649296) asap")
                
    
                    
                                    
