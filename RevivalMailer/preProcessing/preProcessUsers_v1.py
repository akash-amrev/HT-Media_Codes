#!/usr/bin/python
__author__="Ashish.Jain"
__date__ ="15 Feb 2017"


#################################################################################################################           
############-----------------  Summary of Script
#################################################################################################################
#--> Extract last 2 days candidate data from SOLR (172.22.65.28:8989)
#--> Process candidate details to bring them to desired format
#--> Dumping the final data in 'candidates_processed' collection in 'mailer_weekly' Database (172.22.66.233)
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
import MySQLdb
import simplejson as json
from httplib2 import Http
import pymongo
import requests
import time
import datetime
import calendar
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

from DataConnections.MySQLConnect.MySQLConnect import MySQLConnect  #Custom Module - /data/Projects/RevivalMailer/DataConnections/MySQLConnect/MySQLConnect.py
from DataConnections.MongoConnect.MongoConnect import MongoConnect  #Custom Module - /data/Projects/RevivalMailer/DataConnections/MongoConnect/MongoConnect.py
from Utils.Utils_1 import cleanToken                                #Custom Module - /data/Projects/RevivalMailer/Utils/Utils_1.py
from Utils.HtmlCleaner import HTMLStripper                          #Custom Module - /data/Projects/RevivalMailer/Utils/HtmlCleaner.py
from Features.LSI_common.MyBOW import MyBOW                         #Custom Module - /data/Projects/RevivalMailer/Features/LSI_common/MyBOW.py
from Notifier.Notifier import send_email                            #Custom Module - /data/Projects/RevivalMailer/Notifier/Notifier.py 




#########################################################################################################             
############-----------------Function to get date range for Extracting candidates form SOLR
#########################################################################################################

def getWeeklyLoginRangeFor(Qdate):
    now = Qdate - timedelta(days=1)
    yr = now.year
    mt = now.month
    dy = now.day
    weekday = now.weekday()
    delta = 364/2/7
    day1 =  datetime.datetime(yr, mt, dy, 00, 00, 00)
    startdate = day1 - timedelta(days=364) + timedelta(days=delta*weekday)
    enddate = startdate + timedelta(days=delta)
    return startdate, enddate



#########################################################################################################             
############-----------------Function to return a Mongo Database
#########################################################################################################
def getMongoMaster():
        MONGO_HOST = 'localhost'
        MONGO_PORT = 27017
        connection = Connection(MONGO_HOST,MONGO_PORT)#leave blank for local db
        #db=Database(connection,'JobAlerts')
        db=Database(connection,'mailer_weekly')
        #db.authenticate('sumoplus','sumoplus')
        return db



#########################################################################################################             
############-----------------Try Except to provide alert in case of code failure
#########################################################################################################
  
try:
    send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Revival Mailer Weekly",'Candidates Processing Started !!!')
    
    date= datetime.datetime.now()
    print date
    startdate, enddate= getWeeklyLoginRangeFor(date)
    print startdate
    days_from =  (date - startdate).days
    print enddate
    days_to =  (date-enddate).days
    

    #########################################################################################################             
    ############-----------------Loading the mappings for Bag of Words
    #########################################################################################################    
    print 'Loading the mappings for bow'
    synMappingFileName = '../Features/rawData/LSI/Model_UnifiedTKE/unifiedtkelist.csv'
    keywordIdMappingFileName = '../Features/rawData/LSI/Model_UnifiedTKE/unifiedtkelist_numbered.csv' #This file is created
    mb = MyBOW(synMappingFileName, keywordIdMappingFileName)
    print 'Loading the mappings for bow...finished'



    #########################################################################################################             
    ############-----------------Creating Mongo Connection for candidates database
    #########################################################################################################    
    mongo_conn = getMongoMaster()
    collection = getattr(mongo_conn,"candidates_processed")
    collection.drop()



    #########################################################################################################             
    ############-----------------Creating SOLR query for incremental candidates
    ############-----------------Contact Rajat incase of any issues/change in query
    #########################################################################################################
    SOLR_URL_NEW = 'http://172.22.65.28:8989/solr/cda/select/?wt=json&rows=1000&q=*:*'
    #SOLR_URL_NEW = 'http://172.22.65.28:8983/solr/cda6m/select/?wt=json&rows=1000&q=*:*'
    query="&fq=sEAS:(1%203)&fq=sPSS:0&fq=sPBS:0&fq=sLH:[NOW/DAY-"+str(days_from)+"DAY TO NOW/DAY-"+str(days_to)+"DAY]"



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
    ############-----------------Loop to dump candidates in batch of 1000 
    #########################################################################################################
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
                    ############----------------- Creating the Bag of Words of Candidate's Details
                    #########################################################################################################
                    text = 5*(" "+cleanToken(datadict["user_profiletitle"])) + ' ' + 5*(" "+cleanToken(datadict["user_skills"])) + ' ' + 2*(" "+cleanToken(datadict["user_jobtitle"])) + ' ' +1*(" "+cleanToken(datadict["user_industry"])) + ' ' + 1*(" "+cleanToken(datadict["user_functionalarea"]))
                    datadict["user_bow"] = mb.getBow(text, getbowdict = 0)
                
                       
                except Exception as e :
                    print e
                    continue
                datadict["p"]=pidcounter
                datadict["s"]=False 

                insert.append(datadict)                    
                pidcounter =pidcounter + 1


                #########################################################################################################             
                ############-----------------Inserting in mongo in batch of 10000
                #########################################################################################################
                            
                if querycount%10000==0:
                    print "query count is " ,querycount

                    collection.insert(insert)
                    insert=[]

            except Exception as e:
                insert=[]
                

    #########################################################################################################             
    ############-----------------Adding last batch of candidates 
    #########################################################################################################
        
    collection.insert(insert)
    collection.create_index([("p",pymongo.ASCENDING)])
    send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Revival Mailer Weekly",'Candidates Processing Completed !!!')
            
except Exception as e:
    print e
    
    send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com','progressiveht@hindustantimes.com'],"Revival Mailer Weekly - URGENT!!!","Candidate Processing Failed!!!!\nCall Akash (+91-8527716555) or Kanika (+91-9560649296) asap")    
                    
                                    
