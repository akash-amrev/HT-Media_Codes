#!/usr/bin/python
__author__="Ashish.Jain"
__date__ ="18 Mar 2017"


#################################################################################################################           
############-----------------  Summary of Script
#################################################################################################################
#--> Exract the Spam = 1 Candidates from SOLR (172.22.65.28:8989) and remove these candidates from 
#    'candidates_processed_4' collection in 'JobAlerts' Database (172.22.66.233)
#--> Exract the Bounce = 1 Candidates from SOLR (172.22.65.28:8989) and remove these candidates from 
#    'candidates_processed_4' collection in 'JobAlerts' Database (172.22.66.233)
#--> Exract the EAS = 1 Candidates from SOLR (172.22.65.28:8989) and remove these candidates from 
#    'candidates_processed_4' collection in 'JobAlerts' Database (172.22.66.233)
#--> Alert Mailers implemented to inform incase of any failure.
#--> Removal is done in batch of 1000





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
import time
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





#########################################################################################################             
############-----------------Function to return a Mongo Database
#########################################################################################################
def getMongoMaster():
        MONGO_HOST = '172.22.66.198'
        MONGO_PORT = 27017
        connection = Connection(MONGO_HOST,MONGO_PORT)#leave blank for local db
        #db=Database(connection,'JobAlerts')
        db=Database(connection,'JobAlerts')
        #db.authenticate('sumoplus','sumoplus')
        return db
    



#########################################################################################################             
############-----------------Try Except to provide alert in case of code failure
#########################################################################################################
try:
        
    send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Spam Bounce Removal - JAM",'Spam Bounce Removal Started!!\nDB Name : candidates_processed_4')

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
    collection = getattr(mongo_conn,"candidates_processed_4")
    
    
    
    ############################################################
    #Removing the candidates with bounce status = 1
    ###########################################################
    SOLR_URL_NEW = 'http://172.22.65.28:8989/solr/cda6m/select/?wt=json&rows=1000&q=sPBS:1'
    #SOLR_URL_NEW = 'http://172.22.65.28:8983/solr/cda6m/select/?wt=json&rows=1000&q=*:*'
    fl = 'sEm,sPSS,sPBS,id'
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
            
            querycount=querycount+1
            
            user_id =  doc['id']
            user_id_list.append(user_id)
            if querycount%1000 == 0:
                print "query count is " ,querycount
                collection.remove({'_id':{'$in':user_id_list}})
                user_id_list = []
    collection.remove({'_id':{'$in':user_id_list}})             
    time.sleep(5)    
    
    
    
    ############################################################
    # Removing the candidates with spam status = 1 
    ############################################################
    
    SOLR_URL_NEW = 'http://172.22.65.28:8989/solr/cda6m/select/?wt=json&rows=1000&q=sPSS:1'
    fl = 'sEm,sPSS,sPBS,id'
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
            
            querycount=querycount+1
            
            user_id =  doc['id']
            user_id_list.append(user_id)
            if querycount%1000 == 0:
                print "query count is " ,querycount
                collection.remove({'_id':{'$in':user_id_list}})
                user_id_list = []
    collection.remove({'_id':{'$in':user_id_list}})           
    time.sleep(5)           
    
    

    ############################################################
    # Removing the candidates with EAS status not in 1 and 3
    ############################################################
    
    SOLR_URL_NEW = 'http://172.22.65.28:8989/solr/cda6m/select/?wt=json&rows=1000&q=-sEAS:(1%203)'
    fl = 'sEm,sPSS,sPBS,id'
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
            
            querycount=querycount+1
            
            user_id =  doc['id']
            user_id_list.append(user_id)
            if querycount%1000 == 0:
                print "query count is " ,querycount
                collection.remove({'_id':{'$in':user_id_list}})
                user_id_list = []
    collection.remove({'_id':{'$in':user_id_list}})
    send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Spam Bounce Removal - JAM",'Spam Bounce Removal Completed!!\nDB Name : candidates_processed_4')
except:
    send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com','progressiveht@hindustantimes.com'],"Spam Bounce Removal - JAM FAILED",'Spam Bounce Removal Failed !!\nCall Akash (+91-8527716555) or Kanika (+91-9560649296) asap!!')
    