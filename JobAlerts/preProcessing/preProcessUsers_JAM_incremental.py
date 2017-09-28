#!/usr/bin/python
import sys
sys.path.append('./../')
import os
from datetime import datetime
import MySQLdb
import simplejson as json
from httplib2 import Http
import pymongo
import time
import datetime
from datetime import timedelta
from pymongo import Connection
import unicodedata
#from pymongo.bson import BSON
import MySQLdb
import sys, traceback
import bson
import re
import multiprocessing
import logging
from time import sleep
from pymongo.database import Database
from datetime import datetime, timedelta
from DataConnections.MySQLConnect.MySQLConnect import MySQLConnect
from DataConnections.MongoConnect.MongoConnect import MongoConnect
from Utils.Utils_1 import cleanToken
from Utils.HtmlCleaner import HTMLStripper
from Features.LSI_common.MyBOW import MyBOW

import requests


def getMongoMaster():
        MONGO_HOST = 'localhost'
        MONGO_PORT = 27017
        connection = Connection(MONGO_HOST,MONGO_PORT)#leave blank for local db
        db=Database(connection,'JobAlerts')
        #db.authenticate('sumoplus','sumoplus')
        return db

os.system(' echo "Incremental Candidates Processing Started...'' " | mutt -s "Job Alert Mailer Inc." ashish4669@gmail.com ,himanshusolanki53@gmail.com')
print 'Loading the mappings for bow'
synMappingFileName = '../Features/rawData/LSI/Model_UnifiedTKE/unifiedtkelist.csv'
keywordIdMappingFileName = '../Features/rawData/LSI/Model_UnifiedTKE/unifiedtkelist_numbered.csv' #This file is created
mb = MyBOW(synMappingFileName, keywordIdMappingFileName)
print 'Loading the mappings for bow...finished'
date1 = datetime.now() - timedelta(days=183)
print date1


mongo_conn = getMongoMaster()
collection = getattr(mongo_conn,"candidates_processed")
collection.remove({'user_lastlogin' : {'$lt':str(date1)}})
#collection.drop()

SOLR_URL_NEW = 'http://172.22.65.28:8983/solr/cda/select/?wt=json&rows=1000&q=*:*'
#SOLR_URL_NEW = 'http://172.22.65.28:8983/solr/cda6m/select/?wt=json&rows=1000&q=*:*'
query="&fq=sEAS:(1%203)&fq=sPSS:0&fq=sPBS:0&fq=sLH:[NOW/DAY-2DAY TO *]"

#query="&fq=sEAS:(1%203)&fq=sPSS:0&fq=sPBS:0"
SOLR_URL_NEW=SOLR_URL_NEW +query
#print  SOLR_URL_NEW
#fl="sEm,sLH,sCFA,sEx,sLM,sFLN,sAS,sEV"\
fl = "sAS,sEx,sCIT,sCFA,sCTT,sEm,sCL,sLH,sS,sRST,id"
fls=fl
fls="&fl="+fls
SOLR_URL_NEW=SOLR_URL_NEW +fls

response = requests.get(SOLR_URL_NEW)
if not response.ok:
    sys.stderr.write("request for %s failed.\n" % SOLR_URL_NEW)
    sys.exit(1)
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
while(start < count):
        
    #print start,count
    response = requests.get(SOLR_URL_NEW)
    if not response.ok:
        sys.stderr.write("request for %s failed.\n" % SOLR_URL_NEW)
        continue
    
    result_new = response.json()
    new_doc = result_new['response']['docs']
    URL=SOLR_URL_NEW.split(querystart)
    start = start + cnt;
    querystart=""
    querystart="&start="+str(start)
    SOLR_URL_NEW=URL[0]+querystart
    #print SOLR_URL_NEW
    #datadict={}
    
    for doc in new_doc:
        datadict={}
        if pidcounter >79:
            pidcounter=0
        
        try:    
            
            #count=count+1
            querycount=querycount+1
            try:
                datadict["_id"]=(doc['id'])
                datadict["user_ctc"]=(doc['sAS'])
                datadict["user_experience"]=(doc['sEx'])              
                datadict["user_id"]=unicode(doc['id'])
                datadict["user_email"]=unicode(doc['sEm'])
                datadict["user_location"]=(doc['sCL'])
                datadict["user_lastlogin"]=unicode(doc['sLH'])
                
                try:
                    datadict["user_functionalarea"]=cleanToken(doc['sCFA'])
                except:
                    datadict["user_functionalarea"]= ''
                try:
                    datadict["user_industry"]=cleanToken(doc['sCIT'])
                except:
                    datadict["user_industry"] = ''
                
                try:
                    datadict["user_skills"]=  ', '.join(doc['sS']) 
                except:
                    datadict["user_skills"] = ''
                    
                try:
                    datadict["user_jobtitle"]=cleanToken(doc['sCTT'])
                except:
                    datadict["user_jobtitle"]=''
                    
                try:
                    datadict["user_profiletitle"]=cleanToken(doc['sRST'])
                except:
                    datadict["user_profiletitle"] = ''

                #print datadict["user_location"]
                
                text = 5*(" "+datadict["user_profiletitle"]) + ' ' + 3*(" "+datadict["user_skills"]) + ' ' + 2*(" "+datadict["user_jobtitle"]) + ' ' +1*(" "+datadict["user_industry"]) + ' ' + 1*(" "+datadict["user_functionalarea"])
                datadict["user_bow"] = mb.getBow(text, getbowdict = 0)
            
                   
            except Exception as e :
                print e
                continue
            datadict["p"]=pidcounter
            datadict["s"]=False 
            user_id_list.append(doc['id'])
            insert.append(datadict)                    
            pidcounter =pidcounter + 1
                        
            if querycount%10000==0:
                print "query count is " ,querycount
                collection.remove({'user_id':{'$in':user_id_list}})
                collection.insert(insert)
                insert=[]
                user_id_list = []
        
        except Exception as e:
            insert=[]
            user_id_list = []
            #print e
            
collection.remove({'user_id':{'$in':user_id_list}})            
collection.insert(insert)
os.system(' echo "Incremental Candidates Processing Done!!!! \n Number of candidates Processed :'+str(count)+' " | mutt -s "Job Alert Mailer Inc. " ashish4669@gmail.com ,himanshusolanki53@gmail.com')            
#collection.create_index([("pid",pymongo.ASCENDING),("s",pymongo.ASCENDING)])
#d=datetime.now()-timedelta(days=31)
#collection.remove({'ev':False});
print "process completed"
        

                
                                