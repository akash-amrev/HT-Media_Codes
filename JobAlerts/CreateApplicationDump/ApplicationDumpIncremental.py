__author__="Ashish.Jain"
__date__ ="15 Feb 2017"


#########################################################################################################             
############-----------------  Summary of Script
#########################################################################################################
#--> Extract incremental applications from 'CandidateMatch' collection in 'recruiter_master' Database
#    on the basis of last '_id'. Server - 172.22.65.58   Port - 27017
#--> Dumps the extracted applications in 'candidate_applications' collection in 'JobAlerts' Database
#    Server - 172.22.66.233   Port - 27017
#--> Removes the expired applications from 'candidate_applications' collection in 'JobAlerts' Database
#--> Alert Mailers Implemented in case of any failure
#--> Final count of added applications and removed applications sent  through mail.


#########################################################################################################             
############-----------------Importing Python Libraries and Modules
#########################################################################################################
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
from datetime import date, datetime, time
from dateutil.relativedelta import *
from bson.objectid import ObjectId

#Custom Modules
from DataConnections.MySQLConnect.MySQLConnect import MySQLConnect  #Custom Module - /data/Projects/JobAlerts/DataConnections/MySQLConnect/MySQLConnect.py
from DataConnections.MongoConnect.MongoConnect import MongoConnect  #Custom Module - /data/Projects/JobAlerts/DataConnections/MongoConnect/MongoConnect.py
from Utils.Utils_1 import cleanToken                                #Custom Module - /data/Projects/JobAlerts/Utils/Utils_1.py
from Utils.HtmlCleaner import HTMLStripper                          #Custom Module - /data/Projects/JobAlerts/Utils/HtmlCleaner.py
from Features.LSI_common.MyBOW import MyBOW                         #Custom Module - /data/Projects/JobAlerts/Features/LSI_common/MyBOW.py
from Notifier.Notifier import send_email                            #Custom Module - /data/Projects/JobAlerts/Notifier/Notifier.py 





#########################################################################################################             
############-----------------Function to return a Mongo Connection
#########################################################################################################

def getMongoConnection(MONGO_HOST,MONGO_PORT,DB, isSlave=False, isAuth=False, username = 'analytics', password = 'aN*lyt!cs@321'):
    if isAuth:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave, username = username, password = password)
    else:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave)
    #mongo_conn = connection[DB]
    #mongo_conn.authenticate(username, password)
    return connection


#########################################################################################################             
############-----------------Function to return a Mongo Database
#########################################################################################################

def getMongoMaster():
        #MONGO_HOST = 'localhost'
        MONGO_HOST = '172.22.66.198'
        MONGO_PORT = 27017
        connection = Connection(MONGO_HOST,MONGO_PORT)  #######---------- Leave blank for local DB
        #db=Database(connection,'JobAlerts')
        db=Database(connection,'JobAlerts')
        #db.authenticate('sumoplus','sumoplus')
        return db





#########################################################################################################             
############-----------------Main Function
#########################################################################################################

if __name__ == '__main__':
    
    

    #########################################################################################################             
    ############-----------------Try Except to provide alert in case of code failure
    #########################################################################################################
    try:
        
        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Applies Incremental - JAM",'Incremental Applications Extraction started!!!\n Collection Name : candidate_applications \n DB Name : JobAlerts')
    
        #########################################################################################################             
        ############-----------------Finding the date 183 days before today's date
        #########################################################################################################
        todayDate=date.today()
        previousDate=todayDate+relativedelta(days= -183)
        day1 = datetime.combine(previousDate, time(0, 0))
        
        
        #########################################################################################################             
        ############-----------------Connection to the candidate_applications on Mongo(172.22.66.233)
        ######################################################################################################### 
        print 'Connecting to Mongodb..'
        tablename="candidate_applications"
        mongo_conn_local = getMongoMaster()
        collection = getattr(mongo_conn_local,tablename)
        print 'Connecting to Mongodb...finished'
    
    
        
        #########################################################################################################             
        ############-----------------Getting the last objectID from apply collection
        #########################################################################################################  
        print "Getting last object id!!! "
        apply_count = collection.count()
        latest_record_1 = collection.find().sort([('ad',-1)]).limit(1)
        latest_record = list(latest_record_1)
        
        for row in latest_record:
            last_object_id = row["_id"]
            print "Last ObjectID ",last_object_id
        
    
    
        #########################################################################################################             
        ############-----------------Connection to the CandidateMatch on Mongo(172.22.65.58)
        #########################################################################################################   
        print "Connecting to CandidateMatch !!!"
        try:
            mongo_conn = getMongoConnection('172.22.65.58', 27017, 'recruiter_master', True)
        except:
            mongo_conn = getMongoConnection('172.22.65.59', 27017, 'recruiter_master', True)
        db = getattr(mongo_conn, 'recruiter_master')
        print 'Connecting to CandidateMatch...finished'    
       
        
        
        #########################################################################################################             
        ############-----------------Fetching the Incremental Apply data from CandidateMatch 
        #########################################################################################################  
        print "Fetching apply data !!!!"
        count = 0
        try:
            while True:
                data = db.CandidateMatch.find({'_id':{'$gt':ObjectId(last_object_id)}}).limit(1000)
                rows = list(data)
                insert = []
                for row in rows:
                    last_object_id = row['_id']     
                    insert.append(row)
                    count +=1
                    if count%20000 == 0:
                        print "Records added :",count        
                collection.insert(insert)
        except:
            pass
        print "Apply data fetching completed.."
        
    
        
        #########################################################################################################             
        ############-----------------Removing Expired records from Apply DB Mongo(172.22.66.233)
        #########################################################################################################  
        print "Removing expired records!!"
        collection.remove({'ad' : {'$lt':day1}})
        print "Expired records removed !!"    
        final_count = collection.count()
        remove_count = apply_count + count - final_count
        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Applies Incremental - JAM",'Applies Processing Completed!!\n Initial Count :'+str(apply_count)+'\n Applies added :'+str(count)+'\n Applies removed :'+str(remove_count)+'\n Final Count :'+str(final_count))
    except:
        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com','progressiveht@hindustantimes.com'],"Applies Incremental - JAM FAILED",'Call Akash (+91-8527716555) or Kanika (+91-9560649296) asap')
