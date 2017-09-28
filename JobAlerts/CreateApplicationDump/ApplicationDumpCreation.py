__author__="Ashish.Jain"
__date__ ="15 Feb 2015"


##################
''' Abstract '''
##################
'''
This module provides functions for preProcessing Jobs for JAM
For Multiprocessing comment the mailing part otherwise mail will be sent for each chunk.
'''


#############################################             
'''Importing Python Libraries and Modules'''
#############################################
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
from datetime import date, datetime, time
from dateutil.relativedelta import *
from DataConnections.MongoConnect.MongoConnect import MongoConnect
from bson.objectid import ObjectId



#############################################             
'''Function to return a Mongo Connection'''
#############################################

def getMongoConnection(MONGO_HOST,MONGO_PORT,DB, isSlave=False, isAuth=False, username = 'analytics', password = 'aN*lyt!cs@321'):
    if isAuth:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave, username = username, password = password)
    else:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave)
    #mongo_conn = connection[DB]
    #mongo_conn.authenticate(username, password)
    return connection


#############################################             
'''Function to return a Mongo Database'''
#############################################

def getMongoMaster():
        MONGO_HOST = 'localhost'
        MONGO_PORT = 27017
        connection = Connection(MONGO_HOST,MONGO_PORT)#leave blank for local db
        db=Database(connection,'JobAlerts')
        #db=Database(connection,'Candidates')
        #db.authenticate('sumoplus','sumoplus')
        return db




if __name__ == '__main__':


    ####################################################             
    '''Finding the date 183 days before today's date'''
    ####################################################
    todayDate=date.today()
    previousDate=todayDate+relativedelta(days= -183)
    day1 = datetime.combine(previousDate, time(0, 0))
    
    ###################################################################             
    '''Connection to the Candidate Apply DB on Mongo(172.22.66.253)'''
    ###################################################################
 
    print 'Connecting to Mongodb..'
    tablename="candidate_applications"
    mongo_conn_local = getMongoMaster()
    collection = getattr(mongo_conn_local,tablename)
    print 'Connecting to Mongodb...finished'


    
    #####################################################             
    'Getting the last objectID from apply collection'
    #####################################################
    '''
    print "Getting last object id!!! "
    apply_count = collection.count()
    #print apply_count
    latest_record_1 = collection.find().sort([('ad',-1)]).limit(1)
    latest_record = list(latest_record_1)
    
    for row in latest_record:
        last_object_id = row["_id"]
        print "Last ObjectID ",last_object_id
    '''
    #ObjectId("5795092c80cf2c5f1e5656a7")

    ###################################################################             
    'Connection to the CandidateMatch on Mongo(172.22.65.58)'
    ###################################################################
 
    print "Connecting to CandidateMatch !!!"
    mongo_conn = getMongoConnection('172.22.65.58', 27017, 'recruiter_master', True)
    db = getattr(mongo_conn, 'recruiter_master')
    print 'Connecting to CandidateMatch...finished'    
   
    
    
    #####################################################             
    'Fetching the Apply data from CandidateMatch '
    ##################################################### 
    last_object_id = "5795092c80cf2c5f1e5656a7"
    print "Fetching apply data !!!!"
    count = 0
    try:
        while True:
            data = db.CandidateMatch.find({'_id':{'$gt':ObjectId(last_object_id)}}).limit(2000)
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
    

    
    ###################################################################             
    'Removing Expired records from Apply DB Mongo(172.22.66.253) '
    ###################################################################

    '''
    print "Removing expired records!!"
    collection.remove({'ad' : {'$lt':day1}})
    print "Expired records removed !!"    
    #del(mongo_conn)
    final_count = collection.count()
    remove_count = apply_count + count - final_count
    os.system(' echo "Applies Processing Completed!!\nInitial Count :'+str(apply_count)+'\nApplies added :'+str(count)+'\nApplies removed :'+str(remove_count)+'\nFinal Count :'+str(final_count)+' " | mutt -s "Similar Jobs Mailer" ashish4669@gmail.com ,ashish.jain1@hindustantimes.com,himanshusolanki53@gmail.com')
    print "done"
    '''
