from __future__ import division
__author__="Himanshu Solanki"
__date__ ="18 Jul 2016"


##################
''' Abstract '''
##################

'''
This class provides functionality to load a sparse matrix from CSV
Updates : 
Added user ObjectId to the application collection 
Need to save the last application
'''


#############################################             
'''Importing Python Libraries and Modules'''
#############################################
import math
import json
import cPickle as pickle
import numpy as np
import scipy.sparse
import csv
from pprint import pprint
import sys
sys.path.append('/data/Projects/SimilarJobs/Notifier/')
from Notifier import send_email
import numpy as np
from scipy.sparse import coo_matrix
from datetime import date, datetime, time
from dateutil.relativedelta import *
import sys
from _ast import Continue
sys.path.append('./../')
from Features.CTCMatchScore.CTCMatchScore import CTCMatchScore
from Features.ExpMatchScore.ExpMatchScore import ExpMatchScore
from Features.PaidBoostScore.PaidBoostScore import PaidBoostScore
from Features.CityScore.getCityScore import CityMatch
from Model.getOverallMatchScore import getOverallMatchScore
from Main.getLSICosine import getLSICosine
from pprint import pprint
from DataConnections.MySQLConnect.MySQLConnect import MySQLConnect
from DataConnections.MongoConnect.MongoConnect import MongoConnect
from Utils.Utils_1 import cleanToken
from Utils.HtmlCleaner import HTMLStripper, cleanHTML
import pdb
from gensim import corpora, models, similarities
from Features.LSI_common.MyBOW import MyBOW
from bson.objectid import ObjectId
#from Logger.MyLogger import MyLogger
import csv
from multiprocessing import Pool
import random
import heapq
import gensim
import multiprocessing
#from rfc3339 import rfc3339
import os
import pymongo
from pymongo import *
from pymongo import Connection
from pymongo import MongoClient
from pprint import pprint




#############################################             
' Function to return a Mongo Connection '
#############################################

def getMongoConnection(MONGO_HOST,MONGO_PORT,DB, isSlave=False, isAuth=False, username = 'analytics', password = 'aN*lyt!cs@321'):
    if isAuth:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave, username = username, password = password)
    else:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave)
    #mongo_conn = connection[DB]
    #mongo_conn.authenticate(username, password)
    return connection



######################################################             
' Function to create forward and reverse mapping '
######################################################
 
def getMapping(self, columnNumber, reverse = False):
    if columnNumber == 1:
        if reverse == False:
            return intIdToUseridMapping
        else:
            return useridToIntIdMapping
    
    elif columnNumber == 2:
        if reverse == False:
            return intIdToJobidMapping
        else:
            return jobidToIntIdMapping




#############################################             
'Function to create indexes for applications'
#############################################

def ApplicationIndexing():
    
    #######################################             
    'Initiating and Declaring variables ' 
    #######################################
    
    user_mapping = {}
    i = 0
    user_index = 0
     
    ###############################             
    ' Creating the previous date ' 
    ###############################
    todayDate=date.today()
    previousDate=todayDate+relativedelta(days=-183)
    day1 = datetime.combine(previousDate, time(0, 0))    
    day2 = datetime.combine(todayDate, time(0, 0))
    
    ###########################################################             
    ' Connecting to the Candidate Apply DB (without indexes) ' 
    ###########################################################
    tablename = 'candidate_applications'
    mongo_conn = MongoConnect(tablename, host='172.22.66.198', database='JobAlerts')
    mongo_conn_cur = mongo_conn.getCursor()
    
    #################################################################             
    ' Connecting to DB where indexed applications are to be dumped ' 
    #################################################################
    tablename="apply_data"
    monconn_user = MongoConnect(tablename, host='172.22.66.198', database='SimilarJobs')
    
    ######################             
    ' Creating indexes  ' 
    ######################

    last_user_ObjectId = 1 
    previous_id = "0" 
    id = "0"
    
    #recency_score = 
    
    try:
        while True:    
            myCondition = { "fcu" : {'$gt':id}}
            data = mongo_conn_cur.find(myCondition).sort('fcu').limit(100000)
            insert = []
    
            for row in data:    
                try:  
                    userid = row['fcu']
                    user_ObjectID = row['_id']
                    
                    if userid == previous_id :
                        pass
                    else:
                        previous_id = userid
                        user_index +=1
                    
                    index = user_index
                except:
                    continue
    
                
                
                jobid = row['fjj']
                application_date = row['ad']
                #print "application_date",application_date
                current_time = datetime.now()
                #print "today",current_time
                
                difference = abs((current_time - application_date).days)
                #print difference
                #recency_score = 1/(1+ math.sqrt(difference))
                if difference <= 10:
                    recency_score = 1 
                elif difference > 10 and difference <= 20:
                    recency_score = 0.9
                elif difference > 20 and difference <= 30:
                    recency_score = 0.8
                else:
                    recency_score = 0.6
                
                #print "recency_score",recency_score
                
                #break
                pid = i%5000
                document = {"userid":userid,\
                                "user_index":index, \
                                "jobid": jobid , \
                                'score':recency_score, \
                                'application_date':application_date, \
                                '_id': user_ObjectID , \
                                'pid':pid
                                }
            
                insert.append(document)
                id = row['fcu']
                i += 1
                if i%100000 == 0:
                    print "Records Processed :",i
                    #sys.exit(0)
             
            monconn_user.insert(insert)
    except Exception as E:
        print E

if __name__ == '__main__':
    try:
        
        #os.system(' echo "Application Indexing Started.... '' " | mutt -s "Similar Jobs Mailer" himanshu.solanki@hindustantimes.com,kanika.khandelwal@hindustantimes.com, akash.verma@hindustantimes.com')
        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Similar Jobs Mailer applies preprocessing",'Application Indexing Started.... !!')        
        #send_email(['himanshu.solanki@hindustantimes.com'],"Similar Jobs Mailer applies preprocessing",'Application Indexing Started.... !!')
        #############################             
        'Dropping the old collection'
        #############################
        tablename="apply_data"
        monconn_user = MongoConnect(tablename, host='172.22.66.198', database='SimilarJobs')
        monconn_user.dropTable()
        monconn_user.close()
    
    
        #############################             
        'Starting Index Creation'
        #############################
        ApplicationIndexing()
    
        #############################             
        'Creating Index on Collection'    
        #############################
        tablename="apply_data"
        monconn_user = MongoConnect(tablename, host='172.22.66.198', database='SimilarJobs')
        monconn_user.doIndexing('user_index')
        monconn_user.doIndexing('pid')
        monconn_user.close()
        #os.system(' echo "Application Indexing Completed !!!'' " | mutt -s "Similar Jobs Mailer" himanshu.solanki@hindustantimes.com, kanika.khandelwal@hindustantimes.com, akash.verma@hindustantimes.com')    
        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Similar Jobs Mailer applies preprocessing",'Application Indexing Completed !!!')
        #send_email(['himanshu.solanki@hindustantimes.com'],"Similar Jobs Mailer applies preprocessing",'Application Indexing Completed !!!')
    except Exception as e :
        print e
        #os.system(' echo "Similar Job Mailer Failed : Application Indexing Failed!!!!!\nCall Himanshu (+91-7738982847) asap.'  ' " | mutt -s "Urgent!!!" progressiveht@hindustantimes.com,himanshu.solanki@hindustantimes.com')
        #send_email(['himanshu.solanki@hindustantimes.com'],"Urgent!!!",'Similar Job Mailer Failed : Application Indexing Failed!!!!!\n Call Kanika (+91-9560649296) or Akash (+91-8527716555) asap.')
        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com','progressiveht@hindustantimes.com'],"Urgent!!!",'Similar Job Mailer Failed : Application Indexing Failed!!!!!\n Call Kanika (+91-9560649296) or Akash (+91-8527716555) asap.')