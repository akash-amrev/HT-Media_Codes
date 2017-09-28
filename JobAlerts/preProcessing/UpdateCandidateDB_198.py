#!/usr/bin/python
__author__="Ashish.Jain"
__date__ ="10 Apr 2017"


#################################################################################################################           
############-----------------  Summary of Script
#################################################################################################################
#--> Taking up all the candidates in 'candidates_processed_4' chunk wise and then checking their applications 
#    from 'candidate_applications' collection. Those candidates with Zero application are then dumped to 
#    'candidates_processed_5' collection in 'JobAlerts' database ( 172.22.66.233)
#--> This is done to avoid the overlap in Similar Job Mailer and JAM so that no candidate gets  both mailer in 
#    same day.
#--> Alerts check mailers implemented to inform in case of any failure




#################################################################################################################            
############-----------------Importing Python Libraries and Modules
#################################################################################################################
#Inbuilt Modules
import sys
sys.path.append('./../')
from pprint import pprint
import pdb
import csv
import time
from multiprocessing import Pool
import os
import datetime
from datetime import timedelta
import re
from multiprocessing import Pool
import multiprocessing
from pymongo import Connection
import pymongo
from pymongo.database import Database

#Custom Modules
from DataConnections.MongoConnect.MongoConnect import MongoConnect  #Custom Module - /data/Projects/JobAlerts/DataConnections/MongoConnect/MongoConnect.py
from Notifier.Notifier import send_email                            #Custom Module - /data/Projects/JobAlerts/Notifier/Notifier.py                  
from Features.JobAlert_Functions import *                           #Custom Module - /data/Projects/JobAlerts/Features/JobAlert_Functions.py
from Features.LSI_common.MyBOW import MyBOW                         #Custom Module - /data/Projects/JobAlerts/Features/LSI_common/MyBOW.py





#################################################################################################################            
############-----------------Function to find the candidates with Zero applicaitons from 'candidates_processed_4'
############-----------------and dumping them in 'candidates_processed_5' collection in 'JobAlerts' database.
#################################################################################################################

def computeAlertsChunk(chunkID):

    #########################################################################################################             
    ############-----------------Creating the connection to Mongo (172.22.66.233)
    #########################################################################################################
    #monconn_users_static = MongoConnect('candidates_processed_4', host = 'localhost', database = 'JobAlerts')
    monconn_users_static = MongoConnect('candidates_processed_4', host = '172.22.66.198', database = 'JobAlerts')	
    monconn_users_static_cur = monconn_users_static.getCursor()
    
    #monconn_applications = MongoConnect('candidate_applications', host = 'localhost', database = 'JobAlerts')
    monconn_applications = MongoConnect('candidate_applications', host = '172.22.66.198', database = 'JobAlerts')
    monconn_applications_cur = monconn_users_static.getCursor()
    
    tablename = 'candidates_processed_5'
    #monconn_recommendations = MongoConnect(tablename, host='localhost', database='JobAlerts')    
    monconn_recommendations = MongoConnect(tablename, host='172.22.66.198', database='JobAlerts')
    print 'Chunk:', chunkID, 'initiated at:', time.ctime()
    
    myCondition = {'p':chunkID}  
    users = monconn_users_static.loadFromTable(myCondition)
  
    for row in users :
        user_profiletitle = row['user_profiletitle']
        user_industry  = row['user_industry']
        user_functionalarea = row['user_functionalarea']
        user_jobtitle = row['user_jobtitle']
        user_skills   = row['user_skills']
        preferred_subfa = row["preferred_sub_fa"]
        subject_status = row["subject_status"]
        user_experience = row["user_experience"]
        
        apply_data = monconn_applications.loadFromTable({'fcu':row['_id']})
        apply_data_list = list(apply_data)
        application_list = []
        if len(apply_data) == 0:
            pass
        else:
            for element  in apply_data_list:
                application_list.append(element['fjj'])
                
        application_list.sort()
        row['application_list'] = application_list
        application_count = len(application_list)
        row['application_count'] = application_count
        
        if application_count == 0:
            monconn_recommendations.saveToTable(row)
        






if __name__ == '__main__':  

    #########################################################################################################             
    ############-----------------Try Except to provide alert in case of code failure
    #########################################################################################################
  
    try:
        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Job Alert Mailer","Candidate DB Updation Started!!")

        #########################################################################################################             
        ############-----------------  Loading the mappings for bow
        #########################################################################################################
        print "Loading Mapping for BOW"
        synMappingFileName = '/data/Projects/JobAlerts/Features/rawData/LSI/Model_UnifiedTKE/unifiedtkelist.csv'
        keywordIdMappingFileName = '/data/Projects/JobAlerts/Features/rawData/LSI/Model_UnifiedTKE/unifiedtkelist_numbered.csv' 
        mb = MyBOW(synMappingFileName, keywordIdMappingFileName)
    
        
        
        
        #########################################################################################################             
        ############-----------------Creating the connection to candidates_processed_5 and dropping if already exist
        #########################################################################################################
    
        tablename = 'candidates_processed_5'
        #monconn_recommendations = MongoConnect(tablename, host='localhost', database='JobAlerts')
        monconn_recommendations = MongoConnect(tablename, host='172.22.66.198', database='JobAlerts')
        monconn_recommendations.dropTable()
        monconn_recommendations.close()
    
    
    
        #########################################################################################################             
        ############-----------------  Initiating Multiprocessing and computing Recommendations
        ######################################################################################################### 
        print "Starting the Process"      
        pprocessing = 1
    
        if pprocessing == 0:            
            numChunks = 80
            computeAlertsChunk(0)
    
        if pprocessing == 1:                                                    #######---------- Initiating Multiprocessing
            numChunks = 80                                                      #######---------- Define the number of chunks to break data into and number of concurrent threads
            numConcurrentThreads = 6 
            print "numConcurrentThreads:",numConcurrentThreads
            chunkStart = 0
            chunkEnd = chunkStart + numChunks -1
            poolArgList=range(chunkStart, chunkEnd+1)
            print 'chunkStart:', chunkStart, 'chunkEnd:', chunkEnd
            pool=multiprocessing.Pool(numConcurrentThreads)
            pool.map(computeAlertsChunk, poolArgList)
            pool.close()
            pool.join()
    
    
    
        #########################################################################################################             
        ############-----------------  Creating the index on 'p' field in 'candidates_processed_5' collection
        ######################################################################################################### 
        print "Creating the index"
        tablename = 'candidates_processed_5'
        #monconn_recommendations = MongoConnect(tablename, host='localhost', database='JobAlerts')   
        monconn_recommendations = MongoConnect(tablename, host='172.22.66.198', database='JobAlerts')
        monconn_recommendations.doIndexing(tablename,'p') 
        monconn_recommendations.close()
        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Job Alert Mailer","Candidate DB Updation Completed")
    except Exception as e:
        print e
        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com','progressiveht@hindustantimes.com'],"Job Alert Mailer","Candidate DB Updation Failed . Call Akash (+91-8527716555) or Kanika (+91-9560649296) asap.")
