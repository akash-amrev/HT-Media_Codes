#!/usr/bin/python
__author__="Ashish.Jain"
__date__ ="26 Oct 2016"


#########################################################################################################             
############-----------------  Summary of Script
#########################################################################################################
#--> Script to shoot an alert mailer in case anuy number (jobs,tech dump of jobs, Recommendations) is lower 
#    than the expected limit 



#########################################################################################################             
############-----------------Importing Python Libraries and Modules
#########################################################################################################
#Inbuilt Modules
import sys
sys.path.append('./../')
from pprint import pprint
import pdb
from gensim import corpora, models, similarities
import csv
import time
from multiprocessing import Pool
import random
import heapq
import gensim
import multiprocessing
from rfc3339 import rfc3339
import datetime
import os

#Custom Modules
from Features.CTCMatchScore.CTCMatchScore import CTCMatchScore      #Custom Module - /data/Projects/JobAlerts/Features/CTCMatchScore/CTCMatchScore.py
from Features.ExpMatchScore.ExpMatchScore_v1 import ExpMatchScore   #Custom Module - /data/Projects/JobAlerts/Features/ExpMatchScore/ExpMatchScore_v1.py
from Features.PaidBoostScore.PaidBoostScore import PaidBoostScore   #Custom Module - /data/Projects/JobAlerts/Features/PaidBoostScore/PaidBoostScore.py
from Features.CityScore.getCityScore import CityMatch               #Custom Module - /data/Projects/JobAlerts/Features/CityScore/getCityScore.py
from Features.LSI_common.MyBOW import MyBOW                         #Custom Module - /data/Projects/JobAlerts/Features/LSI_common/MyBOW.py
from Model.getOverallMatchScore import getOverallMatchScore         #Custom Module - /data/Projects/JobAlerts/Model/getOverallMatchScore.py
from Main.getLSICosine import getLSICosine                          #Custom Module - /data/Projects/JobAlerts/Main/getLSICosine.py
from DataConnections.MySQLConnect.MySQLConnect import MySQLConnect  #Custom Module - /data/Projects/JobAlerts/DataConnections/MySQLConnect/MySQLConnect.py
from DataConnections.MongoConnect.MongoConnect import MongoConnect  #Custom Module - /data/Projects/JobAlerts/DataConnections/MongoConnect/MongoConnect.py
from Utils.Utils_1 import cleanToken                                #Custom Module - /data/Projects/JobAlerts/Utils/Utils_1.py
from Utils.HtmlCleaner import HTMLStripper                          #Custom Module - /data/Projects/JobAlerts/Utils/HtmlCleaner.py
from Utils.Cleaning import *                                        #Custom Module - /data/Projects/JobAlerts/Utils/Cleaning.py
from Notifier.Notifier import send_email                            #Custom Module - /data/Projects/JobAlerts/Notifier/Notifier.py




#########################################################################################################             
############-----------------    Creating a Mongo Connection to Jobs Database
#########################################################################################################
tableName = 'jobs_processed'
monconn_jobs_local = MongoConnect(tableName, host = '172.22.66.198', database = 'JobAlerts')
monconn_jobs_local_cur = monconn_jobs_local.getCursor()
jobs_processed_count = monconn_jobs_local_cur.count()
del(monconn_jobs_local)


#########################################################################################################             
############-----------------    Creating a Mongo Connection to Tech dump of Jobs
#########################################################################################################
tableName = 'JobDesc_analytics'
monconn_jobs_local_1 = MongoConnect(tableName, host = '172.22.66.198', database = 'JobDescDB_analytics')
#monconn_jobs_local_1 = MongoConnect(tableName, host = 'localhost', database = 'JobDescDB_analytics')
monconn_jobs_local_cur_1 = monconn_jobs_local_1.getCursor()
jobs_processed_tech_dump_count = monconn_jobs_local_cur_1.count()
del(monconn_jobs_local_1)



#########################################################################################################             
############-----------------    Creating a Mongo Connection to Candidates used for JAM Computaion
#########################################################################################################
tableName = 'candidates_processed_5'
monconn_candidates = MongoConnect(tableName, host = '172.22.66.198', database = 'JobAlerts')
monconn_candidates_cur = monconn_candidates.getCursor()
candidates_count = monconn_candidates_cur.count()
del(monconn_candidates)



#########################################################################################################             
############-----------------    Creating a Mongo Connection to Recommendations collection
#########################################################################################################
tablename = 'DailyMsgQueue'
monconn_recommendations = MongoConnect(tablename, host='172.22.66.198', database='mailer_daily_analytics')
monconn_recommendations_cur = monconn_recommendations.getCursor()
recommendations_count = monconn_recommendations_cur.count()
del(monconn_recommendations)



#########################################################################################################             
############-----------------    Printing all the numbers obtained
#########################################################################################################
print "jobs_processed_count",jobs_processed_count
print "jobs_processed_tech_dump_count",jobs_processed_tech_dump_count
print "candidates_count",candidates_count
print "recommendations_count",recommendations_count



#########################################################################################################             
############-----------------    Alert Mailer in case any number in smaller than permissible limit
#########################################################################################################

if jobs_processed_count > jobs_processed_tech_dump_count or jobs_processed_count <3000 :
    send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com','progressiveht@hindustantimes.com'],"Urgent!!!","Job Alert Mailer : Jobs Count is Less!!!!!\nCall Akash (+91-8527716555) or Kanika (+91-9560649296) asap.")
else:
    pass

if recommendations_count < 2500000 :
    send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com','progressiveht@hindustantimes.com'],"Urgent!!!","Job Alert Mailer : Recommendations Count is Less!!!!!\nCall Akash (+91-8527716555) or Kanika (+91-9560649296) asap.")
else:
    text = 'Count of jobs for Processing : ' +str(jobs_processed_count) +'\nCount of jobs for Tech Dump : '+str(jobs_processed_tech_dump_count)+'\nCount of candidates for JAM : '+str(candidates_count)+ '\nCount of Recommendations  : '+str(recommendations_count)    
    send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Job Alert Mailer",text)


