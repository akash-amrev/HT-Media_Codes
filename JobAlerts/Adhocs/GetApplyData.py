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
import csv
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

print "Connecting to CandidateMatch !!!"
mongo_conn = getMongoConnection('172.22.65.58', 27017, 'recruiter_master', True)
db = getattr(mongo_conn, 'recruiter_master')
print 'Connecting to CandidateMatch...finished'

job_list = []
job_to_company = {}
jobs = []
#ifile = open('non_bo_jobs_3months.csv','r')
ifile = open('recruiter_data_for_candidates.csv','r')
reader = csv.reader(ifile)
reader.next()
company = []
company_to_posted_date = {}
candidate_id = []
for row in reader:
    #job_to_company[int(row[0])] = int(row[1])
    #jobs.append(int(row[0]))
    #company.append(int(row[1]))
    candidate_id.append(str(row[0]))
    #company_to_posted_date[int(row[0])] = row[2]

candidate_uniq = list(set(candidate_id))
#company_uniq = list(set(company))
print "Number of companies",len(candidate_uniq)

ofile = open('RecrActivity_data.csv','w')
writer = csv.writer(ofile,lineterminator = '\n')
writer.writerow(["CompanyId","UserId","Type","JobId"])
#writer.writerow(["JobId","UserId"])
i = 0

for id in candidate_uniq:
    i += 1 
    if i% 100 == 0:
        print "records processed",i
    #print id
    activity = db.Activity.find({'c':id})
    for data in activity:
        fcu = data.get('c',None)
        type = data.get('t',None)
        company = data.get('ci',None)
        try:
            jobid = data['rvc']['wj']['j']
            #posted_date = company_to_posted_date[int(jobid)]
        except:
            jobid = ''
        #print fcu , type , company , jobid 
        writer.writerow([company,fcu,type,jobid])

"""        
for id in jobs:
    if len(job_list) == 1000:
        apply_data = db.CandidateMatch.find({'fjj':{'$in':job_list}})    
        apply_data_list = list(apply_data)
        print len(apply_data_list)
        for apply in apply_data_list:
            writer.writerow([apply['fjj'],apply['fcu']])
        
        
        i = 0
        for data in apply_data_list:
            i+=1
            if i%10 == 0:
                print i
            fjj = data['fjj']
            fcu = data['fcu']
            cid = job_to_company[fjj]
            
            activity = db.Activity.find({'c':fcu,'ci':cid},{'t':1,'rvc':1})
            if len(list(activity)) == 0:
                writer.writerow([fjj,fcu,None])
            else:            
                for element in activity:
                    type = element.get('t',None)
                    company_job = element['rvc']['wj']['j']
                    print type,company_job,fjj,fcu
                    sys.exit(0)
                    writer.writerow([fjj,fcu,type])

        job_list = []
    else:
        job_list.append(id)
"""        
    