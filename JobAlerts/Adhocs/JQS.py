import re
import csv
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



##mongo_conn = getMongoConnection('172.22.66.233', 27017, 'JobAlerts', True)
#db = getattr(mongo_conn, 'candidate_applications')

#monconn_users_static = MongoConnect('candidate_applications', host = 'localhost', port = 27018, database = 'JobAlerts', username = username, password = password, authenticate = True)
tablename = "candidate_applications"
monconn_users = MongoConnect(tablename, host='localhost', database='JobAlerts')






ifile = open('/data/Projects/JobAlerts/preProcessing/JQS_JobsData.csv','r')
reader = csv.reader(ifile)
reader.next()

ofile = open('JQS_Results.csv','w')
writer = csv.writer(ofile,lineterminator = '\n')
writer.writerow(['JobId','JobTitle','JobSkills','HiringFor','JT_url','JT_email','JT_phone','JT_len','JD_url','JD_email','JD_phone','JD_len','Skills Count','HF_url','HF_email','HF_phone','JobTitleScore','JobDescriptionScore','HiringForScore','SkillScore','TotalScore','PublishedDate','DateDifference','Applications'])


for row in reader:
    jobid = row[0]
    jt = row[1]
    jd = row[2]
    jd_clean = row[3]
    skills = row[4]
    hf = row[5]
    pub_date = row[6]
    date_diff = row[7]
    
    
    applications = len(monconn_users.loadFromTable({'fjj':int(jobid)}))
    
    


    jt_score = 0
    jd_score = 0
    hf_score = 0
    skill_score = 0

    #JT Score
    jt_phone = 0
    jt_url = len(re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', jt))
    jt_email = len(re.findall(r'[\w\.-]+@[\w\.-]+', jt))
    jt_numbers = re.findall('\d+', jt)
    for number in jt_numbers:
        if len(number) >= 10:
            jt_phone = 1
        else:
            pass
    jt_len = len(jt)

    if len(jt) < 10 or jt_url >0 :
        jt_score = 0
    elif jt_email > 0 or jt_phone > 0:
        jt_score = 1
    elif len(jt) > 30:
        jt_score = 2
    else:
        jt_score = 3

    #JD Score
    jd_phone = 0
    jd_url = len(re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', jd))
    jd_email = len(re.findall(r'[\w\.-]+@[\w\.-]+', jd))
    jd_numbers = re.findall('\d+', jd)

    for number in jd_numbers:
        if len(number)>= 10:
            jd_phone =1
        else:
            pass
    jd_len = len(jd_clean)
    
    if jd_len < 50 or jd_url > 0:
        jd_score = 0
    elif jd_phone > 0 or jd_email > 0:
        jd_score = 1
    elif jd_len > 1500:
        jd_score = 2
    else:
        jd_score = 3
        
    #print jd_phone
    #Skill Score
    skill_len = len(skills.split(','))
    if skill_len == 0:
        skill_score = 0
    elif skill_len <= 2 :
        skill_score = 1
    elif skill_len <=4:
        skill_score = 2
    else:
        skill_score = 3





    #Hiring for score
    hf_phone = 0
    hf_url = len(re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', hf))
    hf_email = len(re.findall(r'[\w\.-]+@[\w\.-]+', hf))
    hf_numbers = re.findall('\d+', hf)
    for number in hf_numbers:
        if len(number) >= 10:
            hf_phone = 1
        else:
            pass

    if hf_url > 0 or hf_email > 0 or hf_phone > 0:
        hf_score = 0
    else:
        hf_score = 1

    total = hf_score + jd_score + jt_score + skill_score
    writer.writerow([jobid,jt,skills,hf,jt_url,jt_email,jt_phone,jt_len,jd_url,jd_email,jd_phone,jd_len,skill_len,hf_url,hf_email,hf_phone,jt_score,jd_score,hf_score,skill_score,total,pub_date,date_diff,applications])
