###############################
#Description: Similar Jobs Digest Archieve
#Details: Similar Jobs Digest related to Unique Applicants in last 1 month
#Last Changed Date: Tue Dec 25 23:00:04 IST 2014
###############################

import poplib, sys, datetime, email, math, operator, copy, itertools, random
import os, re, string, ftplib, traceback, pymongo, MySQLdb, subprocess, smtplib, socket
import multiprocessing
from pymongo import MongoClient, ReadPreference
from datetime import date, datetime, time, timedelta
from dateutil.relativedelta import *
from bson.objectid import ObjectId
from multiprocessing import Pool, Lock
from multiprocessing.sharedctypes import Value, Array
import pandas as pd
import scipy.sparse as sp
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from __builtin__ import range
from string import join
from logging import exception

todayDate=date.today()

projectHome = '/data/Projects/JobDigest'

day1 = datetime.combine(todayDate, time(0, 0))

# #Mongo Connection
def getMongoConnection(MONGO_HOST, MONGO_PORT, DB, isAuth=False, username='analytics', password='aN*lyt!cs@321'):
    if isAuth:
        connection = MongoClient(MONGO_HOST, MONGO_PORT, read_preference=ReadPreference.SECONDARY)
        mongo_conn = connection[DB]
        mongo_conn.authenticate(username, password)
    else:
        connection = MongoClient(MONGO_HOST, MONGO_PORT, read_preference=ReadPreference.SECONDARY)
        mongo_conn = connection[DB]
    return mongo_conn

##MySql Connection
def getMySqlConnection(HOST, PORT, DB_USER, DB_PASSWORD, DB):
    return MySQLdb.connect(host = HOST,port = PORT,user =DB_USER,passwd =DB_PASSWORD,db = DB)

jobDigestArchieveDB = 'job_digest_archieve'
jobDigestMailerDB = 'similar_job_mailer'
similarCandMailerDB = 'similar_profile_mailer'

mongo_conn1 = getMongoConnection('172.22.66.198', 27017, jobDigestArchieveDB)
collectionSJD_Archieve = getattr(mongo_conn1, 'SimilarJobDigestArchieve')

mongo_conn2 = getMongoConnection('172.22.66.198', 27017, jobDigestMailerDB)
collectionSJDActive = getattr(mongo_conn2, 'SimilarJobDigestActive')
collectionSJDActiveWednesday = getattr(mongo_conn2, 'SimilarJobDigestActiveWednesday')
collectionSJDOutstanding = getattr(mongo_conn2, 'SimilarJobDigestOutstanding')
collectionSJDInActive = getattr(mongo_conn2, 'SimilarJobDigestInActive')
#collectionSJDRevival = getattr(mongo_conn2, 'SimilarJobDigestRevival')

mongo_conn3 = getMongoConnection('172.22.66.198', 27017, similarCandMailerDB)
collectionSPDActiveMonday = getattr(mongo_conn3, 'SimilarProfileDigestActiveMonday')
collectionSPDActive = getattr(mongo_conn3, 'SimilarProfileDigestActive')
collectionSPDInActive = getattr(mongo_conn3, 'SimilarProfileDigestInActive')
collectionSPDRevival = getattr(mongo_conn3, 'SimilarProfileDigestRevival')

def updateArchieveDigest(JDCursor): 
    for jobDigest in JDCursor:
        archieveDigest = collectionSJD_Archieve.find({'_id':jobDigest['_id']}).distinct('sjdArc')
        archieveDigest.extend(jobDigest['sjd'])
        collectionSJD_Archieve.update({'_id':jobDigest['_id']}, {'$set':{'sjdArc':archieveDigest, 'lud':str(todayDate)}}, **{'upsert':True})

def updateCollectionSJDActive():
    st = datetime.now()
    JDCursor = collectionSJDActive.find(timeout=False)
    updateArchieveDigest(JDCursor)
    JDCursor = collectionSJDActiveWednesday.find(timeout=False)
    updateArchieveDigest(JDCursor)
    print 'SJDActive Archieve length for SJD : ', collectionSJD_Archieve.count(), str(datetime.now()-st)

def updateCollectionSJDOutstanding():
    st = datetime.now()
    JDCursor = collectionSJDOutstanding.find(timeout=False)
    updateArchieveDigest(JDCursor)
    print 'SJDOutstanding Archieve length for SJD : ', collectionSJD_Archieve.count(), str(datetime.now()-st)

def updateCollectionSJDInActive():
    st = datetime.now()
    JDCursor = collectionSJDInActive.find(timeout=False)
    updateArchieveDigest(JDCursor)
    print 'SJDInActive Archieve length for SJD : ', collectionSJD_Archieve.count(), str(datetime.now()-st)

def updateCollectionSPDActive():
    st = datetime.now()
    JDCursor = collectionSPDActiveMonday.find(timeout=False)
    updateArchieveDigest(JDCursor)
    JDCursor = collectionSPDActive.find(timeout=False)
    updateArchieveDigest(JDCursor)
    print 'SPDActive Archieve length for SPD : ', collectionSJD_Archieve.count(), str(datetime.now()-st)

def updateCollectionSPDInActive():
    st = datetime.now()
    JDCursor = collectionSPDInActive.find(timeout=False)
    updateArchieveDigest(JDCursor)
    print 'SPDInActive Archieve length for SPD : ', collectionSJD_Archieve.count(), str(datetime.now()-st)

def updateCollectionSPDRevival():
    st = datetime.now()
    JDCursor = collectionSPDRevival.find(timeout=False)
    updateArchieveDigest(JDCursor)
    print 'SPDRevival Archieve length for SPD : ', collectionSJD_Archieve.count(), str(datetime.now()-st)

### Remove jobs that are not in Live Jobs
def RemoveExpiredJobsFromArchieve():
    st = datetime.now()
    mysql_conn = getMySqlConnection('172.16.66.64', 3306, 'analytics', '@n@lytics', 'SumoPlus')
    cursor = mysql_conn.cursor()
    query = "select rj.jobid from SumoPlus.recruiter_job rj where rj.jobstatus IN (3,9);"
    cursor.execute(query)
    rows = cursor.fetchall()
    LiveJobs = []
    for row in rows:
        LiveJobs.append(int(row[0]))
    print 'Live Jobs :', len(LiveJobs)
    LiveJobs = set(LiveJobs)
    JDArcCursor = collectionSJD_Archieve.find({})
    c = 0
    ArcRemoveList = []
    updations = 0
    for jdArc in JDArcCursor:
        c += 1
        app = jdArc['_id']
        sjdArcList = jdArc['sjdArc']
        sjdArcListNew = list(set(sjdArcList).intersection(LiveJobs))
        if len(sjdArcListNew) < len(sjdArcList):
            if len(sjdArcListNew) == 0:
                ArcRemoveList.append(app)
            else:
                updations +=1
                collectionSJD_Archieve.update({'_id':app}, {'$set':{'sjdArc':sjdArcListNew}})
        if c%1000000 == 0:        print c, datetime.now()
    print len(ArcRemoveList), updations
    print datetime.now()
    collectionSJD_Archieve.remove({'_id':{'$in':ArcRemoveList}})
    print 'Similar Job Digest Archieve length after Expired Jobs Removal :', collectionSJD_Archieve.count(), str(datetime.now()-st)

def BackupandJobSuggDump():
    # Daily Backup for job_digest db
    backUpRoot = projectHome + '/backup'
    db_name = jobDigestArchieveDB
    # mongo dump for job_digest
    os.system("mongodump -h 172.22.66.198 --collection SimilarJobDigestArchieve --db "+db_name+" --out "+backUpRoot+"/JobDigestArchieve")
    os.system("tar -cvf "+backUpRoot+"/JobDigestArchieve/"+db_name+"_"+str(todayDate)+".tar.gz "+backUpRoot+"/JobDigestArchieve/"+db_name)
    os.system("rm -rf "+backUpRoot+"/JobDigestArchieve/"+db_name)

def main():
    st = datetime.now()
    print 'Start at Time : ' + str(datetime.now())
    print day1
    updateCollectionSJDActive()
    print 'SJDActive update Archieve at Time : ' + str(datetime.now())
    updateCollectionSJDOutstanding()
    print 'SJDOutstanding update Archieve at Time : ' + str(datetime.now())
    updateCollectionSJDInActive()
    print 'SJDInActive update Archieve at Time : ' + str(datetime.now())
    updateCollectionSPDActive()
    print 'SPDActive update Archieve at Time : ' + str(datetime.now())
    updateCollectionSPDInActive()
    print 'SPDInActive update Archieve at Time : ' + str(datetime.now())
    updateCollectionSPDRevival()
    print 'SPDRevival update Archieve at Time : ' + str(datetime.now())
    RemoveExpiredJobsFromArchieve()
    print 'Expired Jobs removal at Time : ' + str(datetime.now())
    BackupandJobSuggDump()
    print 'BackupandJobDigestDump creation at time: '+str(datetime.now())
    open(projectHome + '/Docs/archieve_updated_till.txt','a').write(str(todayDate)+'\n')
    print 'Total run time :', str(datetime.now() - st)

if __name__=='__main__':
    main()

#db.copyDatabase("similar_jobs","similar_jobs_26Apr")
#cp /Projects/JobDigest/backup/JobDigestArchieve/job_digest_2014-03-06.tar.gz /Projects/JobDigest/backup/JobDigestArchieve/job_digest_2014-03-06.tar
#cd /Projects/JobDigest/backup/JobDigestArchieve/
#tar -xvf /Projects/JobDigest/backup/JobDigestArchieve/job_digest_2014-03-06.tar
#mongorestore --db job_digest_test /Projects/JobDigest/backup/JobDigestArchieve/Projects/JobDigest/backup/JobDigestArchieve/job_digest/SimilarJobDigestArchieve.bson

