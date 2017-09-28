###############################
#Description: Similar Jobs Suggestion
#Details: Similar Jobs Suggestion Based on common Application and Cosine Similarity
#Last Changed Date: Tue Dec 08 10:10:00 IST 2014
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

todayDate = date.today()
interval1 = 365

previousDate = todayDate + relativedelta(days=-1)
oneYearWindow = todayDate + relativedelta(days=-interval1)
previousYearDate = oneYearWindow + relativedelta(days=-1)

day1 = datetime.combine(todayDate, time(0, 0))
day2 = datetime.combine(oneYearWindow, time(0, 0))

projectHome = '/data/Projects/SimilarJobs'

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

# #MySql Connection
def getMySqlConnection(HOST, PORT, DB_USER, DB_PASSWORD, DB):
    return MySQLdb.connect(host=HOST, port=PORT, user=DB_USER, passwd=DB_PASSWORD, db=DB)

recruiterDB = 'recruiter_master'
similarJob = 'similar_jobs'

mongo_conn1 = getMongoConnection('172.22.66.198', 27017, similarJob)
collectionJS = getattr(mongo_conn1, 'JobSuggestions')
collectionCA = getattr(mongo_conn1, 'CandidateApplication')
collectionJSExport = getattr(mongo_conn1, 'JobSuggestionsExport')

host = '172.16.66.64'
port = 3306
user = 'analytics'
pwd = '@n@lytics'
database = 'SumoPlus'

def getCandidateApplicationSummmary():
    st = datetime.now()
    st1 = datetime.now()
    print "Candidate Application summary start at :" + str(st)
    os.system("mongoexport --csv -d " + recruiterDB + " -c CandidateMatchOneYear -f 'fcu','fjj' -o " + projectHome + "/Input/ApplicationData.csv")
    # exported 41307876 records
    os.system("sed 's/\"//g' " + projectHome + "/Input/ApplicationData.csv > " + projectHome + "/Input/dummy1.csv")
    os.system("tail -n +2 " + projectHome + "/Input/dummy1.csv > " + projectHome + "/Input/ApplicationData.csv")
    os.system("rm " + projectHome + "/Input/dummy1.csv")
    os.system("sort -t, -k1,2 " + projectHome + "/Input/ApplicationData.csv | uniq > " + projectHome + "/Input/ApplicationData_sorted.csv")
    print "Run time for sorted Application Data:" + str(datetime.now() - st1)
    st1 = datetime.now()
    dbconn = getMySqlConnection (host, port, user, pwd, database)
    #query = "select rj.jobid from SumoPlus.recruiter_job rj where rj.jobstatus IN (3,9) and rj.expirydate >= date_sub('" + str(todayDate) + "', interval -7 day) order by rj.jobid;"
    query = "select rj.jobid from SumoPlus.recruiter_job rj where rj.jobstatus IN (3,9) and rj.publisheddate >= date_sub('" + str(todayDate) + "', interval 61 day) and rj.expirydate >= date_sub('" + str(todayDate) + "', interval -7 day) order by rj.jobid;"
    cursor = dbconn.cursor()
    cursor.execute (query)
    rows = cursor.fetchall()
    LiveJobs = [int(j[0]) for j in rows]
    appDF = pd.read_csv( projectHome + '/Input/ApplicationData_sorted.csv', names=['userid', 'jobid'])
    jobDF = pd.DataFrame({'jobid':LiveJobs})
    appDF = pd.merge(appDF, jobDF, on='jobid', how='inner')
    appDF.sort(['userid', 'jobid'], ascending=[True, True], inplace=True)
    appDF.to_csv( projectHome + '/Input/ApplicationData.csv', sep=',', header=False, index=False)
    print "Run time for Live Job Applications:" + str(datetime.now() - st1)
    st1 = datetime.now()
    collectionCA.drop()
    ifile = open( projectHome +'/Input/ApplicationData.csv', 'rb')
    userApps = {}
    for line in ifile:
        data = line.strip().split(',')
        userApps[str(data[0])] = userApps.get(str(data[0]), []) + [int(data[1])]
    ifile.close()
    for item in userApps.keys():
        apps = userApps[item]
        apps.sort()
        collectionCA.insert({'_id':item, 'userApps':apps, 'totalApps' : len(apps)})
    print "Run time for Candidate Apps Collection:" + str(datetime.now() - st1) + " Count: " + str(collectionCA.count())
    print "Total Run time :" + str(datetime.now() - st)  # 1 min 30 sec

def getJobDFs():
    global appDF, jobDF, liveJobDF, LiveJobs
    st = datetime.now()
    dbconn = getMySqlConnection (host, port, user, pwd, database)
    #query = "select rj.jobid from SumoPlus.recruiter_job rj where rj.jobstatus IN (3,9) and rj.expirydate >= date_sub('" + str(todayDate) + "', interval -7 day) order by rj.jobid;"
    #query = "select rj.jobid from SumoPlus.recruiter_job rj where rj.jobstatus IN (3,9) and rj.expirydate >= date_sub('" + str(todayDate) + "', interval -7 day) order by rj.jobid;"
    query = "select rj.jobid from SumoPlus.recruiter_job rj where rj.jobstatus IN (3,9) and rj.publisheddate >= date_sub('" + str(todayDate) + "', interval 61 day) and rj.expirydate >= date_sub('" + str(todayDate) + "', interval -7 day) order by rj.jobid;"
    cursor = dbconn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    LiveJobs = [int(j[0]) for j in rows]
    appDF = pd.read_csv( projectHome + '/Input/ApplicationData_sorted.csv', names=['userId', 'jobId', 'ad'])
    appDF = appDF[['userId', 'jobId']]
    jobDF = appDF['jobId'].unique()
    jobDF.sort()
    jobDF = pd.DataFrame({'jobId':jobDF})
    liveJobDF = list(set(jobDF['jobId']).intersection(set(LiveJobs)))
    liveJobDF.sort()
    liveJobDF = pd.DataFrame({'jobId':liveJobDF})
    print 'Job DF Count:', len(jobDF['jobId'].unique()), 'Live Job DF Count:', len(liveJobDF['jobId'].unique())
    print "Run time :" + str(datetime.now() - st)  # 10 min
    return appDF, jobDF, liveJobDF, LiveJobs
 
def getSJSuggestion(jobPoolLocal):
    global userDF, appDF, jobDF, jobPool, liveJobDF, liveJobDict, LiveJobs, JobUserSparseMatrix, LiveJobUserSparseMatrix
    jobPool = jobPoolLocal
    st = datetime.now()
    userDF = pd.DataFrame({'userId': appDF['userId'].unique()})
    appDF['userLookUp'] = pd.match(appDF['userId'], userDF['userId'])
    appDF['jobLookUp'] = pd.match(appDF['jobId'], jobDF['jobId'])
    appDF['liveJobLookUp'] = pd.match(appDF['jobId'], liveJobDF['jobId'])
    row = appDF['jobLookUp']
    col = appDF['userLookUp']
    data = np.repeat(1,appDF.shape[0])
    JobUserSparseMatrix = sp.coo_matrix((np.array(data), (np.array(row),np.array(col))), shape=(jobDF.shape[0], userDF.shape[0])) # 30L x 5L
    del row, col, data
    JobUserSparseMatrix = JobUserSparseMatrix.tocsr()
    row = appDF[~(appDF.liveJobLookUp == -1)]['liveJobLookUp']
    col = appDF[~(appDF.liveJobLookUp == -1)]['userLookUp']
    data = np.repeat(1,appDF[~(appDF.liveJobLookUp == -1)].shape[0])
    LiveJobUserSparseMatrix = sp.coo_matrix((np.array(data), (np.array(row),np.array(col))), shape=(liveJobDF.shape[0], userDF.shape[0])) # 30L x 1L
    del row, col, data
    LiveJobUserSparseMatrix = LiveJobUserSparseMatrix.tocsr()
    liveJobDict = dict(zip(liveJobDF.index, liveJobDF['jobId']))
    collectionJS.drop()
    collectionJSExport.drop()
    print 'JobSuggestions Count:', collectionJS.count(), 'JobSuggestionsExport Count:', collectionJSExport.count() 
    st1 = datetime.now()
    for jobPosition in range(0, JobUserSparseMatrix.shape[0], jobPool):
        getSJSuggestionPoolWise(jobPosition)
    print datetime.now() - st1
    print 'JobSuggestions Count:', collectionJS.count(), 'JobSuggestionsExport Count:', collectionJSExport.count()
    print "Run time :" + str(datetime.now() - st)  # 10 min

def getSJSuggestionPoolWise(jobPosition):
    global jobDF, jobPool, liveJobDict, JobUserSparseMatrix, LiveJobUserSparseMatrix, JobLiveJobDF_FA_Pool_Mongo
    if jobPosition + jobPool > JobUserSparseMatrix.shape[0]:
        jobPool = JobUserSparseMatrix.shape[0] - jobPosition
    jobDF_Pool = jobDF[jobDF.index.isin(range(jobPosition, jobPosition + jobPool))]
    jobDF_Pool.index = range(0, jobPool)
    jobDict_Pool = dict(zip(jobDF_Pool.index, jobDF_Pool['jobId']))
    del jobDF_Pool
    JobUserSparseMatrix_Pool = JobUserSparseMatrix[range(jobPosition, jobPosition + jobPool),:]
    ## Job - Job based on Common Application
    JobLiveJobSparseMatrix_Pool_CA = JobUserSparseMatrix_Pool * LiveJobUserSparseMatrix.T
    JobLiveJobSparseMatrix_Pool_Cosine = sp.csr_matrix(cosine_similarity(JobUserSparseMatrix_Pool, LiveJobUserSparseMatrix))
    ### JobLiveJobSparseMatrix
    JobLiveJobSparseMatrix_Pool_CA = JobLiveJobSparseMatrix_Pool_CA.tocoo()
    JobLiveJobSparseMatrix_Pool_Cosine = JobLiveJobSparseMatrix_Pool_Cosine.tocoo()
    rowsCA, colsCA, dataCA = JobLiveJobSparseMatrix_Pool_CA.row, JobLiveJobSparseMatrix_Pool_CA.col, JobLiveJobSparseMatrix_Pool_CA.data
    rowsCS, colsCS, dataCS = JobLiveJobSparseMatrix_Pool_Cosine.row, JobLiveJobSparseMatrix_Pool_Cosine.col, JobLiveJobSparseMatrix_Pool_Cosine.data
    JobLiveJobDF_Pool_CA = pd.DataFrame({'jobId': rowsCA, 'liveJobId': colsCA, 'cA': dataCA})
    JobLiveJobDF_Pool_CA = JobLiveJobDF_Pool_CA[JobLiveJobDF_Pool_CA['cA'] >=5]
    JobLiveJobDF_Pool_CS = pd.DataFrame({'jobId': rowsCS, 'liveJobId': colsCS, 'cosSim': dataCS})
    del rowsCA, colsCA, dataCA, rowsCS, colsCS, dataCS
    JobLiveJobDF_Pool = pd.merge(JobLiveJobDF_Pool_CA, JobLiveJobDF_Pool_CS,  on = ['jobId', 'liveJobId'], how = 'inner')
    del JobLiveJobDF_Pool_CA, JobLiveJobDF_Pool_CS
    JobLiveJobDF_Pool['cosSim'] = 1000 * JobLiveJobDF_Pool['cosSim']
    JobLiveJobDF_Pool['cosSim'] = JobLiveJobDF_Pool['cosSim'].apply(lambda x: round(x))
    JobLiveJobDF_Pool['cosSim'] = JobLiveJobDF_Pool['cosSim'].astype('int64') 
    JobLiveJobDF_Pool['jobId'] = JobLiveJobDF_Pool['jobId'].apply(lambda x: jobDict_Pool[x])
    JobLiveJobDF_Pool['liveJobId'] = JobLiveJobDF_Pool['liveJobId'].apply(lambda x: liveJobDict[x])
    JobLiveJobDF_Pool = JobLiveJobDF_Pool.sort(['jobId', 'cosSim'], ascending = [True, False])
    JobLiveJobDF_Pool = JobLiveJobDF_Pool[['jobId','liveJobId','cA','cosSim']]
    JobLiveJobDF_FA_Pool_Mongo = {}
    for row in JobLiveJobDF_Pool.values:
        JobLiveJobDF_FA_Pool_Mongo[row[0]] = JobLiveJobDF_FA_Pool_Mongo.get(int(row[0]), []) + [[int(row[1]), int(row[2]), float(row[3])]]
    del JobLiveJobDF_Pool, row
    pool = Pool(processes=6)
    try:
        pool.map(startSimilarJobProcessing, JobLiveJobDF_FA_Pool_Mongo.keys())
        pool.close()
    except:
        traceback.print_exc()
        pool.close()
    del JobLiveJobDF_FA_Pool_Mongo
    if jobPosition % 50000 == 0:
        print jobPosition, datetime.now()

def startSimilarJobProcessing(jobId):
    global JobLiveJobDF_FA_Pool_Mongo, LiveJobs
    simJobList = JobLiveJobDF_FA_Pool_Mongo[jobId]
    collectionJS.insert({'_id':jobId, 'sjlen': len(simJobList), 'lud':day1, 'sj': simJobList})
    simJobListExport = [job[0] for job in simJobList if job[0] != jobId]
    if (jobId in LiveJobs) and (len(simJobListExport) > 0):
        collectionJSExport.insert({'_id':jobId, 'sjlen': len(simJobListExport), 'lud':day1, 'sj': simJobListExport})

def BackupandJobSuggDump():
    st = datetime.now()
    print "Similar Jobs Mongo Dump start at :" + str(st)
    if (todayDate.weekday() == 0):
        backUpRoot = projectHome + '/backup'
        db_name = similarJob
        os.system("mongodump --db "+db_name+" --out "+backUpRoot+"/Similar_Jobs_Backups")
        os.system("tar -cvf "+backUpRoot+"/Similar_Jobs_Backups/similar_jobs_"+str(todayDate)+".tar.gz "+backUpRoot+"/Similar_Jobs_Backups/similar_jobs")
        os.system("rm -rf "+backUpRoot+"/Similar_Jobs_Backups/similar_jobs")
        print 'Backup Successfull'
    else:
        print 'Backup Skipped as not a Monday'
    print "Similar Jobs Backup completed at :" + str(datetime.now())
    print "Run time :" +  str(datetime.now() - st)

def main():
    st = datetime.now()
    print 'Start at Time :', str(st)
    print day1
    print day2
    getCandidateApplicationSummmary()
    print 'Candidate Application in Sorted order at Time : ' + str(datetime.now())
    getJobDFs()
    print 'Job DFs at Time : ' + str(datetime.now())
    getSJSuggestion(10000)
    print 'Similar Jobs generated at Time : ' + str(datetime.now())
    BackupandJobSuggDump()
    print 'BackupandJobDigestDump creation at time: '+str(datetime.now())
    open(projectHome + '/Docs/sj_updated_till.txt','a').write(str(todayDate)+'\n')
    print 'Total run time :', str(datetime.now() - st)

if __name__=='__main__':
    main()

