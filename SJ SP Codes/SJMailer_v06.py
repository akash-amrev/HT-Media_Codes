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
import math
import pdb
from operator import itemgetter

todayDate = date.today()
#Date10 = todayDate + relativedelta(days=-5)

interval = 365 
oneYearWindow = todayDate + relativedelta(days=-interval)

day1 = datetime.combine(todayDate, time(0, 0))
day2 = datetime.combine(oneYearWindow, time(0, 0))

projectHome = '/data/Projects/JobDigest'
projectHomeSJ = '/data/Projects/SimilarJobs'

host = '172.16.66.64'
port = 3306
user = 'analytics'
pwd = '@n@lytics'
database = 'SumoPlus'

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

initialdigestcutoff = 100
digestcutoff = 45

similarJob = 'similar_jobs'
jobDigestDB = 'job_digest_test'
jobDigestArchieveDB = 'job_digest_archieve'
candidateDB = 'sumoplus'

mongo_conn1 = getMongoConnection('172.22.66.198', 27017, similarJob)
collectionJS = getattr(mongo_conn1, 'JobSuggestions')
collectionCA = getattr(mongo_conn1, 'CandidateApplication')

mongo_conn2 = getMongoConnection('172.22.66.198', 27017, jobDigestArchieveDB)
collectionSJDArchieve = getattr(mongo_conn2, 'SimilarJobDigestArchieve')

mongo_conn3 = getMongoConnection('172.22.66.198', 27017, jobDigestDB)
collectionCJS = getattr(mongo_conn3, 'CandJobSuggestion')
collectionSJD = getattr(mongo_conn3, 'SimilarJobDigest')

mongo_conn4 = getMongoConnection('172.22.65.157', 27018, candidateDB, True)
collectionCS = getattr(mongo_conn4, 'CandidateStatic')

def executeMySQLQuery(query):
    host1 = 'localhost'
    port1 = 3306
    user1 = 'root'
    pwd1 = 'mysql@123'
    database1 = 'SimilarProfiles'
    dbconn = getMySqlConnection (host1, port1, user1, pwd1, database1)
    cursor = dbconn.cursor()
    cursor.execute(query)
    rs = cursor.fetchall()
    dbconn.close()
    return rs

def getSimilarJobMatrix():
    #global jobDF, SJSparseMatrix, jobLookUp, liveJobIdDict
    ###################################
    global jobDF, SJSparseMatrix, jobLookUp, liveJobIdDict, liveJobDF
    ###################################
    st = datetime.now()
    SimilarJobsCur = collectionJS.find()
    count = 0
    SJDict = {}
    liveJobList = []
    for cur in SimilarJobsCur:
        SJDict[cur['_id']] = {}
        for item in cur['sj']:
            if int(item[2]) >= 50: # Mean of Similar Jobs is 50 and Median is 31
                SJDict[cur['_id']][item[0]] = int(item[2])
            liveJobList.append(item[0])
        for item in cur['sj']:
            SJDict[cur['_id']][item[0]] = int(item[2])
            liveJobList.append(item[0])
        count+=1
        if count%50000 == 0:
            print count, datetime.now()
    del SimilarJobsCur, count
    liveJobList = list(set(liveJobList))
    liveJobList.sort()
    uniqueJobs = []
    for item in SJDict:
        uniqueJobs.append(item)
        uniqueJobs += SJDict[item].keys()
    len(uniqueJobs)
    len(set(uniqueJobs))
    uniqueJobsList = list(set(uniqueJobs))
    uniqueJobsList.sort()
    jobDF = pd.DataFrame({'job': uniqueJobsList})
    liveJobDF = pd.DataFrame({'job': liveJobList})
    del uniqueJobsList, liveJobList, uniqueJobs
    jobLookUp = pd.Series(jobDF.index, index = jobDF['job']).to_dict()
    jobIdDict = pd.Series(jobDF['job'], index = jobDF.index).to_dict()
    liveJobLookUp = pd.Series(liveJobDF.index, index = liveJobDF['job']).to_dict()
    liveJobIdDict = pd.Series(liveJobDF['job'], index = liveJobDF.index).to_dict()
    data = []
    row = []
    col = []
    count = 0
    for job in SJDict.keys():
        jobLookUpVal = jobLookUp[job]
        dummySJDict = {}
        dummySJDict = SJDict[job]
        count+=1
        for sj in dummySJDict.keys():
            row.append(jobLookUpVal)
            col.append(liveJobLookUp[sj])
            data.append(dummySJDict[sj])
        if count%50000 ==0:
            print str(count) + " " + str(datetime.now())
    SJSparseMatrix = sp.coo_matrix((np.array(data), (np.array(row),np.array(col))), shape=(jobDF.shape[0],liveJobDF.shape[0]))
    SJSparseMatrix = SJSparseMatrix.astype('int32')
    SJSparseMatrix
    #del row, col, data, count, dummySJDict, SJDict, liveJobDF
    ##############################
    del row, col, data, count, dummySJDict, SJDict
    ##############################
    print "Similar Jobs Matrix generated at: " + str(datetime.now())
    print "Run time :" +  str(datetime.now() - st)

def getUserArchieveDF():
    global userArchieveDF
    st = datetime.now()
    os.system("mongoexport --csv -d " + jobDigestArchieveDB + " -c SimilarJobDigestArchieve -f '_id' -o " + projectHome + "/Input/Mailer/ArchivedCandidateList.csv")
    userArchieveDF = pd.read_csv( projectHome + '/Input/Mailer/ArchivedCandidateList.csv')
    userArchieveDF.columns = ['userId']
    print "Job Archieves for Users generated at: " + str(datetime.now())
    print "Run time :" +  str(datetime.now() - st)

def getUserAppsDF():
    global jobDF, userDF, userAppsDF, userAppsDict, UserAppsSparseMatrix
    userAppsDF = pd.read_csv(projectHomeSJ + "/Input/ApplicationData_sorted.csv", names=['userId', 'jobId'])
    userAppsDF = userAppsDF[userAppsDF.jobId.isin(jobDF.job)]
    userDF = pd.DataFrame({'userId':userAppsDF.userId.unique()})
    userAppsDF['userLookUp'] = pd.match(userAppsDF['userId'], userDF['userId'])
    userAppsDF['jobLookUp'] = pd.match(userAppsDF['jobId'], jobDF['job'])
    userAppsDF_1 = pd.DataFrame(list(collectionCA.find({},{'_id':1,'userApps':1})))
    userAppsDict = dict(zip(userAppsDF_1['_id'], userAppsDF_1['userApps']))
    del userAppsDF_1
    UserAppsSparseMatrix = sp.coo_matrix((np.repeat(1, userAppsDF.shape[0]), (userAppsDF['userLookUp'],userAppsDF['jobLookUp'])), shape=(userDF.shape[0], jobDF.shape[0]))
    UserAppsSparseMatrix = UserAppsSparseMatrix.tocsr()

def getUserArchieveDF_Pool(userDF_Pool):
    global userArchieveDF, userAppsDF
    userAppsDF_dummy = userAppsDF[['userId','jobId']]
    userAppsDF_dummy.columns = ['userId','job']
    userArchieveList = list(set(userArchieveDF['userId']).intersection(set(userDF_Pool['userId'])))
    userArchieveList.sort()
    len(userArchieveList)
    ofile = open(projectHome + "/Input/Mailer/ArchivedUsersJobList.csv", 'wb')
    dummyPool = 10000
    if dummyPool > userDF_Pool.shape[0]:
        dummyPool = userDF_Pool.shape[0]
    for dummyPos in range(0, len(userArchieveList), dummyPool):
        userArchieveCur = list(collectionSJDArchieve.find({'_id':{'$in': userArchieveList[dummyPos: dummyPos + dummyPool]}},{'_id':1,'sjdArc':1}))  
        for itemCur in userArchieveCur:
            for job in itemCur['sjdArc']:
                ofile.write(str(itemCur['_id']) + "," + str(job)+ "\n")
    ofile.close()
    userArchieveDF_Pool = pd.read_csv(projectHome + "/Input/Mailer/ArchivedUsersJobList.csv", names = ['userId', 'job'])
    userArchieveDF_Pool['job'] = userArchieveDF_Pool['job'].astype('int32')
    userArchieveDF_Pool = userArchieveDF_Pool.append(userAppsDF_dummy[userAppsDF_dummy['userId'].isin(userArchieveList)])
    try:
        del userArchieveCur, userArchieveList, dummyPos, dummyPool
    except:
        pass
    return userArchieveDF_Pool

#######################################
def getJobRecencyScore():
    st = datetime.now()
    global liveJobDF, jobRecencyDF
    uniqueU1String = str(list(liveJobDF['job'])).strip("[]")
    dbconn = getMySqlConnection (host, port, user, pwd, database)
    query = "select a.jobid, a.publisheddate, case b.account_type when 0 then 'Company' when 1 then 'Consultant' when 2 then 'Others' else 'Not Specified' end "
    query += "as account_type, if(xy.enabled = 1 and xy.price != 0 and xy.expiry_date > curdate(),'Paid','Free') AS 'Free/Paid', IFNULL(a.maxexperience,30) as maxexperience "
    query += "from SumoPlus.recruiter_job a left join SumoPlus.backoffice_companyaccount b on a.companyid_id = b.id "
    query += "left join (select company_account_id, sum(final_sale_price) as price, enabled, max(expiry_date) as expiry_date from SumoPlus.backoffice_accountsales a1 "
    query += "where enabled in (select min(enabled) from SumoPlus.backoffice_accountsales where a1.company_account_id = company_account_id) group by 1) as xy "
    query += "on b.id = xy.company_account_id where a.jobid in ("+uniqueU1String+") group by 1 order by a.jobid;"
    cursor = dbconn.cursor()
    cursor.execute (query)
    rows = cursor.fetchall()
    live_jobs = [(int(row[0]),str(row[1]),str(row[2]),str(row[3]),int(row[4])) for row in rows]
    jobRecencyDF = pd.DataFrame(live_jobs,columns=['job','pubdate','acc_type','free_paid','maxexperience'])
    jobRecencyDF['jobtype'] = jobRecencyDF['free_paid'] + ' ' + jobRecencyDF['acc_type']
    jobRecencyDF['pubdate'] = jobRecencyDF['pubdate'].apply(lambda x: max(0,min(1,(datetime.strptime(x,"%Y-%m-%d %H:%M:%S").date()-todayDate).days)))
    #jobRecencyDF = jobRecencyDF.drop('pubdate',1)
    print "Job Recency Scores calculated at: " + str(datetime.now())
    print "Run time :" +  str(datetime.now() - st)

def getCandidateExp():
    st = datetime.now()
    global candExpDF
    #candExpDF = pd.DataFrame(list(collectionCS.find({'lad':{'$gte':day2}}, {'_id': 1, 'ex': 1})))
    SJ_DF_Path = "/data/Projects/JobDigest/Input/SJ_Exp_DF.csv"
    candExpDF = pd.read_csv(SJ_DF_Path)
    #candExpDF.columns = ['userId','exp']
    candExpDF['userId'] = candExpDF['userId'].apply(lambda x:str(x))
    #candExpDF = candExpDF.fillna(0)
    print "Candidate Exp DF count : ",candExpDF.shape[0]
    print "Candidate Exp generated at :"+str(datetime.now())
    print "Time Taken : "+str(datetime.now()-st)

#######################################

def getUserSJSuggestionPoolWise(userPool):
    global SJSparseMatrix, liveJobIdDict, userDF, jobRecencyDF
    global SJSuggestionDF_Pool_Mongo, collectionCJS, candExpDF
    #####################################
    userDF = userDF.ix[:60000]
    #####################################
    collectionCJS.drop()
    collectionSJD.drop()
    for userPoolPosition in range(0, userDF.shape[0], userPool):
        if (userPoolPosition % (10*userPool) == 0):
            print 'Pool Position:', userPoolPosition, 'Time:', datetime.now()
        if userPool > userDF.shape[0]:
            userPool = userDF.shape[0]
        if userDF.shape[0] - userPoolPosition < userPool:
            userPool = userDF.shape[0] - userPoolPosition
        userDF_Pool = userDF[userDF.index.isin(range(userPoolPosition,userPoolPosition+userPool))]
        userDF_Pool.index = range(0,userPool)
        try:
            userArchieveDF_Pool = getUserArchieveDF_Pool(userDF_Pool)
        except:
            userArchieveDF_Pool = pd.DataFrame(columns = ['userId', 'job'])
        userIdDict_Pool = dict(zip(userDF_Pool.index, userDF_Pool['userId']))
        UserAppsSparseMatrix_Pool = UserAppsSparseMatrix[userPoolPosition:userPoolPosition+userPool, :]
        SJSuggestionSparseMatrix_Pool = UserAppsSparseMatrix_Pool * SJSparseMatrix
        SJSuggestionSparseMatrix_Pool = SJSuggestionSparseMatrix_Pool.tocoo()        
        row = SJSuggestionSparseMatrix_Pool.row
        col  = SJSuggestionSparseMatrix_Pool.col
        data = SJSuggestionSparseMatrix_Pool.data
        del SJSuggestionSparseMatrix_Pool, UserAppsSparseMatrix_Pool
        SJSuggestionDF_Pool = pd.DataFrame({'userId':row,'job':col,'val':data})
        del row, col, data
        #SJSuggestionDF_Pool = SJSuggestionDF_Pool.sort(['userId','val'], ascending = [1,0])
        SJSuggestionDF_Pool['job'] = SJSuggestionDF_Pool['job'].apply(lambda x:liveJobIdDict[x])
        SJSuggestionDF_Pool['userId'] = SJSuggestionDF_Pool['userId'].apply(lambda x:userIdDict_Pool[x])
        ##############################################
        #pdb.set_trace()
        #print "SJSuggestion Pool DF jobid type: ", type(SJSuggestionDF_Pool['job'].tolist()[0])
        #print "Job Recency DF jobid type: ", type(jobRecencyDF['job'].tolist()[0])
        ##############################################
        SJSuggestionDF_Pool = SJSuggestionDF_Pool[~SJSuggestionDF_Pool.index.isin(SJSuggestionDF_Pool.reset_index().merge(userArchieveDF_Pool, on = ['userId', 'job'], how = 'inner')['index'])]
        SJSuggestionDF_Pool = pd.merge(SJSuggestionDF_Pool,jobRecencyDF,on='job')
        ##############################################
        SJSuggestionDF_Pool['userId']=SJSuggestionDF_Pool['userId'].apply(lambda x:str(x))
        SJSuggestionDF_Pool = pd.merge(SJSuggestionDF_Pool,candExpDF,on='userId')
        SJSuggestionDF_Pool['exp'] = SJSuggestionDF_Pool['exp'].apply(lambda x:int(x))
        SJSuggestionDF_Pool['maxexperience'] = SJSuggestionDF_Pool['maxexperience'].apply(lambda x:int(x))
        SJSuggestionDF_Pool = SJSuggestionDF_Pool[(SJSuggestionDF_Pool.exp-SJSuggestionDF_Pool.maxexperience)<=2].reset_index(drop=True)
        SJSuggestionDF_Pool = SJSuggestionDF_Pool.drop(['maxexperience','exp'],1)
        ##############################################
        SJSuggestionDF_Pool['valwithrecency'] = SJSuggestionDF_Pool['val']*SJSuggestionDF_Pool['factor']
        SJSuggestionDF_Pool = SJSuggestionDF_Pool.sort(['userId','val'], ascending = [1,0]).reset_index(drop=True)
        #SJSuggestionDF_Pool = SJSuggestionDF_Pool.ix[:initialdigestcutoff]
        #SJSuggestionDF_Pool = SJSuggestionDF_Pool.sort(['userId','valwithrecency'], ascending = [1,0])
        SJSuggestionDF_Pool_Mongo = {}
        SJSuggestionDF_Pool = SJSuggestionDF_Pool[['userId', 'job', 'valwithrecency']]
        for row in SJSuggestionDF_Pool.values:
            userDigest = SJSuggestionDF_Pool_Mongo.get(row[0], [])
            if len(userDigest) < initialdigestcutoff:
                SJSuggestionDF_Pool_Mongo[row[0]] = userDigest + [[int(row[1]), float(row[2])]]
            else:
                pass
        for key in SJSuggestionDF_Pool_Mongo:
            SJSuggestionDF_Pool_Mongo[key] = sorted(SJSuggestionDF_Pool_Mongo[key],key=itemgetter(1),reverse=True)[:digestcutoff]
        del SJSuggestionDF_Pool
        pool = Pool(processes=2)
        try:
            aa = pool.map(startDigestProcessing, SJSuggestionDF_Pool_Mongo.keys())
            pool.close()
        except:
            traceback.print_exc()
            pool.close()
        del SJSuggestionDF_Pool_Mongo
    print 'CandJobSuggestion', 'Count:', collectionCJS.count(), 'SimilarJobDigest', 'Count:', collectionSJD.count()

def startDigestProcessing(cand):
    candJobSuggList = SJSuggestionDF_Pool_Mongo[cand]
    candJobSuggList = [[int(item[0]), int(item[1])] for item in candJobSuggList]
    collectionCJS.insert({'_id':str(cand), 'sjdlen': len(candJobSuggList), 'lud':day1, 'sjd': candJobSuggList[:digestcutoff]})
    candJobSuggListJobs = [job[0] for job in candJobSuggList]
    if len(candJobSuggListJobs) > 0:
        collectionSJD.insert({'_id':str(cand), 'sjdlen': len(candJobSuggListJobs), 'lud':day1, 'sjd': candJobSuggListJobs[:digestcutoff]})

def main():
    global jobRecencyDF
    st = datetime.now()
    print 'Similar Profile Mailer Start at Time :', str(st)
    print day1
    print day2
    print "Similar Job Matrix start at:", datetime.now()
    getSimilarJobMatrix()
    print "Job Archieves for Users start at:", datetime.now()
    getUserArchieveDF()
    print "Users Applications start at:", datetime.now()
    getUserAppsDF()
    ##########################################
    print "Job Recency Score Calculations start at:", datetime.now()
    getJobRecencyScore()
    paid_boost = {'Paid Company':1.6,'Paid Consultant':1.4,'Free Company':1.1,'Free Consultant':1.05,'Paid Others':0,'Free Others':0,'NULL Others':0}
    jobRecencyDF['recencyboost'] = jobRecencyDF['pubdate'].apply(lambda x: math.exp(.0048*x))
    jobRecencyDF['paidboost'] = jobRecencyDF['jobtype'].apply(lambda x: float(paid_boost.get(x,1)))
    jobRecencyDF['factor'] = jobRecencyDF['recencyboost']*jobRecencyDF['paidboost']
    jobRecencyDF = jobRecencyDF.drop(['recencyboost','paidboost','pubdate'],1)
    jobRecencyDF = jobRecencyDF.drop(['acc_type','free_paid','jobtype'],1)
    print "Candidate Exp DF start at:", datetime.now()
    getCandidateExp()
    ##########################################
    print "Similar Job Digest Pool Wise start at:", datetime.now()
    getUserSJSuggestionPoolWise(5000)
    open(projectHome + '/Docs/sjdigest_updated_till.txt','a').write(str(todayDate)+'\n')
    os.system(' echo "SJ Mailer Run is completed at ' + str(datetime.now()) + '\nTotal run time :' +  str(datetime.now() - st) + ' " | mutt -s "SJ Run Complete" saurabh24292@gmail.com,mayank.singh.iit@gmail.com ')
    print 'Total run time :', str(datetime.now() - st)

if __name__=='__main__':
    main()

