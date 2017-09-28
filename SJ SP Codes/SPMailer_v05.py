###############################
#Description: Similar Profile's Similar Job Digest
#Details: Similar Jobs to Jobs Applied by Similar Profiles
#Last Changed Date: Tue Jun 11 10:00:00 IST 2015
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
import pdb
from operator import itemgetter
todayDate = date.today()

interval = 365 
oneYearWindow = todayDate + relativedelta(days=-interval)

day1 = datetime.combine(todayDate, time(0, 0))
day2 = datetime.combine(oneYearWindow, time(0, 0))

host = '172.16.66.64'
port = 3306
user = 'analytics'
pwd = '@n@lytics'
database = 'SumoPlus'

projectHome = '/data/Projects/SimilarProfiles'

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

recruiterDB = 'recruiter_master'
similarJob = 'similar_jobs'
jobDigestArchieveDB = 'job_digest_archieve'
similarCandDB = 'similar_profile'

mongo_conn1 = getMongoConnection('172.22.66.198', 27017, similarJob)
collectionJS = getattr(mongo_conn1, 'JobSuggestions')
collectionCA = getattr(mongo_conn1, 'CandidateApplication')

mongo_conn2 = getMongoConnection('172.22.66.198', 27017, jobDigestArchieveDB)
collectionSJDArchieve = getattr(mongo_conn2, 'SimilarJobDigestArchieve')

mongo_conn3 = getMongoConnection('172.22.66.198', 27017, similarCandDB)
collectionSPD = getattr(mongo_conn3, 'SimilarProfileDigest')

def executeMySQLQuery(query):
    host = 'localhost'
    port = 3306
    user = 'root'
    pwd = 'mysql@123'
    database = 'SimilarProfiles'
    dbconn = getMySqlConnection (host, port, user, pwd, database)
    cursor = dbconn.cursor()
    cursor.execute(query)
    rs = cursor.fetchall()
    dbconn.close()
    return rs

def getSimilarJobMatrix():
    #global jobDF, SJSparseMatrix, jobLookUp, liveJobIdDict
    ###################################################
    global jobDF, SJSparseMatrix, jobLookUp, liveJobIdDict, liveJobDF
    ###################################################
    st = datetime.now()
    SimilarJobsCur = collectionJS.find()
    count = 0
    SJDict = {}
    liveJobList = []
    for cur in SimilarJobsCur:
        SJDict[cur['_id']] = {}
        for item in cur['sj']:
            if int(item[2]) >= 50: # Mean of Similar Jobs is 50 and Median is 31
                if cur['_id'] != item[0]:
                    SJDict[cur['_id']][item[0]] = int(0.33*item[2])
                else:
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
    ##########################################
    del row, col, data, count, dummySJDict, SJDict
    ##########################################
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

#######################################
def getJobRecencyScore():
    st = datetime.now()
    global liveJobDF, jobRecencyDF
    uniqueU1String = str(list(liveJobDF['job'])).strip("[]")
    dbconn = getMySqlConnection (host, port, user, pwd, database)
    query = "select a.jobid, a.publisheddate, case b.account_type when 0 then 'Company' when 1 then 'Consultant' when 2 then 'Others' else 'Not Specified' end "
    query += "as account_type, if(xy.enabled = 1 and xy.price != 0 and xy.expiry_date > curdate(),'Paid','Free') AS 'Free/Paid', IFNULL(a.maxexperience,30) as maxexperience "
    query += "from SumoPlus.recruiter_job a left join SumoPlus.backoffice_companyaccount b on a.companyid_id = b.id left join "
    query += "(select company_account_id, sum(final_sale_price) as price, enabled, max(expiry_date) as expiry_date from SumoPlus.backoffice_accountsales a1 "
    query += "where enabled in (select min(enabled) from SumoPlus.backoffice_accountsales where a1.company_account_id = company_account_id) group by 1) as xy "
    query += "on b.id = xy.company_account_id where a.jobid in ("+uniqueU1String+") group by 1 order by a.jobid;"
    cursor = dbconn.cursor()
    cursor.execute (query)
    rows = cursor.fetchall()
    live_jobs = [(int(row[0]),str(row[1]),str(row[2]),str(row[3]),int(row[4])) for row in rows if str(row[1]) != 'None']
    jobRecencyDF = pd.DataFrame(live_jobs,columns=['job','pubdate','acc_type','free_paid','maxexperience'])
    jobRecencyDF['jobtype'] = jobRecencyDF['free_paid'] + ' ' + jobRecencyDF['acc_type']
    jobRecencyDF['pubdate'] = jobRecencyDF['pubdate'].apply(lambda x: max(0,min(1,(datetime.strptime(x,"%Y-%m-%d %H:%M:%S").date()-todayDate).days)))
    #jobRecencyDF = jobRecencyDF.drop('pubdate',1)
    print "Job Recency Scores calculated at: " + str(datetime.now())
    print "Run time :" +  str(datetime.now() - st)

def getCandidateExp():
    st = datetime.now()
    global candExpDF
    SP_DF_Path = "/data/Projects/SimilarProfiles/Input/SP_Exp_DF.csv"
    candExpDF = pd.read_csv(SP_DF_Path)
    candExpDF['userId'] = candExpDF['userId'].apply(lambda x:str(x))
    print "Candidate Exp DF count : ",candExpDF.shape[0]
    print "Candidate Exp generated at :"+str(datetime.now())
    print "Time Taken : "+str(datetime.now()-st)

#######################################

### User Application Dict {id:apps}
def getUserAppDF():
    global userAppsDF, userAppsDict
    st = datetime.now() 
    col1 = []
    col2 = []
    for cur in list(collectionCA.find({},{'_id':1,'userApps':1})):
        for job in cur['userApps']:
            col1.append(cur['_id'])
            col2.append(job)
    print 'Total Applications', len(col1)
    print 'Unique Users:', len(set(col1)), 'Unique Live Jobs:', len(set(col2))
    userAppsDF = pd.DataFrame({'userId' : col1, 'job':col2})
    userAppsDF.shape
    userAppsDF_1 = pd.DataFrame(list(collectionCA.find({},{'_id':1,'userApps':1})))
    userAppsDict = dict(zip(userAppsDF_1['_id'], userAppsDF_1['userApps']))
    del userAppsDF_1, col1, col2, cur
    print "Users Applications generated at: " + str(datetime.now())
    print "Run time :" +  str(datetime.now() - st)

### All Users LookUp DF userDF --> {'userId':[], 'userLookUp':[]} and userIdAll --> {'userId': LookUp}
def getUserMySQLLookUp():
    global userDF, userIdAll
    st = datetime.now()
    query = "select Value, Lookup from SimilarProfiles.Lookup_UserID;"
    userLookUpCur = executeMySQLQuery(query)
    userLookUpAll = {}
    userIdAll = {}
    for item in userLookUpCur:
        userIdAll[str(item[0])] = int(item[1])
    userListAll = userIdAll.keys()
    userListAll.sort()
    userDF = pd.DataFrame({'userId': userListAll})
    userDF['userLookUp'] = userDF['userId'].apply(lambda x:userIdAll[x])
    del userListAll, userLookUpCur 
    print "Users MySQL Lookup generated at: " + str(datetime.now())
    print "Run time :" +  str(datetime.now() - st)

### Var --> userDF {'userId':[], 'userLookUp':[]}

def getFAWiseUserDF(fa):
    st = datetime.now()
    query = "select distinct(U1) from SimilarProfiles.SimilarProfiles_FA" + str(fa) + " ;"
    uniqueU1 = executeMySQLQuery(query)
    uniqueU1 = [int(item[0]) for item in uniqueU1]
    print "Unique U1's in FA " + str(fa) + ":", len(uniqueU1)
    query = "select distinct(U2) from SimilarProfiles.SimilarProfiles_FA" + str(fa) + " ;"
    uniqueU2 = executeMySQLQuery(query)
    uniqueU2 = [int(item[0]) for item in uniqueU2]
    print "Unique U2's in FA " + str(fa) + ":", len(uniqueU2)
    uniqueUser_FA = list(set(uniqueU1 + uniqueU2))
    uniqueUser_FA.sort()
    print "Total Unique Users in FA " + str(fa) + ":", len(uniqueUser_FA)
    del uniqueU1, uniqueU2
    userDF_FA = userDF[userDF['userLookUp'].isin(uniqueUser_FA)]
    userDF_FA.index = range(0,userDF_FA.shape[0])
    print 'FA', fa, 'User DF Shape :', userDF_FA.shape[0], 'in time:', (datetime.now() - st)
    del uniqueUser_FA
    return userDF_FA

def getFAWiseUserArchieveDF(userDF_FA):
    global userArchieveDF
    userArchieveList_FA = list(set(userArchieveDF['userId']).intersection(set(userDF_FA['userId'])))
    userArchieveList_FA.sort()
    len(userArchieveList_FA)
    ofile = open(projectHome + "/Input/Mailer/ArchivedUsersJobList.csv", 'wb')
    dummyPool = 10000
    if dummyPool > userDF_FA.shape[0]:
        dummyPool = userDF_FA.shape[0]
    for dummyPos in range(0, len(userArchieveList_FA), dummyPool):
        userArchieveCur_FA = list(collectionSJDArchieve.find({'_id':{'$in': userArchieveList_FA[dummyPos: dummyPos + dummyPool]}},{'_id':1,'sjdArc':1}))  
        for itemCur in userArchieveCur_FA:
            for job in itemCur['sjdArc']:
                ofile.write(str(itemCur['_id']) + "," + str(job)+ "\n")
    ofile.close()
    userArchieveDF_FA = pd.read_csv(projectHome + "/Input/Mailer/ArchivedUsersJobList.csv", names = ['userId', 'job'])
    userArchieveDF_FA['job'] = userArchieveDF_FA['job'].astype('int32')
    userArchieveDF_FA = userArchieveDF_FA.append(userAppsDF[userAppsDF['userId'].isin(userArchieveList_FA)])
    del userArchieveCur_FA, userArchieveList_FA, dummyPos, dummyPool
    return userArchieveDF_FA

def getFAWiseUserAppsMatrix(userDF_FA):
    global userAppsDict, userIdAll, jobLookUp, liveJobIdDict
    userAppsList_FA = list(set(userAppsDict).intersection(set(userDF_FA['userId'])))
    userAppsList_FA.sort()
    userAppsDF_FA = pd.DataFrame({'userId':userAppsList_FA})
    userAppsDF_FA['userLookUp'] = userAppsDF_FA['userId'].apply(lambda x:userIdAll[x])
    userAppsDict_FA = dict(zip(userAppsDF_FA['userLookUp'], userAppsDF_FA.index))
    data = []
    row = []
    col = []
    count = 0
    errorCand = []
    for index, user in enumerate(userAppsList_FA):
        try:
            userIndex = index 
            count +=1
            for job in userAppsDict[user]:
                col.append(jobLookUp[job]) ## Col before row so that if any error in job lookup then that row is skipped
                data.append(1)
                row.append(userIndex)
        except:
            errorCand.append(user)
    UserAppsSparseMatrix_FA = sp.coo_matrix((np.array(data), (np.array(row),np.array(col))), shape=(len(userAppsList_FA),jobDF.shape[0]))
    UserAppsSparseMatrix_FA = UserAppsSparseMatrix_FA.astype('int32')
    del row, col, data
    return userAppsDF_FA, userAppsDict_FA, UserAppsSparseMatrix_FA

def getFAWiseJobSuggestion(fa, userPool, cutoff):
    global SJSparseMatrix, liveJobIdDict, jobRecencyDF, candExpDF
    global SPSJSuggestionDF_FA_Pool_Mongo, falocal, collectionCJS
    userDF_FA = getFAWiseUserDF(fa)
    userArchieveDF_FA = getFAWiseUserArchieveDF(userDF_FA)
    userAppsDF_FA, userAppsDict_FA, UserAppsSparseMatrix_FA = getFAWiseUserAppsMatrix(userDF_FA)
    falocal = fa
    collectionCJS = getattr(mongo_conn3, 'CandJobSuggestion_FA' + str(falocal))
    collectionCJS.drop()
    for userPoolPosition in range(0, userDF_FA.shape[0], userPool):
        if (userPoolPosition % (10*userPool) == 0):
            print 'FA', falocal, 'Pool Position:', userPoolPosition, 'Time:', datetime.now()
        if userPool > userDF_FA.shape[0]:
            userPool = userDF_FA.shape[0]
        if userDF_FA.shape[0] - userPoolPosition < userPool:
            userPool = userDF_FA.shape[0] - userPoolPosition
        userDF_FA_Pool = userDF_FA[userDF_FA.index.isin(range(userPoolPosition,userPoolPosition+userPool))]
        userDF_FA_Pool.index = range(0,userPool)
        userLookUpDict_FA_Pool = dict(zip(userDF_FA_Pool['userLookUp'], userDF_FA_Pool.index))
        userIdDict_FA_Pool = dict(zip(userDF_FA_Pool.index, userDF_FA_Pool['userId']))
        uniqueU1String = str(list(userDF_FA_Pool['userLookUp'])).strip("[]")
        query = "select U1, U2, Overall_Sim from SimilarProfiles.SimilarProfiles_FA" + str(fa) + " where U1 IN (" + uniqueU1String + ") AND (JT_Sim > 0 OR Skill_Sim > 0) ;"
        simUserList = executeMySQLQuery(query)
        userUserSimDF_FA_Pool = pd.DataFrame(list(simUserList), columns = ['U1', 'U2', 'Sim'])
        del uniqueU1String, query, simUserList
        userUserSimDF_FA_Pool = userUserSimDF_FA_Pool[userUserSimDF_FA_Pool['U1'] != userUserSimDF_FA_Pool['U2']]
        userUserSimDF_FA_Pool = userUserSimDF_FA_Pool[userUserSimDF_FA_Pool.U2.isin(userAppsDF_FA.userLookUp)]
        userUserSimDF_FA_Pool = userUserSimDF_FA_Pool[userUserSimDF_FA_Pool.Sim < 1001]
        userUserSimDF_FA_Pool['U2'] = userUserSimDF_FA_Pool['U2'].apply(lambda x:userAppsDict_FA[x])
        userUserSimDF_FA_Pool['U1'] = userUserSimDF_FA_Pool['U1'].apply(lambda x:userLookUpDict_FA_Pool[x])
        UserUserSimSparseMatrix_FA_Pool = sp.coo_matrix((np.array(userUserSimDF_FA_Pool['Sim']), (np.array(userUserSimDF_FA_Pool['U1']),np.array(userUserSimDF_FA_Pool['U2']))), shape=(userPool,userAppsDF_FA.shape[0]))
        UserUserSimSparseMatrix_FA_Pool = UserUserSimSparseMatrix_FA_Pool.astype('int32')
        del userUserSimDF_FA_Pool
        SPSJSuggestionSparseMatrix_FA_Pool = UserUserSimSparseMatrix_FA_Pool * UserAppsSparseMatrix_FA * SJSparseMatrix
        SPSJSuggestionSparseMatrix_FA_Pool = SPSJSuggestionSparseMatrix_FA_Pool.tocoo()
        row = SPSJSuggestionSparseMatrix_FA_Pool.row
        col  = SPSJSuggestionSparseMatrix_FA_Pool.col
        data = SPSJSuggestionSparseMatrix_FA_Pool.data
        del SPSJSuggestionSparseMatrix_FA_Pool, UserUserSimSparseMatrix_FA_Pool
        SPSJSuggestionDF_FA_Pool = pd.DataFrame({'userId':row,'job':col,'val':data})
        del row, col, data
        SPSJSuggestionDF_FA_Pool = SPSJSuggestionDF_FA_Pool[SPSJSuggestionDF_FA_Pool['val']>=cutoff]
        #SPSJSuggestionDF_FA_Pool = SPSJSuggestionDF_FA_Pool.sort(['userId','val'], ascending = [1,0])
        SPSJSuggestionDF_FA_Pool['job'] = SPSJSuggestionDF_FA_Pool['job'].apply(lambda x:liveJobIdDict[x])
        SPSJSuggestionDF_FA_Pool['userId'] = SPSJSuggestionDF_FA_Pool['userId'].apply(lambda x:userIdDict_FA_Pool[x])
        SPSJSuggestionDF_FA_Pool = SPSJSuggestionDF_FA_Pool[~SPSJSuggestionDF_FA_Pool.index.isin(SPSJSuggestionDF_FA_Pool.reset_index().merge(userArchieveDF_FA, on = ['userId', 'job'], how = 'inner')['index'])]
        SPSJSuggestionDF_FA_Pool = pd.merge(SPSJSuggestionDF_FA_Pool,jobRecencyDF,on='job')
        SPSJSuggestionDF_FA_Pool['userId'] = SPSJSuggestionDF_FA_Pool['userId'].apply(lambda x:str(x))
        SPSJSuggestionDF_FA_Pool = pd.merge(SPSJSuggestionDF_FA_Pool,candExpDF,on='userId')
        SPSJSuggestionDF_FA_Pool['exp'] = SPSJSuggestionDF_FA_Pool['exp'].apply(lambda x:int(x))
        SPSJSuggestionDF_FA_Pool['maxexperience'] = SPSJSuggestionDF_FA_Pool['maxexperience'].apply(lambda x:int(x))
        SPSJSuggestionDF_FA_Pool = SPSJSuggestionDF_FA_Pool[(SPSJSuggestionDF_FA_Pool.exp-SPSJSuggestionDF_FA_Pool.maxexperience)<=2].reset_index(drop=True)
        SPSJSuggestionDF_FA_Pool = SPSJSuggestionDF_FA_Pool.drop(['maxexperience','exp'],1)
        SPSJSuggestionDF_FA_Pool['valwithrecency'] = SPSJSuggestionDF_FA_Pool['val']*SPSJSuggestionDF_FA_Pool['factor']
        SPSJSuggestionDF_FA_Pool = SPSJSuggestionDF_FA_Pool.sort(['userId','val'], ascending = [1,0]).reset_index(drop=True)
        SPSJSuggestionDF_FA_Pool_Mongo = {}
        SPSJSuggestionDF_FA_Pool = SPSJSuggestionDF_FA_Pool[['userId', 'job', 'valwithrecency']]
        SPSJSuggestionDF_FA_Pool['userId'] = SPSJSuggestionDF_FA_Pool['userId'].apply(lambda x:str(x))
        for row in SPSJSuggestionDF_FA_Pool.values:
            userDigest = SPSJSuggestionDF_FA_Pool_Mongo.get(row[0],[])
            if len(userDigest) < initialdigestcutoff:
                SPSJSuggestionDF_FA_Pool_Mongo[row[0]] = userDigest + [[int(row[1]), float(row[2])]]
            else:
                pass
        for key in SPSJSuggestionDF_FA_Pool_Mongo:
            SPSJSuggestionDF_FA_Pool_Mongo[key] = sorted(SPSJSuggestionDF_FA_Pool_Mongo[key],key=itemgetter(1),reverse=True)[:digestcutoff]
        del SPSJSuggestionDF_FA_Pool
        pool = Pool(processes=4)
        try:
            aa = pool.map(startDigestProcessing, SPSJSuggestionDF_FA_Pool_Mongo.keys())
            pool.close()
        except:
            traceback.print_exc()
            pool.close()
        del SPSJSuggestionDF_FA_Pool_Mongo
    print 'CandJobSuggestion_FA' + str(falocal), 'Count:', collectionCJS.count(), 'SimilarProfileDigest FA' + str(falocal), 'Count:', collectionSPD.find({'fa': falocal}).count()

def startDigestProcessing(cand):
    try:
        candJobSuggList = SPSJSuggestionDF_FA_Pool_Mongo[cand]
        candJobSuggList = [[int(item[0]), int(item[1])] for item in candJobSuggList]
        collectionCJS.insert({'_id':cand, 'sjdlen': len(candJobSuggList), 'lud':day1, 'sjd': candJobSuggList[:digestcutoff], 'fa': falocal})
        candJobSuggListJobs = [job[0] for job in candJobSuggList]
        if len(candJobSuggListJobs) > 0:
            collectionSPD.insert({'_id':cand, 'sjdlen': len(candJobSuggListJobs), 'lud':day1, 'sjd': candJobSuggListJobs[:digestcutoff], 'fa': falocal})
    except:
        pass

def main():
    global jobRecencyDF
    st = datetime.now()
    try:
        faStart = int(sys.argv[1])
        faEnd = int(sys.argv[2])
    except:
        print "Error in Input Arguments add FA Start and End"
        sys.exit()
    print 'Similar Profile Mailer Start at Time :', str(st)
    print day1
    print day2
    print "Similar Job Matrix start at:", datetime.now()
    getSimilarJobMatrix()
    ##########################################
    print "Job Recency Score Calculations start at:", datetime.now()
    getJobRecencyScore()
    paid_boost = {'Paid Company':1.3,'Paid Consultant':1.2,'Free Company':1.1,'Free Consultant':1.05,'Paid Others':0,'Free Others':0,'NULL Others':0}
    jobRecencyDF['recencyboost'] = jobRecencyDF['pubdate'].apply(lambda x: math.exp(.005*x))
    jobRecencyDF['paidboost'] = jobRecencyDF['jobtype'].apply(lambda x: float(paid_boost.get(x,1)))
    jobRecencyDF['factor'] = jobRecencyDF['recencyboost']*jobRecencyDF['paidboost']
    jobRecencyDF = jobRecencyDF.drop(['recencyboost','paidboost','pubdate'],1)
    jobRecencyDF = jobRecencyDF.drop(['acc_type','free_paid','jobtype'],1)
    print "Candidate Exp DF start at:", datetime.now()
    getCandidateExp()
    ##########################################
    print "Job Archieves for Users start at:", datetime.now()
    getUserArchieveDF()
    print "Users Applications start at:", datetime.now()
    getUserAppDF()
    print "Users MySQL Lookup start at:", datetime.now()
    getUserMySQLLookUp()
    print "Similar Profile Digest FA Wise start at:", datetime.now()
    collectionSPD.drop()
    for fa in range(faStart,faEnd+1):
        st1 = datetime.now()
        print 'FA:', fa, 'Start Time:', st1
        getFAWiseJobSuggestion(fa, 10000, 2000000)
        print 'FA:', fa, 'End Time:', datetime.now()
        print 'FA:', fa, 'Time Taken:', (datetime.now() - st1)
    open(projectHome + '/Docs/spdigest_updated_till.txt','a').write(str(todayDate)+'\n')
    os.system(' echo "SP Mailer Run is completed at ' + str(datetime.now()) + '\nTotal run time :' +  str(datetime.now() - st) + ' " | mutt -s "SP Run Complete" saurabh24292@gmail.com,mayank.singh.iit@gmail.com ')
    print 'Total run time :', str(datetime.now() - st)

if __name__=='__main__':
    main()

