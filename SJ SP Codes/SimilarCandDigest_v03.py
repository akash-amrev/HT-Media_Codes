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

todayDate = date.today()
interval1 = 92

threeMonth = todayDate + relativedelta(days=-interval1)

day1 = datetime.combine(todayDate, time(0, 0))
day2 = datetime.combine(threeMonth, time(0, 0))

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

similarityCutOff = 600
simCandMinCountCutOff = 10
simCandMaxCountCutOff = 250

similarCandDB = 'similar_candidates'
candidateDB = 'sumoplus'

mongo_conn1 = getMongoConnection('172.22.66.198',27017,similarCandDB)
collectionSimCand = getattr(mongo_conn1,'SimilarCandidates')

mongo_conn2 = getMongoConnection('172.22.65.157', 27018, candidateDB, True)
collectionCS = getattr(mongo_conn2, 'CandidateStatic')

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

def getLoginData():
    global LoginDF
    st = datetime.now()
    LoginDF = pd.DataFrame(list(collectionCS.find({'ll':{'$gte':day2}}, {'_id': 1, 'll': 1})))
    LoginDF.columns = ['userId', 'll']
    LoginDF.sort('ll', inplace=True)
    LoginDF = LoginDF[~LoginDF.duplicated('userId',take_last=True)]
    LoginDF.index = range(0, LoginDF.shape[0])
    LoginDF['userId'] = LoginDF['userId'].apply(lambda x:str(x))
    LoginDF = LoginDF[['userId']]

def getUserMySQLLookUp():
    global userDF, userIdAll, LoginDF
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
    LoginDF = pd.merge(LoginDF, userDF, on = 'userId').reset_index(drop=True)
    LoginDF['userLookUp'] = LoginDF['userId'].apply(lambda x:userIdAll[x])
    del userListAll, userLookUpCur 
    print "Users MySQL Lookup generated at: " + str(datetime.now())
    print "Run time :" +  str(datetime.now() - st)

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

def getFAWiseSimCand(fa,userPool):
    global userDF, userIdAll, LoginDF
    global userUserSimDF_FA_Pool_Mongo
    userDF_FA = getFAWiseUserDF(fa)
    userDF_FA_LookUp_Dict = dict(zip(userDF_FA['userLookUp'],userDF_FA.index))
    userDA_FA_Id_Dict = dict(zip(userDF_FA.index,userDF_FA['userId']))
    for userPoolPosition in range(0, userDF_FA.shape[0], userPool):
        print 'FA', fa, 'Pool Position:', userPoolPosition, 'Time:', datetime.now()
        if userPool > userDF_FA.shape[0]:
            userPool = userDF_FA.shape[0]
        if userDF_FA.shape[0] - userPoolPosition < userPool:
            userPool = userDF_FA.shape[0] - userPoolPosition
        userDF_FA_Pool = userDF_FA[userDF_FA.index.isin(range(userPoolPosition,userPoolPosition+userPool))]
        userDF_FA_Pool.index = range(0,userPool)
        userLookUpDict_FA_Pool = dict(zip(userDF_FA_Pool['userLookUp'], userDF_FA_Pool.index))
        userIdDict_FA_Pool = dict(zip(userDF_FA_Pool.index, userDF_FA_Pool['userId']))
        uniqueU1String = str(list(userDF_FA_Pool['userLookUp'])).strip("[]")
        query = "select U1, U2, Overall_Sim from SimilarProfiles.SimilarProfiles_FA" + str(fa) + " where U1 IN (" + uniqueU1String + ") AND Overall_Sim >=" + str(similarityCutOff) + " ;"
        simUserList = executeMySQLQuery(query)
        userUserSimDF_FA_Pool = pd.DataFrame(list(simUserList), columns = ['U1', 'U2', 'Sim'])
        del uniqueU1String, query, simUserList
        userUserSimDF_FA_Pool = userUserSimDF_FA_Pool[userUserSimDF_FA_Pool['U1'] != userUserSimDF_FA_Pool['U2']]
        #userUserSimDF_FA_Pool = userUserSimDF_FA_Pool[userUserSimDF_FA_Pool.U1.isin(userDF_FA_Pool.userLookUp)]
        userUserSimDF_FA_Pool = userUserSimDF_FA_Pool[userUserSimDF_FA_Pool.U2.isin(userDF_FA.userLookUp)]
        userUserSimDF_FA_Pool = userUserSimDF_FA_Pool[userUserSimDF_FA_Pool.U2.isin(LoginDF.userLookUp)]
        userUserSimDF_FA_Pool = userUserSimDF_FA_Pool[userUserSimDF_FA_Pool.Sim < 1001]
        userUserSimDF_FA_Pool['U1'] = userUserSimDF_FA_Pool['U1'].apply(lambda x:userLookUpDict_FA_Pool[x])
        userUserSimDF_FA_Pool['U2'] = userUserSimDF_FA_Pool['U2'].apply(lambda x:userDF_FA_LookUp_Dict[x])
        userUserSimDF_FA_Pool = userUserSimDF_FA_Pool.sort(['U1','Sim'], ascending = [1,0])
        userUserSimDF_FA_Pool['U1'] = userUserSimDF_FA_Pool['U1'].apply(lambda x:userIdDict_FA_Pool[x])
        userUserSimDF_FA_Pool['U2'] = userUserSimDF_FA_Pool['U2'].apply(lambda x:userDA_FA_Id_Dict[x])
        userUserSimDF_FA_Pool_Mongo = {}
        userUserSimDF_FA_Pool = userUserSimDF_FA_Pool[['U1', 'U2', 'Sim']]
        for row in userUserSimDF_FA_Pool.values:
            userUserSimDF_FA_Pool_Mongo[row[0]] = userUserSimDF_FA_Pool_Mongo.get(row[0], []) + [[str(row[1]), int(row[2])]]
        del userUserSimDF_FA_Pool
        pool = Pool(processes=6)
        try:
            aa = pool.map(startDigestProcessing, userUserSimDF_FA_Pool_Mongo.keys())
            pool.close()
        except:
            traceback.print_exc()
            pool.close()
        del userUserSimDF_FA_Pool_Mongo
    print 'Similar Candidate Suggestion Count:', collectionSimCand.count()

def startDigestProcessing(cand):
    try:
        simCandList = userUserSimDF_FA_Pool_Mongo[cand]
        simCandList = [[str(item[0]), int(item[1])] for item in simCandList]
        simCandList = sorted(simCandList,key=lambda x:x[1],reverse=True)
        simCandList = simCandList[:simCandMaxCountCutOff]
        simCandListExport = [simCandList[i][0] for i in range(len(simCandList))]
        if len(simCandListExport) >= simCandMinCountCutOff:
            collectionSimCand.insert({'_id':cand,'simcandlen':len(simCandListExport),'simcands':simCandListExport})
    except:
        pass

def main():
    st = datetime.now()
    try:
        faStart = int(sys.argv[1])
        faEnd = int(sys.argv[2])
    except:
        print "Error in Input Arguments add FA Start and End"
        sys.exit()
    print "Users MySQL Lookup start at:", datetime.now()
    getLoginData()
    getUserMySQLLookUp()
    collectionSimCand.drop()
    print "Collection building start at:", datetime.now()
    for fa in range(faStart,faEnd+1):
        st1 = datetime.now()
        print 'FA:', fa, 'Start Time:', st1
        getFAWiseSimCand(fa,100000)
        print 'FA:', fa, 'End Time:', datetime.now()
        print 'FA:', fa, 'Time Taken:', (datetime.now() - st1)
    print 'Total run time :', str(datetime.now() - st)

if __name__=='__main__':
    main()

