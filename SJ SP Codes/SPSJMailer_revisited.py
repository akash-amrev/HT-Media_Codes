###############################
#Description: Similar Job and Profile Mailer
#Details: Similar Jobs and Similar Profiles Mailer from SimilarJobDigest and SimilarProfileDigest
#Last Changed Date: Tue Dec 10 10:10:00 IST 2014
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
interval1 = 30
interval1_1 = 92
interval2 = 183
interval3 = 365

#oneMonthMonth = todayDate + relativedelta(days=-interval1) ---- change it to three months
oneMonthMonth = todayDate + relativedelta(days=-interval1_1)
sixMonthWindow = todayDate + relativedelta(days=-interval2)
oneYearWindow = todayDate + relativedelta(days=-interval3)

day1 = datetime.combine(todayDate, time(0, 0))
day2 = datetime.combine(oneMonthMonth, time(0, 0))
day3 = datetime.combine(sixMonthWindow, time(0, 0))
day4 = datetime.combine(oneYearWindow, time(0, 0))

projectHomeSP = '/data/Projects/SimilarProfiles'
projectHomeSJ = '/data/Projects/JobDigest'

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

digestcutoff = 10

candidateDB = 'sumoplus'
recruiterDB = 'recruiter_master'
jobDigestDB = 'job_digest'
jobDigestMailerDB = 'similar_job_mailer'
jobDigestMailerDBnew = 'similar_job_mailer'
similarCandDB = 'similar_profile'
similarCandMailerDB = 'similar_profile_mailer'
similarCandMailerDBnew = 'similar_profile_mailer'

mongo_conn1 = getMongoConnection('172.22.65.157', 27018, candidateDB, True)
collectionCS = getattr(mongo_conn1, 'CandidateStatic')

mongo_conn2 = getMongoConnection('172.22.66.198', 27017, jobDigestDB)
collectionSJD = getattr(mongo_conn2, 'SimilarJobDigest')

mongo_conn3 = getMongoConnection('172.22.66.198', 27017, similarCandDB)
collectionSPD = getattr(mongo_conn3, 'SimilarProfileDigest')

mongo_conn4 = getMongoConnection('172.22.66.198', 27017, jobDigestMailerDB)
collectionSJDActive = getattr(mongo_conn4, 'SimilarJobDigestActive')
collectionSJDOutstanding = getattr(mongo_conn4, 'SimilarJobDigestOutstanding')
collectionSJDInActive = getattr(mongo_conn4, 'SimilarJobDigestInActive')
collectionSJDRevival = getattr(mongo_conn4, 'SimilarJobDigestRevival')

mongo_conn5 = getMongoConnection('172.22.66.198', 27017, similarCandMailerDB)
collectionSPDActive = getattr(mongo_conn5, 'SimilarProfileDigestActive')
collectionSPDInActive = getattr(mongo_conn5, 'SimilarProfileDigestInActive')
collectionSPDRevival = getattr(mongo_conn5, 'SimilarProfileDigestRevival')

#####################################################
mongo_conn6 = getMongoConnection('172.22.66.198', 27017, jobDigestMailerDBnew)
collectionSJDActive = getattr(mongo_conn6, 'SimilarJobDigestActiveMonday')
collectionSJDActiveWednesday = getattr(mongo_conn6, 'SimilarJobDigestActiveWednesday')
collectionSJDOutstanding = getattr(mongo_conn6, 'SimilarJobDigestOutstanding')
collectionSJDInActive = getattr(mongo_conn6, 'SimilarJobDigestInActive')
collectionSJDRevival = getattr(mongo_conn6, 'SimilarJobDigestRevival')

mongo_conn7 = getMongoConnection('172.22.66.198', 27017, similarCandMailerDBnew)
collectionSPDActiveMonday = getattr(mongo_conn7, 'SimilarProfileDigestActiveMonday')
collectionSPDActive = getattr(mongo_conn7, 'SimilarProfileDigestActiveWednesday')
#collectionSPDOutstanding = getattr(mongo_conn6, 'SimilarProfileDigestOutstanding')
collectionSPDInActive = getattr(mongo_conn7, 'SimilarProfileDigestInActive')
collectionSPDRevival = getattr(mongo_conn7, 'SimilarProfileDigestRevival')
#####################################################

def getLoginData():
    global LoginDF, LoginDFActive, LoginDFInActive, LoginDFRevival
    st = datetime.now()
#     Logins = collectionCS.find({'rm':0, 'mo':0, 'll':{'$gte':day4}}, {'_id': 1, 'll': 1})
    print datetime.now()    
    LoginDF = pd.DataFrame(list(collectionCS.find({'ll':{'$gte':day4}}, {'_id': 1, 'll': 1})))
    LoginDF.columns = ['userId', 'll']
    LoginDF.sort('ll', inplace=True)
    LoginDF = LoginDF[~LoginDF.duplicated('userId',take_last=True)]
    LoginDF.index = range(0, LoginDF.shape[0])
    LoginDF['userId'] = LoginDF['userId'].apply(lambda x:str(x)) 
    LoginDFActive = LoginDF[LoginDF.ll >= day2] #increased from one month to three months
    LoginDFInActive = LoginDF[(LoginDF.ll >= day3) & (LoginDF.ll < day2)]
    LoginDFRevival = LoginDF[LoginDF.ll < day3] 
    print "Count :: LoginDF:", LoginDF.shape[0], "LoginDFActive:", LoginDFActive.shape[0], "LoginDFInActive:", LoginDFInActive.shape[0], "LoginDFRevival:", LoginDFRevival.shape[0]  
    print "Run time :" +  str(datetime.now() - st)

#######################################################
def getSJSPCand():
    global LoginDFActive, ActiveSJOnlyDF, ActiveSPOnlyDF, ActiveSJSPDF
    st = datetime.now()
    print datetime.now()
    SJCandDF = pd.DataFrame(list(collectionSJD.find({},{'_id':1})))
    SJCandDF.columns = ['userId']
    print "SJCand Done"
    SPCandDF = pd.DataFrame(list(collectionSPD.find({},{'_id':1})))
    SPCandDF.columns = ['userId']
    print "SPCand Done"
    ActiveSJOnlyDF = LoginDFActive[LoginDFActive['userId'].isin(SJCandDF['userId'].tolist())].reset_index(drop=True)
    ActiveSJOnlyDF = ActiveSJOnlyDF[~ActiveSJOnlyDF['userId'].isin(SPCandDF['userId'].tolist())].reset_index(drop=True)
    print "SJOnly Done"
    ActiveSPOnlyDF = LoginDFActive[LoginDFActive['userId'].isin(SPCandDF['userId'].tolist())].reset_index(drop=True)
    ActiveSPOnlyDF = ActiveSPOnlyDF[~ActiveSPOnlyDF['userId'].isin(SJCandDF['userId'].tolist())].reset_index(drop=True)
    print "SPOnly Done"
    ActiveSJSPDF = SJCandDF[SJCandDF['userId'].isin(SPCandDF['userId'].tolist())].reset_index(drop=True)
    print "SJSP Done"
    print "Count :: ACTSJOnlyDF:", ActiveSJOnlyDF.shape[0], "ACTSPOnlyDF:", ActiveSPOnlyDF.shape[0], "ACTSJSPDF:", ActiveSJSPDF.shape[0]  
    print "Run time :" +  str(datetime.now() - st)

def digestMailer():
    global LoginDFActive, ActiveSJOnlyDF, ActiveSPOnlyDF, ActiveSJSPDF, LoginDFInActive, LoginDFRevival
    st = datetime.now()
    collectionSJDActive.drop()
    collectionSJDActiveWednesday.drop()
    collectionSJDOutstanding.drop()
    collectionSJDInActive.drop()
    collectionSJDRevival.drop()
    collectionSPDActiveMonday.drop()
    collectionSPDActive.drop()
    collectionSPDInActive.drop()
    collectionSPDRevival.drop()
    for type, startDigestProcessing in [['ActiveSJOnly', startDigestProcessingActiveSJOnly], ['ActiveSPOnly', startDigestProcessingActiveSPOnly], ['ActiveSJSP', startDigestProcessingActiveSJSP], ['InActive', startDigestProcessingInActive], ['Revival', startDigestProcessingRevival]]:
        st1 = datetime.now()
        print "SJ and SP Mailer generation for", type, "Candidates at", datetime.now()
        if type == 'ActiveSJOnly':
            LoginDF_Type = ActiveSJOnlyDF
        elif type == 'ActiveSPOnly':
            LoginDF_Type = ActiveSPOnlyDF
        elif type == 'ActiveSJSP':
            LoginDF_Type = ActiveSJSPDF
        elif type == 'InActive':
            LoginDF_Type = LoginDFInActive
        elif type == 'Revival':
            LoginDF_Type = LoginDFRevival
        pool = Pool(processes=6)
        try:
            aa = pool.map(startDigestProcessing, LoginDF_Type['userId'].values)
            pool.close()
        except:
            traceback.print_exc()
            pool.close()
        print "Run time for", type, "Candidates :", datetime.now() - st1
    print "SJ Count :: Active:", collectionSJDActive.count(), "ActiveWednesday:", collectionSJDActiveWednesday.count(), "Outstanding:", collectionSJDOutstanding.count(), "InActive:", collectionSJDInActive.count(), "Revival:", collectionSJDRevival.count()
    print "SP Count :: ActiveMonday:", collectionSPDActiveMonday.count(), "Active:", collectionSPDActive.count(), "InActive:", collectionSPDInActive.count(), "Revival:", collectionSPDRevival.count()
    print "Run time :" +  str(datetime.now() - st)
    
def startDigestProcessingActiveSJOnly(cand):
    SJList = collectionSJD.find_one({'_id':cand},{'sjd':1, '_id':0})
    SJList = [] if SJList is None else SJList['sjd']
    if len(SJList) > 0:
        if len(SJList) > (2*digestcutoff):
            collectionSJDOutstanding.insert({'_id':cand, 'sjdlen': len(SJList), 'lud':day1, 'sjd': SJList[(2*digestcutoff):(3*digestcutoff)]})
        if len(SJList) > digestcutoff:
            collectionSJDActiveWednesday.insert({'_id':cand, 'sjdlen': len(SJList), 'lud':day1, 'sjd': SJList[digestcutoff:(2*digestcutoff)]})
        collectionSJDActive.insert({'_id':cand, 'sjdlen': len(SJList), 'lud':day1, 'sjd': SJList[:digestcutoff]})

def startDigestProcessingActiveSPOnly(cand):
    SPList = collectionSPD.find_one({'_id':cand},{'sjd':1, '_id':0})
    SPList = [] if SPList is None else SPList['sjd']
    if len(SPList) > 0:
        if len(SPList) > (2*digestcutoff):
            collectionSJDOutstanding.insert({'_id':cand, 'sjdlen': len(SPList), 'lud':day1, 'sjd': SPList[(2*digestcutoff):(3*digestcutoff)]})
        if len(SPList) > digestcutoff:
            collectionSPDActive.insert({'_id':cand, 'sjdlen': len(SPList), 'lud':day1, 'sjd': SPList[digestcutoff:(2*digestcutoff)]})
        collectionSPDActiveMonday.insert({'_id':cand, 'sjdlen': len(SPList), 'lud':day1, 'sjd': SPList[:digestcutoff]})

def startDigestProcessingActiveSJSP(cand):
    SJList = collectionSJD.find_one({'_id':cand},{'sjd':1, '_id':0})
    SJList = [] if SJList is None else SJList['sjd']
    SPList = collectionSPD.find_one({'_id':cand},{'sjd':1, '_id':0})
    SPList = [] if SPList is None else SPList['sjd']
    SJList = [job for job in SJList if job not in SPList]
    if len(SJList) > 0:
        if len(SJList) > digestcutoff:
            collectionSJDOutstanding.insert({'_id':cand, 'sjdlen': len(SJList), 'lud':day1, 'sjd': SJList[digestcutoff:(2*digestcutoff)]})
        elif len(SPList) > digestcutoff:
            collectionSJDOutstanding.insert({'_id':cand, 'sjdlen': len(SPList), 'lud':day1, 'sjd': SPList[digestcutoff:(2*digestcutoff)]})
        collectionSJDActive.insert({'_id':cand, 'sjdlen': len(SJList), 'lud':day1, 'sjd': SJList[:digestcutoff]})
    if len(SPList) > 0:
        collectionSPDActive.insert({'_id':cand, 'sjdlen': len(SPList), 'lud':day1, 'sjd': SPList[:digestcutoff]})
#######################################################

def digestMailer():
    global LoginDFActive, LoginDFInActive, LoginDFRevival, ActiveSJDF, ActiveNoSJDF, ActiveSPDF, ActiveNoSPDF 
    st = datetime.now()
    collectionSJDActive.drop()
    collectionSJDOutstanding.drop()
    collectionSJDInActive.drop()
    collectionSJDRevival.drop()
    collectionSPDActive.drop()
    collectionSPDInActive.drop()
    collectionSPDRevival.drop()
    for type, startDigestProcessing in [['Active', startDigestProcessingActive], ['InActive', startDigestProcessingInActive], ['Revival', startDigestProcessingRevival]]:
        st1 = datetime.now()
        print "SJ and SP Mailer generation for", type, "Candidates at", datetime.now()
        if type == 'Active':
            LoginDF_Type = LoginDFActive
        elif type == 'InActive':
            LoginDF_Type = LoginDFInActive
        elif type == 'Revival':
            LoginDF_Type = LoginDFRevival
        pool = Pool(processes=6)
        try:
            aa = pool.map(startDigestProcessing, LoginDF_Type['userId'].values)
            pool.close()
        except:
            traceback.print_exc()
            pool.close()
        print "Run time for", type, "Candidates :", datetime.now() - st1
    print "SJ Count :: Active:", collectionSJDActive.count(), "Outstanding:", collectionSJDOutstanding.count(), "InActive:", collectionSJDInActive.count(), "Revival:", collectionSJDRevival.count()
    print "SP Count :: Active:", collectionSPDActive.count(), "InActive:", collectionSPDInActive.count(), "Revival:", collectionSPDRevival.count()
    print "Run time :" +  str(datetime.now() - st)

def startDigestProcessingActive(cand):
    SJList = collectionSJD.find_one({'_id':cand},{'sjd':1, '_id':0})
    SJList = [] if SJList is None else SJList['sjd']
    SPList = collectionSPD.find_one({'_id':cand},{'sjd':1, '_id':0})
    SPList = [] if SPList is None else SPList['sjd']
    SJList = [job for job in SJList if job not in SPList]
    if len(SJList) > 0:
        if len(SJList) > 15:
            collectionSJDOutstanding.insert({'_id':cand, 'sjdlen': len(SJList), 'lud':day1, 'sjd': SJList[digestcutoff:(2*digestcutoff)]})
        collectionSJDActive.insert({'_id':cand, 'sjdlen': len(SJList), 'lud':day1, 'sjd': SJList[:digestcutoff]})
    if len(SPList) > 0:
        collectionSPDActive.insert({'_id':cand, 'sjdlen': len(SPList), 'lud':day1, 'sjd': SPList[:digestcutoff]})

def startDigestProcessingInActive(cand):
    SJList = collectionSJD.find_one({'_id':cand},{'sjd':1, '_id':0})
    SJList = [] if SJList is None else SJList['sjd']
    SPList = collectionSPD.find_one({'_id':cand},{'sjd':1, '_id':0})
    SPList = [] if SPList is None else SPList['sjd']
    SJList = [job for job in SJList if job not in SPList]
    if len(SJList) > 0:
        collectionSJDInActive.insert({'_id':cand, 'sjdlen': len(SJList), 'lud':day1, 'sjd': SJList[:digestcutoff]})
    if len(SPList) > 0:
        collectionSPDInActive.insert({'_id':cand, 'sjdlen': len(SPList), 'lud':day1, 'sjd': SPList[:digestcutoff]})

def startDigestProcessingRevival(cand):
    SJList = collectionSJD.find_one({'_id':cand},{'sjd':1, '_id':0})
    SJList = [] if SJList is None else SJList['sjd']
    SPList = collectionSPD.find_one({'_id':cand},{'sjd':1, '_id':0})
    SPList = [] if SPList is None else SPList['sjd']
    SJList = [job for job in SJList if job not in SPList]
    SPList = SPList + SJList
    if len(SJList) > 0:
        collectionSJDRevival.insert({'_id':cand, 'sjdlen': len(SJList), 'lud':day1, 'sjd': SJList[:digestcutoff]})
    if len(SPList) > 0:
        collectionSPDRevival.insert({'_id':cand, 'sjdlen': len(SPList), 'lud':day1, 'sjd': SPList[:digestcutoff]})

def BackupandJobSuggDumpSJ():
    st = datetime.now()
    print "Similar Jobs Mailer Mongo Dump start at :" + str(st)
    backUpRoot = projectHomeSJ + '/backup'
    db_name = jobDigestMailerDB
    os.system("mongodump --db "+db_name+" --out "+backUpRoot+"/Similar_Job_Mailer")
    os.system("tar -cvf "+backUpRoot+"/Similar_Job_Mailer/similar_job_mailer_"+str(todayDate)+".tar.gz "+backUpRoot+"/Similar_Job_Mailer/similar_job_mailer")
    os.system("rm -rf "+backUpRoot+"/Similar_Job_Mailer/similar_job_mailer")
    print "Similar Jobs Mailer Backup completed at :" + str(datetime.now())
    print "Run time :" +  str(datetime.now() - st)

def BackupandJobSuggDumpSP():
    st = datetime.now()
    print "Similar Jobs Mailer Mongo Dump start at :" + str(st)
    backUpRoot = projectHomeSP + '/backup'
    db_name = similarCandMailerDB
    os.system("mongodump --db "+db_name+" --out "+backUpRoot+"/Similar_Profile_Mailer")
    os.system("tar -cvf "+backUpRoot+"/Similar_Profile_Mailer/similar_profile_mailer_"+str(todayDate)+".tar.gz "+backUpRoot+"/Similar_Profile_Mailer/similar_profile_mailer")
    os.system("rm -rf "+backUpRoot+"/Similar_Profile_Mailer/similar_profile_mailer")
    print "Similar Profile Mailer Backup completed at :" + str(datetime.now())
    print "Run time :" +  str(datetime.now() - st)

def main():
    st = datetime.now()
    print 'Similar Profile Mailer Start at Time :', str(st)
    print day1
    print day2
    print day3
    print day4
    print "Candidate Id Login Date Wise :", datetime.now()
    getLoginData()
    ##########################
    print "SJSP Candidate Segregation Start at Time :", datetime.now()
    getSJSPCand()
    ##########################
    print "Mailer Digest start at:", datetime.now()
    digestMailer()
    print 'BackupandJobDigestDump SJ creation at time: '+str(datetime.now())
    BackupandJobSuggDumpSJ()
    print 'BackupandJobDigestDump SP creation at time: '+str(datetime.now())
    BackupandJobSuggDumpSP()
    open(projectHomeSP + '/Docs/mailer_digest_updated_till.txt','a').write(str(todayDate)+'\n')
    os.system(' echo "SJSP Mailer Run is completed at ' + str(datetime.now()) + '\nTotal run time :' +  str(datetime.now() - st) + ' " | mutt -s "SJ & SP Mailer Run Complete" saurabh24292@gmail.com,mayank.singh.iit@gmail.com ')
    print 'Total run time :', str(datetime.now() - st)

if __name__=='__main__':
    main()


# mon_sj_active = set(pd.DataFrame(list(collectionSJDActive.find({},{'_id':1})))['_id'])
# wed_sp_active = set(pd.DataFrame(list(collectionSPDActive.find({},{'_id':1})))['_id'])
# wed_sp_inactive = set(pd.DataFrame(list(collectionSPDInActive.find({},{'_id':1})))['_id'])
# fri_sj_out = set(pd.DataFrame(list(collectionSJDOutstanding.find({},{'_id':1})))['_id'])
# fri_sj_inactive = set(pd.DataFrame(list(collectionSJDInActive.find({},{'_id':1})))['_id'])
# fri_sp_revival = set(pd.DataFrame(list(collectionSPDRevival.find({},{'_id':1})))['_id'])
# 
# len(wed_sp_active.intersection(wed_sp_inactive))
# len(fri_sj_out.intersection(fri_sj_inactive))
# len(fri_sj_out.intersection(fri_sp_revival))
# len(fri_sp_revival.intersection(fri_sj_inactive))
# 
# len(mon_sj_active.intersection(wed_sp_active))
# len(wed_sp_inactive.intersection(fri_sj_inactive))
# len(wed_sp_active.intersection(fri_sj_out))
# 
# len(set(list(mon_sj_active) + list(wed_sp_active) + list(wed_sp_inactive) + list(fri_sj_out) + list(fri_sj_inactive) + list(fri_sp_revival)))
