__author__  = 'Ashish.Jain'
__date__ = '28-Oct-2015'


'''
This script generates recommendations for Job Alert Mailer
'''


import sys
sys.path.append('./../')
from Features.CTCMatchScore.CTCMatchScore import CTCMatchScore
from Features.ExpMatchScore.ExpMatchScore import ExpMatchScore
from Features.PaidBoostScore.PaidBoostScore import PaidBoostScore
from Features.CityScore.getCityScore import CityMatch
from Model.getOverallMatchScore import getOverallMatchScore
#from Main.getLSICosine import getLSICosine
from pprint import pprint
from DataConnections.MySQLConnect.MySQLConnect import MySQLConnect
from DataConnections.MongoConnect.MongoConnect import MongoConnect
from Utils.Utils_1 import cleanToken
from Utils.HtmlCleaner import HTMLStripper, cleanHTML
import pdb
from gensim import corpora, models, similarities
from Features.LSI_common.MyBOW import MyBOW
#from Logger.MyLogger import MyLogger
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




def computeAlertsChunk(chunkID):
    
    #########################################
    '''Creating a connection to output mongodb'''
    #########################################
    
    tablename = 'WeeklyMsgQueue'
    monconn_recommendations = MongoConnect(tablename, host='localhost', database='mailer_weekly')
    
    print 'Chunk:', chunkID, 'initiated at:', time.ctime()
   
   
   
    #################################################
    '''Fetch the user data from the database'''
    #################################################
    
    #print "Fetching the users data from Mongodb for ChunkID:",chunkID
    #tablename="candidates_processed"
    tablename = "candidates_processed"
    monconn_users = MongoConnect(tablename, host='localhost', database='mailer_weekly')
    mongo_users_cur=monconn_users.getCursor()
        
    myCondition = {'p':chunkID}
    #myCondition = {}
    users = monconn_users.loadFromTable(myCondition)
    #print "Fetching the users data from Mongodb....completed for ChunkID:",chunkID
    
   

    ##################################################################
    '''Get the top 10 matching jobs based on cosine for each candidate'''
    ##################################################################
    
    
    count = 0

    for user in users:
        
        count += 1    
        user_ctc=user['user_ctc']
        user_exp=user['user_experience']
        user_id = user['user_id']
        user_email = user['user_email']
        user_bow=user['user_bow']['bow']
        user_current_time = datetime.datetime.now()
        user_jobtitle = user['user_jobtitle']
        user_lastlogin = user['user_lastlogin']
        user_phone = user['user_phone']
        user_gender = user['user_gender']
        user_current_company = user['user_current_company']
        user_functionalarea_id = user['user_functionalarea_id']
        user_lastmodified  = user['user_lastmodified']
        user_fullname = user['user_fullname']
        user_phone_verified = user['user_phone_verified']
        user_location_id = user['user_location_id']
        user_ctc_id = user['user_ctc_id']
        user_highest_qual = user['user_highest_qual']
        user_edu_special = user['user_edu_special']
        user_email_verified = user['user_email_verified']
        user_spam_status = user['user_spam_status']
        user_bounce_status = user['user_bounce_status']
        user_email_alert_status = user['user_email_alert_status']
        user_functionalarea = user['user_functionalarea']
        user_industry  = user['user_industry']
        user_jobtitle = user['user_jobtitle']
        user_profiletitle = user['user_profiletitle']              
        user_edom = user['user_edom']
        user_industry  = user['user_industry']
        user_skills   = user['user_skills']
        user_profiletitle = user['user_profiletitle']
        user_pid = user['p']
        
        
        
        
        
        
        
        
        

        lsi_user = lsiModel[tfIdfModel[user_bow]]
        simScrChunk = index[lsi_user]                 
        sortingExcelSheetList=[]
        
        for (jobIntIndex, lsiCosine) in simScrChunk:
            
            if lsiCosine < 0.18:
                continue
            
            job = jobIntIdToJobDict[jobIntIndex]
            jobid = job['job_id']
            job_title = job['job_title']
            job_skills = job['job_skills']
            job_minsal=job['job_minsal']
            job_maxsal=job['job_maxsal']
            job_minexp=job['job_minexp']
            job_maxexp=job['job_maxexp']
            job_bow=job['job_bow']['bow']
            job_accounttype =job['job_accounttype']
            job_flag =job['job_flag']
        
            
            
            #######################################################
            ''' Calculating the CTC and Experience Match Scores'''
            #######################################################
            
            ctc_match_score=CTCMatchScore(job_minsal,job_maxsal,user_ctc).CTCMatchScore()
            exp_match_score=ExpMatchScore(job_minexp,job_maxexp,user_exp).ExpMatchScore()
            paid_boost = PaidBoostScore(job_flag,job_accounttype).PaidBoostScore()
            #ctc_match_score = 1
            #exp_match_score = 1
            paid_boost = 0
            
            
            #######################################################
            ''' For Low earning desperate guy uncomment this '''
            #######################################################
                
            '''
            if (1 + user_ctc)/(1 + user_exp) < 0.3: 
                ctc_match_score = 1
                exp_match_score = 1
            
            '''
            
            if ctc_match_score==1 and exp_match_score==1:
                jobid = job['job_id']

                try:
                    job_city = job['job_location']
                except:
                    job_city='Delhi'
                try:
                    user_city = user['user_location']
                except:
                    user_city='Delhi'
                
                #print user_city, job_city
                try:
                    user_city_list = user_city.lower().replace('other', '').strip().split(',')
                    user_city_list = [x.strip() for x in user_city_list]
                except:
                    user_city_list = ['']
                    
                try:
                    job_city_list = job_city.lower().replace('other', '').strip().split(',')
                    job_city_list = [x.strip() for x in job_city_list]
                except :
                    job_city_list= ['']
                #print user_city_list, job_city_list
                try:
                    cityScore = cm.getCityScore(user_city_list, job_city_list)
                except :
                    cityScore=0
              
                #if cityScore == 0:
                    #count = count +1
                    #print user_city_list, job_city_list, cityScore
                
                overallMatchScore = getOverallMatchScore(lsiCosine, cityScore,paid_boost) 
        
                
                
                
                s = (user_id, user_email, jobid, overallMatchScore, job_title, job_skills, job_minsal, job_maxsal, job_minexp, job_maxexp)
                sortingExcelSheetList.append(s)
                
            else:
                continue
                
            
            
        #################################
        '''Finding the top 10 Jobs'''
        #################################     
        topN = 10
        sortingExcelSheetListTopNJobs = heapq.nlargest(topN, sortingExcelSheetList, key = lambda x: x[3])
        #pprint(sortingExcelSheetListTopNJobs)
        
        jobs2bsent = []
        for (user_id, user_email, jobid, overallMatchScore, job_title, job_skills, job_minsal, job_maxsal, job_minexp, job_maxexp) in sortingExcelSheetListTopNJobs:
            #print (userid, jobid, lsiCosine, job_title, job_skills, job_minsal, job_maxsal, job_minexp, job_maxexp)
            
            jobs2bsent.append(int(jobid))
        
        document = {"c":user_id,
                    "_id": user_email,
                    "m": user_phone,
                    "te": user_exp,
                    "cr": user_jobtitle,
                    "g": user_gender,
                    "cc":  user_current_company,
                    "fa": user_functionalarea,
                    "faid": user_functionalarea_id,
                    "pd": user_lastmodified,
                    "fn": user_fullname,
                    "cpv": user_phone_verified,
                    "sCLID": user_location_id,
                    "sASID": user_ctc_id,
                    "eq":  user_highest_qual,
                    "es":  user_edu_special,
                    "ev": user_email_verified, 
                    "ll": user_lastlogin,
                    "sal": user_ctc,
                    "edom": user_edom,
                    "t": user_current_time,
                    "mj": jobs2bsent,
                    "bj": [],
                    "oj": [],
                    "pid": user_pid,
                    "s": False
                    }


        
        if len(jobs2bsent) > 0:
            monconn_recommendations.saveToTable(document)

        #print 'Chunk:', chunkID, 'processed in:', time.ctime()

    monconn_recommendations.close()




if __name__ == '__main__':

    ######################################
    '''Start the timer'''
    ######################################
    os.system(' echo "Recommendations generation started... '  ' " | mutt -s "Revival Mailer Weekly" ashish4669@gmail.com ,himanshusolanki53@gmail.com')
    start_time = time.time()
    print "Started at time", start_time, "seconds"
    
    tablename = 'WeeklyMsgQueue'
    monconn_recommendations = MongoConnect(tablename, host='localhost', database='mailer_weekly')
    monconn_recommendations.dropTable()
    monconn_recommendations.close()
    
    
        
    ######################################             
    '''Load the LSI and tfidf models'''
    ######################################
    
    tfIdfModelFilename_unifiedtke = '/data/Projects/JobAlerts/Model/tfidf_model.tfidf'
    lsiModelFilename_unifiedtke = '/data/Projects/JobAlerts/Model/lsi_model.lsi'

    tfIdfModel=gensim.models.tfidfmodel.TfidfModel.load(tfIdfModelFilename_unifiedtke)
    lsiModel=models.lsimodel.LsiModel.load(lsiModelFilename_unifiedtke)
    
    
    
    ######################################             
    '''Load the city Mapping in RAM'''
    ######################################    
    
    cityScoreMatrixFilename ='../Features/CityScore/CityMatrix.json'
    stateCapitalMappingFileName = '../Features/CityScore/stateCapitalMapping.csv'
    cm = CityMatch(cityScoreMatrixFilename, stateCapitalMappingFileName)
    
    
    
    #########################################
    '''Fetch the jobs data from the database'''
    #########################################
    
    print "Fetching the jobs data from Mongodb"
    tablename="jobs_processed"
    monconn_jobs = MongoConnect(tablename, host='localhost', database='mailer_weekly')
    mongo_jobs_cur=monconn_jobs.getCursor()
    print "Fetching the jobs data from Mongodb....completed"
    
    myCondition = {}
    jobs = monconn_jobs.loadFromTable(myCondition)
    
    
    
    #########################################
    ''' Creating Index on Jobs'''
    #########################################
    
    jobs_bow = []
    i = 0
    jobIntIdToJobDict = {}
    
    for job in jobs:
        job_bow = job['job_bow']['bow']
        jobs_bow.append(job_bow)
        jobIntIdToJobDict[i] = job
        
        i += 1
        
    #pprint(jobIntIdToJobDict)
    #lsi_jobs = lsiModel[tfIdfModel[jobs_bow]]
        
    index = similarities.MatrixSimilarity(lsiModel[tfIdfModel[jobs_bow]])
    index.num_best = 300


    
    ###############################################################
    ''' Initiating Multiprocessing and computing Recommendations'''
    ###############################################################
    
    pprocessing = 1

    if pprocessing == 0:
        
        numChunks = 79
        
        for chunkID in xrange(0,numChunks):
            computeAlertsChunk(chunkID)
        
        #computeAlertsChunk(1737)

    if pprocessing == 1:

        #Define the number of chunks to break data into and number of concurrent threads
        numChunks = 79
        #numConcurrentThreads= multiprocessing.cpu_count()
        numConcurrentThreads = 6 #Change June 20 1:18AM
        print "numConcurrentThreads:",numConcurrentThreads

        chunkStart = 0
        chunkEnd = chunkStart + numChunks -1
        poolArgList=range(chunkStart, chunkEnd+1)
    
        print 'chunkStart:', chunkStart, 'chunkEnd:', chunkEnd
        #print poolArgList
        pool=multiprocessing.Pool(numConcurrentThreads)
        pool.map(computeAlertsChunk, poolArgList)
        pool.close()
        pool.join()
    
    end_time = time.time()
    time_taken = end_time - start_time
    print "End  at time", end_time, "seconds"
    os.system(' echo "Recommendations generated in' + str(end_time - start_time) + ' " | mutt -s "Revival Mailer Weekly" ashish4669@gmail.com ,himanshusolanki53@gmail.com')