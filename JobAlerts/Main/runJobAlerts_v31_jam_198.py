#!/usr/bin/python
import abc
__author__="Ashish.Jain"
__date__ ="07 Mar 2017"


#########################################################################################################             
############-----------------  Summary of Script
#########################################################################################################
#--> For each candidate in 'candidates_processed_5' collection in 'JobAlerts' database (172.22.66.233)
#    best matching jobs are calculated from a pool of last 7 days active jobs stored in 'jobs_processed'
#    collection in 'JobAlerts' database.
#--> All the Jobs are indexed so as to make computation for each candidate fast enough
#--> Multiprocessing is then initiated chunkwise for candidate pool
#--> Candidate's Bag of Words are calculated on the go and LSI vector is created .
#--> This LSI of user is compared with all the jobs and cosine score is determined 
#--> Top 300 Jobs are determined based on that matching score (300 are calculated so that after business 
#    rules the no. is enough to be sent in mailer
#--> All those Jobs which satisfy the business filters (Similar FA's , CTCscore, EXPscore) HARD FILTERS,
#    CityScore and paidBoost Score is calculated for that Job along with the matching score.
#--> Final aggregated score is calculated based on Matching Score, CityScore, paidBoost , SimilarFAscore
#--> Sorting is done on the basis of Aggregated score
#--> Out of these Jobs if 15 Paid Jobs are available then those are sent in mail, If Paid jobs are less 
#    than 15 then top free jobs are added to make the number 15
#--> Final recommendation are stored in Mongo (172.22.66.233) in 'DailyMsgQueue' collection in 
#    'mailer_daily_analytics' database.
#--> Alerts check mailers implemented to inform in case of any failure




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
import traceback
from pymongo import Connection
import pymongo
from pymongo.database import Database

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
from Features.JobAlert_Functions import *                           #Custom Module - /data/Projects/JobAlerts/Features/JobAlert_Functions.py
from Notifier.Notifier import send_email                            #Custom Module - /data/Projects/JobAlerts/Notifier/Notifier.py



#########################################################################################################             
############-----------------Function to generate recommendations
#########################################################################################################

def computeAlertsChunk(chunkID):
    check = []
    #########################################################################################################             
    ############-----------------Creating a connection to output mongodb
    #########################################################################################################
    
    tablename = 'DailyMsgQueue'
    monconn_recommendations = MongoConnect(tablename, host='172.22.66.198', database='mailer_daily_analytics')    
    #monconn_recommendations = MongoConnect(tablename, host='localhost', database='mailer_daily_analytics')
    print 'Chunk:', chunkID, 'initiated at:', time.ctime()
    date_4days = datetime.datetime.now() - datetime.timedelta(days=4)
    
  
    #########################################################################################################             
    ############-----------------Fetch the user data from the database
    #########################################################################################################    
    tablename = "candidates_processed_5"
    monconn_users =  MongoConnect(tablename, host='172.22.66.198', database='JobAlerts')
    mongo_users_cur=monconn_users.getCursor()
    myCondition = {'p':chunkID}
    users = monconn_users.loadFromTable(myCondition)
    
    #########################################################################################################             
    ############-----------------Loop to generate recommendations and save in Mongo
    #########################################################################################################     
    count = 0
    for user in users:
        user_check = False
        #start = time.clock()
        count += 1    
        
        
        if count > 159 :
            count = 0
        
        #count = count%160

        #########################################################################################################             
        ############-----------------Extracting the user details
        #########################################################################################################     
        user_ctc=user['user_ctc']
        user_exp=user['user_experience']
        user_id = user['user_id']
        user_email = user['user_email']
        user_current_time = datetime.datetime.now()
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
        user_applications = user["application_list"] 
        user_edom = user['user_edom']
        user_pid = user['p']
        user_firstname = user_fullname.split(" ")[0]
        
        
        resume_jobtitle = user["resume_jobtitle"]
        user_skills   = user['user_skills']        
        resume_skills = user["resume_skills"]
        user_jobtitle = user['user_jobtitle']      
        user_profiletitle = user['user_profiletitle']
        user_industry  = user['user_industry']
        user_functionalarea = user['user_functionalarea']
        subject_status = user["subject_status"]
        actual_faid_user = user["actual_faid"]
        preferred_subfaid = user["preferred_sub_faid"]
        preferred_subfa = user["preferred_sub_fa"]
        application_count = user["application_count"]
        

        
        
        #########################################################################################################             
        ############----------------- Try Except in case user's FA id is 'null' (some cases)
        #########################################################################################################     
        similar_fa_dict = {}
        try:
            similar_fa_dict =  similar_fa[actual_faid_user]
            user_check = True
        except:
            pass
        

        #########################################################################################################             
        ############----------------- Creating Bag of Words of a user depending on his subject_status
        #########################################################################################################     
        if subject_status == 1:
            text = 4*(" "+cleanText(user_profiletitle)) +' ' + 6*(" "+cleanText(user_skills)) +' ' + 3*(" "+cleanText(user_jobtitle))  +' ' +  1*(" "+cleanText(user_functionalarea))         
        else:
            text = 4*(" "+cleanText(user_profiletitle)) +' ' + 5*(" "+cleanText(user_skills)) +' ' + 3*(" "+cleanText(user_jobtitle))  +' ' +  2*(" "+cleanText(preferred_subfa))
        text =  re.sub(' +',' ',text).strip()
        user_bow  = mb.getBow(text, getbowdict = 1)['bow'] 




        #########################################################################################################             
        ############-----------------Creating LSI for user's BOW and finding the best 300 jobs based on a 
        ############-----------------based on a matching score 
        #########################################################################################################     
        lsi_user = lsiModel[tfIdfModel[user_bow]]
        simScrChunk = index[lsi_user]                 



        #########################################################################################################             
        ############-----------------Initializing the lists for recommended jobs
        #########################################################################################################     
        sortingExcelSheetList_new = []
        sortingExcelSheetList_new_back_office = []
        sortingExcelSheetList_old = []
        sortingExcelSheetList_old_back_office = []
        sortingExcelSheetList = []
        sortingExcelSheetList_backoffice = []

        

        for (jobIntIndex, lsiCosine) in simScrChunk:

            #########################################################################################################             
            ############-----------------Loading the Jobs Data
            #########################################################################################################                 
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
            job_companyname = job['job_company_name']
            job_companyid = job['job_company_id']
            job_published = job['job_published']
            job_republished = job['job_republished']
            job_back_office = job['job_back_office']
        


            #########################################################################################################             
            ############-----------------Cosine cutoff for jobs on the basis of published date
            #########################################################################################################    
            if lsiCosine <0.4 or job_published is None or job['job_faid'] == None or jobid in user_applications:
                continue
            
            
            if user_check == True:
                
                job_fa_list = job['job_faid'].split(',')            
                job_fa_list = [int(x) for x in job_fa_list]
                intersection = list(set(similar_fa_dict.keys()).intersection(job_fa_list))
                if len(intersection) == 0:
                    continue
                fa_score = findFA_Score(intersection,similar_fa_dict)
                
                
            else:
                fa_score = 0




            #########################################################################################################             
            ############-----------------Calculating the CTC and Experience Match Scores
            #########################################################################################################                             
            ctc_match_score=CTCMatchScore(job_minsal,job_maxsal,user_ctc).CTCMatchScore()
            exp_match_score=ExpMatchScore(job_minexp,job_maxexp,user_exp).ExpMatchScore()
            paid_boost = PaidBoostScore(job_flag,job_accounttype).PaidBoostScore()
            

            #########################################################################################################             
            ############-----------------Calculating the City Score between a candidate and a job
            #########################################################################################################                             
            if ctc_match_score==1 and exp_match_score==1:
                jobid = job['job_id']
                try:
                    job_city = job['job_location']
                except:
                    job_city=['Delhi']
                try:
                    user_city = user['user_location']
                except:
                    user_city=['Delhi']  
                try:
                    cityScore = cm.getCityScore(user_city, job_city)
                except :
                    cityScore=0
              
              
                #########################################################################################################             
                ############-----------------Calculating the overall match score and appending the details to the list 
                ############-----------------based on job's published date
                #########################################################################################################                             
                overallMatchScore = getOverallMatchScore(lsiCosine, cityScore,paid_boost) + lsiCosine*fa_score
                s = (user_id, user_email, jobid, overallMatchScore, job_companyid,job_published,lsiCosine,job_back_office)

                if job_flag == 'Paid':
                    sortingExcelSheetList.append(s)
                else:
                    sortingExcelSheetList_backoffice.append(s)
            else:
                continue
                
            
            
        
        topN = 60
        jobs2bsent = []
        company_ids = []
        cosine_score = []

        ##############################################################################################################             
        ############-----------------Finding the top 15 Jobs Non Back office posted with published date in last 3 days
        ##############################################################################################################  
        sortingExcelSheetListTopNJobs = heapq.nlargest(topN, sortingExcelSheetList, key = lambda x: x[3])
        for (user_id, user_email, jobid, overallMatchScore, job_companyid,job_published,lsiCosine,job_back_office) in sortingExcelSheetListTopNJobs:
            if job_companyid not in company_ids:
                company_ids.append(job_companyid)
                jobs2bsent.append(int(jobid))
                cosine_score.append(round(lsiCosine,2))
            else:
                if company_ids.count(job_companyid)<3:
                    company_ids.append(job_companyid)
                    jobs2bsent.append(int(jobid))
                    cosine_score.append(round(lsiCosine,2))
                else:
                    pass
            if len(jobs2bsent) >= 15:
                break
            else:
                pass
        len_paid = len(jobs2bsent)
        
        

        ##############################################################################################################             
        ############-----------------Appending the remaining jobs from old non back office posted jobs 
        ############-----------------incase 15 recent jobs were not found for the candidate
        ##############################################################################################################                                     
        sortingExcelSheetList_backoffice_TopNJobs = heapq.nlargest(topN, sortingExcelSheetList_backoffice, key = lambda x: x[3])
        for (user_id, user_email, jobid, overallMatchScore, job_companyid,job_published,lsiCosine,job_back_office) in sortingExcelSheetList_backoffice_TopNJobs:
            
            if len(jobs2bsent) >= 15:
                break
            else:
                pass

            if job_companyid not in company_ids:
                company_ids.append(job_companyid)
                jobs2bsent.append(int(jobid))
                cosine_score.append(round(lsiCosine,2))
            else:
                if company_ids.count(job_companyid)<3:
                    company_ids.append(job_companyid)
                    jobs2bsent.append(int(jobid))
                    cosine_score.append(round(lsiCosine,2))
                else:
                    pass
        
        len_free = len(jobs2bsent) - len_paid


        ##############################################################################################################             
        ############-----------------Creating Subject Line for a candidate  
        ##############################################################################################################                                     \        
        if subject_status == 1:
            user_subject = user_firstname + ", new " + user_functionalarea.replace(' /',',') +" jobs for you"
        elif subject_status == 2:
            user_subject = user_firstname + ", new " + preferred_subfa.replace(' /',',') +" jobs for you"
        else:
            user_subject = user_firstname + ", don't miss out on these new jobs"
            


        
        ##############################################################################################################             
        ############-----------------Creating a document to be saved in mongo collection  
        ##############################################################################################################                                     \        
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
                    "pid": count,
                    "s": False,
                    "sub":user_subject,
                    "cosine": cosine_score,
                    "len":len(jobs2bsent),
                    "len_paid": len_paid,
                    "len_free":len_free,

                    }



        ##############################################################################################################             
        ############-----------------Dumping the document in mongo collection if recommendations were generated
        ############################################################################################################## 
                                             
        if len(jobs2bsent) > 0:
            monconn_recommendations.saveToTable(document)
        
        
        
    monconn_recommendations.close()
    check.sort()



#########################################################################################################             
############-----------------  Main Function
#########################################################################################################
if __name__ == '__main__':

    
    #########################################################################################################             
    ############----------------- Try Except to avoid Failure of code
    #########################################################################################################
   
    #try:
    #send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Job Alert Mailer","Recommendations generation started !!!\nCollection Name : DailyMsgQueue\nDB Name : mailer_daily_analytics")
    
    
    #########################################################################################################             
    ############-----------------  Loading FA Lookup and Similar FA file
    #########################################################################################################
    fa_lookup = LoadFA_lookup('/data/Projects/JobAlerts_Improvements/Data/fa_lookup.csv')     
    similar_fa = LoadSimilarFA('/data/Projects/JobAlerts_Improvements/Data/Similar_FA.csv')
    
    

    #########################################################################################################             
    ############-----------------  Start the timer
    #########################################################################################################
    start_time = time.time()
    print "Started at time", start_time, "seconds"
    
    
    #########################################################################################################             
    ############-----------------  Drop the existing collection
    #########################################################################################################
    tablename = 'DailyMsgQueue'
    monconn_recommendations = MongoConnect(tablename, host='172.22.66.198', database='mailer_daily_analytics')
    #monconn_recommendations = MongoConnect(tablename, host='localhost', database='mailer_daily_analytics')
    monconn_recommendations.dropTable()
    monconn_recommendations.close()
    
    
    #########################################################################################################             
    ############-----------------  Load the LSI and tfidf models
    #########################################################################################################
    tfIdfModelFilename_unifiedtke = '/data/Projects/JobAlerts/Model/tfidf_model.tfidf'
    lsiModelFilename_unifiedtke = '/data/Projects/JobAlerts/Model/lsi_model.lsi'
    tfIdfModel=gensim.models.tfidfmodel.TfidfModel.load(tfIdfModelFilename_unifiedtke)
    lsiModel=models.lsimodel.LsiModel.load(lsiModelFilename_unifiedtke)
    
    
    #########################################################################################################             
    ############-----------------  Load the city Mapping in RAM
    #########################################################################################################
    cityScoreMatrixFilename ='/data/Projects/JobAlerts/Features/CityScore/CityMatrix.json'
    stateCapitalMappingFileName = '/data/Projects/JobAlerts/Features/CityScore/stateCapitalMapping.csv'
    cm = CityMatch(cityScoreMatrixFilename, stateCapitalMappingFileName)
    

    #########################################################################################################             
    ############-----------------  Loading the mappings for bow
    #########################################################################################################
    synMappingFileName = '/data/Projects/JobAlerts/Features/rawData/LSI/Model_UnifiedTKE/unifiedtkelist.csv'
    keywordIdMappingFileName = '/data/Projects/JobAlerts/Features/rawData/LSI/Model_UnifiedTKE/unifiedtkelist_numbered.csv' 
    mb = MyBOW(synMappingFileName, keywordIdMappingFileName)
 
    
    
    #########################################################################################################             
    ############-----------------  Fetch the jobs data from the Mongo
    #########################################################################################################
    print "Fetching the jobs data from Mongodb"
    tablename="jobs_processed"
    monconn_jobs = MongoConnect(tablename, host='172.22.66.198', database='JobAlerts')
    mongo_jobs_cur=monconn_jobs.getCursor()
    print "Fetching the jobs data from Mongodb....completed"        
    myCondition = {}
    jobs = monconn_jobs.loadFromTable(myCondition)
    
    
    #########################################################################################################             
    ############-----------------  Creating Index on Jobs
    #########################################################################################################
    jobs_bow = []
    i = 0
    jobIntIdToJobDict = {}        
    for job in jobs:
        job_bow = job['job_bow']['bow']
        jobs_bow.append(job_bow)
        jobIntIdToJobDict[i] = job
        i += 1
    index = similarities.MatrixSimilarity(lsiModel[tfIdfModel[jobs_bow]])
    index.num_best = 300


    
    #########################################################################################################             
    ############-----------------  Initiating Multiprocessing and computing Recommendations
    #########################################################################################################       
    pprocessing = 1

    if pprocessing == 0:            
        numChunks = 79
        computeAlertsChunk(0)
        #for chunkID in xrange(0,numChunks):
        #    computeAlertsChunk(chunkID)

    if pprocessing == 1:                                                    #######---------- Initiating Multiprocessing
        numChunks = 80                                                      #######---------- Define the number of chunks to break data into and number of concurrent threads
        numConcurrentThreads = 6 #Change June 20 1:18AM
        print "numConcurrentThreads:",numConcurrentThreads
        chunkStart = 0
        chunkEnd = chunkStart + numChunks -1
        poolArgList=range(chunkStart, chunkEnd+1)
        print 'chunkStart:', chunkStart, 'chunkEnd:', chunkEnd
        pool=multiprocessing.Pool(numConcurrentThreads)
        pool.map(computeAlertsChunk, poolArgList)
        pool.close()
        pool.join()
        
    
    #########################################################################################################             
    ############-----------------  Condition for Backup JAM stored in /Backup Location
    #########################################################################################################       
    ' 0-Monday , 1-Tuesday, 2-Wednesday ,3-Thursday ,4-Friday ,5-Saturday ,6-Sunday '       
    backup_days = [1,3,6]                                                   #######---------- Create a bac
    today_weekday = datetime.datetime.now().weekday()
    if today_weekday in backup_days:
        os.system('/usr/bin/mongodump -d mailer_daily_analytics -c DailyMsgQueue  -o /Backup')
        os.system('/usr/bin/mongodump -d JobDescDB_analytics -c JobDesc_analytics  -o /Backup')
    else:
        pass
    

    #########################################################################################################             
    ############-----------------  End the timer and generate completion mail
    #########################################################################################################       
    end_time = time.time()
    time_taken = end_time - start_time
    #send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Job Alert Mailer",'Recommendations generated in' + str(end_time - start_time))
#    except Exception as e:
#        print e
#        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com','progressiveht@hindustantimes.com'],"URGENT !!!","Job Alert Mailer : RECOMMENDATION GENERATION FAILED!!!!!\nCall Akash (+91-8527716555) or Kanika (+91-9560649296) asap.")
    
        






#########################################################################################################             
############-----------------  Changing pid's as decided by Rajat to optimizing Sending of JAM and SJ
############-----------------  on same day
#########################################################################################################       

def getMongoMaster():
        MONGO_HOST = '172.22.66.198'
        #MONGO_HOST = 'localhost'
        MONGO_PORT = 27017
        connection = Connection(MONGO_HOST,MONGO_PORT)  #######---------- Leave blank for local DB
        #db=Database(connection,'JobAlerts')
        db=Database(connection,'mailer_daily_analytics')
        #db.authenticate('sumoplus','sumoplus')
        return db
    
    
mongo_conn = getMongoMaster()
collection = getattr(mongo_conn,"DailyMsgQueue")
for i in range(8,73,16):
    print i,i+7
    collection.update({'pid':{'$gte':i,'$lte':i+7}},{'$inc':{'pid':-8}},upsert=False, multi=True)
