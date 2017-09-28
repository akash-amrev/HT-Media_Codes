__author__  = 'Ashish.Jain'
__date__ = '28-Oct-2015'


#########################################################################################################             
############-----------------  Summary of Script
#########################################################################################################
#--> For each candidate in 'candidates_processed' collection in 'mailer_monthly' database (172.22.66.233)
#    best matching jobs are calculated from a pool of jobs stored in 'jobs_processed'
#    collection in 'mailer_monthly' database.
#--> All the Jobs are indexed so as to make computation for each candidate fast enough
#--> Multiprocessing is then initiated chunkwise for candidate pool
#--> This LSI of user is compared with all the jobs and cosine score is determined 
#--> Top 300 Jobs are determined based on that matching score (300 are calculated so that after business 
#    rules the no. is enough to be sent in mailer
#--> All those Jobs which satisfy the business filters (CTCscore, EXPscore) HARD FILTERS,
#    CityScore and paidBoost Score is calculated for that Job along with the matching score.
#--> Final aggregated score is calculated based on Matching Score, CityScore, paidBoost 
#--> Sorting is done on the basis of Aggregated score
#--> Out of these Jobs top 10 jobs are extracted 
#--> Final recommendation are stored in Mongo (172.22.66.233) in 'MonthlyMsgQueue' collection in 
#    'mailer_monthly' database.
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

#Custom Modules
from Features.CTCMatchScore.CTCMatchScore import CTCMatchScore      #Custom Module - /data/Projects/RevivalMailerMonthly/Features/CTCMatchScore/CTCMatchScore.py
from Features.ExpMatchScore.ExpMatchScore import ExpMatchScore      #Custom Module - /data/Projects/RevivalMailerMonthly/Features/ExpMatchScore/ExpMatchScore_v1.py
from Features.PaidBoostScore.PaidBoostScore import PaidBoostScore   #Custom Module - /data/Projects/RevivalMailerMonthly/Features/PaidBoostScore/PaidBoostScore.py
from Features.CityScore.getCityScore import CityMatch               #Custom Module - /data/Projects/RevivalMailerMonthly/Features/CityScore/getCityScore.py
from Model.getOverallMatchScore import getOverallMatchScore         #Custom Module - /data/Projects/RevivalMailerMonthly/Model/getOverallMatchScore.py
from DataConnections.MySQLConnect.MySQLConnect import MySQLConnect  #Custom Module - /data/Projects/RevivalMailerMonthly/DataConnections/MySQLConnect/MySQLConnect.py
from DataConnections.MongoConnect.MongoConnect import MongoConnect  #Custom Module - /data/Projects/RevivalMailerMonthly/DataConnections/MongoConnect/MongoConnect.py
from Utils.Utils_1 import cleanToken                                #Custom Module - /data/Projects/RevivalMailerMonthly/Utils/Utils_1.py
from Utils.HtmlCleaner import HTMLStripper                          #Custom Module - /data/Projects/RevivalMailerMonthly/Utils/HtmlCleaner.py
from Notifier.Notifier import send_email                            #Custom Module - /data/Projects/RevivalMailerMonthly/Notifier/Notifier.py




#########################################################################################################             
############-----------------Function to generate recommendations
#########################################################################################################

def computeAlertsChunk(chunkID):
    
    #########################################################################################################             
    ############-----------------Creating a connection to output mongodb
    #########################################################################################################
    tablename = 'MonthlyMsgQueue'
    monconn_recommendations = MongoConnect(tablename, host='localhost', database='mailer_monthly')
    
    print 'Chunk:', chunkID, 'initiated at:', time.ctime()
   
   

    ifile = open('CompanyNames.csv','r')
    reader = csv.reader(ifile)
    company_dict = {}
    for row in reader:
        company_dict[row[0]] = row[1]
        
   
    #########################################################################################################             
    ############-----------------Fetch the user data from the database
    #########################################################################################################    
    tablename = "candidates_processed"
    monconn_users = MongoConnect(tablename, host='localhost', database='mailer_monthly')
    mongo_users_cur=monconn_users.getCursor()
        
    myCondition = {'p':chunkID}
    #myCondition = {}
    users = monconn_users.loadFromTable(myCondition)
    #print "Fetching the users data from Mongodb....completed for ChunkID:",chunkID
    
   

    #########################################################################################################             
    ############-----------------Loop to generate recommendations and save in Mongo
    #########################################################################################################     
    count = 0

    for user in users:


        #########################################################################################################             
        ############-----------------Extracting the user details
        #########################################################################################################     
        
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
        user_firstname = user_fullname.split(" ")[0]
        
        
        
        
        

        lsi_user = lsiModel[tfIdfModel[user_bow]]
        simScrChunk = index[lsi_user]                 
        sortingExcelSheetList=[]
        
        for (jobIntIndex, lsiCosine) in simScrChunk:
            
            if lsiCosine < 0.18:
                continue


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
              

                
                #########################################################################################################             
                ############-----------------Calculating the overall match score and appending the details to the list 
                ############-----------------based on job's published date
                #########################################################################################################                             
                overallMatchScore = getOverallMatchScore(lsiCosine, cityScore,paid_boost) 
        
                
                
                
                s = (user_id, user_email, jobid, overallMatchScore, job_title, job_skills, job_minsal, job_maxsal, job_minexp, job_maxexp,job_companyid)
                sortingExcelSheetList.append(s)
                
            else:
                continue
                
            
            
        ##############################################################################################################             
        ############-----------------Finding the top 10 Jobs based on Overall Score
        ##############################################################################################################  
        topN = 30
        sortingExcelSheetListTopNJobs = heapq.nlargest(topN, sortingExcelSheetList, key = lambda x: x[3])
        #pprint(sortingExcelSheetListTopNJobs)
        
        jobs2bsent = []
        company_ids = []
        cosine_score = []
        for (user_id, user_email, jobid, overallMatchScore, job_title, job_skills, job_minsal, job_maxsal, job_minexp, job_maxexp,job_companyid) in sortingExcelSheetListTopNJobs:
            #print (userid, jobid, lsiCosine, job_title, job_skills, job_minsal, job_maxsal, job_minexp, job_maxexp)
            if job_companyid not in company_ids:
                company_ids.append(job_companyid)
                jobs2bsent.append(int(jobid))
                cosine_score.append(round(overallMatchScore,2))
            else:
                if company_ids.count(job_companyid)<3:
                    company_ids.append(job_companyid)
                    jobs2bsent.append(int(jobid))
                    cosine_score.append(round(overallMatchScore,2))
                else:
                    pass
            if len(jobs2bsent) >= 10:
                break
            else:
                pass
        #print user_id
        #print company_ids
        #print jobs2bsent
        
        companies = []
        #print company_ids
        for comp_id in company_dict.keys():
            
            if int(comp_id) in company_ids:
            
                companies.append(company_dict[comp_id])
                #print companies
                #print "Hello"
            else:
                pass



        
        ##############################################################################################################             
        ############-----------------Creating Subject Line for a candidate  
        ##############################################################################################################                                     \                    
            
        if len(companies) !=0:
            try:
                user_subject = user_firstname + ": "+ ', '.join(companies) +" and other top company jobs matching your profile"
                #print user_subject
            except Exception as e:
                pass
        else:
            try:
                if user_functionalarea == "Fresher (No Experience)":
                    user_subject = user_firstname + ", don't miss out on these new jobs"
                else:
                    user_subject = user_firstname + ", new " + user_functionalarea.replace(' /',',') +" jobs for you"
                #print user_subject
            except Exception as e:
                user_subject = user_firstname + ", don't miss out on these new jobs"
                
        
            

        ##############################################################################################################             
        ############-----------------Creating a document to be saved in mongo collection  
        ##############################################################################################################          
        
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
                    "cosine": cosine_score,
                    "edom": user_edom,
                    "t": user_current_time,
                    "mj": jobs2bsent,
                    "bj": [],
                    "oj": [],
                    "pid": user_pid,
                    "s": False,
                    "sub":user_subject
                    }

        

        ##############################################################################################################             
        ############-----------------Dumping the document in mongo collection if recommendations were generated
        ############################################################################################################## 
        
        if len(jobs2bsent) > 0:
            monconn_recommendations.saveToTable(document)

        #print 'Chunk:', chunkID, 'processed in:', time.ctime()

    monconn_recommendations.close()




#########################################################################################################             
############-----------------  Main Function
#########################################################################################################

if __name__ == '__main__':

    #########################################################################################################             
    ############----------------- Try Except to avoid Failure of code
    #########################################################################################################
    try:
        
        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Revival Monthly","Recommendation Generation Started !!")

        #########################################################################################################             
        ############-----------------  Start the timer
        #########################################################################################################
        start_time = time.time()
        print "Started at time", start_time, "seconds"
    
    
        #########################################################################################################             
        ############-----------------  Drop the existing collection
        #########################################################################################################        
        tablename = 'MonthlyMsgQueue'
        monconn_recommendations = MongoConnect(tablename, host='localhost', database='mailer_monthly')
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
        cityScoreMatrixFilename ='../Features/CityScore/CityMatrix.json'
        stateCapitalMappingFileName = '../Features/CityScore/stateCapitalMapping.csv'
        cm = CityMatch(cityScoreMatrixFilename, stateCapitalMappingFileName)
        
        
        
        #########################################################################################################             
        ############-----------------  Fetch the jobs data from the Mongo
        #########################################################################################################
        print "Fetching the jobs data from Mongodb"
        tablename="jobs_processed"
        monconn_jobs = MongoConnect(tablename, host='localhost', database='mailer_monthly')
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
            
        #pprint(jobIntIdToJobDict)
        #lsi_jobs = lsiModel[tfIdfModel[jobs_bow]]
            
        index = similarities.MatrixSimilarity(lsiModel[tfIdfModel[jobs_bow]])
        index.num_best = 300
    
    
        
        #########################################################################################################             
        ############-----------------  Initiating Multiprocessing and computing Recommendations
        #########################################################################################################       
        pprocessing = 1
        
        if pprocessing == 0:
            
            numChunks = 79
            
            for chunkID in xrange(0,numChunks):
                computeAlertsChunk(chunkID)
            
            #computeAlertsChunk(1737)
    
        if pprocessing == 1:
    
            #Define the number of chunks to break data into and number of concurrent threads
            numChunks = 80
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
        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Revival Monthly","Recommendation Generation Completed !!")
    except:
        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com','progressiveht@hindustantimes.com'],"Revival Monthly - Urgent!!!","Recommendation Generation Failed!!!!!\nCall Akash (+91-8527716555) or Kanika (+91-9560649296) asap.")