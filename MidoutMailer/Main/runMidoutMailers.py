__author__  = 'Ashish.Jain'
__date__ = '10-Dec-2015'


#########################################################################################################             
############-----------------  Summary of Script
#########################################################################################################
#--> For each candidate in 'candidates_processed_midout' collection in 'mailer_daily_midout' database 
#    (172.22.66.233) best matching jobs are calculated from a pool of jobs stored in 'jobs_processed_midout'
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
#--> Final recommendation are stored in Mongo (172.22.66.233) in 'DailyMsgQueue' collection in 
#    'mailer_daily_midout' database.
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
import os
import multiprocessing
from rfc3339 import rfc3339
import datetime

from Features.CTCMatchScore.CTCMatchScore import CTCMatchScore      #Custom Module - /data/Projects/MidoutMailer/Features/CTCMatchScore/CTCMatchScore.py
from Features.ExpMatchScore.ExpMatchScore import ExpMatchScore      #Custom Module - /data/Projects/MidoutMailer/Features/ExpMatchScore/ExpMatchScore_v1.py
from Features.CityScore.getCityScore import CityMatch               #Custom Module - /data/Projects/MidoutMailer/Features/CityScore/getCityScore.py
from Model.getOverallMatchScore import getOverallMatchScore         #Custom Module - /data/Projects/MidoutMailer/Model/getOverallMatchScore.py
from DataConnections.MySQLConnect.MySQLConnect import MySQLConnect  #Custom Module - /data/Projects/MidoutMailer/DataConnections/MySQLConnect/MySQLConnect.py
from DataConnections.MongoConnect.MongoConnect import MongoConnect  #Custom Module - /data/Projects/MidoutMailer/DataConnections/MongoConnect/MongoConnect.py
from Utils.Utils_1 import cleanToken                                #Custom Module - /data/Projects/MidoutMailer/Utils/Utils_1.py
from Utils.HtmlCleaner import HTMLStripper,cleanHTML                #Custom Module - /data/Projects/MidoutMailer/Utils/HtmlCleaner.py
from Features.LSI_common.MyBOW import MyBOW                         #Custom Module - /data/Projects/MidoutMailer/Features/LSI_common/MyBOW.py
from Notifier.Notifier import send_email                            #Custom Module - /data/Projects/MidoutMailer/Notifier/Notifier.py




#########################################################################################################             
############-----------------Function to generate recommendations
#########################################################################################################

def computeAlertsChunk(chunkID):
  
    #########################################################################################################             
    ############-----------------Creating a connection to output mongodb
    #########################################################################################################
    tablename = 'DailyMsgQueue'
    monconn_recommendations = MongoConnect(tablename, host='localhost', database='mailer_daily_midout')
    print 'Chunk:', chunkID, 'initiated at:', time.ctime()
   
   
   
    #########################################################################################################             
    ############-----------------Fetch the user data from the database
    #########################################################################################################    
    tablename="candidates_processed_midout"
    monconn_users = MongoConnect(tablename, host='localhost', database='Midout_Mailers')
    mongo_users_cur=monconn_users.getCursor()
        
    myCondition = {'pid':chunkID}
    users = monconn_users.loadFromTable(myCondition)
    
    
   
   
    #########################################################################################################             
    ############-----------------Loop to generate recommendations and save in Mongo
    #########################################################################################################     
    count = 0
    for user in users:
        count += 1

        #########################################################################################################             
        ############-----------------Extracting the user details
        #########################################################################################################     
        
        _id = user['user_email']
        user_ctc=user['user_ctc']
        user_exp=user['user_experience']
        user_id = user['user_id']
        user_email = user['user_email']
        user_bow=user['user_bow']['bow']
        user_current_time = datetime.datetime.now()
        user_countrycode = user.get('user_countrycode','')
        user_pid = user['pid']
        user_phone_number = "+" + str(user_countrycode)+"-"+str(user.get('user_phone_number',''))
        user_firstname = user.get('user_firstname','')
        user_lastname = user.get('user_lastname','')
        user_fullname = user_firstname +" "+user_lastname

        if len(user_fullname) < 3:
            user_fullname = "Candidate"
        
        try:
            user_registration_start_date = rfc3339(user.get('user_registration_start_date',''), utc =True)
        except:
            user_registration_start_date = ""
            
        
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
        
        
        
            #########################################################################################################             
            ############-----------------Calculating the CTC and Experience Match Scores
            #########################################################################################################                                         
            ctc_match_score=CTCMatchScore(job_minsal,job_maxsal,user_ctc).CTCMatchScore()
            exp_match_score=ExpMatchScore(job_minexp,job_maxexp,user_exp).ExpMatchScore()
            
            
            
            #########################################################################################################             
            ############-----------------Calculating the City Score between a candidate and a job
            #########################################################################################################                                         
                
            if ctc_match_score==1 and exp_match_score==1:
                jobid = job['job_id']
                #lsiCosine = getLSICosine(user_bow, job_bow).getLSICosine()
                
                #City Score
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
                paidboost = 0


                #########################################################################################################             
                ############-----------------Calculating the overall match score  
                #########################################################################################################                             
                overallMatchScore = getOverallMatchScore(lsiCosine, cityScore,paidboost)
                    
                s = (user_id, user_email, jobid, overallMatchScore, job_title, job_skills, job_minsal, job_maxsal, job_minexp, job_maxexp)
                sortingExcelSheetList.append(s)
                
            else:
                continue
            
                
            
        ##############################################################################################################             
        ############-----------------Finding the top 10 Jobs based on Overall Score
        ##############################################################################################################  
        topN = 10
        sortingExcelSheetListTopNJobs = heapq.nlargest(topN, sortingExcelSheetList, key = lambda x: x[3])

        
        jobs2bsent = []
        for (user_id, user_email, jobid, overallMatchScore, job_title, job_skills, job_minsal, job_maxsal, job_minexp, job_maxexp) in sortingExcelSheetListTopNJobs:
            
            jobs2bsent.append(int(jobid))
        


        ##############################################################################################################             
        ############-----------------Creating a document to be saved in mongo collection  
        ##############################################################################################################          
        
        document = {"_id":user_email, \
                    "fn" : user_fullname,\
                    "c": user_id,\
                    "mj":jobs2bsent, \
                    "oj":[], \
                    "s": False ,\
                    "t": user_current_time ,\
                    "pid": user_pid,\
                    "m": user_phone_number, \
                    "pd":user_registration_start_date
                    }
        


        ##############################################################################################################             
        ############-----------------Dumping the document in mongo collection if recommendations were generated
        ############################################################################################################## 
        
        if len(jobs2bsent) > 0:
            monconn_recommendations.saveToTable(document)



    monconn_recommendations.close()







#########################################################################################################             
############-----------------  Main Function
#########################################################################################################

if __name__ == '__main__':

    #########################################################################################################             
    ############----------------- Try Except to avoid Failure of code
    #########################################################################################################
    
    try:
        
        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Midout Mailers","Recommendation Generation Started !!")
        
    
    
    
        #########################################################################################################             
        ############-----------------  Start the timer
        #########################################################################################################
        start_time = time.time()
        print "Started at time", start_time, "seconds"
        
    
        #########################################################################################################             
        ############-----------------  Drop the existing collection
        #########################################################################################################                
        tablename = 'DailyMsgQueue'
        monconn_recommendations = MongoConnect(tablename, host='localhost', database='mailer_daily_midout')
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
        tablename="jobs_processed_midout"
        monconn_jobs = MongoConnect(tablename, host='localhost', database='Midout_Mailers')
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
        pprocessing = 0
    
        if pprocessing == 0:
            numChunks = 79
            computeAlertsChunk(0)
    
    
        if pprocessing == 1:
            numChunks = 80
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
    
            
        end_time = time.time()
        time_taken = end_time - start_time
    
        
        
        #########################################################################################################             
        ############-----------------  Checks implemented for Jobs Count
        #########################################################################################################       
        print "Fetching the count of jobs data from Mongodb"
        tablename="jobs_processed_midout"
        monconn_jobs = MongoConnect(tablename, host='localhost', database='Midout_Mailers')
        mongo_jobs_cur=monconn_jobs.getCursor()
        job_count = mongo_jobs_cur.count()
        monconn_jobs.close()
        print job_count
        
        print 'Fetching the count of jobs dump for tech data from Mongodb'
        tableName = 'JobDesc_daily'
        monconn_jobs_local = MongoConnect(tableName, host = 'localhost', database = 'JobDescDB')
        monconn_jobs_local_cur = monconn_jobs_local.getCursor()
        job_tech_count = monconn_jobs_local_cur.count()
        monconn_jobs_local.close()
        print job_tech_count
        
        if job_tech_count <job_count :
            print "Fail"
        else:
            print "Success"
        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Midout Mailers","Recommendation Generation Completed !!")
     
    except Exception as e :
        print "********************************************************************************************"
        print '\n\n'
        print e
        print '\n\n'
        print "********************************************************************************************"

        
        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com','progressiveht@hindustantimes.com'],"Midout Mailers -Urgent!!!","Recommendation Generation Failed!!!!!\nCall Akash (+91-8527716555) or Kanika (+91-9560649296) asap.")

    

    
    
    
    
    
    
    
    
    
    
  
