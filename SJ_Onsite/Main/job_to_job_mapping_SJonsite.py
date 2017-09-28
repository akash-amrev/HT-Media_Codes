__author__  = 'Ashish Jain'
__date__ = '03-Feb-2016'


#################################################################################################################           
############-----------------  Summary of Script
#################################################################################################################

#--> This script generates the similar jobs
#--> Loads the 1month jobs from Mongo (172.22.66.233)
#--> Create indexing on the 1month Jobs
#--> Load the Models
#--> Calculate the cosine score of a 3month Job with each 1month Job
#--> Calculate the city,Salary and Experience score for the same 
#--> Evaluate the overall score from the above scores
#--> Sort and find the Top 10 1month jobs for a 3month job based on this overall score
#--> Dump the similar jobs to Mongo (172.22.66.233)



#################################################################################################################            
############-----------------Importing Python Libraries and Modules
#################################################################################################################
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
import datetime
import os

from Features.CTCMatchScore.CTCMatchScore import CTCMatchScore      #Custom Module - /data/Projects/JobAlerts/Features/CTCMatchScore/CTCMatchScore.py
from Features.ExpMatchScore.ExpMatchScore import ExpMatchScore      #Custom Module - /data/Projects/JobAlerts/Features/ExpMatchScore/ExpMatchScore_v1.py
from Features.CityScore.getCityScore import CityMatch               #Custom Module - /data/Projects/JobAlerts/Features/CityScore/getCityScore.py
from Features.LSI_common.MyBOW import MyBOW                         #Custom Module - /data/Projects/JobAlerts/Features/LSI_common/MyBOW.py
from Model.getOverallMatchScore import getOverallMatchScore         #Custom Module - /data/Projects/JobAlerts/Model/getOverallMatchScore.py
from Main.getLSICosine import getLSICosine                          #Custom Module - /data/Projects/JobAlerts/Main/getLSICosine.py
from DataConnections.MySQLConnect.MySQLConnect import MySQLConnect  #Custom Module - /data/Projects/JobAlerts/DataConnections/MySQLConnect/MySQLConnect.py
from DataConnections.MongoConnect.MongoConnect import MongoConnect  #Custom Module - /data/Projects/JobAlerts/DataConnections/MongoConnect/MongoConnect.py
from Utils.Utils_1 import cleanToken                                #Custom Module - /data/Projects/JobAlerts/Utils/Utils_1.py
from Utils.HtmlCleaner import HTMLStripper                          #Custom Module - /data/Projects/JobAlerts/Utils/HtmlCleaner.py
from Notifier.Notifier import send_email                            #Custom Module - /data/Projects/JobAlerts/Notifier/Notifier.py




#########################################################################################################             
############-----------------Function to generate recommendations
#########################################################################################################

def computeAlertsChunk(chunkID):


    #########################################################################################################             
    ############-----------------Creating a connection to output mongodb
    #########################################################################################################
    tablename = 'JobSuggestions'
    monconn_recommendations = MongoConnect(tablename, host='localhost', database='similar_jobs_onsite')
    
    print 'Chunk:', chunkID, 'initiated at:', time.ctime()


   

    #########################################################################################################             
    ############-----------------Fetch the 3 month jobs data from mongo
    #########################################################################################################
    tablename="active_jobs_dump"
    monconn_jobs_1 = MongoConnect(tablename, host='localhost', database='similar_jobs_onsite')
    mongo_jobs_1_cur=monconn_jobs_1.getCursor()
    myCondition = {'pid':chunkID}
    jobs_1 = monconn_jobs_1.loadFromTable(myCondition)
    
    
    
    #########################################################################################################             
    ############-----------------Calculating the overall score of a 3month jobs based on cosine,ctc,
    ############-----------------experience,city scores for each 1month Job
    #########################################################################################################
    

    count = 0
 
    for job_1 in jobs_1:
        count += 1
        jobid_1 = job_1['job_id']
        job_title_1 = job_1['job_title']
        job_skills_1 = job_1['job_skills']
        job_minsal_1=job_1['job_minsal']
        job_maxsal_1=job_1['job_maxsal']
        job_minexp_1=job_1['job_minexp']
        job_maxexp_1=job_1['job_maxexp']
        job_bow_1=job_1['job_bow']['bow']
        job_index_1 = job_1['job_index']
                
        lsi_job_1 = lsiModel[tfIdfModel[job_bow_1]]
        simScrChunk = index[lsi_job_1] 
                
        sortingExcelSheetList=[]
        
        for (jobIntIndex, lsiCosine) in simScrChunk:
            
            job = jobIntIdToJobDict[jobIntIndex]
            jobid = job['job_id']
            job_title = job['job_title']
            job_skills = job['job_skills']
            job_minsal=job['job_minsal']
            job_maxsal=job['job_maxsal']
            job_minexp=job['job_minexp']
            job_maxexp=job['job_maxexp']
            job_bow=job['job_bow']['bow']
            job_index = job['job_index']
            job_company_id = job['job_company_id']
        
        
        
            
            #########################################################################################################             
            ############-----------------Calculating the CTC and Experience and City Match Scores
            #########################################################################################################
            
            
            ctc_match=CTCMatchScore(job_minsal_1,job_maxsal_1,job_minsal,job_maxsal)
            ctc_match_score = ctc_match.CTCMatchScore()
            exp_match_score=ExpMatchScore(job_minexp_1,job_maxexp_1,job_minexp,job_maxexp).ExpMatchScore()
            paid_boost = 0
            if ctc_match_score==1 and exp_match_score==1:            
                if jobid != jobid_1:
                    try :
                        job_city_1 = job_1['job_location']
                    except:
                        job_city_1 = ["Delhi"]
                    
                    try:
                        job_city = job['job_location']
                    except:
                        job_city = ["Delhi"]
                        
                    #lsiCosine = getLSICosine(user_bow, job_bow).getLSICosine()
                    try:
                        cityScore = cm.getCityScore(job_city_1, job_city)
                    except :
                        cityScore=0
                    
                    overallMatchScore = getOverallMatchScore(lsiCosine, cityScore,paid_boost)
                    s = (jobid_1,job_index_1,jobid, job_index,overallMatchScore,job_company_id)
                    sortingExcelSheetList.append(s)
                    
                else:
                    continue
            else:
                continue
                    
            

        #########################################################################################################             
        ############-----------------Finding the top 10 Jobs based on overall sccore
        #########################################################################################################

        topN = 30
        sortingExcelSheetListTopNJobs = heapq.nlargest(topN, sortingExcelSheetList, key = lambda x: x[4])
        
        jobs2bsent = []
        company_ids = []
        for (jobid_1,job_index_1,jobid, job_index,overallMatchScore,job_company_id) in sortingExcelSheetListTopNJobs:
            if job_company_id not in company_ids:
                company_ids.append(job_company_id)
                jobs2bsent.append(int(jobid))
            else:
                if company_ids.count(job_company_id)<2:
                    company_ids.append(job_company_id)
                    jobs2bsent.append(int(jobid))
                else:
                    pass
                
            
            
                
            if len(jobs2bsent)>= 10:
                break
            else:
                pass
            
        
        ##############################################################################################################             
        ############-----------------Creating a document to be saved in mongo collection  
        ##############################################################################################################                                     \                
        document = {'_id':jobid_1,
                    'sj': jobs2bsent,
                    'sjlen':len(jobs2bsent),
                    'lud': datetime.datetime.now()
                    
                    
                    
                    }     


        ##############################################################################################################             
        ############-----------------Dumping the document in mongo collection if recommendations were generated
        ############################################################################################################## 
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
        
        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"SJ Onsite","Similar Jobs Onsite Creation started !! ")

        
        #########################################################################################################             
        ############-----------------  Start the timer
        #########################################################################################################
        start_time = time.time()
        print "Started at time", start_time, "seconds"
    
    
    
        #########################################################################################################             
        ############-----------------  Remove the previous Mongo dump of Similar Jobs
        #########################################################################################################
        tablename = 'JobSuggestions'
        monconn_recommendations = MongoConnect(tablename, host='localhost', database='similar_jobs_onsite')
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
        ############-----------------  Fetch the 1 month jobs data from mongo
        #########################################################################################################
        print "Fetching the jobs data from Mongodb"
        tablename="new_jobs_dump"
        monconn_jobs = MongoConnect(tablename, host='localhost', database='similar_jobs_onsite')
        mongo_jobs_cur=monconn_jobs.getCursor()
        print "Fetching the jobs data from Mongodb....completed"
        myCondition = {}
        jobs = monconn_jobs.loadFromTable(myCondition)
        
        
        
        #########################################################################################################             
        ############-----------------  Creating Index on 1 month jobs
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
            numChunks = 10000
            
            for chunkID in xrange(0,1):
                computeAlertsChunk(chunkID)
            
    
        if pprocessing == 1:
            numChunks = 5000
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

        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"SJ Onsite","Similar Jobs Onsite Creation Completed. !!")
      
    except:
        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com','progressiveht@hindustantimes.com'],"Urgent!!!","SJ Onsite : Similar Jobs Computation Failed !!!!!\nCall Kanika (+91-9560649296) or Akash (+91-8527716555) asap.")
        