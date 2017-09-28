#!/usr/bin/python
__author__="Ashish.Jain"
__date__ ="26 Oct 2016"


#########################################################################################################             
############-----------------  Summary of Script
#########################################################################################################
#--> Extract last 7 days Live Jobs from recruiter SQL (172.22.66.204), processing the fields, cleaning 
#    textual fields, creating Bag of Words and storing the final document in 'jobs_processed' collection
#    in 'JobAlerts' database (172.22.66.233)
#--> Alerts check mailers implemented to inform in case of any failure




#########################################################################################################             
############-----------------Importing Python Libraries and Modules
#########################################################################################################
#Inbuilt Modules
import cmd
import sys
sys.path.append('./../')
from pprint import pprint
import pdb
from gensim import corpora, models, similarities
import csv
import time
from multiprocessing import Pool
import os

#Custom Modules
from DataConnections.MySQLConnect.MySQLConnect import MySQLConnect  #Custom Module - /data/Projects/JobAlerts/DataConnections/MySQLConnect/MySQLConnect.py
from DataConnections.MongoConnect.MongoConnect import MongoConnect  #Custom Module - /data/Projects/JobAlerts/DataConnections/MongoConnect/MongoConnect.py
from Utils.Utils_1 import cleanToken                                #Custom Module - /data/Projects/JobAlerts/Utils/Utils_1.py
from Utils.HtmlCleaner import HTMLStripper                          #Custom Module - /data/Projects/JobAlerts/Utils/HtmlCleaner.py
from Utils.Cleaning import *                                        #Custom Module - /data/Projects/JobAlerts/Utils/Cleaning.py
from Features.LSI_common.MyBOW import MyBOW                         #Custom Module - /data/Projects/JobAlerts/Features/LSI_common/MyBOW.py
from Notifier.Notifier import send_email                            #Custom Module - /data/Projects/JobAlerts/Notifier/Notifier.py 





#########################################################################################################             
############-----------------Function to remove duplicate elements
#########################################################################################################
def removeDup(x):
    
    try:
        skills = x.split(',')
        skills = [x.strip() for x in skills]
        
        x_list= list(set(skills))
        x_string=','.join(x_list)
        return x_string
    except :
        return " "
   


#########################################################################################################             
############-----------------Function to extract Jobs from SQL
#########################################################################################################
def preProcessChunk(chunkID):

    #########################################################################################################             
    ############-----------------    Creating a Mongo Connection to status collection
    #########################################################################################################
    print 'Connecting to Mongodb..'
    tableName = 'jobs_status_check'
    monconn_status_check = MongoConnect(tableName, host = 'localhost', database = 'jam_status')
    monconn_status_check_cur = monconn_status_check.getCursor()
    


    #########################################################################################################             
    ############-----------------    SQL Credentials
    #########################################################################################################
    '''
    host="172.22.65.157"
    user="analytics"
    password="Anal^tics@11"
    database="SumoPlus"
    unix_socket="/tmp/mysql.sock"
    port = 3308
    '''
    host="172.22.66.204"
    user="analytics"
    password="Anal^tics@11"
    database="SumoPlus"
    unix_socket="/tmp/mysql.sock"
    port = 3306

    
    #########################################################################################################             
    ############-----------------    Creating the SQL Query
    #########################################################################################################
    print "Loading Jobs From MySql...."
    mysql_conn = MySQLConnect(database, host, user, password, unix_socket, port)
    cmd1='''drop table if exists SumoPlus.XY'''
    cmd2='''create table SumoPlus.XY as 
         SELECT company_account_id,SUM(final_sale_price)as price,enabled,MAX(expiry_date)as expiry_date 
         from SumoPlus.backoffice_accountsales a1 
         where enabled in 
         (select min(enabled) from SumoPlus.backoffice_accountsales where a1.company_account_id=company_account_id)
         group by 1
        '''
    cmd3='''ALTER TABLE SumoPlus.XY add index company_account_id (company_account_id)'''
    cmd4='''SELECT
         rj.jobid as Jobid,
         rj.jobtitle as JobTitle,
         rj.description as JD,
         rj.isbocreated as back_office_job,
         rj.publisheddate as publisheddate,
         rj.republisheddate as republisheddate,
         rj.companyid_id as Company_id,
         rj.displayname as Company_name,
         la1.text_value_MAX as SalaryMax,
         la2.text_value_MIN as SalaryMin,
         le1.display as ExpMin,
         le2.display as ExpMax,
         li.industry_desc as Industry,
         group_concat(c.AttValueCustom,'') as keySkills,
         group_concat(fn.field_enu,'') as function,
         group_concat(fn.field_id,'') as faid,
         group_concat(l.city_desc,'') as location,
         group_concat(fn.sub_field_enu,'') as subfunction,
         case account_type
         when 0 THEN "Company"
         when 1 THEN "Consultant"
         when 2 THEN "Others"
         when 3 THEN "Enterprise"
         ELSE "Not Specified"
         END AS account_type,
         IF(XY.enabled = 1 AND XY.price != 0 AND XY.expiry_date > CURDATE(),'Paid','Free') AS 'flag'        
         
         from 
         (select * from recruiter_job 
            where recruiter_job.jobstatus in (3,9) 
            and (DATEDIFF( CURDATE(),DATE(recruiter_job.publisheddate)) < 8 OR DATEDIFF( CURDATE(),DATE(recruiter_job.republisheddate)) < 8) 
         ) AS rj 
         left join lookup_annualsalary AS la1 on rj.salarymax = la1.salary_id 
         left join  lookup_annualsalary AS la2 on rj.salarymin = la2.salary_id 
         left join lookup_experience AS le1 on rj.minexperience = le1.value 
         left join  lookup_experience AS le2 on rj.maxexperience = le2.value 
         left join recruiter_jobattribute as c on rj.jobid = c.jobid_id 
         left join  lookup_industry AS li on rj.industry=li.industry_id 
         left join lookup_subfunctionalarea_new163 AS fn on fn.sub_field_id = c.AttValue AND c.AttType = 12 
         left join lookup_city_new512 AS l on  l.city_id = c.AttValue AND c.AttType = 13 
         left join SumoPlus.XY AS XY on XY.company_account_id = rj.companyid_id
         left join SumoPlus.backoffice_companyaccount AS F on  F.id= rj.companyid_id       
         WHERE 
        
         c.AttType in (3,12,13) 
        
         group by rj.jobid
         '''
        
    cmd5= '''drop table if exists SumoPlus.XY
        '''



    #########################################################################################################             
    ############-----------------    Executing the SQL Query
    #########################################################################################################
    print 'chnukID:', chunkID, ': Loading jobs from SQL....', time.ctime()
    mysql_conn.query(cmd1)
    mysql_conn.query(cmd2)
    mysql_conn.query(cmd3)
    jobs = mysql_conn.query(cmd4)
    mysql_conn.query(cmd5)
    print 'chunkID:', chunkID,': Loading jobs from SQL....completed..', time.ctime()
    print 'chunkid:', chunkID, ' : Number of jobs loaded: ', len(jobs)



    #########################################################################################################             
    ############-----------------Connecting to Jobs Collections Mongo (172.22.66.233)
    #########################################################################################################
    print 'Connecting to Mongodb..'
    tableName = 'jobs_processed'
    monconn_jobs_local = MongoConnect(tableName, host = 'localhost', database = 'JobAlerts')
    monconn_jobs_local_cur = monconn_jobs_local.getCursor()
    print 'Connecting to Mongodb...finished'
 
 
    
    #########################################################################################################             
    ############-----------------Processing the Jobs data extracted from SQL
    #########################################################################################################
    i = 0
    for job in jobs:
        if i%1000 == 0:
            print '\tchunkID:', chunkID, ' numRecords:' , i,  ' completed in ', time.time() - start_time, ' seconds'
        job_id = job['Jobid']
        #job_title = cleanToken(job['JobTitle'])
        job_maxexp = cleanToken(job['ExpMax'])
        job_minexp = cleanToken(job['ExpMin'])  
        job_maxsal = cleanToken(job['SalaryMax'])
        job_minsal = cleanToken(job['SalaryMin'])  
        #job_jd = cleanHTML(cleanToken(job['JD']) )
        #job_industry = cleanToken(job['Industry'])
        job_location=removeDup(job['location'])
        #job_subfunction=removeDup(cleanToken(job['subfunction']))
        #job_function=removeDup(cleanToken(job['function']))
        #job_skills=removeDup(cleanToken(job['keySkills']))
        job_flag = job['flag']
        job_accounttype = job['account_type']
        job_company_id = job['Company_id']
        job_company_name = cleanToken(job['Company_name'])
        job_published_date = job['publisheddate']
        job_republished_date = job['republisheddate']
        #job_faid = job['faid']
        job_back_office = int(job['back_office_job'])
        job_location = job_location.replace(', ', ',').lower().split(',')        
        if job_company_id == 421880:                                            #######---------- Altimetrik Jobs removed 
            continue


        job_faid = job['faid']
        job_title = cleanText(job['JobTitle'])                                  #######---------- cleanText Function is present in Cleaning.py in Utils folder
        job_jd = cleanText(cleanHTML(job['JD'])) 
        job_industry = cleanText(job['Industry'])
        job_function=removeDup(cleanText(job['function']))
        job_subfunction=removeDup(cleanText(job['subfunction']))
        job_skills=removeDup(cleanText(job['keySkills']))
       
        
        
        #########################################################################################################             
        ############-----------------Creating Bag of Words for Text
        #########################################################################################################
        text = 5*(" "+job_title) + ' ' + 5*(" "+job_skills) + ' ' + 1*(" "+job_jd) +' '+2*(" "+job_function)+' '+2*(" "+job_subfunction)
        text =  re.sub(' +',' ',text).strip()
        '''
        try:
            text = 5*(" "+job_title) + ' ' + 5*(" "+job_skills.replace(',', ' ')) + ' ' + 1*(" "+job_jd) +' '+2*(" "+job_industry)+' '+2*(" "+job_function)+' '+2*(" "+job_subfunction)
        except:
            text = 5*(" "+job_title) + ' ' + 5*(" "+job_skills) + ' ' + 1*(" "+job_jd) +' '+2*(" "+job_industry)+' '+2*(" "+job_function)+' '+2*(" "+job_subfunction)
        '''
        #text = text.replace('candidates', ' ')                    
        job_bow = mb.getBow(text, getbowdict = 0)
        
        
    
        #########################################################################################################             
        ############-----------------Creating Job document to be saved in Mongo
        #########################################################################################################
        document = {'job_id': job_id, 'job_title': job_title,'job_function':job_function, \
             'job_maxexp': job_maxexp, 'job_minexp': job_minexp,\
             'job_location':job_location, 'job_subfunction':job_subfunction,\
             'job_maxsal':job_maxsal,'job_minsal':job_minsal, 'job_skills': job_skills, \
             'job_bow': job_bow, 'job_industry': job_industry, 'job_jd': job_jd, \
             'job_flag':job_flag,'job_accounttype':job_accounttype, \
             'job_company_id':job_company_id,'job_company_name':job_company_name,
             'job_published':job_published_date,'job_republished':job_republished_date,'job_back_office':job_back_office,'job_faid':job_faid
             }


        #########################################################################################################             
        ############-----------------Saving the document in Job collection Mongo (172.22.66.233)
        #########################################################################################################
        monconn_jobs_local.saveToTable(document)
        i += 1
        

    print "Processing finished....."    
    print 'chunkID:', chunkID, ' Total time taken is: ', time.time() - start_time, ' seconds.'
    end_time = time.time()
    time_taken = end_time - start_time
    send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Job Alert Mailer",'Jobs Processed '+str(i)+' in :' + str(end_time - start_time) + ' seconds')



    #########################################################################################################             
    ############-----------------Changing the status of completion and deleting the mongo connections
    #########################################################################################################
    del(monconn_jobs_local)
    del(mysql_conn)
    monconn_status_check.saveToTable({'_id':1 ,'status':1}) 
    del(monconn_status_check)   








#########################################################################################################             
############-----------------    Main Function
#########################################################################################################

if __name__ == '__main__':

    #########################################################################################################             
    ############----------------- Try Except to avoid Failure of code
    #########################################################################################################
    try:
        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Job Alert Mailer","Jobs Processing from SQL Started!!!")

        
        #########################################################################################################             
        ############-----------------    Start the timer
        #########################################################################################################
        print 'preProcessing Jobs...', time.ctime()
        start_time = time.time()
        htmls = HTMLStripper()
        
        
        #########################################################################################################             
        ############-----------------    Remove the completion status if already exist
        #########################################################################################################    
        print 'Connecting to Mongodb..'
        tableName = 'jobs_status_check'
        monconn_status_check = MongoConnect(tableName, host = 'localhost', database = 'jam_status')
        monconn_status_check_cur = monconn_status_check.getCursor()
        monconn_status_check.dropTable()
        del(monconn_status_check)
        #monconn_status_check.saveToTable({'_id':1,'status':0})
    
    
        
        #########################################################################################################             
        ############-----------------    Loading the mapping for Bag of Words
        #########################################################################################################                
        print 'Loading the mappings for bow'
        synMappingFileName = '../Features/rawData/LSI/Model_UnifiedTKE/unifiedtkelist.csv'
        keywordIdMappingFileName = '../Features/rawData/LSI/Model_UnifiedTKE/unifiedtkelist_numbered.csv' #This file is created
        mb = MyBOW(synMappingFileName, keywordIdMappingFileName)
        print 'Loading the mappings for bow...finished'
        


        #########################################################################################################             
        ############-----------------    Initiating the completion status to zero
        #########################################################################################################                    
        print 'Connecting to Mongodb..'
        tableName = 'jobs_status_check'
        monconn_status_check = MongoConnect(tableName, host = 'localhost', database = 'jam_status')
        monconn_status_check_cur = monconn_status_check.getCursor()
        monconn_status_check.saveToTable({'_id':1,'status':0})
        
        

        #########################################################################################################             
        ############-----------------    Dropping the existing collection of Jobs
        #########################################################################################################                
        print 'Connecting to Mongodb..'
        tableName = 'jobs_processed'
        monconn_jobs_local = MongoConnect(tableName, host = 'localhost', database = 'JobAlerts')
        monconn_jobs_local_cur = monconn_jobs_local.getCursor()
        monconn_jobs_local.dropTable()
        print 'Connecting to Mongodb...finished'
        del(monconn_jobs_local)
        
        
        
        #########################################################################################################             
        ############----------------- Initiating Multiprocessing and extracting Jobs
        ############----------------- Set flag pprocessing = 1 for multiprocessing (avoid)
        #########################################################################################################                
        numChunks = 100
        chunkIDs = range(0, numChunks)
        print chunkIDs
        pprocessing = 0        
        if pprocessing == 0:
            preProcessChunk(1)
            pass        
        else:
            numConcurrentThreads = 5
            pool = Pool(numConcurrentThreads)
            pool.map(preProcessChunk, chunkIDs)
    except:
        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com','progressiveht@hindustantimes.com'],"Urgent!!!","Job Alert Mailer : Jobs Processing from SQL Failed!!!!!\nCall Akash (+91-8527716555) or Kanika (+91-9560649296) asap.")
        
