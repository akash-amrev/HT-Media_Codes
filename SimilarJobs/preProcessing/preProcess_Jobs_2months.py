__author__="Himanshu.Solanki"
__date__ ="2/15/2015"

##################
''' Abstract '''
##################

'''
This module provides functions for preProcessing Jobs for JAM
For Multiprocessing comment the mailing part otherwise mail will be sent for each chunk
'''

#############################################             
'''Importing Python Libraries and Modules'''
#############################################
import cmd
import sys
sys.path.append('./../')
sys.path.append('/data/Projects/SimilarJobs/Notifier/')
from Notifier import send_email
from pprint import pprint
from DataConnections.MySQLConnect.MySQLConnect import MySQLConnect
from DataConnections.MongoConnect.MongoConnect import MongoConnect
from Utils.Utils_1 import cleanToken
from Utils.HtmlCleaner import HTMLStripper, cleanHTML
import pdb
from gensim import corpora, models, similarities
from Features.LSI_common.MyBOW import MyBOW
import csv
import time
from multiprocessing import Pool
import os


##########################################             
'''This removes the duplicate elements'''
##########################################
def removeDup(x):
    
    try:
        x_list= list(set((x).split(',')))
        x_string=', '.join(x_list)
        return x_string
    except :
        return " "
   


def preProcessChunk(chunkID):

    ######################################             
    '''Fetching the Jobs from SQL'''
    ######################################

    #host="172.22.65.157"
    host = "172.22.66.204"
    user="analytics"
    password="Anal^tics@11"
    database="SumoPlus"
    unix_socket="/tmp/mysql.sock"
    port = 3306
    
    print "Loading Jobs From MySql...."
    mysql_conn = MySQLConnect(database, host, user, password, unix_socket, port)
    #cmd = '''SELECT rj.jobid as Jobid,rj.jobtitle as JobTitle,rj.description as JD,la1.text_value_MAX as SalaryMax,la2.text_value_MIN as SalaryMin,le1.display as ExpMin,le2.display as ExpMax,li.industry_desc as Industry,c.AttValueCustom as keySkills,l.city_desc as location,fn.field_enu as function,fn.sub_field_enu as subfunction from recruiter_job AS rj left join lookup_annualsalary AS la1 on rj.salarymax = la1.salary_id left join  lookup_annualsalary AS la2 on rj.salarymin = la2.salary_id left join lookup_experience AS le1 on rj.minexperience = le1.value left join  lookup_experience AS le2 on rj.maxexperience = le2.value left join recruiter_jobattribute as c on rj.jobid = c.jobid_id left join  lookup_industry AS li on rj.industry=li.industry_id left join lookup_subfunctionalarea_new163 AS fn on fn.sub_field_id = c.AttValue AND c.AttType = 12 left join lookup_city_new512 AS l on  l.city_id = c.AttValue AND c.AttType = 13 WHERE rj.jobstatus in (3,5,6,9) and c.AttType in (3,12,13) and (DATEDIFF( CURDATE(),DATE(rj.publisheddate)) < 4 OR DATEDIFF( CURDATE(),DATE(rj.republisheddate)) < 4)  and rj.jobid%''' + str(numChunks) + '=' + str(chunkID)
    #cmd = '''SELECT rj.jobid as Jobid,rj.jobtitle as JobTitle,rj.description as JD,la1.text_value_MAX as SalaryMax,la2.text_value_MIN as SalaryMin,le1.display as ExpMin,le2.display as ExpMax,li.industry_desc as Industry,c.AttValueCustom as keySkills,l.city_desc as location,fn.field_enu as function,fn.sub_field_enu as subfunction from recruiter_job AS rj left join lookup_annualsalary AS la1 on rj.salarymax = la1.salary_id left join  lookup_annualsalary AS la2 on rj.salarymin = la2.salary_id left join lookup_experience AS le1 on rj.minexperience = le1.value left join  lookup_experience AS le2 on rj.maxexperience = le2.value left join recruiter_jobattribute as c on rj.jobid = c.jobid_id left join  lookup_industry AS li on rj.industry=li.industry_id left join lookup_subfunctionalarea_new163 AS fn on fn.sub_field_id = c.AttValue AND c.AttType = 12 left join lookup_city_new512 AS l on  l.city_id = c.AttValue AND c.AttType = 13 WHERE rj.jobstatus in (3,5,6,9) and c.AttType in (3,12,13) and (DATEDIFF( CURDATE(),DATE(rj.publisheddate)) < 4 OR DATEDIFF( CURDATE(),DATE(rj.republisheddate)) < 4)'''
    #print cmd
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
         rj.companyid_id as Company_id,
         rj.displayname as Company_name,
         la1.text_value_MAX as SalaryMax,
         la2.text_value_MIN as SalaryMin,
         le1.display as ExpMin,
         le2.display as ExpMax,
         li.industry_desc as Industry,
         group_concat(c.AttValueCustom,'') as keySkills,
         group_concat(fn.field_enu,'') as function,
         group_concat(l.city_desc,'') as location,
         group_concat(fn.sub_field_enu,'') as subfunction,
         K.Applications as Applications,
         K.MatchedApplications as MatchedApplications,
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
            and ((DATEDIFF( CURDATE(),DATE(recruiter_job.publisheddate)) < 62 AND (DATEDIFF( CURDATE(),DATE(recruiter_job.publisheddate)) > 0)) OR (DATEDIFF( CURDATE(),DATE(recruiter_job.republisheddate)) < 62 AND (DATEDIFF( CURDATE(),DATE(recruiter_job.republisheddate)) > 0)))  
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
         left join ShineReport.LiveJobsApplications as K on K.JobId = rj.jobid     
         WHERE 
        
         c.AttType in (3,12,13) 
        
         group by rj.jobid
         '''
        
    cmd5= '''drop table if exists SumoPlus.XY
        '''

    print 'chnukID:', chunkID, ': Loading jobs from SQL....', time.ctime()
    mysql_conn.query(cmd1)
    mysql_conn.query(cmd2)
    mysql_conn.query(cmd3)
    jobs = mysql_conn.query(cmd4)
    mysql_conn.query(cmd5)
    print 'chunkID:', chunkID,': Loading jobs from SQL....completed..', time.ctime()
    
    print 'chunkid:', chunkID, ' : Number of jobs loaded: ', len(jobs)



    ######################################             
    '''Connecting to Mongo 253 Server'''
    ######################################
    
    print 'Connecting to Mongodb..'
    tableName = 'jobs_processed_2months'
    monconn_jobs_local = MongoConnect(tableName, host = '172.22.66.198', database = 'SimilarJobs')
    monconn_jobs_local_cur = monconn_jobs_local.getCursor()
    print 'Connecting to Mongodb...finished'
 
 
    
    ######################################             
    '''Processing the Jobs'''
    ######################################

    i = 0
    for job in jobs:
        #pprint(job)
        #print i
        if i%1000 == 0:
            print '\tchunkID:', chunkID, ' numRecords:' , i,  ' completed in ', time.time() - start_time, ' seconds'
        
        job_id = job['Jobid']
        job_title = cleanToken(job['JobTitle'])
        job_maxexp = cleanToken(job['ExpMax'])
        job_minexp = cleanToken(job['ExpMin'])  
        job_maxsal = cleanToken(job['SalaryMax'])
        job_minsal = cleanToken(job['SalaryMin'])  
        job_jd = cleanHTML(cleanToken(job['JD']) )
        job_industry = cleanToken(job['Industry'])
        job_location=removeDup(job['location'])
        job_subfunction=removeDup(cleanToken(job['subfunction']))
        job_function=removeDup(cleanToken(job['function']))
        job_skills=removeDup(cleanToken(job['keySkills']))
        job_flag = job['flag']
        job_accounttype = job['account_type']
        job_company_id = job['Company_id']
        job_company_name = cleanToken(job['Company_name'])
        job_index = i 
        job_location = job_location.replace(', ', ',').lower().split(',')
        job_applications = job['Applications']
        job_MatchedApplications = job['MatchedApplications']    
        
        #################################################             
        '''Creating Bag of Words from the text fields'''
        #################################################

        text = 5*(" "+job_title) + ' ' + 3*(" "+job_skills) + ' ' + 1*(" "+job_jd) +' '+2*(" "+job_industry)+' '+2*(" "+job_function)+' '+2*(" "+job_subfunction)
        text = text.replace('candidates', ' ')        
        job_bow = mb.getBow(text, getbowdict = 0)

        
        
        ##################################################             
        '''Dumping Job Details in Mongo (172.22.66.253)'''
        ##################################################
            
        document = {'job_id': job_id, 'job_title': job_title,'job_function':job_function, \
             'job_maxexp': job_maxexp, 'job_minexp': job_minexp,\
             'job_location':job_location, 'job_subfunction':job_subfunction,\
             'job_maxsal':job_maxsal,'job_minsal':job_minsal, 'job_skills': job_skills, \
             'job_bow': job_bow, 'job_industry': job_industry, 'job_jd': job_jd, \
             'job_flag':job_flag,'job_accounttype':job_accounttype, \
             'job_company_id':job_company_id,'job_company_name':job_company_name,'job_index':job_index, \
             'job_applications':job_applications,'job_MatchedApplications':job_MatchedApplications
             }
        
        monconn_jobs_local.saveToTable(document)
            
            
        i += 1
        

    print "Processing finished....."    
    print 'chunkID:', chunkID, ' Total time taken is: ', time.time() - start_time, ' seconds.'
    end_time = time.time()
    time_taken = end_time - start_time
    #os.system(' echo "Jobs Processing 2 Month Completed !!\nJobs Processed '+str(i)+' in :' + str(end_time - start_time) + ' seconds' +' " | mutt -s "Similar Jobs Mailer" himanshu.solanki@hindustantimes.com,kanika.khandelwal@hindustantimes.com, akash.verma@hindustantimes.com')
    #send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com','himanshu.solanki@hindustantimes.com'],"Similar Jobs Mailer 2 Month Jobs",'Jobs Processing 2 Months Completed !!\nJobs Processed '+str(i)+' in :' + str(end_time - start_time) + ' seconds')
    del(monconn_jobs_local)
    del(mysql_conn)


   
if __name__ == '__main__':

    try:
        
        ######################################
        '''Start the timer'''
        ######################################
        #os.system(' echo "Jobs Processeing 2 Month Started !!' ' " | mutt -s "Similar Jobs Mailer" himanshu.solanki@hindustantimes.com, kanika.khandelwal@hindustantimes.com, akash.verma@hindustantimes.com')
        #send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com','himanshu.solanki@hindustantimes.com'],"Similar Jobs Mailer 2 Month Jobs",'Jobs Processing 2 Months Started !!')
        send_email(['himanshu.solanki@hindustantimes.com'],"Similar Jobs Mailer 2 Month Jobs",'Jobs Processing 2 Months Started !!')
        print 'preProcessing Jobs...', time.ctime()
        start_time = time.time()
        htmls = HTMLStripper()
        
        ######################################             
        '''Load the mapping for Bag of Words'''
        ######################################
            
        print 'Loading the mappings for bow'
        synMappingFileName = '../Features/rawData/LSI/Model_UnifiedTKE/unifiedtkelist.csv'
        keywordIdMappingFileName = '../Features/rawData/LSI/Model_UnifiedTKE/unifiedtkelist_numbered.csv' #This file is created
        mb = MyBOW(synMappingFileName, keywordIdMappingFileName)
        print 'Loading the mappings for bow...finished'
        
        #############################################        
        '''Dropping the existing collection of jobs'''
        #############################################
    
        print 'Connecting to Mongodb..'
        tableName = 'jobs_processed_2months'
        monconn_jobs_local = MongoConnect(tableName, host = '172.22.66.198', database = 'SimilarJobs')
        monconn_jobs_local_cur = monconn_jobs_local.getCursor()
        monconn_jobs_local.dropTable()
        print 'Connecting to Mongodb...finished'
        del(monconn_jobs_local)
        
        
        
        ######################################             
        '''Preprocessing of Jobs(avoid)'''
        ######################################
    
        numChunks = 100
        chunkIDs = range(0, numChunks)
        #print chunkIDs
        pprocessing = 0
        
        if pprocessing == 0:
            preProcessChunk(1)
            #for chunkID in chunkIDs:
            #    preProcessChunk(chunkID)
            pass
        
        else:
            numConcurrentThreads = 5
            pool = Pool(numConcurrentThreads)
            pool.map(preProcessChunk, chunkIDs)
        
        send_email(['himanshu.solanki@hindustantimes.com'],"Similar Jobs Mailer 2 Month Jobs",'Jobs Processing 2 Months Completed !!')
        #os.system(' echo "Jobs for JAM Processed  in :' + str(end_time - start_time) + ' seconds' +' " | mutt -s "Job Alert Mailer " ashish4669@gmail.com ,himanshusolanki53@gmail.com')
    except Exception as E:
        print E
        send_email(['himanshu.solanki@hindustantimes.com'],"Urgent",'Jobs Processing 2 Months Failed !!')
        #os.system(' echo "Similar Job Mailer Failed : 2 Months Job Processing from SQL Failed!!!!!\nCall Himanshu (+91-7738982847) asap.'  ' " | mutt -s "Urgent!!!" progressiveht@hindustantimes.com, himanshu.solanki@hindustantimes.com ,kanika.khandelwal@hindustantimes.com, akash.verma@hindustantimes.com')
        #send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com','himanshu.solanki@hindustantimes.com','progressiveht@hindustantimes.com'],"Urgent!!!",'Similar Job Mailer Failed : 9 Month Jobs Processing from SQL Failed!!!!! \n Call Kanika (+91-9560649296) or Akash (+91-8527716555) asap.')
            
