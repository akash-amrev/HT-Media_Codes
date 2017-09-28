# -*- coding: utf-8 -*-
import cmd
__author__="Ashish.Jain"
__date__ ="Oct-26-2015"



#################################################################################################################           
############-----------------  Summary of Script
#################################################################################################################
#--> This module provides functions for creating Jobs Dump of Midout Mailer for Tech Team.
#--> It connects to the SQL Server(172.16.66.204) and fetches Jobs of last 8 days .
#--> It create Bag of Words(BOW) from the text fields of the Job and dump these BOW along with other
#    details of Job in Mongo (172.22.66.233)
#--> For Multiprocessing comment the mailing part otherwise mail will be sent for each chunk.




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
import os
import string
from string import replace


from Notifier.Notifier import send_email                            #Custom Module - /data/Projects/MidoutMailer/Notifier/Notifier.py
from DataConnections.MySQLConnect.MySQLConnect import MySQLConnect  #Custom Module - /data/Projects/MidoutMailer/DataConnections/MySQLConnect/MySQLConnect.py              
from DataConnections.MongoConnect.MongoConnect import MongoConnect  #Custom Module - /data/Projects/MidoutMailer/DataConnections/MongoConnect/MongoConnect.py
from Utils.Utils_1 import cleanToken                                #Custom Module - /data/Projects/MidoutMailer/Utils/Utils_1.py
from Utils.HtmlCleaner import HTMLStripper                          #Custom Module - /data/Projects/MidoutMailer/Utils/HtmlCleaner.py
from Features.LSI_common.MyBOW import MyBOW                         #Custom Module - /data/Projects/MidoutMailer/Features/LSI_common/MyBOW.py



#################################################################################################################            
############-----------------This function cleans the text
#################################################################################################################
def cleanToken_1(text):
    if text is not None:
        filtered_string = filter(lambda x: x in string.printable, text)    
        return filtered_string.replace('\0',' ').replace('\x00',' ').replace('\\', ' ').replace('\r', ' ').replace('\n', ' ').replace('\t', ' ').replace('/',' ').replace('< p>', ' ').replace('< div>', ' ').replace('<span>', ' ').replace('<strong>', ' ')
    else:
        return ' '


#########################################################################################################             
############-----------------Function to remove duplicate elements
#########################################################################################################
def removeDup(x):
    
    try:
        x_list= list(set((x).split(',')))
        x_string=', '.join(x_list)
        return x_string
    except :
        return " "
   


#########################################################################################################             
############-----------------Function to extract Jobs from SQL
#########################################################################################################
def preProcessChunk(chunkID):
    
    #########################################################################################################             
    ############-----------------    SQL Credentials
    #########################################################################################################

    #Connect to SQL table and get the jobs data
    #host="172.16.66.64"
    #user="analytics"
    #password="@n@lytics"
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
         rj.publisheddate as publisheddate,
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
            and (DATEDIFF( CURDATE(),DATE(recruiter_job.publisheddate)) < 9 OR DATEDIFF( CURDATE(),DATE(recruiter_job.republisheddate)) < 9)  
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
    tableName = 'JobDesc_daily'
    monconn_jobs_local = MongoConnect(tableName, host = 'localhost', database = 'JobDescDB')
    monconn_jobs_local_cur = monconn_jobs_local.getCursor()
    print 'Connecting to Mongodb...finished'
 
 
    

    #########################################################################################################             
    ############-----------------Processing the Jobs data extracted from SQL
    #########################################################################################################
    i = 0
    for job in jobs:
        #pprint(job)
        #print i
        if i%1000 == 0:
            print '\tchunkID:', chunkID, ' numRecords:' , i,  ' completed in ', time.time() - start_time, ' seconds'
        _id = job['Jobid']
        comp_name = cleanToken_1(job.get('Company_name',None))

        loc=(removeDup(job.get('location',None))).replace(', ', ',').split(',')
        min_exp = job.get('ExpMin',None)
        title = cleanToken_1(job.get('JobTitle',None))

        max_exp = job.get('ExpMax',None)
        pub_date = job.get('publisheddate',None)
        id = job['Jobid']
        job_flag = job.get('flag')
        
        p=0
        if job_flag == "Paid":
            p= 1
        else:
            p=0
            
        desc = None
        


        #########################################################################################################             
        ############-----------------Creating Job document to be saved in Mongo
        #########################################################################################################        
        document = {'_id':_id,
                    'comp_name':comp_name,
                    'loc':loc,
                    'min_exp':min_exp,
                    'title':title,
                    'max_exp':max_exp,
                    'pub_date':pub_date,
                    'id':id,
                    'p':p,
                    'desc':desc
                    
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
    send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Midout Mailers",'Tech Dump Jobs Processed '+str(i)+' in :' + str(end_time - start_time) + ' seconds')
    del(monconn_jobs_local)
    del(mysql_conn)
        



#########################################################################################################             
############-----------------    Main Function
#########################################################################################################
   
if __name__ == '__main__':
    
    #########################################################################################################             
    ############----------------- Try Except to avoid Failure of code
    #########################################################################################################
    try:
        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Midout Mailers","Tech Dump Jobs Processeing Started !!")

        
        #########################################################################################################             
        ############-----------------    Start the timer
        #########################################################################################################
        print 'preProcessing Jobs...', time.ctime()
        start_time = time.time()
        htmls = HTMLStripper()
        

        
        #########################################################################################################             
        ############-----------------    Loading the mapping for Bag of Words
        #########################################################################################################                
        print 'Loading the mappings for bow'
        synMappingFileName = '../Features/rawData/LSI/Model_UnifiedTKE/unifiedtkelist.csv'
        keywordIdMappingFileName = '../Features/rawData/LSI/Model_UnifiedTKE/unifiedtkelist_numbered.csv' #This file is created
        mb = MyBOW(synMappingFileName, keywordIdMappingFileName)
        print 'Loading the mappings for bow...finished'
        
        
        

        #########################################################################################################             
        ############-----------------    Dropping the existing collection of Jobs
        #########################################################################################################                
        print 'Connecting to Mongodb..'
        tableName = 'JobDesc_daily'
        monconn_jobs_local = MongoConnect(tableName, host = 'localhost', database = 'JobDescDB')
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
        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com','progressiveht@hindustantimes.com'],"Midout Mailers -Urgent!!!","Tech Dump Jobs Processing from SQL Failed!!!!!\nCall Akash (+91-8527716555) or Kanika (+91-9560649296) asap.")