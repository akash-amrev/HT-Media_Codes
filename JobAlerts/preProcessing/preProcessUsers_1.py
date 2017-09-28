import cmd
__author__="Ashish.Jain"
__date__ ="$Oct 20, 2015 15:45PM$"

'''
This module provides functions for preProcessing Candidates
'''

#Include the root directory in the path
import sys
sys.path.append('./../')
import csv
import datetime  
from pprint import pprint
from DataConnections.MySQLConnect.MySQLConnect import MySQLConnect
from DataConnections.MongoConnect.MongoConnect import MongoConnect
from Utils.Utils_1 import cleanToken
from Utils.HtmlCleaner import HTMLStripper
from Features.LSI_common.MyBOW import MyBOW

import time
import codecs

if __name__ == '__main__':
    
    
    print 'preProcessing Candidates...', time.ctime()
    st_time = time.time()
    
    htmls = HTMLStripper()
        
    print 'Loading the mappings for bow'
    
    synMappingFileName = '../Features/rawData/LSI/Model_UnifiedTKE/unifiedtkelist.csv'
    keywordIdMappingFileName = '../Features/rawData/LSI/Model_UnifiedTKE/unifiedtkelist_numbered.csv' #This file is created
    mb = MyBOW(synMappingFileName, keywordIdMappingFileName)
    print 'Loading the mappings for bow...finished'
    
    print 'Connecting to Mongodb..'
    
    tableName = 'candidates_processed_jam'
    monconn_jobs_local = MongoConnect(tableName , host = 'localhost', database = 'JobAlerts')
    monconn_jobs_local_cur = monconn_jobs_local.getCursor()
    monconn_jobs_local.dropTable()
    
    print 'Connecting to Mongodb...finished'
    
    
    #Read csv for skills and take it in RAM as Dict
    g = open('/data/Projects/Data/skills_dump_final.csv', 'rU')
    reader_g = csv.reader( g ,delimiter=',')
    userSkillDict = {} #userid -> skill
    i=0
    for line in reader_g:
        if i%500000 == 0:
            print i, ' completed in ', time.time() - st_time, ' seconds'
        
        
        #print line[0]
        #print line[1]
        userid = line[0].strip()
        skill = cleanToken(line[1].strip())
        userSkillDict[userid] = skill
        i+=1
            
        
    g.close()
    
    #Read csv and get the candidate data

    ifile = '/data/Projects/Data/Activedb_dump.csv'
    with open(ifile,'rU') as inputfile:
        reader = csv.DictReader((line.replace('\0','') for line in inputfile) ,delimiter=',')
        i=0
        for line in reader:
            
            if i%1000 == 0:
                print i, ' completed in ', time.time() - st_time, ' seconds'
            #Cleaning the data
            
            user_ctc = line['sa_Salary']
            user_experience = line['sa_experience']
            user_industry = cleanToken(line['sa_Industry'])
            user_functionalarea = cleanToken(line['sa_Function'])  
            user_jobtitle = cleanToken(line['sa_JobTitle'])
            user_profiletitle = cleanToken(line['sa_ProfileTitle'])
            user_id = line['sa_UserId']
            #user_lastlogin= line['da_LastLogin']
            user_email=line['sa_Email']
            user_location=line['sa_City']
            
            user_chunkid = i%10000
            try:
                user_lastlogin = datetime.datetime.strptime(line['da_LastLogin'], "%d%b%Y:%H:%M:%S")
            except: 
                user_lastlogin = datetime.datetime.now()
            try:
                user_skills = userSkillDict[user_id]
            except:
                user_skills = ''
            
            #try:
            #    user_specialization=line['sa_Specialization']
            #except:
            #    user_specialization = ''
            
            
            text = 5*(" "+user_profiletitle) + ' ' + 3*(" "+user_skills) + ' ' + 2*(" "+user_jobtitle) + ' ' +1*(" "+user_industry) + ' ' + 1*(" "+user_functionalarea)
            user_bow = mb.getBow(text, getbowdict = 0)
            
            document = {'user_ctc':user_ctc , 'user_jobtitle': user_jobtitle, \
                     'user_experience': user_experience, 'user_industry': user_industry,\
                     'user_functionalarea':user_functionalarea,'user_profiletitle':user_profiletitle,\
                     'user_skills':user_skills,'user_id':user_id,'user_bow':user_bow,\
                     'user_email':user_email,'user_location':user_location, \
                     'user_chunkid': user_chunkid,'user_lastlogin':user_lastlogin 
                     }
            
            monconn_jobs_local.saveToTable(document)
            i+=1             
            #print line
                   
    
    ifile.close()
     #Memory Management
    del(monconn_jobs_local)
    print 'Total time taken is: ', time.time() - st_time, ' seconds.'
    
