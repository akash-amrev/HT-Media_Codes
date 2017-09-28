import sys
from Tkconstants import FIRST
sys.path.append('/data/Projects/JobAlerts')
import os
from datetime import datetime
import pymongo
import time
import datetime
from datetime import timedelta
from pymongo import Connection
from pymongo.database import Database
from datetime import datetime, timedelta
import csv
import pandas as pd
import re
from DataConnections.MongoConnect.MongoConnect import MongoConnect

#### Defining UserName and Password for Mongo Connection ##########
###################################################################

username = 'analytics'
password = 'aN*lyt!cs@321'

def getMongoMaster():
    MONGO_HOST = '172.22.66.198'
    MONGO_PORT = 27017
    connection = Connection(MONGO_HOST,MONGO_PORT)
    db = Database(connection,'JobAlerts')
    return db

def getMongoMaster_Cand():
    MONGO_HOST = '172.22.65.88'
    MONGO_PORT = 27018
    connection = Connection(MONGO_HOST,MONGO_PORT)
    db = Database(connection,'sumoplus')
    return db

def salary_data():
    
    date1 = datetime.now() - timedelta(days=183)
    print datetime.now()
    print date1
    
    ofile = open('/data/Projects/Salary_Tool_HT_Campus/Output/Cand_Data.csv','w')
    writer = csv.writer(ofile)
    writer.writerow(['user_id','specialization','specialization_id','total_exp_months','city','city_id','industry','industry_id','company','company_id','salary_lacs','job_title'])
    
    ###### Loading Mongo Cursors #############
    ##########################################
    
    mongo_conn = getMongoMaster()
    collection = getattr(mongo_conn,"candidates_processed_4")
    lookup_industry = MongoConnect('LookupIndustry', host = '172.22.65.88', port = 27018, database = 'sumoplus', username = username, password = password, authenticate = True).getCursor()
    lookup_company = MongoConnect('LookupCompanyName', host = '172.22.65.88', port = 27018, database = 'sumoplus', username = username, password = password, authenticate = True).getCursor()
    
    
    ###### Creating Industry Dict#############
    ##########################################
    
    industry_dict = {}
    Industry_Name = lookup_industry.find({},{'ii':1,'idesc':1})
    for records in Industry_Name:
        industry_dict[records['idesc']] = records['ii']
        
    ####### Creating Specialization Dict###########
    ###############################################
    
    specialization_dict = {}
    ifile = open('/data/Projects/Salary_Tool_HT_Campus/Output/Specilization.csv','rb')
    reader = csv.reader(ifile)
    for records in reader:
        specialization_dict[records[0].strip()] = records[1]
    
    ####### Creating Company Dict ############
    ##########################################
    
    company_dict = {}
    Company_Name = lookup_company.find({},{'v':1,'d':1})
    for records in Company_Name:
        company_dict[records['d']] = records['v']
    
    ######Fetching Last Six Months Active Cands#############
    ########################################################    
    
    required_data = collection.find({'user_lastlogin' : {'$gt':str(date1)}}).limit(100000)
    #required_data = collection.find({'_id':'10000083'})
    
    try:
        for data in required_data:
    
            try:
                user_id = data.get('_id','')
            except:
                user
            
            try:
                specialization = str(data.get('user_edu_special',''))
                print specialization
            except:
                specialization = ''
                
            try:
                specialization_id = specialization_dict[str(data.get('user_edu_special',''))]
                print specialization_id
            except:
                specialization_id = ''
            
            try:
                total_exp = str(data.get('user_experience',''))
                total_exp = re.split('Yrs|Yr|Months|Month',total_exp)
                exp_yrs = int(str(total_exp[0]).strip())
                
            except:
                exp_yrs = 0
            
            try:
                exp_months = int(str(total_exp[1]).strip())
            except:
                exp_months = 0
            
            total_exp_months = exp_yrs*12 + exp_months
            
            try:
                city = data.get('user_location','')
                city = str(city[0])
                
            except:
                city = ''
            
            try:
                city_id = data.get('user_location_id','')
            except:
                city_id = ''
            
            try:
                industry = data.get('user_industry')
                
            except:
                industry = ''
            
            try:
                industry_id = industry_dict[data.get('user_industry')]
                
            except:
                industry_id = ''
                
            try:
                company = str(data.get('user_current_company','')).title()
            except:
                company = ''
                
            try:
                company_id = company_dict[str(data.get('user_current_company','')).title()]
            except:
                company_id = ''
            
            try:
                salary = str(data.get('user_ctc',''))
                salary = re.split('-|Lakh',salary)
                salary = str(salary[1]).strip()
            except:
                salary = ''
            
            try:
                job_title = str(data.get('user_jobtitle','')).title()
            except:
                job_title = ''
            
            writer.writerow([user_id,specialization,specialization_id,total_exp_months,city,city_id,industry,industry_id,company,company_id,salary,job_title])
    
    except:
        print user_id,specialization,specialization_id,total_exp_months,city,city_id,industry,industry_id,company,company_id,salary,job_title
    ofile.close()

def getedu_details():
    
    ######### Creating Mongo Cursors#########
    #########################################
    
    monconn_users_edu = MongoConnect('CandidateEducation', host = '172.22.65.88', port = 27018, database = 'sumoplus', username = username, password = password, authenticate = True).getCursor()
    lookup_educationstudy = MongoConnect('LookupEducationStream', host = '172.22.65.88', port = 27018, database = 'sumoplus', username = username, password = password, authenticate = True).getCursor() 
    lookup_institute = MongoConnect('LookupEducationInstitute', host = '172.22.65.88', port = 27018, database = 'sumoplus', username = username, password = password, authenticate = True).getCursor()
    
    ###### Creating Study Field Dict ########
    #########################################
    
    Study_Field = lookup_educationstudy.find({},{'si':1,'sd':1})
    study_field_dict = {}
    for records in Study_Field:
        study_field_dict[records['si']] = records['sd']
    
    ###### Creating Institute Dict ###########
    ##########################################
    
    institute_dict = {}
    Institute_Name = lookup_institute.find({},{'asi':1,'asd':1})
    for records in Institute_Name:
        institute_dict[records['asi']] = records['asd']    
    
    ifile = open('/data/Projects/Salary_Tool_HT_Campus/Output/Cand_Data.csv','rb') #### Loading Candidate Level csv File ######
    reader = csv.reader(ifile)
    reader.next()
    ofile = open('/data/Projects/Salary_Tool_HT_Campus/Output/Cand_Edu_Data.csv','wb')
    writer = csv.writer(ofile)
    writer.writerow(['user_id','institute','institute_id','stream','stream_id','course_type','course_type_id','most_recent'])

    try:
        for records in reader:
            try:
                required_data = monconn_users_edu.find({'fcu':str(records[0])})
                for data in required_data:
                    user_id = data.get('fcu','')
                    if data.has_key('ins') == True and data.get('ins','') is not None:
                        institute = institute_dict[data['ins']].encode('utf8','ignore').encode('utf-8')
                    else:
                        institute = data.get('inc').encode('utf8','ignore').encode('utf-8')
                    ins_id = data.get('ins','')
                    stream = study_field_dict[data.get('el')]   
                    stream_id = data.get('el','')
                    course_type_id = data.get('ct','')
                    if course_type_id == 1:
                        course_type = 'Full Time'
                    if course_type_id == 2:
                        course_type = 'Part Time'
                    if course_type_id == 3:
                        course_type = 'Correspondence'
                    mr = data.get('mr','')
                    writer.writerow([user_id,institute,ins_id,stream,stream_id,course_type,course_type_id,mr])
            except:
                user_id = records[0]
                institute = '' 
                ins_id = ''
                stream = '' 
                stream_id = ''
                course_type = '' 
                course_type_id = ''
                mr = ''
                writer.writerow([user_id,institute,ins_id,stream,stream_id,course_type,course_type_id,mr])
        
    except:
        print records[0]

    ofile.close()
    df = pd.read_csv('/data/Projects/Salary_Tool_HT_Campus/Output/Cand_Edu_Data.csv')
    
    ########Imputing Missing Value of "mr" field with -100 #######################
    ##############################################################################
    df[['most_recent']] = df[['most_recent']].fillna(value =-100)
    
    ##### Sorting Dataframe ascending on user id and descending on mr field ######
    ##############################################################################
    df_1 = df.sort(['user_id','most_recent'],ascending = [1,0])
    
    ##### Grouping on User_Id Level to Fetch Latest Institute of Candidate #######
    ##############################################################################
    df_2 = df_1.groupby('user_id', group_keys=False).apply(lambda x: x.ix[x.most_recent.idxmax()])
    
    df_3 = df_2[['user_id','institute','institute_id','stream','stream_id','course_type','course_type_id','most_recent']]
    df_3.to_csv('/data/Projects/Salary_Tool_HT_Campus/Output/Institute_Level_Data.csv')
    
def get_cons_data():
    df_1 = pd.read_csv('/data/Projects/Salary_Tool_HT_Campus/Output/Cand_Data.csv') #### Loading Candidate Level Data in DataFrame#####
    df_2 = pd.read_csv('/data/Projects/Salary_Tool_HT_Campus/Output/Institute_Level_Data.csv') #### Loading Institute Level Data in DataFrame#####
    df_merge = pd.merge(df_1,df_2 ,left_on = 'user_id',right_on = 'user_id',how = 'inner') #### Merging Above two Dataframes #####
    df_3 = df_merge[['user_id','specialization','specialization_id','total_exp_months','city','city_id',
                     'industry','industry_id','company','company_id','salary_lacs','job_title','institute',
                     'institute_id','stream','stream_id','course_type','course_type_id']]
    df_3 = df_3.fillna('')
    df_3.to_csv('/data/Projects/Salary_Tool_HT_Campus/Output/Shine_HTC_Salary_Predictor_data.csv',index = False)#### Final Data File to be shared with HTCampus #####
        
def main():
    
    salary_data()
    print 'data picked for salary'
    getedu_details()
    print 'data picked for cand_education'
    get_cons_data()
    print 'Data Merged successfully'
    
if __name__=='__main__':
    main()

    

