__author__="Ashish.Jain"
__date__ ="$Oct 20, 2015 15:45PM$"



#################################################################################################################           
############-----------------  Summary of Script
#################################################################################################################
#--> This script fetch users for Midout mailers
#--> Converts the Lookups of Experience , Jobtitles , salary , FA , City , industry ,Skills into respective dictionaries
#--> Creates Bag of Words(BOW) and dump these along with other details of candidate in Mongo (172.22.66.233)




#################################################################################################################            
############-----------------Importing Python Libraries and Modules
#################################################################################################################
#Inbuilt Modules
import sys
sys.path.append('./../')
import os
import csv
import datetime  
from collections import defaultdict
from pprint import pprint
from bson.objectid import ObjectId
import time
import codecs
import cmd
from _sqlite3 import Row
from pprint import pprint, _id
import time

#Custom Modules
from Notifier.Notifier import send_email                            #Custom Module - /data/Projects/MidoutMailer/Notifier/Notifier.py
from DataConnections.MySQLConnect.MySQLConnect import MySQLConnect  #Custom Module - /data/Projects/MidoutMailer/DataConnections/MySQLConnect/MySQLConnect.py              
from DataConnections.MongoConnect.MongoConnect import MongoConnect  #Custom Module - /data/Projects/MidoutMailer/DataConnections/MongoConnect/MongoConnect.py
from Utils.Utils_1 import cleanToken                                #Custom Module - /data/Projects/MidoutMailer/Utils/Utils_1.py
from Utils.HtmlCleaner import HTMLStripper                          #Custom Module - /data/Projects/MidoutMailer/Utils/HtmlCleaner.py
from Features.LSI_common.MyBOW import MyBOW                         #Custom Module - /data/Projects/MidoutMailer/Features/LSI_common/MyBOW.py





#################################################################################################################            
############----------------- Main Function
#################################################################################################################

if __name__ == '__main__':
    
    #########################################################################################################             
    ############-----------------Try Except to provide alert in case of code failure
    #########################################################################################################

    try:
        
        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Midout Mailers","Midouts Candidates Processing  Started!!!!")

        print 'Mongo connect module:'
        
        username = 'analytics'
        password = 'aN*lyt!cs@321' 
        
        

        
        #########################################################################################################             
        ############----------------- Dictionary for LookupExperience
        #########################################################################################################
        print "Loading Dictionary for Experience"
        tableName = 'LookupExperience'
        monconn_users = MongoConnect(tableName, host = '172.22.65.88', port = 27018, database = 'sumoplus', username = username, password = password, authenticate = True)
        monconn_users_cur = monconn_users.getCursor()
        user_experience_dict = {}
        for user in monconn_users_cur.find():
            user_experience_dict[user['v']] = user['d']
        
        

        
        #########################################################################################################             
        ############----------------- Dictionary for LookupJobTitle
        #########################################################################################################
        print "Loading Dictionary for JobTitle"   
        tableName = 'LookupJobTitle'
        monconn_users = MongoConnect(tableName, host = '172.22.65.88', port = 27018, database = 'sumoplus', username = username, password = password, authenticate = True)
        monconn_users_cur = monconn_users.getCursor()
        user_jobtitle_dict = {}
        for user in monconn_users_cur.find():
            user_jobtitle_dict[user['jti']] = user['jtd']  
        
        
        
        
        #########################################################################################################             
        ############----------------- Dictionary for LookupAnnualSalary
        #########################################################################################################
        print "Loading Dictionary for AnnualSalary"
        tableName = 'LookupAnnualSalary'
        monconn_users = MongoConnect(tableName, host = '172.22.65.88', port = 27018, database = 'sumoplus', username = username, password = password, authenticate = True)
        monconn_users_cur = monconn_users.getCursor()
        user_ctc_dict = {}
        for user in monconn_users_cur.find():
            user_ctc_dict[user['si']] = user['tv']  
        
        


        #########################################################################################################             
        ############----------------- Dictionary for LookupSubFunctionalArea
        #########################################################################################################
        print "Loading Dictionary for Functional Area"
        tableName = 'LookupSubFunctionalArea'
        monconn_users = MongoConnect(tableName, host = '172.22.65.88', port = 27018, database = 'sumoplus', username = username, password = password, authenticate = True)
        monconn_users_cur = monconn_users.getCursor()
        user_functionalarea_dict = {}
        for user in monconn_users_cur.find():
            user_functionalarea_dict[user['sfi']] = user['fe'] 
            
        

        #########################################################################################################             
        ############----------------- Dictionary for LookupCity_OLD_445
        #########################################################################################################
        print "Loading Dictionary for Location"
        tableName = 'LookupCity_OLD_445'
        monconn_users = MongoConnect(tableName, host = '172.22.65.88', port = 27018, database = 'sumoplus', username = username, password = password, authenticate = True)
        monconn_users_cur = monconn_users.getCursor()
        user_location_dict = {}
        for user in monconn_users_cur.find():
            user_location_dict[user['ci']] = user['cd']  
        
        

        #########################################################################################################             
        ############----------------- Dictionary for LookupIndustry.
        #########################################################################################################
        print "Loading Dictionary for Industry"
        tableName = 'LookupIndustry'
        monconn_users = MongoConnect(tableName, host = '172.22.65.88', port = 27018, database = 'sumoplus', username = username, password = password, authenticate = True)
        monconn_users_cur = monconn_users.getCursor()
        user_industry_dict = {}
        for user in monconn_users_cur.find():
            user_industry_dict[user['ii']] = user['idesc']      
            
            
            
            
        #########################################################################################################             
        ############----------------- Dictionary for LookupSkill
        #########################################################################################################
        print "Loading Dictionary for Skills"
        tableName = 'LookupSkill'
        monconn_users = MongoConnect(tableName, host = '172.22.65.88', port = 27018, database = 'sumoplus', username = username, password = password, authenticate = True)
        monconn_users_cur = monconn_users.getCursor()
        user_skills_dict = {}
        for user in monconn_users_cur.find():
            user_skills_dict[user['si']] = user['sd']
            
        
        
        #########################################################################################################             
        ############----------------- Loading the mapping for Bag of Words
        #########################################################################################################
        print 'Loading the mappings for bow'
        synMappingFileName = '../Features/rawData/LSI/Model_UnifiedTKE/unifiedtkelist.csv'
        keywordIdMappingFileName = '../Features/rawData/LSI/Model_UnifiedTKE/unifiedtkelist_numbered.csv' #This file is created
        mb = MyBOW(synMappingFileName, keywordIdMappingFileName)
        print 'Loading the mappings for bow...finished' 
        
        
        
        #########################################################################################################             
        ############----------------- Connection to Mongo (CandidateStatic) & (CandidatePreferences)
        #########################################################################################################
        print "Loading the Candidates.."
        monconn_users_static = MongoConnect('CandidateStatic', host = '172.22.65.88', port = 27018, database = 'sumoplus', username = username, password = password, authenticate = True).getCursor()
        monconn_users_preferences = MongoConnect('CandidatePreferences', host = '172.22.65.88', port = 27018, database = 'sumoplus', username = username, password = password, authenticate = True).getCursor()
        
        
        
        #########################################################################################################             
        ############----------------- Connecting to Mongo 233 Server
        #########################################################################################################
        print 'Connecting to Mongodb..' 
        tableName = 'candidates_processed_midout'
        monconn_jobs_local = MongoConnect(tableName , host = 'localhost', database = 'Midout_Mailers')
        monconn_jobs_local_cur = monconn_jobs_local.getCursor()
        monconn_jobs_local.dropTable()
        print 'Connecting to Mongodb...finished'
        
        
            
        #########################################################################################################             
        ############----------------- Deciding the daterange to extract midout candidates data
        #########################################################################################################

        weekday =  datetime.datetime.now().weekday() 
        weekofyear =  datetime.datetime.now().isocalendar()[1]
        
        if weekday !=6:
            date1 = datetime.datetime.now() - datetime.timedelta(days=2)
        
        elif weekday == 6 and weekofyear%2 !=0:
            date1 = datetime.datetime.now() - datetime.timedelta(days=3)
        
        else :
            date1 = datetime.datetime.now() - datetime.timedelta(days=15)
            
    
        #########################################################################################################             
        ############----------------- Processing the candidates data
        #########################################################################################################
        pid = 0
        i = 1
        id = "0"
        while True:
            if i%5 == 0:    
                print i*1000," Records Processed\n"         

            data_user = monconn_users_static.find({ '$and': [
                                                             {'rm':1},
                                                             { 'mo':{'$in':[0,2]}},
                                                             {'eas': {'$in':[1,3]}},
                                                             {'bs':{'$ne':1}},
                                                             {'ss':{'$ne':1}},
                                                             {'ll':{'$gt':date1}},
                                                             {'_id':{'$gt':id}}
                                                             ]
                                                   }).sort('_id').limit(1000)  
            print id
            
            i+=1
            
            user_dict = {str(user['_id']): user for user in data_user}
            print "length :",len(user_dict)
    
            pref_dict = defaultdict(list)
            data_pref = monconn_users_preferences.find({'fcu':{'$in':user_dict.keys()}})    
            
            for pref in data_pref:
                pref_dict[pref['fcu']].append(pref)
                
            
            for key, value in pref_dict.items():
                user_dict[key]['prefs'] = value
            
            
            for key,value in user_dict.items():
                _id = key
                user_id = key
                user_firstname = value.get('fn','')
                user_lastname = value.get('ln','')
                user_phone_number = value.get('cp','')
                user_countrycode = value.get('cc','')
                user_email = value.get('e','')
                user_lastlogin = value.get('ll','')
                user_lastmodified = value.get('lm','')
                user_profiletitle = value.get('rst', '')
                user_registration_start_date = value.get('rsd','')
                try:
                    user_experience = user_experience_dict[value.get('ex')]
                except:
                    user_experience = ""
                
                
                user_location_list = []
                user_industry_list = []
                user_ctc_list = []
                user_functionalarea_list = []  
                user_skills_list = []


                #########################################################################################################             
                ############----------------- Getting User's Preference data
                #########################################################################################################
                for row in value.get('prefs','') :
                 
                    if (row.get('t'))==8: 
                        try:
                            user_location_list.append(user_location_dict[(row.get('v',''))])
                        except:
                            user_location =" "
                        
                    if row.get('t') == 9:  
                        try:
                            user_industry_list.append(user_industry_dict[row.get('v','')])
                        except:
                            user_industry =" "
                          
                    if row.get('t')== 12:
                        try:
                            user_ctc_list.append(user_ctc_dict[row.get('v','')])
                        except:
                            user_ctc =""
                        
                    if row.get('t') == 10:
                        try:
                            user_functionalarea_list.append(user_functionalarea_dict[row.get('v','')])
                        except:
                            user_functionalarea =" "
                        
                user_skills = ""               
    
                try:
                    user_functionalarea = cleanToken(','.join(user_functionalarea_list))
                except:
                    user_functionalarea = ""
                
                try:
                    user_ctc = ','.join(user_ctc_list)
                except:
                    user_ctc = ""
                try:
                    user_industry = cleanToken(','.join(user_industry_list))
                except:
                    user_industry =""
                try:
                    user_location = ','.join(user_location_list)
                except:
                    user_location =""



                
                #########################################################################################################             
                ############----------------- Creating the Bag of Words of Candidate's Details
                #########################################################################################################
               
                text = 5*(" "+user_profiletitle) + ' ' + 3*(" "+user_skills) + ' '  +1*(" "+user_industry) + ' ' + 1*(" "+user_functionalarea)
                user_bow = mb.getBow(text, getbowdict = 0)
               
                document = {'_id':_id,'user_firstname':user_firstname,'user_lastname':user_lastname ,  \
                            'user_phone_number':user_phone_number, 'user_id':user_id , \
                            'user_email':user_email , 'user_lastlogin':user_lastlogin ,\
                            'user_lastmodified' :user_lastmodified, 'user_profiletitle' :user_profiletitle ,\
                            'user_registration_start_date':user_registration_start_date , 'user_experience':user_experience ,\
                            'user_location':user_location , 'user_industry':user_industry, 'user_ctc':user_ctc ,\
                            'user_functionalarea':user_functionalarea , 'user_skills':user_skills , 'user_bow': user_bow, \
                            'user_countrycode':user_countrycode , 'pid':pid
                            
                             }
                
                monconn_jobs_local.saveToTable(document)
                
                
            
            #########################################################################################################             
            ############----------------- Creating a method to cater all typer of User ID's
            #########################################################################################################
               
            if len(user_dict) <1000 and len(str(user_id)) > 20:
                break
             
            if len(user_dict) <1000 and len(str(user_id)) < 20:
                user_id = ObjectId("11c7dc9de839202efae641ef") 
            
            
            if len(str(user_id)) <20:
                id =user_id
            else:
                id = ObjectId(user_id)
                
                           
        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com'],"Midout Mailers","Midouts Candidates Processing  Completed!!!!")            
            
    except:
        send_email(['akash.verma@hindustantimes.com', 'kanika.khandelwal@hindustantimes.com','progressiveht@hindustantimes.com'],"Midout Mailers -Urgent!!!","Candidate Processing Failed!!!!!\nCall Akash (+91-8527716555) or Kanika (+91-9560649296) asap.")
            
            
          
            
        
            
    
            
