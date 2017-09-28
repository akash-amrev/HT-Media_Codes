from _sqlite3 import Row
__author__="Ashish.Jain"
__date__ ="$Dec 14, 2015 15:45PM$"

'''
This module check the status of midouts
'''

#Include the root directory in the path
import sys
sys.path.append('./../')

#Include the root directory in the path
import sys
sys.path.append('./../')
from pprint import pprint
from DataConnections.MySQLConnect.MySQLConnect import MySQLConnect
from DataConnections.MongoConnect.MongoConnect import MongoConnect
from datetime import date
import pdb
import csv
import time
from multiprocessing import Pool
import os
import re
import datetime
from datetime import timedelta
from datetime import *
#from datetime import date, datetime, time
import calendar
import pandas as pd
import numpy as np
from random import sample



reject_industries = ["Sports / Fitness" , "Gifts / Toys / Stationary"  , "Astrology"  , "Matrimony" , "Unskilled Labor / Domestic Help" , "Veterinary Science / Pet Care","Politics","Religion / Spirituality","Sculpture / Craft"]

reject_SubFA = ["Cabin Crew","Marine Deck Department","Marine Engineering Department","Marine Service / Steward Department","Pilot",
          "Reservation / Ticketing","Tour / Travel Guide",      "Police",       "Medical Transcription",        "Nursing",
        "Language / Translation",       "Library Management",   "Pre-School / Day Care",        "Special Education",
        "Actor / Anchor",       "Cinematography",       "Direction / Editing",  "Production Design / Art",
        "Script / Screenplay",  "Music",        "Sound Mixing / Editing",       "Programming / Scheduling",
        "Real Estate Consultant / Agent",       "Detective Services",   "Astrology",    "Animal Husbandry",
        "Matrimony",    "Pet Care / Breeding",  "Sculpture / Craft",    "Politics",     "Religion / Spirituality",
        "Sports / Fitness",     "Beautician / Stylist", "Unskilled / Manual Labour",    "Painting",     "Curriculum Design",
        "Teaching Assistant",   "Liasion",      "Entrepreneur", "Visual Effects",       "Fashion Modelling",    "Photography",
        "Fire Prevention / Control",    "In-Store Promoter",    "Handset Repair Engineer",      "Optical Fiber Splicer",
        "Optical Fibre Technician",     "Distributor Sales Rep",        "Client Server",        "Data Entry",
        "Bio Tech / R&D / Scientist",   "Self Employed / Freelancer",   "Embedded, VLSI"]

reject_FA = ["Certified Telecom Professionals"]

todayDate=date.today()
weekday = date.today()
weekday = calendar.day_name[weekday.weekday()]

print "WeekDay:" + str(weekday)



if __name__ == '__main__':
    
    username = 'analytics'
    password = 'aN*lyt!cs@321'
    print 'Mongo connect module:'
    
    output = open('cold_calling_file.csv',"w") 
    writer = csv.writer(output, lineterminator='\n')
    
    
    i=0
    user_email_list = []
    
    date1 = datetime.now() - timedelta(days= 2)
    date1 = date1.isoformat()	
    print date1
    
    monconn_users_static = MongoConnect('candidates_processed_4', host = 'localhost', database = 'JobAlerts').getCursor()
    mon_conn_sub_fa = MongoConnect('LookupSubFunctionalArea', host = '172.22.65.88', port = 27018,database = 'sumoplus',username= username,password = password,authenticate = True).getCursor()
    
    print 'Mongo_Connected',monconn_users_static
    
    data_user = monconn_users_static.find({'user_lastlogin':{'$gt':date1}})
    data_user_1 = monconn_users_static.find({'user_lastlogin':{'$gt':date1}}).count()
    
    sub_fa_lookup = mon_conn_sub_fa.find()
    sub_fa = {}
    for records in sub_fa_lookup:
        sub_fa[records['sfe']] = records['fe']
        
    print 'Candidates_picked:',str(data_user_1)
    
    writer.writerow(["Email",'Candidate_Name','Phone','City','cpv','applications','edu_qual','loc_id','Total_Experience','Industry','Salary','Functional_Area','last_login','Sub_FA'])        
    count = 0     
    for row in data_user :
        #print row
        count += 1 
        id = row.get('_id',0)
	try:
            email = row.get('user_email','').encode('ascii','ignore').decode('ascii')
        except:
	    email = 'missing'		
	try:
	    name = row.get('user_fullname','').encode('ascii','ignore').decode('ascii')
	except:
	    name = 'missing'
	try:
	    phone = row.get('user_phone','').encode('ascii','ignore').decode('ascii')
	except:
	    phone = 'missing'
	try:
	    city = row.get('user_location','')
            city = str(city[0])     
	    #sys.exit(0)
	except:
	    city = 'missing'
	try:
	    cpv = row.get('user_phone_verified','')
	except:
	    cpv = 'missing'
	try:
	    applications = len(row.get('application_list',''))
	except:
	    applications = 'missing'
	try:
	    edu_qualification = row.get('user_highest_qual','').encode('ascii','ignore').decode('ascii')
	except:
	    edu_qualification = 'missing'
	try:
	    user_loc_id = row.get('user_location_id','').encode('ascii','ignore').decode('ascii')
	except:
	    user_loc_id = 'missing'
        try:
            user_experience = row.get("user_experience","None").encode('ascii','ignore').decode('ascii')
	    user_experience = float((re.findall("[-+]?\d+[\.]?\d*", user_experience.replace(',','').replace("-"," ")))[1])
	    #print user_experience
	    #sys.exit(0)
        except:
            user_experience = "None" 
        try:
            user_industry = row.get("user_industry","None").encode('ascii','ignore').decode('ascii')
        except:
            user_industry  = "None" 
        try:
            user_ctc = row.get("user_ctc","None").encode('ascii','ignore').decode('ascii')
	    #print user_ctc
	    if user_ctc != "< Rs 50,000 / Yr" and user_ctc is not None:
                 user_ctc_min = float((re.findall("[-+]?\d+[\.]?\d*", user_ctc.replace(',','')))[0])
	         
            elif user_ctc == "< Rs 50,000 / Yr":
                 user_ctc_min = 0.5
            else:
                 user_ctc_min = -1
            #print user_ctc_min
            if user_ctc != "Rs 0 / Yr" and user_ctc != "< Rs 50,000 / Yr" and user_ctc is not None:
                 user_ctc_max = float((re.findall("[-+]?\d+[\.]?\d*", user_ctc.replace(',','').replace("-"," ")))[1])
            else:
                 user_ctc_max = -1
            #print user_ctc_max
            #sys.exit(0)
	    '''if user_ctc = "Rs 0 / Yr":
	        user_sal = 'N/A'
	    else:
		user_sal = (user_ctc_min + user_ctc_max)/2'''
	    
        except Exception as e:
	    print e
            #sys.exit(0)
            user_ctc = "None"
            user_ctc_min = -10 
        try:
            user_sub_functionalarea = row.get("user_functionalarea","None").encode('ascii','ignore').decode('ascii')
            user_functionalarea = sub_fa[user_sub_functionalarea]
        except:
            user_sub_functionalarea = "None"
            user_functionalarea = "None" 
        try:
            user_lastlogin = row.get("user_lastlogin","None").encode('ascii','ignore').decode('ascii')
        except:
            user_lastlogin = "None"    
        if user_ctc_min >= 4 and user_ctc_min != -10 and user_experience > 4.0 and user_loc_id != 1022 and user_industry not in reject_industries and user_sub_functionalarea not in reject_SubFA and cpv == True and user_functionalarea not in reject_FA :
            writer.writerow([email,name,phone,city,cpv,applications,edu_qualification,user_loc_id,user_experience,user_industry,user_ctc,user_functionalarea,user_lastlogin,user_sub_functionalarea])
        
    df_cold_calling = pd.read_csv('cold_calling_file.csv')
    df_cold_calling = df_cold_calling[['Email','Candidate_Name','Phone','City','Industry','Functional_Area','Salary','Total_Experience','Sub_FA']]
    
    #### Making Files for Ktech and Concentrix###
    #############################################
    
    
    if weekday == 'Saturday':
        pass
    else:
        rindex = np.array(sample(xrange(len(df_cold_calling)),750))
        df_concentrix = df_cold_calling.ix[rindex]
        df_merge = pd.merge(df_cold_calling.reset_index(),df_concentrix)
        df_final = df_cold_calling.drop(df_merge['index'])
        #print df_cold_calling.count()
        #print df_concentrix.count()
        #print df_final.count()
        df_concentrix.to_csv('/data/Projects/JobAlerts/Adhocs/concentrix_leads.csv',index = False)
        df_final.to_csv('/data/Projects/JobAlerts/Adhocs/cold_calling_leads_v1.csv',index = False)
    
    ## Sharing Leads for Ktech######
    ################################
    
    df_cold_calling_1 = pd.read_csv('/data/Projects/JobAlerts/Adhocs/cold_calling_leads_v1.csv')
    
    if weekday == 'Saturday':
        pass
    else:
        rindex = np.array(sample(xrange(len(df_cold_calling_1)),1000))
        df_ktech = df_cold_calling_1.ix[rindex]
        df_merge = pd.merge(df_cold_calling_1.reset_index(),df_ktech)
        df_final = df_cold_calling_1.drop(df_merge['index'])
        #print df_cold_calling_1.count()
        #print df_ktech.count()
        #print df_final.count()
        df_ktech.to_csv('/data/Projects/JobAlerts/Adhocs/ktech_leads.csv',index = False)
        df_final.to_csv('/data/Projects/JobAlerts/Adhocs/cold_calling_leads_v2.csv',index = False)
        
    
    input_file = open('/data/Projects/JobAlerts/Adhocs/cold_calling_leads_v2.csv','rb')
    reader = csv.reader(input_file)
    reader.next()
    
    ofile_file_1 = open('/data/Projects/JobAlerts/Adhocs/cold_calling_file_1.csv','w')
    writer_1 = csv.writer(ofile_file_1)
    writer_1.writerow(['Email','Candidate_Name','Phone','City','Industry','Functional_Area','Salary','Total_Experience','Sub_FA'])
    
    ofile_file_2 = open('/data/Projects/JobAlerts/Adhocs/cold_calling_file_2.csv','w')
    writer_2 = csv.writer(ofile_file_2)
    writer_2.writerow(['Email','Candidate_Name','Phone','City','Industry','Functional_Area','Salary','Total_Experience','Sub_FA'])
    
    ofile_file_3 = open('/data/Projects/JobAlerts/Adhocs/cold_calling_file_3.csv','w')
    writer_3 = csv.writer(ofile_file_3)
    writer_3.writerow(['Email','Candidate_Name','Phone','City','Industry','Functional_Area','Salary','Total_Experience','Sub_FA'])
    
    ofile_file_4 = open('/data/Projects/JobAlerts/Adhocs/cold_calling_file_4.csv','w')
    writer_4 = csv.writer(ofile_file_4)
    writer_4.writerow(['Email','Candidate_Name','Phone','City','Industry','Functional_Area','Salary','Total_Experience','Sub_FA'])
    
    ofile_file_5 = open('/data/Projects/JobAlerts/Adhocs/cold_calling_file_5.csv','w')
    writer_5 = csv.writer(ofile_file_5)
    writer_5.writerow(['Email','Candidate_Name','Phone','City','Industry','Functional_Area','Salary','Total_Experience','Sub_FA'])
    
    ofile_file_6 = open('/data/Projects/JobAlerts/Adhocs/cold_calling_file_6.csv','w')
    writer_6 = csv.writer(ofile_file_6)
    writer_6.writerow(['Email','Candidate_Name','Phone','City','Industry','Functional_Area','Salary','Total_Experience','Sub_FA'])
    
    counter = 1
    
    for row in reader:
        if counter % 6 ==1:
            writer_1.writerow([row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8]])
        elif counter %6 == 2:
            writer_2.writerow([row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8]])
        elif counter %6 == 3:
            writer_3.writerow([row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8]])
        elif counter %6 == 4:
            writer_4.writerow([row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8]])
        elif counter %6 == 5:
            writer_5.writerow([row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8]])
        elif counter %6 == 0:
            writer_6.writerow([row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8]])
        counter = counter + 1    
            
        
                
    
        
            
