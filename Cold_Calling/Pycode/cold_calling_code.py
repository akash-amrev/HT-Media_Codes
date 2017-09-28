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
#sys.path.append('./../')
sys.path.append('/data/Projects/JobAlerts/')
from pprint import pprint
from DataConnections.MySQLConnect.MySQLConnect import MySQLConnect
from DataConnections.MongoConnect.MongoConnect import MongoConnect
from datetime import date
import pdb
import csv
import time
from multiprocessing import Pool
import os, os.path
import re
import datetime
from datetime import timedelta
from dateutil.relativedelta import *
from datetime import *
#from datetime import date, datetime, time
import calendar
import pandas as pd
from pandas import DataFrame
import numpy as np
from random import sample
import MySQLdb


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
previousDate = todayDate+relativedelta(days = -1)


print "WeekDay:" + str(weekday)

def replace_data1(data):
    #data = data.replace('+91-','')
    #data = re.sub(r'*-', '', data)
    try:
        temp = data.split("-",1)[-2]
    except:
        temp = ""
    return temp

def replace_data(data):
    #data = data.replace('+91-','')
    #data = re.sub(r'*-', '', data)
    temp = data.split("-",1)[-1]
    return temp


def crm_replace_data(data):
    data = data.replace('Ahmednagar','')
    #data = re.sub(r'*-', '', data)
    #temp = data.split("-",1)[-1]
    return data

def moveData(sourceFile,destination):
    if os.path.isdir(destination):
        print "destination directory present"
    else:
            os.makedirs(destination)
    moveCommand="mv "+sourceFile+" "+destination
    os.system(moveCommand)


def create_connect_mysql(username, password, host, port, database, unix_socket = "/tmp/mysql.sock"):
    
    db = MySQLdb.connect(host = host, user = username, passwd=password, db = database, unix_socket = unix_socket, port = port)
    db.autocommit(True)
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    return cursor

def connect_mysql(DB):
    if DB == 'candidate':
        db_cursor = create_connect_mysql('Analytics', 'An@lytics', '172.22.65.170', 3306, 'sumoplus', "/tmp/mysql.sock")
    elif DB == 'recruiter':
        db_cursor = create_connect_mysql('analytics', '@n@lytics', '172.16.66.64', 3306, 'SumoPlus', "/tmp/mysql.sock")
    elif DB == 'coldcalling':
        db_cursor = create_connect_mysql('analytics', '@n@L4t1cs', '172.22.65.48', 3306, 'shinecpcrm', "/tmp/mysql.sock")
    return db_cursor

def crm_data_extraction():
    crm_curr = connect_mysql("coldcalling")
    query = """select mobile_number from leads_leaduser where last_action > DATE(DATE_SUB(NOW(), INTERVAL 61 DAY)) or dnd = 1"""
    crm_curr.execute(query)

    last_21days_cc_cand = crm_curr.fetchall()
    exclusion_list = []

    for cand in last_21days_cc_cand:
        #print cand
        
        try:
            exclusion_list.append(cand["mobile_number"].replace("-", '').replace(",", '').strip())
        except:
            pass
        
    exclusion_list = list(set(exclusion_list))
    exclusion_list = [x for x in exclusion_list if x.isdigit()]
    exclusion_list = [x for x in exclusion_list if x != ""]
    exclusion_list = map(str.strip,exclusion_list)
    exclusion_list = map(int,exclusion_list)
    
    print 'Unique Candidates called in last 55 days', len(exclusion_list)
    #fout = open("ata/CRM_last_55_days_exclusion_list.csv", 'wb')
    fout = open('/data/Projects/Cold_Calling/Output_Files/CRM_last_55_days_exclusion_list.csv', 'wb')
    writer = csv.writer(fout)
    writer.writerow(['Phone','dummy'])
    for phonenumber in exclusion_list:
        writer.writerow([phonenumber, "1"])
    fout.close()
    return exclusion_list

if __name__ == '__main__':
    
    try:
        print 'Data Fetching from CRM DB Started'
        exclusion_list = crm_data_extraction()
        print 'Data Fetched from CRM DB'
        #sys.exit(0)
    except Exception as E:
        print E
        
        
       
    username = 'analytics'
    password = 'aN*lyt!cs@321'
    print 'Mongo connect module:'
    
    output = open('/data/Projects/Cold_Calling/Output_Files/cold_calling_file.csv',"w") 
    writer = csv.writer(output, lineterminator='\n')
    
    
    i=0
    user_email_list = []
    
    date1 = (datetime.now() - timedelta(days= 6)).date()
    date3 = datetime.combine(date1, time(18, 30)) 
    date3 = date3.isoformat("T") + "Z"

    #date2 = (datetime.now() - timedelta(days=2)).date()
    #date4 = datetime.combine(date2,time(18,30))
    #date4 = date4.isoformat("T") + "Z"
   	
    print date1,date3,datetime.now()
   # sys.exit(0)
    
    monconn_users_static = MongoConnect('candidates_processed_4', host = '172.22.66.198', database = 'JobAlerts').getCursor()
    mon_conn_sub_fa = MongoConnect('LookupSubFunctionalArea', host = '172.22.65.88', port = 27018,database = 'sumoplus',username= username,password = password,authenticate = True).getCursor()
    
    print 'Mongo_Connected',monconn_users_static
    
    data_user = monconn_users_static.find({'user_lastlogin':{'$gt':date3}})
    data_user_1 = monconn_users_static.find({'user_lastlogin':{'$gt':date3}}).count()
    #sys.exit(0)
    sub_fa_lookup = mon_conn_sub_fa.find()
    sub_fa = {}
    for records in sub_fa_lookup:
        sub_fa[records['sfe']] = records['fe']
        
    print 'Candidates_picked:',str(data_user_1)
    #sys.exit(0)
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
    	    phone = ''
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
                #print row.get('user_experience')
            user_experience = row.get("user_experience","None").encode('ascii','ignore').decode('ascii')
            user_experience = float((re.findall("[-+]?\d+[\.]?\d*", user_experience.replace(',','').replace("-"," ")))[0])
        except:
            user_experience = "None" 
        try:
            user_industry = row.get("user_industry","None").encode('ascii','ignore').decode('ascii')
        except:
            user_industry  = "None" 
        try:
            user_ctc = row.get("user_ctc","None").encode('ascii','ignore').decode('ascii')
        
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
    
    df_cold_calling = pd.read_csv('/data/Projects/Cold_Calling/Output_Files/cold_calling_file.csv')
    print df_cold_calling.count()
    
    df_cold_calling = df_cold_calling[['Email','Candidate_Name','Phone','City','Industry','Functional_Area','Salary','Total_Experience','Sub_FA']]
    df_cold_calling.to_csv('/data/Projects/Cold_Calling/Output_Files/cold_calling_v1.csv',index = False)
    #sys.exit(0)
    #df_cold_calling['CellPhone'] = df_cold_calling['Phone'].astype(str)
    #df_cold_calling['CellPhone'] = df_cold_calling['CellPhone'].apply(replace_data)
    df_cold_calling['Phone'].replace('',np.nan,inplace = True)
    
    #print df_cold_calling.count()
    df_cold_calling.dropna(subset=['Phone'],inplace = True)
    #print df_cold_calling.count()
    df_cold_calling['Phone'] = df_cold_calling['Phone'].astype(str)
    df_cold_calling['CountryCode'] = df_cold_calling['Phone'].apply(replace_data1)
    #print temp[1:25]
    #sys.exit(0)
    df_cold_calling['Phone'] = df_cold_calling['Phone'].apply(replace_data)
    df_cold_calling['Phone'] = df_cold_calling['Phone'].str.strip()
    df_cold_calling['Phone'] = df_cold_calling['Phone'].astype(int)
    
    
    df_cold_calling.to_csv('/data/Projects/Cold_Calling/Output_Files/cold_calling_v2.csv',index = False)
    #sys.exit(0)
    #df_cold_calling['CellPhone'] = df_cold_calling['CellPhone'].re.sub(r'*-','')
    print df_cold_calling.count()
    
    #print df_cold_calling[['CellPhone','Phone']].head(20)
    #sys.exit(0)
    ####### Removing Duplicates From CRM DB #####
    #############################################
    
    exclude_flag = df_cold_calling['Phone'].isin(exclusion_list)
    df_excluded = df_cold_calling[~exclude_flag]
    print df_excluded.count()
    df_excluded.to_csv('/data/Projects/Cold_Calling/Output_Files/cold_calling_excluded.csv',index = False)
    fout = open('/data/Projects/Cold_Calling/Output_Files/exclusion_list.csv', 'wb')
    writer = csv.writer(fout)
    writer.writerow(['Phone'])
    for phonenumber in exclusion_list:
        writer.writerow([phonenumber])
    fout.close()
    
    #sys.exit(0)
    
    '''df_crm_db = pd.read_csv('/data/Projects/Cold_Calling/Output_Files/CRM_last_55_days_exclusion_list.csv')
    df_crm_db['Phone'].replace('',np.nan,inplace = True)
    
    
    print df_crm_db.count()
    #sys.exit(0)
    #df_final = pd.concat([df_cold_calling,df_crm_db])
    #print df_final.head(3)
    #df_cold_calling['key1'] = 1
    #df_crm_db['key2'] = 1
    #df_crm_db['Phone'] = df_crm_db['Phone'].str.strip()
    #df_crm_db['Phone'] = df_crm_db['Phone'].astype(str)
    #df_crm_db['Phone'] = df_crm_db['Phone'].apply(crm_replace_data)
    df_crm_db['Phone'].replace('',np.nan,inplace = True)
    df_crm_db_Phone = df_crm_db['Phone'].tolist()
    print df_crm_db_Phone[1:10]
    
    
    #df_crm_db['Phone'] = df_crm_db['Phone'].astype(int)
    
    
    merged1 = df_cold_calling.loc[~df_cold_calling['Phone'].isin(df_crm_db_Phone)]
    print "Final merged:", merged1.count()
    merged = pd.merge(df_cold_calling,df_crm_db, left_on='Phone',right_on='Phone',how = 'left')
    merged.to_csv('/data/Projects/Cold_Calling/Output_Files/cold_calling_afterduplicates3.csv',index = False)

    #df_cold_calling = df_cold_calling[~(df_cold_calling.key1==df_cold_calling.key2)]
    merged = merged[(merged['dummy'] != 1)] 
    #merged = merged.drop(['dummy'],axis = 1)
    del merged['dummy']
    merged.to_csv('/data/Projects/Cold_Calling/Output_Files/cold_calling_afterduplicates4.csv',index = False)
    
    print 'Count_Before Excluding crm Leads:',df_cold_calling.count()
    #df_final_v1 = df_final.drop_duplicates()
    
    print 'Count After Excluding crm Leads:',merged.count()
    #sys.exit(0)'''
    
    
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
        report = '/data/Projects/Cold_Calling/Output_Files/concentrix_leads_' +str(todayDate) +'.csv'
        df_concentrix.to_csv(report,index = False)
        
        df_final.to_csv('/data/Projects/Cold_Calling/Output_Files/cold_calling_leads_v1.csv',index = False)
        
        command = "cp " + str(report) + " " + "/data/Projects/Cold_Calling/Output_Files/concentrix_leads_Today.csv"
        print command
        os.system(command)
        #os.system('mutt -s "Concentrix cold calling leads" kanika.khandelwal@hindustantimes.com  -a /data/Projects/Cold_Calling/Output_Files/concentrix_leads_Today.csv')
        print "Concentrix cold Calling Leads Mailed Successfully"
        uploadDir="/data/Projects/Cold_Calling/Output_Files/Concentrix_Leads/"+previousDate.strftime("%b-%Y")
        print uploadDir
        status=moveData(report,uploadDir)
        #sys.exit(0)

    ## Sharing Leads for Ktech######
    ################################
    
    df_cold_calling_1 = pd.read_csv('/data/Projects/Cold_Calling/Output_Files/cold_calling_leads_v1.csv')
    #sys.exit(0)     
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
        report = '/data/Projects/Cold_Calling/Output_Files/ktech_leads_' +str(todayDate) +'.csv'
        df_ktech.to_csv(report,index = False)
        
        df_final.to_csv('/data/Projects/Cold_Calling/Output_Files/cold_calling_leads_v2.csv',index = False)
        
        command = "cp " + str(report) + " " + "/data/Projects/Cold_Calling/Output_Files/ktech_leads_Today.csv"
        print command 
        os.system(command)
        try:
            os.system('mutt -s "Ktech cold calling leads" kanika.khandelwal@hindustantimes.com  -a /data/Projects/Cold_Calling/Output_Files/ktech_leads_Today.csv')
        except exception as E:
            print E
        print "Ktech cold Calling Leads Mailed Successfully"
        uploadDir="/data/Projects/Cold_Calling/Output_Files/Ktech_Leads/"+previousDate.strftime("%b-%Y")
        print uploadDir
        status=moveData(report,uploadDir)
    sys.exit(0)
    input_file = open('/data/Projects/Cold_Calling/Output_Files/cold_calling_leads_v2.csv','rb')
    reader = csv.reader(input_file)
    reader.next()
    
    ## Splitting Leads for Vinod######
    ################################
    
    ofile_file_1 = open('/data/Projects/Cold_Calling/Output_Files/cold_calling_file_1_' +str(todayDate) + '.csv','w')
    writer_1 = csv.writer(ofile_file_1)
    writer_1.writerow(['Email','Candidate_Name','Phone','City','Industry','Functional_Area','Salary','Total_Experience','Sub_FA'])
    
    ofile_file_2 = open('/data/Projects/Cold_Calling/Output_Files/cold_calling_file_2_' +str(todayDate) + '.csv','w')
    writer_2 = csv.writer(ofile_file_2)
    writer_2.writerow(['Email','Candidate_Name','Phone','City','Industry','Functional_Area','Salary','Total_Experience','Sub_FA'])
    
    ofile_file_3 = open('/data/Projects/Cold_Calling/Output_Files/cold_calling_file_3_' +str(todayDate) + '.csv','w')
    writer_3 = csv.writer(ofile_file_3)
    writer_3.writerow(['Email','Candidate_Name','Phone','City','Industry','Functional_Area','Salary','Total_Experience','Sub_FA'])
    
    ofile_file_4 = open('/data/Projects/Cold_Calling/Output_Files/cold_calling_file_4_' +str(todayDate) + '.csv','w')
    writer_4 = csv.writer(ofile_file_4)
    writer_4.writerow(['Email','Candidate_Name','Phone','City','Industry','Functional_Area','Salary','Total_Experience','Sub_FA'])
    
    ofile_file_5 = open('/data/Projects/Cold_Calling/Output_Files/cold_calling_file_5_' +str(todayDate) + '.csv','w')
    writer_5 = csv.writer(ofile_file_5)
    writer_5.writerow(['Email','Candidate_Name','Phone','City','Industry','Functional_Area','Salary','Total_Experience','Sub_FA'])
    
    ofile_file_6 = open('/data/Projects/Cold_Calling/Output_Files/cold_calling_file_6_' +str(todayDate) + '.csv','w')
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
            

                
    
        
            
