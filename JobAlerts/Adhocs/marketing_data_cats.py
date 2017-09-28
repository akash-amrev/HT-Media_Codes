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
#from Utils.Utils_1 import cleanToken
#from Utils.HtmlCleaner import HTMLStripper, cleanHTML
import pdb
#from gensim import corpora, models, similarities
#from Features.LSI_common.MyBOW import MyBOW
import csv
import time
from multiprocessing import Pool
import os

import datetime
from datetime import timedelta
from datetime import timedelta
from datetime import date


if __name__ == '__main__':
    print 'Mongo connect module:'
    
    d0 = date(2017, 5, 02)
    
    username = 'analytics'
    password = 'aN*lyt!cs@321'
    date1 = datetime.datetime.now() - datetime.timedelta(days=183) 
        
    output = open("/data/Projects/MailersOptimization/Output/jam_clicks_reg.csv", "w") 
    writer = csv.writer(output, lineterminator='\n')
    writer.writerow(['UserId','email','UserType','RegistrationStartDate','RegistrationEndDate'])
    
    infile = open('/data/Projects/MailersOptimization/Logs/jam_clicks.csv','r')
    reader = csv.reader(infile)
    
    
    
    i=0
    user_email_list = []
    for row in reader:
        user_email_list.append(row[0]) 
        i+=1
    print len(user_email_list)
    ''' 
        #if i>1000:
        #    break
        
    
    #print user_email_list    
    '''
    date1 = datetime.datetime.now() - datetime.timedelta(days=58)
    
    monconn_users_static = MongoConnect('CandidateStatic', host = '172.22.65.88', port = 27018, database = 'sumoplus', username = username, password = password, authenticate = True).getCursor()
    
    j=0
    while True:
        emails_list = user_email_list[j:j+5000]
        j = j+5000
    
    
        print j
    
        data_user = monconn_users_static.find({'e':{'$in':emails_list}})
        #data_user = monconn_users_static.find({'ut':1,'rsd':{'$gt':date1}},{'_id':1,'ut':1,'red':1,'rsd':1})
            
            
        count = 0     
        for row in data_user :
            count += 1 
            
            #if count%5000 == 0:
            #    print count
            id = row.get('_id',0)
            #st = row.get('st',0)
            #sl = row.get('sl',0)
            e = row.get('e',0)
            ut = row.get('ut',None)
            red = row.get('red',None)
            rsd = row.get('rsd',None)
            #writer.writerow([id,e,ut,rsd,red])
            
            #print red
            try:
                user_date_re= red.day
                #date_year_re = int(user_date_re.split('-')[0])
                #date_month_re = int(user_date_re.split('-')[1])
                #date_day_re = int(user_date_re.split('-')[2])
                re = date(red.year,red.month,red.day)
                
            except:
                
                re = date(1970,01,01)
            #print re 
            delta_re = d0 - re
            #print delta_re.days
            #break
    
            try:
                user_date_rs= rsd
                #date_year_rs = int(user_date_rs.split('-')[0])
                #date_month_rs = int(user_date_rs.split('-')[1])
                #date_day_rs = int(user_date_rs.split('-')[2])
                rs = date(rsd.year,rsd.month,rsd.day)
                
            except:
                
                rs = date(1970,01,01)
                
            delta_rs = d0 - rs
            
            writer.writerow([id,e,ut,delta_rs.days,delta_re.days])
            
            if j > len(user_email_list):
                break
    
            
            #rm_status = row.get('rm','')
            #mo_status = row.get('mo','')
            '''
            reg_end_date = row.get("red",'')
            End_venor = row.get("evi",'')
            Last_login = row.get('ll','')
            city = row.get("cl",'')
            exper = row.get("ex",'')
            cellphone = row.get('cp','')
            
            if rm_status == 0 and mo_status == 0:
                status = 'Activated'
                
            elif rm_status == 1 :
                status = 'Resume Midout'
            elif rm_status == 0 and mo_status != 0:
                status = 'Midout'
            else :
                status = 'Not Found'
            '''
            #writer.writerow([id,e,st,sl])
                
