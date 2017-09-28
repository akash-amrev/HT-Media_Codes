from pymongo import *
from datetime import date, datetime, time
from email import parser
from dateutil.relativedelta import *
from pymongo import Connection
import os, re, sendMailUtil, string, ftplib, traceback, pymongo, MySQLdb, subprocess, smtplib, socket
import pdb

todayDate=date.today()
previousDate = todayDate + relativedelta(days = -1)
day1 = datetime.combine(previousDate, time(0, 0)) 
day2 = datetime.combine(todayDate, time(0, 0))
Month = previousDate.strftime("%b-%Y")

def getMongoConnection(MONGO_HOST,MONGO_PORT,DB, isSlave=False, isAuth=False, username = 'analytics', password = 'aN*lyt!cs@321'):
    if isAuth:
    	connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave, username = username, password = password)
    else:
	connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave)
    mongo_conn = connection[DB]
    mongo_conn.authenticate(username, password)
    return mongo_conn

def getDataFromCandidateMatch():
    directory = "/data/Shine/Shine_AdHoc/Output/DailyApplications/" + str(Month) + "/"
    if os.path.isdir(directory):
        print("Directory Exists")
    else:
        os.mkdir(directory)
    Outfile = directory + "Dailyapplications_" + str(previousDate)+".csv"
    ofile = open(Outfile,"w")
    mongo_conn = getMongoConnection('172.22.65.58', 27017, 'recruiter_master', True)
    collection = getattr(mongo_conn, 'CandidateMatch')
    Applicationsdoc = collection.find({'ad':{'$gte':day1,'$lt':day2}})
    count = 0
    for app in Applicationsdoc:
            #print app
            keys=app.keys()
            #print keys
            if (count ==0):
                    for k in range(len(keys)-1):
                            #print k
                            ofile.write(str(keys[k])+',')
                    ofile.write(str(keys[len(keys)-1])+'\n')
            count = 1	
            for k in range(len(keys)-1):
                ofile.write(str(app[keys[k]])+',')
            ofile.write(str(app[keys[len(keys)-1]])+'\n')
    ofile.close()   

def main():
        print datetime.now()
	getDataFromCandidateMatch()
	print datetime.now()
	
if __name__=='__main__':
    main() 
	
