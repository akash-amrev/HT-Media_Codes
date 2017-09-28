import poplib, sys, datetime, email, math, operator
import os, re, string, ftplib, traceback, pymongo, MySQLdb, subprocess, smtplib, socket#, paramiko
from email import parser
#from apiclient.errors import HttpError
#from oauth2client.client import AccessTokenRefreshError
from pymongo import Connection
#from pymongo import ReadPreference
#from pymongo.objectid import ObjectId
from bson.objectid import ObjectId
from datetime import date, datetime, time, timedelta
from dateutil.relativedelta import *
from random import random
#from collections import OrderedDict

todayDate=date.today()
interval1 = 183
interval2 = 365
previousDate = todayDate+relativedelta(days=-1)
sixmonthWindow = todayDate+relativedelta(days=-interval1)
previousIntervalDate = sixmonthWindow+relativedelta(days=-1)
oneYearWindow = todayDate+relativedelta(days=-interval2)
previousYearDate = oneYearWindow+relativedelta(days=-1)

day1 = datetime.combine(todayDate, time(0, 0))
day2 = datetime.combine(previousDate, time(0, 0))
day3 = datetime.combine(sixmonthWindow, time(0, 0))
day4 = datetime.combine(previousIntervalDate, time(0, 0))
day5 = datetime.combine(oneYearWindow, time(0, 0))
day6 = datetime.combine(previousYearDate, time(0, 0))

##Mongo Connection
def getMongoConnection(MONGO_HOST,MONGO_PORT,DB, isSlave=False, isAuth=False, username = 'analytics', password = 'aN*lyt!cs@321'):
    if isAuth:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave, username = username, password = password)
        mongo_conn.authenticate(username, password)
    else:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave)
    mongo_conn = connection[DB]
    return mongo_conn

##MySql Connection
def getMySqlConnection(HOST, PORT, DB_USER, DB_PASSWORD, DB):
    return MySQLdb.connect(host = HOST,port = PORT,user =DB_USER,passwd =DB_PASSWORD,db = DB)

mongo_conn = getMongoConnection('172.22.65.59', 27017, 'recruiter_master', True)
collectionCM = getattr(mongo_conn, 'CandidateMatch')

mongo_conn = getMongoConnection('172.22.66.198', 27017, 'recruiter_master', True)
collectionCMNew = getattr(mongo_conn, 'CandidateMatch')
collectionCMOYNew = getattr(mongo_conn, 'CandidateMatchOneYear')

projectHome = '/data/Projects/ApplicationData/'
db_name = 'recruiter_master'

def getApplicationDataInc():
    st = datetime.now()
    print "Incremental Applications Data start at :" + str(st)
    Applications = collectionCM.find({'ad':{'$gte':day2,'$lt':day1}}, {'fcu': 1, 'fjj': 1, 'ad': 1})
    print Applications.count()
    appList = []
    for app in Applications:
        appList.append([app['fjj'], app['fcu'], app['ad']])
    appList = [[item[0], item[1], item[2].strftime('%d%b%Y:%H:%M:%S')] for item in appList]
    appList.sort(key = operator.itemgetter(1,2))
    ofile = open(projectHome+ 'Input/ApplicationData.csv','wb')
    for item in appList:
        ofile.write(str(item[0]) + ',' + item[1] + ',' + str(item[2]) + '\n')
    print "Inc Application Data at :" + str(datetime.now())
    print "Run time :" +  str(datetime.now() - st)

def updateCandidateMatch():
    st = datetime.now()
    print 'Candidate Application Before Removal Yearly : ' + str(collectionCMOYNew.count()) + ' at time : ' + str(datetime.now())
    collectionCMOYNew.remove({'ad':{'$gte':day6,'$lt':day5}})
    print 'Candidate Application After Removal Yearly : ' + str(collectionCMOYNew.count()) + ' at time : ' + str(datetime.now())
    print 'Candidate Application Before Removal : ' + str(collectionCMNew.count()) + ' at time : ' + str(datetime.now())
    collectionCMNew.remove({'ad':{'$gte':day4,'$lt':day3}})
    print 'Candidate Application After Removal : ' + str(collectionCMNew.count()) + ' at time : ' + str(datetime.now())

    ifile = open(projectHome+ 'Input/ApplicationData.csv','r')
    for line in ifile:
        data = line.strip().split(',')
        JobId = int(data[0])
        UserId = str(data[1])
        CurrentAppTime = datetime.strptime(str(data[2]), '%d%b%Y:%H:%M:%S')
        collectionCMNew.insert({'fjj':JobId, 'fcu':UserId, 'ad':CurrentAppTime})
	collectionCMOYNew.insert({'fjj':JobId, 'fcu':UserId, 'ad':CurrentAppTime})
    print 'Candidate Application Final Yearly : ' + str(collectionCMOYNew.count())  + ' at time : ' + str(datetime.now())
    print 'Candidate Application Final : ' + str(collectionCMNew.count())  + ' at time : ' + str(datetime.now())
    print "Run time :" +  str(datetime.now() - st)

def BackupandJobSuggDump():
    st = datetime.now()
    print "recruiter_master Mongo Backup start at :" + str(st)
    if (todayDate.weekday() == 0):
	backUpRoot = projectHome + 'backup'
	os.system("mongodump -h 172.22.66.198 --db "+db_name+" --out "+backUpRoot+"/CandidateMatch")
	os.system("tar -cvf "+backUpRoot+"/CandidateMatch/CandidateMatch_"+str(todayDate)+".tar.gz "+backUpRoot+"/CandidateMatch/recruiter_master")
	os.system("rm -rf "+backUpRoot+"/CandidateMatch/recruiter_master")
	print 'Backup Successfull'
    else:
	print 'Backup Skipped as not a Monday'
    print "recruiter_master Mongo Backup completed at :" + str(datetime.now())
    print "Run time :" +  str(datetime.now() - st)

def main():
    st = datetime.now()
    print 'Start at Time :', str(st)
    print day1
    print day2
    print day3
    print day4
    print day5
    print day6
    getApplicationDataInc()
    updateCandidateMatch()
    BackupandJobSuggDump()
    open(projectHome + 'Docs/appdata_updated_till.txt','a').write(str(todayDate)+'\n')
    print 'Total run time :', str(datetime.now() - st)

if __name__=='__main__':
    main()
    

