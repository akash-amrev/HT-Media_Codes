from pymongo import Connection
from pymongo.objectid import ObjectId
from datetime import date, datetime, time
from dateutil.relativedelta import *
import sendMailUtil
import csv

todayDate=date.today()

def getMongoConnection(MONGO_HOST,MONGO_PORT,DB, isSlave=False, isAuth=False, username = 'analytics', password = 'aN*lyt!cs@321'):
    if isAuth:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave, username = username, password = password)
    else:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave)
    mongo_conn = connection[DB]
    mongo_conn.authenticate(username, password)
    return mongo_conn

def main():
	Output_File = '/data/Shine/Shine_AdHoc/Output/Exiting_Candidates_Dump.csv'
	ofile = open(Output_File,"wb+")
	ofile.write("LastLogin_Date,Count +\n")
	mongo_conn = getMongoConnection('172.22.65.157',27018, 'sumoplus',True, True)
	collection = getattr(mongo_conn, 'CandidateStatic')
	for i in range(-183,1):
		print i
		date = todayDate+relativedelta(days=  (i+1))
		print date
		ExitingCandidates = collection.find({'ll':{'$gte':datetime.combine(todayDate+relativedelta(days = i), time(0, 0)), '$lt':datetime.combine(todayDate+relativedelta(days= (i+1)), time(0, 0))}, 'rm':0, 'mo':0,'lm':{'$gte':datetime.combine(todayDate+relativedelta(days=-730), time(0, 0))}}).count()
		ofile.write(str(date) + ',' + str(ExitingCandidates) + '\n')
	ofile.close()
	
if __name__ == '__main__':
    main()  
	
		
		