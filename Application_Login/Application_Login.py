from pymongo import *
from datetime import date, datetime, time
from email import parser
from dateutil.relativedelta import *
from pymongo import Connection
import os, re, sendMailUtil, string, ftplib, traceback, pymongo, MySQLdb, subprocess, smtplib, socket
import pdb

todayDate=date.today()
previousDate = todayDate + relativedelta(days = -7)
previousDate2 = todayDate + relativedelta(days = -0)
day1 = datetime.combine(previousDate, time(0, 0))
day2 = datetime.combine(previousDate2, time(0, 0))
#day1 = todayDate + relativedelta(days = -21)
#day2 = day1 + relativedelta(days = +6)

def getMongoConnection(MONGO_HOST,MONGO_PORT,DB, isSlave=False, isAuth=False, username = 'analytics', password = 'aN*lyt!cs@321'):
    if isAuth:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave, username = username, password = password)
    else:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave)
    mongo_conn = connection[DB]
    mongo_conn.authenticate(username, password)
    return mongo_conn

def getDataFromCandidateStatic():
            Outfile = "/data/Shine/Shine_AdHoc/Output/cand_reg_after_" +str(previousDate) +".csv"
            ofile = open(Outfile,"w")
            mongo_conn = getMongoConnection('172.22.65.157', 27018, 'sumoplus', True)
            collection = getattr(mongo_conn, 'CandidateStatic')
            Total_Applications= collection.find({'rsd':{'$gte':day1,'$lt':day2},'ut' :2},{'_id' :1})
            # count = 0
            for app in Total_Applications :
                        #print app
                        userid = app['_id']
                        ofile.write(str(userid)+'\n')
            ofile.close() 

def run_sas_query(sas_program, sas_log_file):
	if os.path.isfile(sas_log_file):
		os.system('rm '+sas_log_file)
        sas_temp_file="/data/Analytics/temp"
        sas_query="/data/SAS/sasinstall_STAT/SASFoundation/9.2/sas -work "+str(sas_temp_file)+" -log "+str(sas_log_file)+" -SYSIN "+str(sas_program)+" -nonews -noterminal"
        print str(sas_query)
        os.system(str(sas_query))
        
def main():
        print datetime.now()
	getDataFromCandidateStatic()
	run_sas_query('/data/Shine/Shine_AdHoc/Model/SASCode/Application_Login.sas','/data/Shine/Shine_AdHoc/Model/SASCode/Application_Login.log')
	print datetime.now()
	
if __name__=='__main__':
    main()
