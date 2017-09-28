import pymongo, os, sys
from datetime import datetime, date, time, timedelta
from dateutil.relativedelta import *
from pymongo import Connection
from getDataFromCollection import *
#import unicodedata
import base64
from Crypto.Cipher import XOR
import sendMailUtil

TOKEN_DT_FORMAT = '%Y%m%d%H%M%S'
todayDate=date.today()
#todayDate=todayDate+relativedelta(days=-1)
previousDate=todayDate+relativedelta(days=-1)
day1 = datetime.combine(previousDate, time(0, 0))
day2 = datetime.combine(todayDate, time(0, 0))
mailing_list = ['prakhar.raj@hindustantimes.com']
cc_list = ['mayank.singh@hindustantimes.com']
bcc_list = ['shailendra.kesarwani@hindustantimes.com']
projectHome = '/data/Analytics/Utils/consolidatedDB'
sas_log_file = projectHome+'/log/updateConsolidatedDB.log'
temp_log_file = projectHome+'/log/temp.log'

def run_sas_stmnt(sas_program, sas_log_file):
        sas_temp_file="/data/Analytics/temp"
        sas_query="/data/SAS/sasinstall_STAT/SASFoundation/9.2/sas -work "+str(sas_temp_file)+" -log "+str(sas_log_file)+" -SYSIN "+str(sas_program)+" -nonews -noterminal"
        print str(sas_query)
        os.system(str(sas_query))

#db.auth('analytics','aN*lyt!cs@321')
def getMongoConnection(MONGO_HOST,MONGO_PORT,DB, isSlave=False, isAuth=False, username = 'analytics', password = 'aN*lyt!cs@321'):
    if isAuth:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave, username = username, password = password)
    else:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave)
    mongo_conn = connection[DB]
    mongo_conn.authenticate(username, password)
    return mongo_conn

def dataUpdationCheck():
	mongo_conn = getMongoConnection('172.22.65.157',27018, 'sumoplus',True, True)
        collection = getattr(mongo_conn, 'CandidateStatic')
        doclist = collection.find({'rsd':{'$gte':day1,'$lt':day2}},{'rsd':1, '_id':0}).sort('rsd',-1).limit(1)
        for doc in doclist:
                rsdMax = doc['rsd']
        print rsdMax
        rsdMaxHour = rsdMax.time().hour
        rsdMaxMin = rsdMax.time().minute
        if rsdMaxHour < 23 or (rsdMaxHour == 23 and rsdMaxMin < 45):
                print "mongo DB Not updated"
                sys.exit()
	else:
		print "mongo DB is upto date"

def getDataFromMongo():
	mongo_conn = getMongoConnection('172.22.65.157',27018, 'sumoplus',True, True)
	getDataFromCollection(mongo_conn, 'CandidateStatic', day1, day2, ['bod', 'lad', 'll', 'lm', 'rsd', 'red', 'pu', 'yeu'])
	getDataFromCollection(mongo_conn, 'CandidateResumes', day1, day2, ['cd'])
        getDataFromCollection2(mongo_conn, 'CandidatePreferences', day1, day2, 'CandidateStatic', ['lm','bod'])
        getDataFromCollection2(mongo_conn, 'CandidateJobs', day1, day2, 'CandidateStatic', ['lm','bod'])
        getDataFromCollection2(mongo_conn, 'CandidateEducation', day1, day2, 'CandidateStatic', ['lm','bod'])
	#getDataFromCollection(mongo_conn, 'mob_verify_logs', day1, day2, ['lm'])
	getDataFromCollection(mongo_conn, 'mobverifylogs', day1, day2, ['lm'])
	getDataFromCollection(mongo_conn, 'my_parichay', day1, day2, ['lad', 'lm', 'red'])
	getDataFromCollection(mongo_conn, 'CandidateAPIToken', day1, day2, ['cd', 'ed'])
	mongo_conn = getMongoConnection('172.22.65.59',27017, 'recruiter_master', True)
	#mongo_conn = getMongoConnection('172.22.65.58',27017, 'recruiter_master', True)
	getDataFromCollection(mongo_conn, 'CandidateMatch', day1, day2, ['ad'])
	try:
		getDataFromCollection(mongo_conn, 'Activity', day1, day2, ['d'])
	except:
		pass

def token_encode(email, type, days=None):
    key_expires = datetime.today() + timedelta(days)
    inp_str = '{salt}|{email}|{type}|{dt}'.format(**{'salt': 'xfxa','email': email, 'type': type, 'dt': key_expires.strftime(TOKEN_DT_FORMAT)})
    ciph = XOR.new('xfxa')
    return base64.urlsafe_b64encode(ciph.encrypt(inp_str))

def token_creator(filename):
        i = 0
        fp1=open(filename,"r")
        fp2=open("/data/Analytics/Utils/consolidatedDB/Input/CandidatesTokenInc.csv","w")
        for line in fp1:
                i+=1
                line = line.rstrip()
                line1 = line.split(',')
                email = line1[0]
                if i == 1:
                        token = 'autoLoginToken'
                else:
                        token = token_encode(email,4,730)
                fp2.write(line+','+token+'\n')
        print str(i)+" tokens generated"

def updateClickLogs(todayDate, previousDate):
	os.system('rm '+projectHome+'/Input/JAClickInc.csv')
	date = previousDate
	while date < todayDate:
		cmd = 'cat /backup/Shine/apachelogs/jobAlertClickData/'+date.strftime('%b-%Y')+'/'+str(date)+'_jobAlertClickData.csv >> '+projectHome+'/Input/JAClickInc.csv'
		print cmd;	os.system(cmd)
		date = date+relativedelta(days=1)

def updateSentMailLogs(todayDate, previousDate):
	os.system('rm '+projectHome+'/Input/JASentInc.csv')
	os.system('rm '+projectHome+'/Input/WhoViewedInc.csv')
	date = previousDate
	while date < todayDate:
		cmd = 'cat /backup/Shine/JobAlert/JobAlertSentInfo/'+date.strftime('%b-%Y')+'/JASent_*ly_'+str(date)+'.csv  >> '+projectHome+'/Input/JASentInc.csv'
		print cmd;	os.system(cmd)
		cmd = 'cat /backup/Shine/whoViewed/whoViewedSentInfo/'+date.strftime('%b-%Y')+'/WhoViewed_'+str(date)+'.csv  >> '+projectHome+'/Input/WhoViewedInc.csv'
		print cmd;	os.system(cmd)
		date = date+relativedelta(days=1)

def updateMysql():
        #os.system("cp /data/Analytics/Utils/consolidatedDB/Output/IncExtract/CandidatesInc.csv /data/Analytics/Utils/consolidatedDB/Output/IncExtract/INDExportInc_"+str(previousDate)+".csv")
        #os.system("mysql -uAnalytics -pAn@lytics < /data/Analytics/Utils/consolidatedDB/Model/SQL/mysql_data_import_inc_v2.sql")
        os.system("cp /data/Analytics/Utils/consolidatedDB/Output/IncExtract/CandidatesInc1.csv /data/Analytics/Utils/consolidatedDB/Output/IncExtract/INDExportInc_"+str(previousDate)+"1.csv")
        os.system("cp /data/Analytics/Utils/consolidatedDB/Output/IncExtract/CandidatesInc2.csv /data/Analytics/Utils/consolidatedDB/Output/IncExtract/INDExportInc_"+str(previousDate)+"2.csv")
        os.system("mysql -uAnalytics -pAn@lytics < /data/Analytics/Utils/consolidatedDB/Model/SQL/mysql_data_import_inc_v3.sql")

def main():
	print day1, day2
	#return
        startTime = datetime.now()
	print startTime
	dataUpdationCheck()
	getDataFromMongo()
	updateSentMailLogs(todayDate, previousDate)
	updateClickLogs(todayDate, previousDate)
	os.system('sh /data/Analytics/Utils/consolidatedDB/Model/ShellScript/updateRecruiterloginhistory.sh')
	run_sas_stmnt('/data/Analytics/Utils/consolidatedDB/Model/SASCode/importAllData_v2.sas', '/data/Analytics/Utils/consolidatedDB/log/importAllData_v2_'+str(previousDate)+'.log')
	print datetime.now()
	
	#Generate New Tokens
	token_creator("/data/Analytics/Utils/consolidatedDB/Input/EmailInc.csv")
	run_sas_stmnt('/data/Analytics/Utils/consolidatedDB/Model/SASCode/consolidatedDB_Updation_v2.sas', '/data/Analytics/Utils/consolidatedDB/log/consolidatedDB_Updation_v2_'+str(previousDate)+'.log')
	print datetime.now()

	run_sas_stmnt('/data/Analytics/Utils/consolidatedDB/Model/SASCode/consolidatedDB_Updation_v2_2.sas', '/data/Analytics/Utils/consolidatedDB/log/consolidatedDB_Updation_v2_2_'+str(previousDate)+'.log')
	print datetime.now()

	run_sas_stmnt('/data/Analytics/Utils/consolidatedDB/Model/SASCode/sqlUpdationDataset_v3.sas', '/data/Analytics/Utils/consolidatedDB/log/sqlUpdationDataset_v2_'+str(previousDate)+'.log')
	finishTime = datetime.now()
	print 'Time Taken:'+str(finishTime - startTime)

	run_sas_stmnt('/data/Analytics/Utils/consolidatedDB/Model/SASCode/consolidatedDB_Updation_v2_1.sas', '/data/Analytics/Utils/consolidatedDB/log/consolidatedDB_Updation_v2_1_'+str(previousDate)+'.log')
	finishTime = datetime.now()
	print 'Time Taken:'+str(finishTime - startTime)

	updateMysql()
	finishTime = datetime.now()
	print 'Time Taken:'+str(finishTime - startTime)

if __name__=='__main__':
        main()
#15 05 * * *  /data/Analytics/Utils/consolidatedDB/Model/Pycode/updateConsolidatedDB_2.py >> /data/Analytics/Utils/consolidatedDB/log/updateConsolidatedDB.log 2>&1
