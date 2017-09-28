from datetime import date, datetime, time
from dateutil.relativedelta import *
from operator import itemgetter
from pymongo import Connection
import MySQLdb
import csv
import sys
import sendMailUtil
import pdb
reload(sys)
sys.setdefaultencoding("utf8")

todayDate=date.today()
previousDate=todayDate+relativedelta(days=-4)
day0 = datetime.combine(todayDate, time(0, 0))
day1 = datetime.combine(previousDate, time(0, 0))

def getMongoConnection(MONGO_HOST,MONGO_PORT,DB, isSlave=False, isAuth=False, username = 'analytics', password = 'aN*lyt!cs@321'):
    if isAuth:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave, username = username, password = password)
    else:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave)
    mongo_conn = connection[DB]
    mongo_conn.authenticate(username, password)
    return mongo_conn

def getDataFromMongo():
    mongo_conn = getMongoConnection('172.22.65.109',27017, 'history_logs',True)
    collection = getattr(mongo_conn, 'jamatch')
    file = '/data/Shine/Shine_AdHoc/Output/jamatch_new.csv'
    ofile = open(file,'w')
    ofile.write('JobId,MatchedJobs,OtherSuggested,Date,datepart,month,year\n')
    file_obj = '/data/Shine/Shine_AdHoc/Input/jam_input.csv'
    with open(file_obj) as inputfile:
        reader = csv.DictReader(inputfile, delimiter=',')
        for line in reader:
            try :
                JobId = int(line["JobId"].strip())
            except :
                continue
            rows = collection.find({'j':JobId},{'j':1,'mj':1,'oj':1,'d':1})
            for row in rows:
		'''
                try:
                    date = row['d'].strftime('%d-%m-%y')
                except:
                    date = str(row['d'])
		'''
		date = str(row['d'])
		datepart = date.split('-')[0]
		month = date.split('-')[1]
		year = date.split('-')[2]
		
                matched = str(row['mj'])
		other = str(row['oj'])
                ofile.write(str(JobId)+','+matched+','+other+','+date+','+datepart+','+month+','+year+'\n')
        ofile.close()
def main():
    getDataFromMongo()

if __name__=='__main__':
    main()
