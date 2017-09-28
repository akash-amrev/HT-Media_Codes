import poplib, sys,datetime, email, os
from pymongo import Connection
from pymongo.objectid import ObjectId
from datetime import date, datetime, time
from dateutil.relativedelta import *
import sendMailUtil
import csv

todayDate=date.today()
file_date = todayDate+relativedelta(days=-1)
file_date = file_date.strftime("%d-%b-%Y")
todayDate = todayDate.strftime("%d-%b-%Y")
todayDate = datetime.strptime(todayDate,"%d-%b-%Y")

previousDate=todayDate+relativedelta(days=-1)
previousDate = previousDate.strftime("%d-%b-%Y")
previousDate = datetime.strptime(previousDate,"%d-%b-%Y")


Month = previousDate.strftime("%b-%Y")
Year = previousDate.strftime("%Y")
day1 = datetime.combine(previousDate, time(0, 0))
day2 = datetime.combine(todayDate, time(0, 0))


def getMongoConnection(MONGO_HOST,MONGO_PORT,DB, isSlave=False, isAuth=False, username = 'analytics', password = 'aN*lyt!cs@321'):
    if isAuth:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave, username = username, password = password)
    else:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave)
    mongo_conn = connection[DB]
    mongo_conn.authenticate(username, password)
    return mongo_conn
	
mongo_conn = getMongoConnection('172.22.65.157',27018, 'sumoplus',True, True)
collection = getattr(mongo_conn, 'CandidateStatic')
collection_1 = getattr(mongo_conn, 'LookupExperience')

directory = '/data1/apacheRoot/html/shine/ReportArchieve/Daily_Report_Eweb' + '/' + str(Year) + '/' + str(Month) + '/' 
if os.path.isdir(directory):
        print "Directory Exists"
else:
        os.makedirs(directory)
        print "Directory_Created"
        
def mail_file(file):
    mailing_list = ['ewebguide@gmail.com']
    cc_list =['jitender.kumar@hindustantimes.com','prateek.agarwal1@hindustantimes.com']
    bcc_list = ['akash.verma@hindustantimes.com']
    Subject = 'Daily_Report_Eweb'
    Content = 'PFA the Daily Report For Eweb  \n\n Regards \n Akash'
    attachment = [file]
    sendMailUtil.send_mail(mailing_list ,cc_list, bcc_list, Subject ,Content,attachment ,0)

def getDataFromMongo():
    Output_File = directory + "Daily_Report_Eweb_Guide_" + str(file_date) + ".csv" 
    ofile = open(Output_File ,'wb')
    writer = csv.writer(ofile)
    writer.writerow(['User_Id','Vendor_Id','Experience'])
    Required_Data = collection.find({"evi": {"$in": [6235,6236]},'red':{'$gte':day1,'$lt':day2},'rm':0,'mo':0},{"_id": 1,"evi": 1,"ex": 1,'ev':1})
    for line in Required_Data:
        if 'ex' not in line.keys():
            continue
        elif len(str(line['ex']).strip()) == 0 or line['ex'] is None:
            continue
        else:
            Exp_Lookup = str(line['ex'])
            User_Id = str(line['_id'])
            Vendor_Id = str(line['evi'])
        Experience = collection_1.find({'v':int(Exp_Lookup)},{'d':1})
        for line in Experience:
                Experience = str(line['d'])
                writer.writerow([User_Id , Vendor_Id , Experience])
    print 'Data Written in Output File'            
    ofile.close()
    print 'Mailing Output File'
    mail_file(Output_File)
    print 'Mailing Done'

def main():
        print 'Start_Time:' +str(datetime.now())
        getDataFromMongo()
        print 'Finish_Time:' +str(datetime.now())

if __name__ == '__main__':
    main()                            
