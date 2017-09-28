import  sys,datetime, email, os
from pymongo import Connection
from pymongo.objectid import ObjectId
from datetime import date, datetime, time
from dateutil.relativedelta import *
import sendMailUtil
import csv
import MySQLdb

todayDate = date.today()
previousDate1 = todayDate+relativedelta(days=-1)
previousDate2 = todayDate+relativedelta(days=-183)
day1 = datetime.combine(todayDate, time(0, 0))
day2 = datetime.combine(previousDate1, time(0, 0))
day3 = datetime.combine(previousDate2, time(0, 0))

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

def mail_file(file):
    mailing_list = ['amit.gupta3@hindustantimes.com']
    cc_list =['prateek.agarwal1@hindustantimes.com','yashasvi.sharma@hindustantimes.com']
    bcc_list = ['akash.verma@hindustantimes.com']
    Subject = 'West Bengal DB BreakUp'
    Content = 'PFA the Daily Report For West Bengal \n\n Regards \n Akash'
    attachment = [file]
    sendMailUtil.send_mail(mailing_list ,cc_list, bcc_list, Subject ,Content,attachment ,0)

def getDataFromMongo():
    Input_File = '/data/Shine/Shine_AdHoc/Input/Cats_Vendor_Id.csv'
    ifile = open(Input_File,'rb+')
    Cats_Vendor_Id = []
    for rows in ifile:
        Cats_Vendor_Id.append(str(rows))
        Cats_Vendor_Id.append(int(rows))
    Cats_Vendor_Id = [int(str(i).replace('\n','')) for i in Cats_Vendor_Id]
    Output_File_Kolkata = '/data/Shine/Shine_AdHoc/Output/Kolkata_Segment.csv'
    Output_File_WestBengal = '/data/Shine/Shine_AdHoc/Output/WestBengal_Segment.csv'
    ofile_Kolkata = open(Output_File_Kolkata ,'wb')
    writer_Kolkata = csv.writer(ofile_Kolkata)
    writer_Kolkata.writerow(['Total_Acquistions_Kolkata','CATS_Acquisitions_Kolkata','ActiveDB_Kolkata','Final_Exits_Kolkata'])
    ofile_WB = open(Output_File_WestBengal ,'wb')
    writer_WB = csv.writer(ofile_WB)
    writer_WB.writerow(['Total_Acquistions_WB','CATS_Acquisitions_WB','ActiveDB_WB','Final_Exits_WB'])
    Total_Acquistions_Kolkata = collection.find({'cl':247,'rm':0,'mo':0,'red':{'$gte':day2,'$lte':day1},'rsd':{'$lte':day2}}).count()
    CATS_Acquisitions_Kolkata = collection.find({'cl':247,'red':{'$gte':day2,'$lte':day1},'rsd':{'$lte':day2},'rm':0,'mo':0,'evi':{"$in":Cats_Vendor_Id}}).count()
    ActiveDB_Kolkata = collection.find({'ll':{'$gte':datetime.combine(todayDate+relativedelta(days=-183), time(0, 0))}, 'rm':0, 'mo':0, 'lm':{'$gte':datetime.combine(todayDate+relativedelta(days=-730), time(0, 0))},'cl':247}).count()
    Final_Exits_Kolkata = collection.find({'ll':{'$gte':datetime.combine(todayDate+relativedelta(days=-184), time(0, 0)),'$lt':datetime.combine(todayDate+relativedelta(days=-183), time(0, 0))}, 'rm':0, 'mo':0,'cl':247,'lm':{'$gte':datetime.combine(todayDate+relativedelta(days=-730), time(0, 0))}}).count()
    Total_Acquistions_WB = collection.find({'cl':{"$in":[393,395,396,457,247,391,397]},'rm':0,'mo':0,'red':{'$gte':day2,'$lte':day1},'rsd':{'$lte':day2}}).count()
    ActiveDB_WB = collection.find({'ll':{'$gte':datetime.combine(todayDate+relativedelta(days=-183), time(0, 0))}, 'rm':0, 'mo':0, 'lm':{'$gte':datetime.combine(todayDate+relativedelta(days=-730), time(0, 0))},'cl':{"$in":[393,395,396,457,247,391,397]}}).count()
    CATS_Acquisitions_WB = collection.find({'cl':{"$in":[393,395,396,457,247,391,397]},'red':{'$gte':day2,'$lte':day1},'rsd':{'$lte':day2},'rm':0,'mo':0,'evi':{"$in": Cats_Vendor_Id}}).count()
    Final_Exits_WB = collection.find({'ll':{'$gte':datetime.combine(todayDate+relativedelta(days=-184), time(0, 0)),'$lt':datetime.combine(todayDate+relativedelta(days=-183), time(0, 0))}, 'rm':0, 'mo':0,'cl':{"$in":[393,395,396,457,247,391,397]},'lm':{'$gte':datetime.combine(todayDate+relativedelta(days=-730), time(0, 0))}}).count()
    print 'Total_Acquistions_Kolkata:' +str(Total_Acquistions_Kolkata) 
    print 'CATS_Acquisitions_Kolkata:' +str(CATS_Acquisitions_Kolkata)
    print 'ActiveDB_Kolkata:' +str(ActiveDB_Kolkata)
    print 'Final_Exits_Kolkata:' +str(Final_Exits_Kolkata)
    print 'Total_Acquistions_WB:' +str(Total_Acquistions_WB) 
    print 'CATS_Acquisitions_WB:' +str(CATS_Acquisitions_WB)
    print 'ActiveDB_WB:' +str(ActiveDB_WB)
    print 'Final_Exits_WB:' +str(Final_Exits_WB)
    writer_Kolkata.writerow([Total_Acquistions_Kolkata , CATS_Acquisitions_Kolkata , ActiveDB_Kolkata,Final_Exits_Kolkata])
    writer_WB.writerow([Total_Acquistions_WB , CATS_Acquisitions_WB , ActiveDB_WB,Final_Exits_WB])
    ofile_Kolkata.close()
    ofile_WB.close()
    print 'Mailing Output Files'
    mail_file(Output_File_Kolkata,Output_File_WestBengal)
    #mail_file(Output_File_WestBengal)
    print 'Mailing Done'

def main():
        print 'Start_Time:' +str(datetime.now())
        getDataFromMongo()
        print 'Finish_Time:' +str(datetime.now())

if __name__ == '__main__':
    main()                            
