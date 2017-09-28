import MySQLdb
import os
from datetime import date, datetime, timedelta
from datetime import date, datetime, time
from dateutil.relativedelta import *
import string
import sendMailUtil

todayDate = date.today()
file_date = todayDate+relativedelta(days=-1)
file_date = file_date.strftime("%d-%b-%Y")

previousDate = todayDate + relativedelta(days =-1)
Month = previousDate.strftime("%b-%Y")
Year = previousDate.strftime("%Y")

directory = '/data/Shine/Shine_AdHoc/Output/Daily_Report_Digital_Sense' + '/' + str(Year) + '/' + str(Month) + '/' 

if os.path.isdir(directory):
        print "Directory Exists"
else:
        os.makedirs(directory)
        print "Directory_Created"
        
def getMySqlConnection(HOST, PORT, DB_USER, DB_PASSWORD, DB):
    return MySQLdb.connect(host = HOST,port = PORT,user =DB_USER,passwd =DB_PASSWORD,db = DB)
	
def getDataFromCandidateMysql(sqlFile, outFile, DB, options = ""):
    command="mysql -uAnalytics -pAn@lytics -hlocalhost -P3306 --skip-column-names "+options+" "+DB+" < "+str(sqlFile)+" |sed 's/\t/,/g' >> "+str(outFile)
    print command
    os.system(command)
    mail_file(outFile)
	

def mail_file(file):
    mailing_list = ['manpreet@sensedigital.in']
    cc_list =['jitender.kumar@hindustantimes.com' , 'prateek.agarwal1@hindustantimes.com']
    bcc_list = ['akash.verma@hindustantimes.com']
    Subject = 'Daily_Acquisition_Report_Digital_Sense'
    Content = 'PFA the Daily Acquisition Report For Digital Sense \n\n Regards \n Akash'
    attachment = [file]
    sendMailUtil.send_mail(mailing_list ,cc_list, bcc_list, Subject ,Content,attachment ,0)	

def main():
	print "Execution Started at:" +str(datetime.now())
	SQlFile = '/data/Shine/Shine_AdHoc/Model/SQL/Daily_Report_Digital_Sense.sql'
	Output_File = directory + "Daily_Report_Digital_Sense_" + str(file_date) + ".csv" 
	ofile = open(Output_File ,'w')
	ofile.write('UserId' + ',' + 'Total_Experience\n')
	ofile.close()
	getDataFromCandidateMysql(SQlFile , Output_File , "INDExportDB")
	print "Execution Completed at:" +str(datetime.now())

if __name__ == '__main__':
    main()
	

