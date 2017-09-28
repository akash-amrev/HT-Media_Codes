import os, sys, glob, time, zipfile, os.path, re
from datetime import date,datetime,timedelta
from dateutil.relativedelta import *
import ftplib
import socket
import traceback
import string
import xlrd
import unicodedata
import sendMailUtil
import base64
from Crypto.Cipher import XOR

TOKEN_DT_FORMAT = '%Y%m%d%H%M%S'

def transferFile(file,uploadLoc):
	host = 'ftp.kenscio.com'
	username = 'shineauto'
	password = 'sh!nEAu20'
	r = 0
	isdir = 0
        print "uploadLoc: "+uploadLoc
        print "files: \n"+file+"\nConnecting "+host+" ..."
        ftp1 = ftplib.FTP()
        ftp1.connect(host)
        #print ftp1.getwelcome()
        try:
                try:
        		print "Logging in..."
                        ftp1.login(username, password)
			directory=uploadLoc[uploadLoc.rfind("/")+1:]
		        loc=uploadLoc[:uploadLoc.rfind("/")]
			#print "loc: "+loc
			#print "directory: "+directory
	                ftp1.cwd(loc)
	                filelist=[]
            		ftp1.retrlines('LIST',filelist.append)
	                for file1 in filelist:
			    #print file
			    #print file.split()[-1]
            		    if file1.split()[-1]==directory:
	                	print "directory exist"
				isdir = 1
				break
			print isdir
			if isdir == 0:
            			ftp1.mkd(str(directory))
	                ftp1.cwd(str(directory))#move to the desired upload directory
                        print "Currently in:", ftp1.pwd()

                        print "Uploading..."
                        f = open(str(file), "rb")
                        name = os.path.split(file)[1]
                        #name=file[file.rfind("\\")+1:]#same as above
                        ftp1.storbinary('STOR ' + name, f)
                        f.close()

                        print "All Files saved OK"

                        #print "Files:"
			r = 1
                        #print ftp.retrlines('LIST')
                finally:
                        print "Quitting..."
                        ftp1.quit()
        except:
                traceback.print_exc()
	print "ok"
        del ftp1
	return r

def token_encode(email, type, days=None):
    key_expires = datetime.today() + timedelta(days)
    inp_str = '{salt}|{email}|{type}|{dt}'.format(**{'salt': 'xfxa','email': email, 'type': type, 'dt': key_expires.strftime(TOKEN_DT_FORMAT)})
    ciph = XOR.new('xfxa')
    return base64.urlsafe_b64encode(ciph.encrypt(inp_str))

def token_creator(filename):
	i = 0
	fp1=open(filename,"r")
	fp2=open("temp.csv","w")
	for line in fp1:
		i+=1
		line = line.rstrip()
		line1 = line.split(',')
                email = line1[0]
		
		if i == 1:
			token = ''
		else:
			token = token_encode(email,4,730)
		fp2.write(line+','+token+'\n')
	print str(i)+" tokens generated"
	os.system("mv temp.csv "+filename)

def generateReport(sql,outFile):
	command="mysql -uAnalytics -pAn@lytics -P3306 INDExportDB < "+str(sql)+" > "+str(outFile)
	print command
	ret = os.system(command)
	fp = open(outFile,"r+")
	result = fp.read()
	fp.close()
	result = result.replace('\t',',')
	fp = open(outFile,"w")
	fp.write(result)
	fp.close()
	return ret

def copyData(sourceFile,destination):
	if os.path.isfile(sourceFile):
		print "ok , file found"
		if os.path.isdir(destination):
			print "destination directory present"
		else:
			os.mkdir(destination)
		copyCommand="cp "+sourceFile+" "+destination
		os.system(copyCommand)
		fileName=sourceFile[sourceFile.rfind("/")+1:]
		destFile=destination+"/"+fileName
		print destFile
		if os.path.isfile(destFile):
			print "file copied successfull"
			return 1
		else:
			return 0

def run_sas_query(sas_program, sas_log_file):
        sas_temp_file="/data/Analytics/temp"
        sas_query="/data/SAS/sasinstall_STAT/SASFoundation/9.2/sas -work "+str(sas_temp_file)+" -log "+str(sas_log_file)+" -SYSIN "+str(sas_program)+" -nonews -noterminal"
        print str(sas_query)
        os.system(str(sas_query))

def main():
	starttime = datetime.now()
	print '\n'+str(starttime)
	todayDate=date.today()
	previousDate = todayDate+relativedelta(days = -1)
	previousDate2 = todayDate + relativedelta(days = -15)

	uploadDir="/data1/apacheRoot/html/shine/ReportArchieve/Midout_mail_extract/"+previousDate.strftime("%b-%Y")
	report_="/data/Analytics/Utils/MarketingReports/Output/sprinklerData/"
	if os.path.isdir(uploadDir):
		print "Directory present"
	else:
		os.mkdir(uploadDir)

	report = report_ + "Shine_SprinklerData_"+previousDate.strftime("%Y-%m-%d")+".csv"
	report1= report_ + "Shine_SprinklerData_Registration_"+previousDate2.strftime("%Y-%m-%d")+".csv"
        run_sas_query('/data/Analytics/Utils/MarketingReports/Model/SASCode/getSprinklerDataIncTemp_modified.sas', '/data/Analytics/Utils/MarketingReports/log/getSprinklerDataInc_modified_'+str(previousDate)+'.log')
	#run_sas_query('/data/Analytics/Utils/MarketingReports/Model/SASCode/getSprinklerDataIncTemp.sas', '/data/Analytics/Utils/MarketingReports/log/getSprinklerDataInc_'+str(previousDate)+'.log')
	#run_sas_query('/data/Analytics/Utils/MarketingReports/Model/SASCode/getSprinklerDataInc.sas', '/data/Analytics/Utils/MarketingReports/log/getSprinklerDataInc.log')
	#ret=generateReport(reportSql, report)
	ret_ = transferFile(report,"/shine_daily_import/"+previousDate.strftime("%Y-%m-%d"))#+"/")
	ret1_ = transferFile(report1,"/shine_daily_import/"+previousDate.strftime("%Y-%m-%d"))#+"/")
	mailing_list=['abhineet.sonkar@hindustantimes.com']
	cc_list=[]
	bcc_list=['mayankchd.29@gmail.com','shailendrakesarwani@gmail.com']
	subject="SprinklerData: Sprinkler email data file successfully generated"
        cont="MidOut and Login Data successfully uploaded on sprinkler FTP at "+date.today().strftime("%d-%m-%Y")+"\n"

	if os.path.isfile(report):
		status=copyData(report,uploadDir)#destination)
		os.system('rm '+report)
	else:
		print "Error creating data reports"
	finishTime = datetime.now()
	print "TimeTaken: "+str(finishTime-starttime)
if __name__ == '__main__':
	main()
