import os, re, string, httpagentparser, MySQLdb
import ftplib
import socket
import traceback
from datetime import date,datetime,timedelta
from dateutil.relativedelta import *
from urllib import unquote

def downloadLog(date):
	concernedDate = string.replace(date.isoformat(),'-','')
	predate = date+relativedelta(days = -1)
	previousDate = string.replace(predate.isoformat(),'-','')
	MonYear = date.strftime("%b-%Y")
	MonYear_1 = predate.strftime("%b-%Y")
	PATH = "/backup/Shine/apachelogs/"+MonYear
        if os.path.isdir(PATH):
                print "destination directory present"
        else:
                os.mkdir(PATH)
	downloadLoc = '.'
        host = '172.22.65.179'
        username = 'analytics'
        password = '@n@Lyt1c$'
	#"httpd-access-analytics_"+concernedDate+".log"
        file1 = 'httpd-access.log-'+previousDate+'.gz'
        file2 = 'httpd-access.log-'+concernedDate
        r = 0
        isdir = 0
        ftp1 = ftplib.FTP()
        ftp1.connect(host)
        try:
                try:
                        ftp1.login(username, password)
			'''
                        print "Downloading..."+ file1
			f = open(file1,"wb")
			ftp1.retrbinary("RETR " + file1,f.write)
			f.close()
			'''
                        print "Downloading..."+ file2
			f = open(file2,"wb")
			ftp1.retrbinary("RETR " + file2,f.write)
			f.close()
                        r = 1
                finally:
                        #print "Quitting..."
                        ftp1.quit()
        except:
                traceback.print_exc()
        #print "ok"
        del ftp1
	if r == 1:
                print "Apache Logs Downloaded"
		#os.system("mv "+file1+" /backup/Shine/apachelogs/"+MonYear_1)
		os.system("mv "+file2+" /backup/Shine/apachelogs/"+MonYear)
        return r

def applyDictionary(file):
	print 'Applying Dictionary ...'
	dictFp = open('/data/Shine/ProductMailerResponse/Docs/Html_Url_Encoding.txt','r')
	dict1 = {}

	for line in dictFp:
		line1 = line.split('\t')
		replaceTo = line1[0]
		replaceFrom = line1[1].strip('\r\n')
		dict1[replaceFrom] = replaceTo
	dictFp.close()

	#print file
	if os.path.isfile(file) == False:
		for key in dict1.keys():
			file = file.replace(key,dict1[key])
	                file = file.replace(key.lower(),dict1[key])
		return file

	outputFile = file+'_'
	fp = open(file,'r')
	fp2 = open(outputFile,'w')
	code = 0

	'''for key in dict1.keys():
		print key.lower()+' '+dict1[key]'''
	lineCount = 0
	for line in fp:
		line1 = line
		lineCount += 1
		while re.search('%[2-7][0-9a-f]', line, re.IGNORECASE):
			for key in dict1.keys():
				#print key.lower()+' '+dict1[key]
				line = line.replace(key,dict1[key])
				line = line.replace(key.lower(),dict1[key])
		fp2.write(line)
	fp.close()
	fp2.close()
	os.system('mv '+outputFile+' '+file)
	'''fp2 = open(outputFile,'r')
	#print fp1.read()
	for line in fp2:
		for key in dict1.keys():
			if re.search(key, line):
				print line
				break
	fp2.close()'''
		
def makeSingleLogofDay(cd, analyticsLog):
	print "Making Single Log of Day "+str(cd)
	concernedDate = string.replace(cd.isoformat(),'-','')
        pd = cd+relativedelta(days=-1)
	previousDate = string.replace(pd.isoformat(),'-','')
	Mon = cd.strftime("%b")
	Mon_1 = pd.strftime("%b")
	Year = cd.strftime("%Y")
	Year_1 = pd.strftime("%Y")
	dt = concernedDate[-2:]
	if dt[0] == '0':
		dt = ' '+dt[1]
	if os.path.isfile("/backup/Shine/apachelogs/httpd-access.log-"+concernedDate+".gz"):
		os.system("mv /backup/Shine/apachelogs/httpd-access.log-"+concernedDate+".gz /backup/Shine/apachelogs/"+Mon+"-"+Year+"/httpd-access.log")
	cdlog = "/backup/Shine/apachelogs/"+Mon+"-"+Year+"/httpd-access.log-"+concernedDate
	pdlog = "/backup/Shine/apachelogs/"+Mon_1+"-"+Year+"/httpd-access.log-"+previousDate
	if os.path.isfile(cdlog+".gz") :
		command="gunzip -cd "+cdlog+".gz > "+cdlog
		print command
		os.system(command)
	if os.path.isfile(pdlog+".gz") :
		command="gunzip -cd "+pdlog+".gz > "+pdlog
		print command
		os.system(command)
	command="grep -i '"+Mon+" "+dt+"' "+pdlog+" > "+analyticsLog
	print command
	os.system(command)
	command="grep -i '"+Mon+" "+dt+"' "+cdlog+" >> "+analyticsLog
	print command
	os.system(command)

def main():
	#Update JAResponse and WVResponse Tables
	startTime = datetime.now()
	print 'Execution Started At: '+str(startTime)
	#return
	todayDate = date.today()
	cd = todayDate+relativedelta(days=-1)#concerned Date
        #cd = date(2012,09,28)          #set manually
	concernedDate = string.replace(cd.isoformat(),'-','')
	MonYear = cd.strftime("%b-%Y")
	analyticsLog="/backup/Shine/apachelogs/analyticslog/"+MonYear+"/httpd-access-analytics-"+concernedDate+".log"
	print analyticsLog
	#if not exists http logs condition
	downloadLog(cd)
	PATH = "/backup/Shine/apachelogs/analyticslog/"+MonYear
	if os.path.isdir(PATH):
                print "destination directory present"
        else:
                os.mkdir(PATH)
        makeSingleLogofDay(cd, analyticsLog)
        open(analyticsLog+'_','w').write( unquote(unquote(open(analyticsLog,'r').read())) )
        os.system('mv '+analyticsLog+'_ '+analyticsLog)
	PATH = "/backup/Shine/apachelogs/jobAlertClickData/"+cd.strftime("%b-%Y")
	if os.path.isdir(PATH):
                print "destination directory present"
        else:
                os.mkdir(PATH)
	jobAlertLog = "/backup/Shine/apachelogs/jobAlertClickData/"+cd.strftime("%b-%Y")+"/"+cd.isoformat()+"_jobAlertClickData.csv"
	print "Parsing into "+jobAlertLog
	oResult = open(jobAlertLog,"w")
	count=0
	lineCount = 0
	file = open(analyticsLog,"r")
	for line in file:
		if re.search('\n', line, re.IGNORECASE) == None:
			continue
		'''
		if re.search('%[2-7][0-9a-f]', line, re.IGNORECASE) != None:
			print 'Dictionary Not Applied Correctly. Quitting...'
			return
		'''

		containsUtmCampaign = re.search(r'tm_campaign=.*?[&| ]', line, re.IGNORECASE)
		dwm = re.search("tm_content=.*?\|(daily|weekly|monthly|2y)", line)
		old_format=re.search("et=21.*?em=.*?&",line)
		#isJobAlertClick = (containsUtmCampaign != None and re.search(r'jobalert', containsUtmCampaign.group(0).split('=')[1], re.IGNORECASE)) or dwm != None
		isNewFormat = re.search(r'jobalert', str(line), re.IGNORECASE)
		isOldFormat = (old_format != None and len(old_format.group(0).split('='))>=4)
		isJobAlertClick = isNewFormat or isOldFormat
	        if isJobAlertClick == False:
			continue
		lineCount+=1
		#continue
		eMail=id_nosql=UserId=jobId=sentDate=clickedDate=freq=linkText=linkPosition=userAgent=domain=''

		if re.search('"', line) != None:#calculation of UserAgent
			userAgent = line.split('"')[-2]
			OS = httpagentparser.simple_detect(userAgent)[0]
			browser = httpagentparser.simple_detect(userAgent)[1]
		#print userAgent

		Str = re.search("\[.*?\]", line)#calculation of clickedDate
		if Str != None:
		    try:
			clickedDate=datetime.strptime(Str.group(0)[:21].strip('['),'%d/%b/%Y:%H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
		    except:
			clickedDate=''
		#print clickedDate

		if isOldFormat:
			eMail = old_format.group(0).split('=')[3].strip('&')
			sentDate = re.search("&ts=.*?[apH]",line)
			if sentDate != None:
				sentDate = sentDate.group(0).split('=')[1][:-2]
				#sentDate = datetime.strptime(sentDate[:-5],'%b. %d, %Y, %H:%M').strftime('%Y-%m-%d %H:%M:%S')
				Month = re.match('[a-z]{4}', sentDate,re.IGNORECASE)
				if Month != None:
					sentDate = sentDate[:3]+sentDate[4:]
				if re.match('[a-z]{3,4}\. [0-9]{1,2}, [0-9]{4}, [0-9]{1,2}:[0-9]{1,2}', sentDate,re.IGNORECASE):
					#print sentDate
					sentDate = datetime.strptime(sentDate,'%b. %d, %Y, %H:%M').strftime('%Y-%m-%d %H:%M:%S')
				elif re.match('[a-z]{3,4}\.\+[0-9]{1,2},\+[0-9]{4},\+[0-9]{1,2}:[0-9]{1,2}', sentDate,re.IGNORECASE):
					sentDate = datetime.strptime(sentDate,'%b.+%d,+%Y,+%H:%M').strftime('%Y-%m-%d %H:%M:%S')
				else:
					sentDate
			else:
				sentDate = ''
			#print sentDate
		elif isNewFormat:
			domain1 = re.search("utm_source=.*?\&", line)#calculation of domain
			if domain1 != None:
				domain = domain1.group(0).split('=')[1]#.replace('%5B','').replace('%5D','').replace('%27','').replace('&','')
			else:
				domain1 = re.search("utm_source=.*?\"", line)
				if domain1 != None:
					domain = domain1.group(0).split('=')[1].strip('"')

			line_split = line.split('|')
			if len(line_split) < 4:
				count += 1
				#print 'line_split < 4'
				#print line
				continue

			eMail = line_split[-2]#calculation of id_nosql

			sentDate=line_split[-3].replace('T',' ')[:19]#calculation of sentDate
		
			freq1 = re.search('.*?ly',line_split[-1])#calculation of freq
			if freq1 != None:
				freq = freq1.group(0)
			#print freq

			is_enb_block_type = re.search('"enb_block":".*?"', line)#calculation of linkText linkPosition jobId
			if is_enb_block_type != None:
				linkText = is_enb_block_type.group(0).split('"')[-2]
				linkPosition = line_split[-4][-2:]
			else:
				is_enb_block_type
				linkText1 = re.search('tm_content=.*?\|',line, re.IGNORECASE)
				if linkText1 != None:
					linkText1 = linkText1.group(0)
					linkText = linkText1[linkText1.find('=')+1:linkText1.find('|')]
				linkPosition = line_split[-4]
				if re.search('(L|O)J=[0-9]*',linkPosition) != None:
					jobId = linkText
					linkText = 'View & Apply'
		#print id_nosql+','+UserId+','+jobId+','+sentDate+','+clickedDate+','+freq+','+linkText+','+linkPosition+','+domain+','+userAgent
		oResult.write(id_nosql+','+eMail+','+UserId+','+jobId+','+sentDate+','+clickedDate+','+freq+','+linkText+','+linkPosition+','+domain+','+browser+','+OS+'\n')
		#make idnosql variable UserId and remove other UserId variable
	print ('Log Parsed')
	print lineCount
	print datetime.now()
	oResult.close()
	file.close()
	os.system("mysql -uAnalytics -pAn@lytics -e \"TRUNCATE TABLE JobAlert.jobAlertClick;\"")
	os.system("cp "+jobAlertLog+" /backup/Shine/apachelogs/jobAlertClickData/jobAlertClick.csv")
        os.system("mysqlimport -uAnalytics -pAn@lytics --local JobAlert /backup/Shine/apachelogs/jobAlertClickData/jobAlertClick.csv --fields-terminated-by=',' --lines-terminated-by='\n' --fields-optionally-enclosed-by='\"'")
	os.system("mysql -uAnalytics -pAn@lytics JobAlert -e\"Update JobAlert.jobAlertClick A, INDExportDB.Candidates B SET A.id_nosql = B.UserId WHERE A.eMail = B.Username AND A.eMail != '' AND A.id_nosql = '';\"")

	#whoViewed
	file = open(analyticsLog,"r")
	PATH = "/backup/Shine/apachelogs/whoviewed/"+cd.strftime("%b-%Y")
	if os.path.isdir(PATH):
                print "destination directory present"
        else:
                os.mkdir(PATH)
        whoViewedFile = "/backup/Shine/apachelogs/whoviewed/"+cd.strftime("%b-%Y")+"/"+cd.isoformat()+"_whoViewedFile.csv"
        oResult = open(whoViewedFile,"w")
        lineCount = 0
	print "Parsing into "+whoViewedFile
        for line in file:
                #containsUtmCampaign = re.search(r'tm_campaign=.*?whoviewed', line, re.IGNORECASE)
		containsUtmCampaign = re.search(r'tm_campaign=.*?whoviewed|tm_campaign=.*?activityMailer', line, re.IGNORECASE)
                if containsUtmCampaign:
                        lineCount += 1
                        #id_nosql = line.split('|')[-1].split('&')[0]#calculation of id_nosql
			id_nosql = line.split('|')[-1].split('&')[0].split(' ')[0].replace('"','')
			try:
                                sentDate = line.split('|')[-2]
                        except:
                                sentDate = ''
                        sentDate = sentDate[:10]+' '+sentDate[11:19]
			clickDate = cd.isoformat()
                        oResult.write(id_nosql+','+sentDate+','+cd.isoformat()+'\n')
        print ('Log Parsed')
        oResult.close()
        file.close()
        os.system("mysql -uAnalytics -pAn@lytics -e \"TRUNCATE TABLE JobAlert.whoViewedMyProfile;\"")
        os.system("cp "+whoViewedFile+" /backup/Shine/apachelogs/whoviewed/whoViewedMyProfile.csv")
        os.system("mysqlimport -uAnalytics -pAn@lytics --local JobAlert /backup/Shine/apachelogs/whoviewed/whoViewedMyProfile.csv --fields-terminated-by=',' --lines-terminated-by='\n' --fields-optionally-enclosed-by='\"'")
	clicksWhoViewed = lineCount
        #os.system('echo -n logins: ;sort '+whoViewedFile+'|uniq|wc -l')
	conn1 = MySQLdb.connect (host='localhost',user='Analytics',passwd='An@lytics',db='JobAlert')
        cursor1=conn1.cursor()
        q1 = "SELECT COUNT(DISTINCT id_nosql) FROM whoViewedMyProfile;"; cursor1.execute(q1); row1=cursor1.fetchone(); 
	loginsWhoViewed = row1[0]
	q1 = "SELECT COUNT(DISTINCT A.id_nosql) FROM whoViewedMyProfile A JOIN INDExportDB.Candidates B ON A.id_nosql = B.UserId WHERE DATE(B.LastUpdatedDate) = DATE_SUB(CURDATE(), INTERVAL 1 DAY);"; cursor1.execute(q1); row1=cursor1.fetchone();
	modifiesWhoViewed = row1[0]
	whoViewedResults = "/data/Shine/ProductMailerResponse/Output/WV/whoViewedResults.csv"
	oResult = open(whoViewedResults,"a")
	oResult.write(cd.isoformat()+','+str(clicksWhoViewed)+','+str(loginsWhoViewed)+','+str(modifiesWhoViewed)+'\n')
	oResult.close()	
	PATH = "/data/Shine/ProductMailerResponse/Output/JA/"+cd.strftime("%b-%Y")
        if os.path.isdir(PATH):
                print "destination directory present"
        else:
                os.mkdir(PATH)
	command = "mysql -uAnalytics -pAn@lytics --skip-column-names JobAlert < /backup/Shine/apachelogs/emailAnalytics/emailAnalytics.sql | sed 's/\t/,/g' > /backup/Shine/apachelogs/emailAnalytics/"+cd.strftime("%b-%Y")+"/"+cd.isoformat()+"_emailAnalyticsResults.csv"
	command = "mysql -uAnalytics -pAn@lytics --skip-column-names JobAlert < /data/Shine/ProductMailerResponse/Model/SQL/emailAnalytics.sql | sed 's/\t/,/g' > /data/Shine/ProductMailerResponse/Output/JA/"+cd.strftime("%b-%Y")+"/"+cd.isoformat()+"_emailAnalyticsResults.csv"
	print command
	os.system(command)

	#MakeVendorLog
	PATH = '/backup/Shine/apachelogs/VendorLogs/'+cd.strftime('%b-%Y')
        if os.path.isdir(PATH):
                print "destination directory present"
        else:
                os.mkdir(PATH)
	perlScript = '/data/Shine/ProductMailerResponse/Model/ShellCode/getedetails.pl'
        cmd = 'date;'+perlScript+' < /backup/Shine/apachelogs/analyticslog/'+cd.strftime('%b-%Y')+'/httpd-access-analytics-'+str(cd).replace('-','')+'.log | sort | uniq | sed \'s/?.*email=\(.*\)&phone.*|\(.*\)/\\1|\\2/g\' | sed \'s/\(.*cmm\).*/\\1/g\' > /backup/Shine/apachelogs/VendorLogs/'+cd.strftime('%b-%Y')+'/'+str(cd)+' ;date'
        print cmd
	os.system(cmd)
	finishTime = datetime.now()
	print str(finishTime - startTime)+'\n'


if __name__=='__main__':
        main()
