import simplejson as json
from httplib2 import Http
from collections import defaultdict
from datetime import date, datetime, timedelta
import operator, os, pymongo, sys, string, re, time
import itertools, sendMailUtil
from datetime import date, datetime, time
from dateutil.relativedelta import *
import xml.etree.ElementTree as ET

todayDate=date.today()

def applyDictionxary(keyw):
    #print keyw,type(keyw)
    kw = keyw.replace('  ',' ')
    kw = kw.lower().replace(' and ','+AND+')
    kw = kw.replace(' or ','+OR+')
    kw = kw.replace(' ','+AND+')
    kw = '('+kw+')'
    return kw

def getNumberOfJobs(kw):
    SOLR_URL = 'http://172.22.66.85:8080/solr/Job/select?q='+str(kw)
    try:
        headers, response = Http().request(SOLR_URL, 'GET')
    except:# socket.error, msg:
        print "exception raised"
        #continue
    #time.sleep(1)
    ifile = open('/data/Shine/Shine_AdHoc/Model/Pycode/mycsv.xml','wb+')
    ifile.write(response)
    ifile.close()
    tree = ET.parse('/data/Shine/Shine_AdHoc/Model/Pycode/mycsv.xml')
    root = tree.getroot()
    for child in root:
        if (child.tag=='result'):
            for k,v in child.attrib.items():
                if (k=='numFound'):
                    return v
                
def mail_files(outFile):
    mailing_list=['parul.agarwal@hindustantimes.com']
    cc_list = []
    bcc_list = []
    subject = "IT - Skills Jobs Count"
    content = "PFA.\n\nRegards,\nParul\n\n\n\n*system generated email*"
    attachment = [file]
    print 'Mailing file......'
    sendMailUtil.send_mail(mailing_list,cc_list,bcc_list,subject, content,attachment,0)
           
def main():
    print datetime.now()
    outFile = '/data/Shine/Shine_AdHoc/Output/skills_jobs.csv'
    ofile = open(outFile,'wb+')
    ofile.write('keyword,Count of Jobs\n')    
    file1 = '/data/Shine/Shine_AdHoc/Input/IT_skills.csv'
    f1 = open(file1, 'r')
    for line in f1:
        keyw = line.strip('\r\n').split(',')[0]
        kw = applyDictionary(keyw)
        jobs = getNumberOfJobs(kw)
        #print keyw,kw,jobs
        if (jobs==None):
            continue
        else:
            ofile.write(keyw+','+jobs+'\n')
    f1.close()
    ofile.close()
    mail_files(outFile)
    print datetime.now()

if __name__=='__main__':
        main()
