from datetime import date, datetime, time
from dateutil.relativedelta import *
from operator import itemgetter
from pymongo import Connection
import MySQLdb
import csv
import sys
import sendMailUtil
import pdb
import pandas,subprocess,os
import re

todayDate=date.today()
previousDate=todayDate+relativedelta(days=-1)
requiredDate=todayDate+relativedelta(days=-107)
day0 = datetime.combine(todayDate, time(0, 0))
day1 = datetime.combine(previousDate, time(0, 0))
day2 = datetime.combine(requiredDate, time(0, 0))

def getMongoConnection(MONGO_HOST,MONGO_PORT,DB, isSlave=False, isAuth=False, username = 'analytics', password = 'aN*lyt!cs@321'):
    if isAuth:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave, username = username, password = password)
    else:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave)
    mongo_conn = connection[DB]
    mongo_conn.authenticate(username, password)
    return mongo_conn

def main():
    mongo_conn = getMongoConnection('172.22.65.59',27017, 'recruiter_master',True, True)
    collection = getattr(mongo_conn, 'recent_search')
    file = "/data/Shine/Shine_AdHoc/Output/recr_non_compl_searches_output.csv"
    file1 = "/data/Shine/Shine_AdHoc/Output/recr_non_compl_all.csv"
    file2 = "/data/Shine/Shine_AdHoc/Output/recr_non_compl_any.csv"
    file3 = "/data/Shine/Shine_AdHoc/Output/recr_non_compl_exclude.csv"
    ofile = open(file,'w+')
    all_searches_ofile = open(file1,'w+')
    any_searches_ofile = open(file2,'w+')
    exclude_searches_ofile = open(file3,'w+')
    keywords = []
    inp = "/data/Shine/Shine_AdHoc/Input/keywords_non_compliant.csv"
    with open(inp) as inputfile:
        reader = csv.DictReader(inputfile,delimiter=',')
        for line in reader:
            keywords.append(str(line["Keywords"]))
    inputfile.close()
    counter_total = 0
    counter_no_rid = 0
    rows = collection.find({'d':{'$lt':day0,'$gte':day2},'p':{'$ne':None}})
    for row in rows:
        counter_total+=1
        try:
            rid = str(row['rid'])
        except:
            rid = ' '
            counter_no_rid+=1
        search = str(row['p']).decode('string_escape')
        search = search.replace('\n',' ').replace('\r',' ')
        try:
            all_search = re.sub(r'\W+',' ',search.split("all=")[1].split("&search=")[0].replace(',',' ').replace('@','_'))
        except:
            all_search = ''
        try:
            any_search = re.sub(r'\W+',' ',search.split("&any=")[1].split("&min")[0].replace(',',' ').replace('@','_'))
        except:
            any_search = ''
        try:
            exclude_search = re.sub(r'\W+',' ',search.split("&exclude=")[1].split("&any")[0].replace(',',' ').replace('@','_'))
        except:
            exclude_search = ''
        flag = 0
        for kw in keywords:
            if flag==0:
                reg = "\\b"+kw+"\\b"
                if re.search(reg,all_search,flags=re.DOTALL):
                    all_searches_ofile.write(rid+'\n')
                    flag = 1
                if re.search(reg,any_search,flags=re.DOTALL):
                    any_searches_ofile.write(rid+'\n')
                    flag = 1
                if re.search(reg,exclude_search,flags=re.DOTALL):
                    exclude_searches_ofile.write(rid+'\n')
                    flag = 1
    print counter_total, counter_no_rid
                    
if __name__=='__main__':
    main()
