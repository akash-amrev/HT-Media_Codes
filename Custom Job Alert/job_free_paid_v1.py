import poplib, sys, datetime, email, math, operator, copy, itertools, random
import os, re, string, ftplib, traceback, pymongo, MySQLdb, subprocess, smtplib, socket
import multiprocessing
#from pymongo import MongoClient, ReadPreference
from datetime import date, datetime, time, timedelta
from dateutil.relativedelta import *
from bson.objectid import ObjectId
from multiprocessing import Pool, Lock
from multiprocessing.sharedctypes import Value, Array
import pandas as pd
#import scipy.sparse as sp
#import numpy as np
#from sklearn.metrics.pairwise import cosine_similarity
#from __builtin__ import range
from string import join
from logging import exception
from pymongo import Connection
import csv
import pdb
todayDate=date.today()
requiredDate=todayDate+relativedelta(days=-25)
requiredDate1=todayDate+relativedelta(days=-12)
day2 = datetime.combine(requiredDate, time(0, 0))
day21 = datetime.combine(requiredDate1, time(0, 0))
def getMongoConnection(MONGO_HOST,MONGO_PORT,DB, isSlave=False, isAuth=False, username = 'analytics', password = 'aN*lyt!cs@321'):
    if isAuth:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave, username = username, password = password)
    else:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave)
    mongo_conn = connection[DB]
    mongo_conn.authenticate(username, password)
    return mongo_conn

def getDataFromMongo():
    #mongo_conn = getMongoConnection('172.22.65.157',27018, 'sumoplus',True, True)
    mongo_conn1 = getMongoConnection('172.22.65.183',27017,'jainbox',True)
    #collection1 = getattr(mongo_conn, 'customjobalerts')
    #collection2 = getattr(mongo_conn, 'CandidateStatic')
    collection3 = getattr(mongo_conn1, 'jamailer')
    file1 = '/data/Shine/Shine_AdHoc/Output/job_frequency.csv'
    ofile = open(file1,'w')
    ofile.write('Jobid,Cands\n')
    file_obj = "/data/Shine/Shine_AdHoc/Input/sj_cja_jobids.csv"
    jobs = []
    jobs_freq = {}
    with open(file_obj) as inputfile:
        reader = csv.DictReader(inputfile,delimiter=',')
        for line in reader:
            jobs.append(str(line['JobId']))
    inputfile.close()
    print "closing input file at: ",datetime.now()
    print "fetching data from mongo at: ",datetime.now()
    rows = collection3.find({'mt':5})
    print "data fetched from mongo at: ",datetime.now()    
    for row in rows:
        #pdb.set_trace()
        mj = map(str,row['mj'])
        for i in range(len(jobs)):
            if jobs[i] in mj:
                jobs_freq[jobs[i]] = jobs_freq.get(jobs[i],0)+1
    print "freq calculated at: ",datetime.now()   
    for job in jobs_freq.keys():
        ofile.write(str(job)+','+str(jobs_freq[job])+'\n')
    ofile.close()
def main():
    getDataFromMongo()

if __name__=='__main__':
    main()
            
