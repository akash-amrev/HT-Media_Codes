import poplib, sys, datetime, email, math, operator, copy, itertools, random
import os, re, string, ftplib, traceback, pymongo, MySQLdb, subprocess, smtplib, socket
import multiprocessing
from pymongo import MongoClient, ReadPreference
from datetime import date, datetime, time, timedelta
from dateutil.relativedelta import *
from bson.objectid import ObjectId
from multiprocessing import Pool, Lock
from multiprocessing.sharedctypes import Value, Array
import pandas as pd
import scipy.sparse as sp
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from __builtin__ import range
from string import join
from logging import exception
import csv

projectHomeSJ = '/data/Projects/SimilarJobs'
sjFreqDict = {}
# #Mongo Connection
def getMongoConnection(MONGO_HOST, MONGO_PORT, DB, isAuth=False, username='analytics', password='aN*lyt!cs@321'):
    if isAuth:
        connection = MongoClient(MONGO_HOST, MONGO_PORT, read_preference=ReadPreference.SECONDARY)
        mongo_conn = connection[DB]
        mongo_conn.authenticate(username, password)
    else:
        connection = MongoClient(MONGO_HOST, MONGO_PORT, read_preference=ReadPreference.SECONDARY)
        mongo_conn = connection[DB]
    return mongo_conn

similar_job_mailer_db = 'similar_job_mailer'
archieve_db = 'job_digest_archieve'
job_digest_db = 'job_digest'
mongo_conn = getMongoConnection('172.22.66.198',27017,archieve_db)
mongo_conn1 = getMongoConnection('172.22.66.198',27017,job_digest_db)
collectionSJD = getattr(mongo_conn1,'SimilarJobDigest')
collectionSJDArchieve = getattr(mongo_conn,'SimilarJobDigestArchieve')
sjFreqDict = {}

def getDataFromMongo(filename):
    jobs = []
    with open(filename) as inputfile:
        reader = csv.DictReader(inputfile,delimiter=',')
        for line in reader:
            jobs.append(line['jobid'])
    inputfile.close()
    jobsDF = pd.DataFrame({'jobid':jobs})
    rows = collectionSJDArchieve.find()
    for row in rows:
        candDF = pd.DataFrame({'jobid':map(str,row['sjdArc'])})
        candDF = pd.merge(jobsDF,candDF,on='jobid')
        if candDF.shape[0] > 0:
            for job in candDF['jobid'].tolist():
                sjFreqDict[job] = sjFreqDict.get(job,0)+1

def compileData(fileObject):
    ofile =  open(fileObject,'w')
    ofile.write('JobId,NumberOfCandidates\n')
    for key in sjFreqDict.keys():
        ofile.write(str(key)+','+str(sjFreqDict[key])+','+'\n')
    ofile.close()

def main():
    getDataFromMongo("/tmp/sj_cja_jobids.csv")
    outFile = '/tmp/SimJobFreq.csv'
    compileData(outFile)

if __name__=='__main__':
    main()
