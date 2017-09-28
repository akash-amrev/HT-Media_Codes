import os
import multiprocessing
from multiprocessing import Process
import subprocess
import sys
from datetime import date, datetime, time, timedelta
from dateutil.relativedelta import *
import pandas as pd
from pymongo import MongoClient, ReadPreference

todayDate = date.today()
interval = 365
oneYearWindow = todayDate + relativedelta(days = -interval)

day1 = datetime.combine(todayDate,time(0,0))
day2 = datetime.combine(oneYearWindow,time(0,0))

def getMongoConnection(MONGO_HOST, MONGO_PORT, DB, isAuth=False, username='analytics', password='aN*lyt!cs@321'):
    if isAuth:
        connection = MongoClient(MONGO_HOST, MONGO_PORT, read_preference=ReadPreference.SECONDARY)
        mongo_conn = connection[DB]
        mongo_conn.authenticate(username, password)
    else:
        connection = MongoClient(MONGO_HOST, MONGO_PORT, read_preference=ReadPreference.SECONDARY)
        mongo_conn = connection[DB]
    return mongo_conn

candidateDB = 'sumoplus'

mongo_conn4 = getMongoConnection('172.22.65.157', 27018, candidateDB, True)
collectionCS = getattr(mongo_conn4, 'CandidateStatic')

def createExpDFs(SJ_DF_path,SP_DF_path):
    st = datetime.now()
    SJ_ExpDF = pd.DataFrame(list(collectionCS.find({'lad':{'$gte':day2}}, {'_id': 1, 'ex': 1})))
    SJ_ExpDF.columns = ['userId','exp']
    SJ_ExpDF['userId'] = SJ_ExpDF['userId'].apply(lambda x:str(x))
    SJ_ExpDF = SJ_ExpDF.fillna(0)
    print "SJ Candidate Exp DF count : ",SJ_ExpDF.shape[0]
    SJ_ExpDF.to_csv(SJ_DF_path,encoding='utf-8',index=False)
    print "SJ Candidate Exp generated at :"+str(datetime.now())
    del SJ_ExpDF
    SP_ExpDF = pd.DataFrame(list(collectionCS.find({'ll':{'$gte':day2}},{'_id': 1, 'ex': 1})))
    SP_ExpDF.columns = ['userId','exp']
    SP_ExpDF['userId'] = SP_ExpDF['userId'].apply(lambda x:str(x))
    SP_ExpDF = SP_ExpDF.fillna(0)
    print "SP Candidate Exp DF count : ",SP_ExpDF.shape[0]
    SP_ExpDF.to_csv(SP_DF_path,encoding='utf-8',index=False)
    print "SP Candidate Exp generated at :"+str(datetime.now())
    del SP_ExpDF
    print "Total time taken to generate experience DFs : "+str(datetime.now()-st)

def SPMailer():
    os.system('python /data/Projects/SimilarProfiles/Model/Pycode/SPMailer_v05.py 30 32 >> /data/Projects/SimilarProfiles/log/SPMailer.log 2>&1')

def SJMailer():
    my_dir = os.path.dirname(sys.argv[0])
    os.system('python /data/Projects/JobDigest/Model/PyCode/SJMailer_v06.py >> /data/Projects/JobDigest/Logs/SJMailer.log 2>&1')

def main():
    try:
        st1 = datetime.now()
        print "Sj Sp Sync started at time :", str(st1)
        os.system(' echo "SJ_SP Sync Run is started at ' + str(datetime.now()) + '\nTotal run time :' +  str(datetime.now() - st1) + ' " | mutt -s "Sync Run Start" saurabh24292@gmail.com')
        SP_DF_Path = "/data/Projects/SimilarProfiles/Input/SP_Exp_DF.csv"
        SJ_DF_Path = "/data/Projects/JobDigest/Input/SJ_Exp_DF.csv"
        createExpDFs(SJ_DF_Path,SP_DF_Path)
        try:
            SP = Process(target = SPMailer)
            SP.start()
            SJ = Process(target = SJMailer)
            SJ.start()
            SP.join()
            SJ.join()
            os.system(' echo "SJ and SP Run is completed at ' + str(datetime.now()) + '\nTotal run time :' +  str(datetime.now() - st1) + ' " | mutt -s "SP and SJ Run Complete" saurabh24292@gmail.com,mayank.singh.iit@gmail.com ')
            os.system('python /data/Projects/SimilarProfiles/Model/Pycode/SJSPMailer_v02.py >> /data/Projects/SimilarProfiles/log/SJSPMailer.log 2>&1')
            os.system(' echo "SJ_SP Sync Run is completed at ' + str(datetime.now()) + '\nTotal run time :' +  str(datetime.now() - st1) + ' " | mutt -s "Sync Run Complete" saurabh24292@gmail.com,mayank.singh.iit@gmail.com ')
        except:
            os.system(' echo "SJ_SP Sync Run failed at ' + str(datetime.now()) + '\nTotal run time :' +  str(datetime.now() - st1) + ' " | mutt -s "Sync Run Failed" saurabh24292@gmail.com,mayank.singh.iit@gmail.com ')
    except:
        os.system(' echo "SJ_SP Sync Failed in the beginning ' + str(datetime.now()) + '\nTotal run time :' +  str(datetime.now() - st1) + ' " | mutt -s "Code Failed Initially" saurabh24292@gmail.com')

if __name__=='__main__':
    main()
