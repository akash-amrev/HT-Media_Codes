import poplib, sys,datetime, email, os
from pymongo import Connection
from pymongo.objectid import ObjectId
from datetime import date, datetime, time
from dateutil.relativedelta import *
import sendMailUtil
import csv

def getMongoConnection(MONGO_HOST,MONGO_PORT,DB, isSlave=False, isAuth=False, username = 'analytics', password = 'aN*lyt!cs@321'):
    if isAuth:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave, username = username, password = password)
    else:
        connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave)
    mongo_conn = connection[DB]
    mongo_conn.authenticate(username, password)
    return mongo_conn

def getDataFromCandidateStatic():
    Output_File = ('/data/Shine/Update_SAS/duplicate_numbers_remove_final_new.csv')
    Input_File = ('/data/Shine/Update_SAS/duplicate_numbers_remove_final.csv')
    ifile = open(Input_File,'rb')
    reader = csv.reader(ifile)
    ofile = open(Output_File,'wb')
    writer = csv.writer(ofile)
    writer.writerow(['sa_UserId','old_Email','sa_Email'])
    mongo_conn = getMongoConnection('172.22.65.157', 27018, 'sumoplus', True)
    collection = getattr(mongo_conn,'CandidateStatic')
    for elements in reader:
        userid = elements[0]
        try:
            userid = int(userid)
        except:
            try:
                userid = ObjectId(userid)
            except:
                print elements[0]
                sys.exit(0)
        old_email = elements[1]
        New_Emails = collection.find({'_id':userid},{'_id':1,'e':1})
        for i in New_Emails:
            #ofile.write(str(i['_id']) + "," + str(old_email) + "," + str(i['e'])+ '\n')
            writer.writerow([str(elements[0]), str(elements[1]), str(i['e'])])
    ofile.close()
    ifile.close()
