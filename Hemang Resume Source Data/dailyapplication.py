from pymongo import *
from datetime import date, datetime, time
from email import parser
from dateutil.relativedelta import *
from pymongo import Connection

todayDate=date.today()
previousDate = todayDate + relativedelta(days = -100)
previousDate_2 = todayDate + relativedelta(days = -8)
day1 = datetime.combine(previousDate, time(0, 0)) 
day2 = datetime.combine(previousDate_2, time(0, 0))
Month = previousDate.strftime("%b-%Y")

def getMongoConnection(MONGO_HOST,MONGO_PORT,DB, isSlave=False, isAuth=False, username = 'analytics', password = 'aN*lyt!cs@321'):
    if isAuth:
    	connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave, username = username, password = password)
    else:
	connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave)
    mongo_conn = connection[DB]
    mongo_conn.authenticate(username, password)
    return mongo_conn

def getDataFromMongo():
    Output_File = '/data/Shine/Shine_AdHoc/Output/Hemang_Resume_Data.csv'
    ofile = open(Output_File , 'w')
    ofile.write('UserType,MidOut,Cand_id,ResumeMidOut,Ti\n')
    mongo_conn = getMongoConnection('172.22.65.157', 27018, 'sumoplus', True)
    collection = getattr(mongo_conn, 'CandidateStatic')
    collection_1 = getattr(mongo_conn , 'CandidateResumes')
    Candidate_Data = collection.find({'rsd':{'$gte':day1,'$lt':day2},'$or':[{'ut':2},{'ut':3}]},{'ut':1,'mo':1,'rm':1,'_id':1})
    cand_user_id = []
    for cand in Candidate_Data:
        user_tye = cand['ut']
        midout = cand['mo']
        cand_id = cand_user_id.append(str(cand['_id']))
        resume_mid_out = cand['rm']
        for i in cand_id:
                resume_data = collection_1.find({'$or':[{'ti':10},{'ti':11},{'ti':12},{'ti':13}],'fcu':str(cand_id[i])}).count()
                ofile.write(str(user_type) + ',' + str(midout) + ',' + str(cand_id) + ',' + str(resume_mid_out) + ',' + str(resume_data)+'\n')
    ofile.close()        
def main():
        print datetime.now()
	getDataFromMongo()
	print datetime.now()
	
if __name__=='__main__':
    main() 
	
