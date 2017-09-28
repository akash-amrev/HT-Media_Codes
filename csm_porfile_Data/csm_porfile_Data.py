import csv
from pymongo import Connection


def getMongoConnection(MONGO_HOST,MONGO_PORT,DB, isSlave=False, isAuth=False, username = 'analytics', password = 'aN*lyt!cs@321'):
    if isAuth:
    	connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave, username = username, password = password)
    else:
	connection = Connection(MONGO_HOST,MONGO_PORT, slave_okay = isSlave)
    mongo_conn = connection[DB]
    mongo_conn.authenticate(username, password)
    return mongo_conn

def get_Shine_Data():
	Input_File = '/data/Shine/Shine_AdHoc/Output/csm_profiles.csv'
	Output_File = '/data/Shine/Shine_AdHoc/Output/Shine_Csm_Candidates.csv'
	ifile = open(Input_File,"rb+")
	ofile = open(Output_File ,'wb')
        writer = csv.writer(ofile)
        writer.writerow(['Email_Id',"Rm_Flag","Mo_Flag"])
	list = []
	for rows in ifile:
	    list.append(rows)
	list = [l1.replace('\n','').replace('\r','').replace('"','') for l1 in list]
	#print list
	mongo_conn = getMongoConnection('172.22.65.157',27018, 'sumoplus',True, True)
	collection = getattr(mongo_conn, 'CandidateStatic')
	print collection
	for i in range(len(list)):
            #print list[i]
	    Required_Data = collection.find({'e':str(list[i])},{'e':1,'rm':1,'mo':1})
	    #print Required_Data
            for data in Required_Data:
                #print data['e']
                writer.writerow([data['e'],data['rm'],data['mo']])
	ofile.close()
	
def main():
	get_Shine_Data()

if __name__ == '__main__':
	main()
	
	
	
	

