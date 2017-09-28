import pymongo, os, sys, copy
from pymongo import Connection
from datetime import datetime, date, time
from dateutil.relativedelta import *

todayDate=date.today()
#todayDate=todayDate+relativedelta(days=-6)
previousDate=todayDate+relativedelta(days=-1)

projectHome = '/data/Analytics/Utils/consolidatedDB'

listKeys = []
def readKeys(file):
	ifile = open(file,'r')
	for line in ifile:
                key = line.strip('\n')
                listKeys.append(key)

def getDataFromCollection(mongo_conn, Collection, day1, day2, keyConds):
	collection = getattr(mongo_conn,Collection)
	conditions = []
	for key in keyConds:
		conditions.append({key:{'$gte':day1,'$lt':day2}})
	doclist = collection.find({'$or':conditions})
	outFile = projectHome+'/Input/dataFromMongo/Incrementals/'+Collection+'/'+Collection+'Inc_'+str(previousDate)+'.csv'
	incFile = projectHome+'/Input/dataFromMongo/Incrementals/'+Collection+'Inc.csv'
	ofile = open(outFile,'w')

	'''
	doclist_copy = copy.copy(doclist)
	idfile = open(projectHome+'/Input/dataFromMongo/Incrementals/CandidateStaticIncId_'+str(previousDate)+'.csv','w')
	if Collection == 'CandidateStatic':
        	for doc in doclist_copy:
			idfile.write(str(doc['_id'])+'\n')
		idfile.close()
	'''

	global listKeys; listKeys=[]
	readKeys(projectHome+'/Input/dataFromMongo/Keys_'+Collection+'.csv')
	print listKeys
	size = len(listKeys)
	k = 0
        for doc in doclist:
		k += 1
		line = ''
		j = 0
                for key in listKeys:
			j += 1
			if k == 1:#put header
				if j <= size-1:
					ofile.write(key+',')
				else:
					ofile.write(key+'\n')
			try:
				if key == 'cfs':
					valueList = []
					values = doc[key]
					for v in values:
						valueList.append(values[v]['y'] + ';' + values[v]['v'])
					value = '|'.join(valueList)
				else:
					value = str(doc[key])
					value = value.replace(',',';').replace('\t',' ').replace('\n',' ').rstrip('\\').strip('"')
					if value == 'None' or value == None:
						value = ''
				if j < size:
	                        	line += str(value)+','
				elif j == size:
	                        	line += str(value)+'\n'
	                except:
        	                if j < size:
                                        line += ','
                                elif j == size:
                                        line += '\n'
		ofile.write(line)
	ofile.close()
	os.system('cp '+outFile+' '+incFile)
	#os.system('cat '+outFile+' >> /data/Analytics/Utils/dataTransfer/Input/dataFromMongo/'+Table+'.csv; rm '+outFile)

def getDataFromCollection2(mongo_conn, Collection, day1, day2, Collection2, keyConds): # cond on collection2, data from collection, overloading (polymorphism)
	collection = getattr(mongo_conn,Collection)
	collection2 = getattr(mongo_conn,Collection2)

	conditions = []
	for key in keyConds:
		conditions.append({key:{'$gte':day1,'$lt':day2}})
	doclist2 = collection2.find({'$or':conditions},{'_id':1}, timeout=False)

	global listKeys; listKeys=[]
	readKeys(projectHome+'/Input/dataFromMongo/Keys_'+Collection+'.csv')
	print listKeys
	size = len(listKeys)
	outFile = projectHome+'/Input/dataFromMongo/Incrementals/'+Collection+'/'+Collection+'Inc_'+str(previousDate)+'.csv'
	incFile = projectHome+'/Input/dataFromMongo/Incrementals/'+Collection+'Inc.csv'
	ofile = open(outFile,'w')
	k = 0
	for doc2 in doclist2:
		_id = str(doc2['_id']).strip('\n')
		doclist = collection.find({'fcu':_id})

		#if k < 100:continue
		#else:break
		for doc in doclist:
			k += 1
			line = ''
			j = 0
			for key in listKeys:
				j += 1
				if k == 1:
					if j <= size-1:
						ofile.write(key+',')
					else:
						ofile.write(key+'\n')					
				try:
					value = str(doc[key])
					value = value.replace(',',';').replace('\t',' ').replace('\n',' ').rstrip('\\').strip('"')
					if value == 'None' or value == None:
						value = ''
					if j < size:
	                        		line += str(value)+','
					elif j == size:
		                        	line += str(value)+'\n'
	        	        except:
	       	        	        if j < size:
	                                        line += ','
        	                        elif j == size:
                	                        line += '\n'
			ofile.write(line)
	ofile.close()
	os.system('cp '+outFile+' '+incFile)

