__author__="Ashish.Jain"
__date__ ="$Sep 25, 2015 10:27AM$"

'''
This module provides functions for connecting to Mongodb and reading and writing data to it
'''

#Include the root directory in the path
import sys
sys.path.append('./../../')

import pymongo
from pymongo import Connection
#from pymongo import MongoClient

from pprint import pprint

class MongoConnect():
	def __init__(self, table,database = 'ja_mongo', host = 'localhost', port = 27017, username = 'analytics', password = 'aN*lyt!cs@321', authenticate = False):
		#Used all times except when first table is created
		self.username = username
		self.password = password
		if authenticate == True:
			#Make a mongodb connection
			self.connection = pymongo.Connection(host, port, slaveOk = True)   
		 	self.mongo_conn = self.connection[database]
			self.mongo_conn.authenticate(self.username, self.password)
			#self.connection = pymongo.mongo_client.MongoClient(host,port) #Works with pymongo2.4+
			self.table = self.mongo_conn[table] #test_cja is the db name
		
		else:
			#print 'In authenticate False'
			#Make a mongodb connection
			self.connection = pymongo.Connection(host, port)
	
			#self.connection = pymongo.mongo_client.MongoClient(host,port) #Works with pymongo2.4+
			self.table = self.connection[database][table] #test_cja is the db name

			
	def doIndexing(self, table, indexFields, database = 'ja_mongo', host = 'localhost', port = 27017, ukey = False):
		#Index some of the fields
		if ukey == False:
			self.table.ensure_index(indexFields)
		else:
			args = {'unique':'true'}
			self.table.ensure_index(indexFields, **args)

	def saveToTable(self, document):
		self.table.save(document)

	def loadFromTable(self, myCondition):
		#print myCondition
		cur = self.table.find(myCondition)
		rows = list(cur)
		return rows
	
	def loadFromTableGeneric(self, myCondition1, myCondition2):
		#print myCondition
		cur = self.table.find(myCondition1, myCondition2)
		rows = list(cur)
		return rows

	def getCursor(self):
		cur = self.table
		return cur
	
	def insert(self,document):
		self.table.insert(document)
		

	def dropTable(self):
		self.table.drop()

	def subsetTable(self, table, database = 'ja_mongo', host = 'localhost', port = 27017):
		#Subset the table to retrieve the data of  a chunk
		s = {'id': { '$mod': [ 4, 0 ] } }
		cur = self.table.find(s)
		rows = list(cur)
		return rows

	def getLastRecord(self):
		args = {'_id':'-1'}
		s = self.table.find().sort([('_id', pymongo.DESCENDING)]).limit(1)
		lastRow = list(s)
		return lastRow

	def getLastRecordByAutoID(self):
		args = {'autoid':'-1'}
		s = self.table.find().sort([('autoid', pymongo.DESCENDING)]).limit(1)
		lastRow = list(s)
		return lastRow

	def close(self):
		self.connection.close()

	def saveOrUpdateToTable(self, myCondition, document):
		self.table.update(myCondition, document, upsert = True)
		
	def saveOrUpdateToTableFields(self, myCondition, document, multi = False):
		self.table.update(myCondition, document, upsert = True,multi=multi)
	
	def removeDocument(self, myCondition):
		self.table.remove(myCondition)

	def __del__(self):
		self.connection.close()

		

if __name__ == '__main__':
	print 'Mongo connect module:'
	
	username = 'analytics'
	password = 'aN*lyt!cs@321' 
	tableName = 'CandidateStatic'
	#monconn = MongoConnect(tableName, indexFields)
	monconn_users = MongoConnect(tableName, host = '172.22.66.198', port = 27018, database = 'sumoplus', username = username, password = password, authenticate = True)
	monconn_users_cur = monconn_users.getCursor()
	
	i = 0
	for user in monconn_users_cur.find():
		print user

		if i > 10:
			break
		
		i += 1
		
	
	'''	
	tableName = 'jobs_processed'
	monconn_users_local = MongoConnect(tableName, host = 'localhost', database = 'JobAlerts')
	monconn_users_local_cur = monconn_users_local.getCursor()
	
	#document = {'name': 'Anand Mishra', 'place':'gurgaon', 'marks': 20, 'ctc': 20, 'bow': [(23,2), (62,4)], 'abc': 1}
	#pprint(document)
	monconn_users_local.dropTable()
	'''