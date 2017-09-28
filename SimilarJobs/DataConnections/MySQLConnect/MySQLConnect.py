__author__="Anand.Mishra"
__date__ ="$Apr12, 2013 11:18 AM$"

'''
This module provides functions for connecting to MySQL database and fetching data from it
Given connection parameters one can make a MySQLConnect object and query the object
5/22/13 -- Added autocommit and insert functionality
'''

#Include the root directory in the path
import sys
sys.path.append('./../../')

import MySQLdb
import traceback
import pdb 

from pprint import pprint

class MySQLConnect():
	def __init__(self, database, host = "localhost", user = "root", password = "Km7Iv80l", unix_socket = "/tmp/mysql.sock", port = 3306,connect_timeout = 1000):
		self.db = MySQLdb.connect(host = host, user = user, passwd=password, db = database, unix_socket = unix_socket, port = port,connect_timeout = connect_timeout)
	
		#Set the autocommit flag to true, not to worry about commit when writing
		self.db.autocommit(True)
	
		self.cur = cursor = self.db.cursor(MySQLdb.cursors.DictCursor)

	def query(self, query, type = 'select', writeVals = ()):
		if type == 'select':
			self.cur.execute(query)
			return self.cur.fetchall()
		else:
			return self.cur

	def getCursor(self):
			return self.cur

	def saveToTable(self, query, writeVals = ()):
		try:
			self.cur.execute(query, writeVals)

			#self.cur.execute(query)
			#writeVals = ("242", "1,2,3")
			#self.cur.execute('''insert into jobrec (resid, jobs2bsent) values (%s, %s)''', ("242", "1,2,3"))
			self.db.commit()
			print '0'
			return 0

		except:
			traceback.print_exc()
			print '-1'
			#self.db.rollback()
			return -1

	def close(self):
		self.cur.close()
		self.db.close()


if __name__ == '__main__':
	
	print 'MySQL connect module:'
	
	host="172.16.66.64"
	user="analytics"
	password="@n@lytics"
	database="SumoPlus"
	unix_socket="/tmp/mysql.sock"
	port = 3306

	db = MySQLConnect(database, host, user, password, unix_socket, port)
	cmd = '''SELECT rj.jobid as Jobid,rj.jobtitle as JobTitle,rj.description as JD,la1.text_value_MAX as SalaryMax,la2.text_value_MIN as SalaryMin,le1.display as ExpMin,le2.display as ExpMax,li.industry_desc as Industry FROM recruiter_job AS rj left join lookup_annualsalary AS la1 on rj.salarymax = la1.salary_id left join lookup_annualsalary AS la2 on rj.salarymin = la2.salary_id left join lookup_experience AS le1 on rj.minexperience = le1.value left join lookup_experience AS le2 on rj.maxexperience = le2.value left join lookup_industry AS li on rj.industry=li.industry_id WHERE rj.jobstatus in (3,5,6,9) limit 5'''
	jobs = db.query(cmd)

	#pprint(s[0]['candidates_active'])

	for job in jobs:
		pprint(job)
		#job_candidates_active = job['description']
		job_candidates_active1 = job['SalaryMax']
		#pprint(job_candidates_active)
		#pprint(job_candidates_active1)
