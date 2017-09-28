__author__="Anand.Mishra"

'''
Class which provides function for calculating City Score given the lists of complete_cities
'''

#Include the root directory in the path
import sys
sys.path.append('./../../')

import csv
import json

import pprint

import traceback

class CityMatch():

	def __init__(self,cityScoreMatrixFilename='CityMatrix.json', stateCapitalMappingFileName = 'stateCapitalMapping.csv'):
		#Get the City_CityMatrix matrix
		#cityScoreMatrixFilename='../storedLookups/CityScore/CityMatrix.json'
		print 'Loading cityScoreMatrix'
		f = open(cityScoreMatrixFilename,'rb')
		self.cityScoreMatrix=json.load(f)
		f.close()
		print 'Loading cityScoreMatrix.....completed'
		
		
		#Get the state->capital mapping
	
		g = open(stateCapitalMappingFileName, 'rb')
		fieldnames = ['State',	'Capital']
		reader_g = csv.DictReader(g,delimiter=',',fieldnames=fieldnames)
	
		self.stateCapitalMapping = {} #capital->state	
		for line in reader_g:
			#pprint(line)
			state = line['State'].lower()
			capital = line['Capital'].lower()
			
			self.stateCapitalMapping[state] = capital
	

	def getCityScore(self,complete_city1,complete_city2):
		#Lookup the score between pairs one each from complete_city1 and complete_city2 and return the maximum match score
		complete_city1 = [x.lower() for x in complete_city1]
		complete_city2 = [y.lower() for y in complete_city2]
		maxscore = 0
		#rscore = 0
		#print self.cityScoreMatrix
		
		for city1 in complete_city1:
			for city2 in complete_city2:
			
				try:
					rScore = self.cityScoreMatrix[city1][city2]
					maxscore = max(maxscore,rScore)			

				except:
					#pass
					#traceback.print_exc()
					#Try stateCapitalMapping
					try:
						if city1 in self.stateCapitalMapping:
							city1_capital = self.stateCapitalMapping[city1]
							if city2 in self.cityScoreMatrix[city1_capital]: 
								#print city1_capital
								#print city2 
								#print 'hello' 
								rScore = self.cityScoreMatrix[city1_capital][city2]
								maxscore = max(maxscore,rScore)	
							    
						    
						elif city2 in self.stateCapitalMapping :
							city2_capital = self.stateCapitalMapping[city2]
							if city2_capital in self.cityScoreMatrix[city1]:
								#print city2_capital
								rScore = self.cityScoreMatrix[city1][city2_capital]
								maxscore = max(maxscore,rScore)	
						
						elif city1 in self.stateCapitalMapping and city2 in self.stateCapitalMapping:
							city1_capital = self.stateCapitalMapping[city1]
							city2_capital = self.stateCapitalMapping[city2]
							if city2_capital in self.cityScoreMatrix[city1_capital]:
								#print city1_capital,city2_capital
								rScore = self.cityScoreMatrix[city1_capital][city2_capital]
								maxscore = max(maxscore,rScore)	
					
					except:
						
						rScore = 0

		return maxscore



if __name__ == '__main__':
	print 'City Score Module:'

	complete_city1 = ["Mumbai Region"]
	complete_city2 = ['Mumbai Region']

	#complete_city1 = ['25.33.183']
	#complete_city2 = ['25.33.183']

	#complete_city1=['purchase manager', 'computer operator']
	#complete_city2= ['safety steward', 'accounts supervisor', 'captain', 'supervisor', 'staff member', 'hr manager', 'working supervisor', 'food beverage captain', 'area training manager', 'member', 'food beverage supervisor', 'administrative staff', 'steward']

	cm=CityMatch()
	#print complete_city1
	#print complete_city2
	print cm.getCityScore(complete_city1,complete_city2)