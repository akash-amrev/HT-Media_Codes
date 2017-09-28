__author__='Anand.Mishra'
__date__ ='$Apr17, 2013 14:56 PM$'

'''
This module provides function for cleaning the bag of words in a given text, also given the synonyms mapping file and id file
9/10: Incorporated n-gram weighting
9/10: Remove commas as discussed with Karan
'''


#Include the root directory in the path
import sys
sys.path.append('./../../')

import csv
import re
import pprint
from collections import defaultdict
from Utils.Utils import cleanToken
from Utils.Utils_1 import cleanToken2
from pprint import pprint

import traceback

class MyBOW():
	def __init__(self, synMappingFileName, keywordIdMappingFileName):
		#synMappingFileName is used and keywordIdMappingFileName is created
		#synMappingFileName = '../Features/rawData/LSI/Model_UnifiedTKE/unifiedtkelist.csv'
		#keywordIdMappingFileName = '../Features/rawData/LSI/Model_UnifiedTKE/unifiedtkelist_numbered.csv'
		#Get the synonymous mapping dictionary
		f = open(synMappingFileName, 'rb')
		reader = csv.reader(f, delimiter = ',')

		self.synMapping = {}
		self.keywords = []

		for line in reader:
			#child|parent type of file
			#print line
			self.synMapping[line[0]] = line[1]
			self.keywords.append(line[1])
	
		f.close()
	
		#Create a uniq list of keywords preserving the order (not important but done)
		self.keywords_uniq = list(set(self.keywords))

		print 'Number of features:' + str(len(self.keywords_uniq))
	
		#Generate keyword id mapping file and the mapping object
		h = open(keywordIdMappingFileName, 'rb')
		reader = csv.reader(h, delimiter = ',')

		self.keywordIdMapping = {}
		i = 1
		for line in reader:
			keyword = line[0]
			self.keywordIdMapping[keyword] = line[1]

			i += 1


	def extractTextKeywords(self, text):
		#Clean the text
		text = cleanToken(text)
		#words = words.replace(',', ' ') #9/10: Remove commas as discussed with Karan
		#No need already done by cleanToken
		words = text.split(' ')
		
		textKeywords = []

		for (index,word) in enumerate(words):
			#print (index, word)

			#Search for the unigrams
			if self.synMapping.has_key(word):
				textKeywords.append(self.synMapping[word]) #Append the parent
			
			#Search for biagrams
			if index<len(words)-1:
				featWord=word+' '+words[index+1]
				if self.synMapping.has_key(featWord):
					textKeywords.append(self.synMapping[featWord])
				
			#Search for trigrams
			if index<len(words)-2:
				featWord=word+' '+words[index+1]+' '+words[index+2]
				if self.synMapping.has_key(featWord):
					textKeywords.append(self.synMapping[featWord])


		textKeywordsPlain = ' '.join(textKeywords)

		return textKeywords


	def getBow(self, text, getbowdict = 0, nGramWeighting = 0):
		#Extract the text keywords
		textKeywords = self.extractTextKeywords(text)

		#Do the counting/math
		bowdict = defaultdict(int)
		
		for keyword in textKeywords:

			if nGramWeighting == 1:
				len_keyword = len(keyword.split(' '))
				bowdict[keyword] += len_keyword
			else:
				bowdict[keyword] += 1

		#print bowdict
		#print keywordIdMapping

		#Map the counting to ids
		bow = []
		for (keyword, keyword_count) in bowdict.iteritems():

			try:
				keyword_id = self.keywordIdMapping[keyword.strip().lower()]
				
				#Synthesize bow string
				bow.append((int(keyword_id), keyword_count))

			except KeyError:
				#traceback.print_exc()
				pass
				#print "KeyError: "+keyword


		if getbowdict == 1:
			s = {'bow':bow, 'bowdict':bowdict}
		else:
			s = {'bow':bow}
		return s


if __name__ == '__main__':
	print 'Bag of words module:'
	text = " linear regression, microsoft excel, microsoft office, business intelligence, c, c++, recommender systems, databases, programming, linear regression, microsoft excel, microsoft office, business intelligence, c, c++, recommender systems, databases, programming, linear regression, microsoft excel, microsoft office, business intelligence, c, c++, recommender systems, databases, programming, linear regression, microsoft excel, microsoft office, business intelligence, c, c++, recommender systems, databases, programming, linear regression, microsoft excel, microsoft office, business intelligence, c, c++, recommender systems, databases, programming, summer intern, summer intern, data analyst, data analyst, data analyst, data analyst , python, sas, mongodb, python, sas, mongodb, python, sas, mongodb, python, sas, mongodb, python, sas, mongodb ,assistant manager,assistant manager ,kpo , analytics ,statistics , analytic"
	#text = "agency manager   max bupa health insurance   location: delhi  ncr,  noida,  kolkata   salary:upto 4.50lac +reimbursement+ incentive   job description job responsibilities     manage health insurance sales goal achievement through:     recruiting agents.  training and developing agents on commission basis.  supervise the activity plan of all agents to ensure these are being fulfilled as per the desired levels.  conduct weekly performance review  prp  with agents &amp; update sales.  management system.  promote &amp; motivate agents for career progression program to make them join the organization.  promote agents to use the agent portal &amp; crm for managing their customer &amp; cross selling them for different products.    desired candidate profile:   experience: minimum 3 years experience of sales   desired background   candidate should possess the following attributes:     working with people.  entrepreneurial and commercial focus.  drive for results.  maturity  high confidence levels,  good communication.  should have stable past career employment history.  shouldbe well networked in the local area and have an understanding of the local market,  and proven track records.  minimum graduate in any stream.    call us at : 09711522990   0971870459 5javed        "
	#text = raw_input('text:')
	synMappingFileName = '/data/Projects/JobAlerts/Features/rawData/LSI/Model_UnifiedTKE/unifiedtkelist.csv'
	#synMappingFileName = '../rawData/LSI/Model_JATKE/jatkelist.csv'
	#keywordIdMappingFileName = '../storedLookups/LSI/Model_JATKE/jatkelist_numbered.csv' #This file is created
	keywordIdMappingFileName = '/data/Projects/JobAlerts/Features/rawData/LSI/Model_UnifiedTKE/unifiedtkelist_numbered.csv' #This file is created

	mb = MyBOW(synMappingFileName, keywordIdMappingFileName)
	#print mb.extractTextKeywords(text)
	#print mb.getBow(text)
	#print mb.getBow(text,getbowdict = 1, nGramWeighting = 0)
	print text
	pprint(mb.getBow(text,getbowdict = 1, nGramWeighting = 1)['bowdict'])