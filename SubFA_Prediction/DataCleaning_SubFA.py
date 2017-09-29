import pandas as pd
import numpy as np
import string, re, os, sys, traceback, csv, collections

from time import time
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer

from sklearn.metrics import *
from sklearn.externals import joblib
from sklearn.base import BaseEstimator
from sklearn.feature_selection import SelectKBest, chi2
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer, TfidfTransformer
from sklearn.linear_model import SGDClassifier, Perceptron, PassiveAggressiveClassifier, RidgeClassifier
from sklearn.naive_bayes import BernoulliNB, MultinomialNB, GaussianNB
from sklearn.neighbors import KNeighborsClassifier, NearestCentroid
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier, GradientBoostingClassifier
from sklearn.utils.extmath import density
from sklearn.pipeline import Pipeline
from sklearn.svm import LinearSVC, SVC
from sklearn import decomposition

from bs4 import BeautifulSoup

#############################################################################################################
##########---------- Creating Stop Word List
#############################################################################################################
#stop = stopwords.words('english')
stop = [u'i', u'me', u'my', u'myself', u'we', u'our', u'ours', u'ourselves', u'you', u'your', u'yours', u'yourself', u'yourselves', u'he', u'him', u'his', u'himself', u'she', u'her', u'hers', u'herself', u'it', u'its', u'itself', u'they', u'them', u'their', u'theirs', u'themselves', u'what', u'which', u'who', u'whom', u'this', u'that', u'these', u'those', u'am', u'is', u'are', u'was', u'were', u'be',u'been', u'being', u'have', u'has', u'had', u'having', u'do', u'does', u'did', u'doing', u'a', u'an', u'the', u'and', u'but', u'if', u'or', u'because', u'as', u'until', u'while', u'of', u'at', u'by', u'for', u'with', u'about', u'against', u'between', u'into', u'through', u'during', u'before', u'after', u'above', u'below', u'to', u'from', u'up', u'down', u'in', u'out', u'on', u'off', u'over', u'under', u'again', u'further', u'then', u'once', u'here', u'there', u'when', u'where', u'why', u'how', u'all', u'any', u'both', u'each', u'few', u'more', u'most', u'other', u'some', u'such', u'no', u'nor', u'not', u'only', u'own', u'same', u'so', u'than', u'too',u'very', u's', u't', u'can', u'will', u'just', u'don', u'should', u'now', u'd', u'll', u'm', u'o', u're', u've', u'y', u'ain', u'aren', u'couldn', u'didn', u'doesn', u'hadn', u'hasn', u'haven', u'isn', u'ma', u'mightn', u'mustn', u'needn', u'shan', u'shouldn', u'wasn', u'weren', u'won', u'wouldn']
fin = open("../InputFiles/stopwords.csv")
reader = csv.reader(fin)
for row in reader:
    stop.append(row[0])
fin.close()
stop = list(set(stop))
stop.remove("in")

#############################################################################################################
##########---------- Creating abbreviation Dict
#############################################################################################################
abbr_dict = {}
fin = open('../InputFiles/abbreviation.csv')
reader = csv.reader(fin)
for row in reader:
    abbr_dict[row[0]] = row[1]
fin.close()

#############################################################################################################
##########---------- Creating Spelling Correction Dict
#############################################################################################################
fin = open('../InputFiles/Taxonomy_SubFA.csv', 'rb')
reader = csv.reader(fin)

taxonomy_cleaner_dict = collections.OrderedDict()

for row in reader:
    taxonomy_cleaner_dict[row[0]] = row[1].lower()

fin.close()

#############################################################################################################
##########---------- Function To Remove Unicode Characters & HTML Tags & Extra Spaces
#############################################################################################################

def remove_HtmlTags_SubFA(data):
    #data = data.apply(lambda s:  BeautifulSoup(str(''.join([i if ord(i) < 128 else ' ' for i in str(s)]))).getText())
    data = ''.join([i if ord(i) < 128 else ' ' for i in str(data)]).encode('ascii', 'ignore')
    data = data.replace('>', '> ').replace(',', ', ')
    data = BeautifulSoup(data, 'html').getText().strip().lower().replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('  ', ' ').replace('  ', ' ').replace('  ', ' ').lower()
    return data

#############################################################################################################
##########---------- Function To Remove Stop Words
#############################################################################################################

def remove_StopWords(data):
    data = ' '.join([i for i in data.split() if i not in stop])
    return data

#############################################################################################################
##########---------- Function To Replace abbreviations
#############################################################################################################

def replace_abbr_SubFA(data):
    #data = data.apply(lambda s: ' '.join([i if i not in abbr_dict else abbr_dict[i] for i in s.split()]))
    data = data.lower()
    #data = ' '.join([i if i not in abbr_dict else abbr_dict[i] for i in data.split()])
    data = data.replace('(', " ").replace(':', " ").replace(')', " ").replace('-', " - ").replace("/", " / ")
    data = data.replace("..", ".").replace("..", ".").replace(",", ", ").replace("_", "_ ").replace("&", " & ")
    data = re.sub('[^A-Za-z0-9 ]', ' ', data).strip().replace("  ", " ").replace("  ", " ").replace("  ", " ").replace("  ", " ")
    return data

#############################################################################################################
##########---------- Function to Correct Spelling
#############################################################################################################

def spell_corrector_SubFA(data):
    for item in taxonomy_cleaner_dict:
        data = data.replace(item,taxonomy_cleaner_dict[item])
    return data

#############################################################################################################
##########---------- Function To Remove Emails and URLs and Junk Words
#############################################################################################################

def remove_URLs_SubFA(data):
    urls = re.findall(".*?(http://www\.[a-z0-9\.\-\/\?=\&\+\_\#\%]+).*?", str(data)) + re.findall(".*?(https://www\.[a-z0-9\.\-\/\?=\&\+\_\#\%]+).*?", str(data))
    for url in urls:
        data = str(data).replace(url, " ")

    urls = re.findall(".*?(www\.[a-z0-9\.\-\/\?=\&\+\_\#\%]+).*?", data)
    for url in urls:
        data = str(data).replace(url, " ")

    remove_list = ["pm", "am", "k", "yrs", "yr", "rd", "st", "th", "l", "lac", "year", "years", "mm", "\.\)", "lakhs", "lakh", "lacs", "experience", "candidate"]
    for i in remove_list:
        data = re.sub('( |\-|^)[0-9]+' + i + '( |\-|$)', ' ', data).strip()

    data = ' '.join([str(' '.join(re.match(r'([0-9,\.]+)([a-z]+)', i, re.I).groups())) if re.match(r'([0-9,.]+)([a-z]+)', i, re.I) else i for i in data.split()]).strip()

    emails = re.findall(r'[\w\.-]+@[\w\.-]+', data)
    for email in emails:
        data = str(data).replace(email, " ")

    return data
