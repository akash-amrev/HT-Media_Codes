import pandas as pd
import string, re, os, sys, traceback, csv
import numpy as np

from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer

from time import time
import matplotlib.pyplot as plt

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

class FeatureMapper:
    def __init__(self, features):
        self.features = features
    
    def fit(self, X, y=None):
        for feature_name, column_name, extractor in self.features:
            extractor.fit(X[column_name], y)
       
    def transform(self, X):
        extracted = []
        for feature_name, column_name, extractor in self.features:
            fea = extractor.transform(X[column_name])
            if hasattr(fea, "toarray"):
                extracted.append(fea.toarray())
            else:
                extracted.append(fea)
        if len(extracted) > 1:
            return np.concatenate(extracted, axis=1)
        else:
            return extracted[0]
    
    def fit_transform(self, X, y=None):
        extracted = []
        for feature_name, column_name, extractor in self.features:
            fea = extractor.fit_transform(X[column_name], y)
            if hasattr(fea, "toarray"):
                extracted.append(fea.toarray())
            else:
                extracted.append(fea)
            
        if len(extracted) > 1:
            return np.concatenate(extracted, axis=1)
        else:
            return extracted[0]


def identity(x):
    return x


class SimpleTransform(BaseEstimator):
    
    def __init__(self, transformer=identity):
        self.transformer = transformer
    
    def fit(self, X, y=None):
        return self
    
    def fit_transform(self, X, y=None):
        return self.transform(X)
    
    def transform(self, X, y=None):
        return np.array([self.transformer(x) for x in X], ndmin=2).T


fin = open('../InputFiles/job_title_vocab.csv')
reader = csv.reader(fin)
counter = 0
job_title_vocab = {}
vocab = []

for row in reader:
    job_title_vocab[row[0]] = counter
    vocab.append(row[0])
    counter += 1

fin.close()
vocab = list(set(vocab))

features = FeatureMapper([('OriginalJobTitleTfIdfBOG', 'original_jobtitle', TfidfVectorizer(vocabulary=vocab)),
                          ('JobTitleTfIdfUnigram', 'jobtitle', TfidfVectorizer(max_features=2000)),
                          ('JobTitleTfIdfBigram', 'jobtitle', TfidfVectorizer(max_features=1000, ngram_range=(2,2))),
                          ('DescriptionTfIdfUnigram', 'description', TfidfVectorizer(max_features=2500)),
                          ('DescriptionTfIdfBigram', 'description', TfidfVectorizer(max_features=1500, ngram_range=(2,2))),
                          ('JobTitleTfIdfTrigram', 'description', TfidfVectorizer(max_features=1000, ngram_range=(3,3))),
                          ('JobTitleTokensInDescription', 'JobTitleTokensInDescription', SimpleTransform()),
                          ('DescriptionTokensInJobTitle', 'DescriptionTokensInJobTitle', SimpleTransform())])


def extract_features(data):
    token_pattern = re.compile(r"(?u)\b\w\w+\b")
    data["JobTitleTokensInDescription"] = 0.0
    data["DescriptionTokensInJobTitle"] = 0.0
    
    for i, row in data.iterrows():
        
        title = set(x.lower() for x in token_pattern.findall(row["jobtitle"]))
        description = set(x.lower() for x in token_pattern.findall(row["description"]))
        
        if len(title) > 0:
            data.set_value(i, "DescriptionTokensInJobTitle", round(float(len(description.intersection(title)))/float(len(title)),4))
        if len(description) > 0:
            data.set_value(i, "JobTitleTokensInDescription", round(float(len(title.intersection(description)))/float(len(description)),4))