import pandas as pd
import string, re, os, sys, traceback, csv

# from __future__ import print_function

import numpy as np

from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
import statsmodels.api as sm
from time import time
import matplotlib.pyplot as plt
import pylab as pl
from sklearn import metrics
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
from statsmodels.api import Logit
from sklearn.cross_validation import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
from sklearn.model_selection import KFold

class FeatureMapper:
    def __init__(self, features):
        self.features = features
    
    def fit(self, X, y=None):
        for feature_name, column_name, extractor in self.features:
            extractor.fit(X[column_name], y)
        print "Hello"
       
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
    
    def get_feature_names(self):
        extracted = []
        for feature_name, column_name, extractor in self.features:
            fea = extractor.get_feature_names()
            for features in fea:
                extracted.append(features)
        if len(extracted) > 1:
            return extracted
        else:
            return "Failure in extraction"

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

if __name__ == '__main__':
    
    print('Reading Data Sets')
    
    train   = pd.read_csv("../Data/Train_Data/Train_final.csv").fillna("")
    train = train.reindex(np.random.permutation(train.index))
    train.reset_index(drop=True,inplace = True)
    
    train['jobtitle_skills'] = train.jobtitle.astype(str).str.cat(train.skills.astype(str), sep=' ')  
    
    train['intercept'] = 1
    train['target'] = train['target'].astype(str)
    print train.head()
    print('Data Sets Read')
    
    vect = CountVectorizer( max_features = 2000 , ngram_range=(1,3), stop_words = 'english')
    X = vect.fit_transform(train['jobtitle_skills'])
    #print type(X), 'vectoriser'
    y = train['target']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size= 0.01)
    logreg = LogisticRegression()
    
    result = logreg.fit(X_train,y_train)
    print 'Model fit over'
    
    feature_list = vect.get_feature_names()
    try:
        joblib.dump(feature_list, '../Model/FA_Binary_vocabulary_v2.pkl')
        joblib.dump(result, '../Model/FA_Binary_model_v2.pkl')
    except:
        pass
    
    prediction_1 = result.predict(X_test)
    Binary_Probability = result.predict_proba(X)
    print Binary_Probability, "Binary Probability"
    prediction = [int(x) for x in prediction_1]
    #print prediction
    expected_1 = y_test.tolist()
    expected = [int(x) for x in expected_1 if x != 'target']
    print classification_report(expected, prediction)
    print confusion_matrix(expected,prediction)
    