'''
Gave higher weightage to recruiter filled FA 
author : Himanshu Solanki 
Date : 16/01/2017
'''

import pandas as pd
import string, re, os, sys, traceback, requests

reload(sys)
sys.setdefaultencoding('utf-8')

sys.path.append('../')
sys.path.append('./')

import numpy as np
import pdb
from nltk.stem.snowball import SnowballStemmer
import heapq
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
import sys
from DataConnections.MySQLConnect.MySQLConnect import *
from DataCleaning import *
from Feature_Creation import *
from datetime import datetime, timedelta, date
from DataCleaning_SubFA import *
from Feature_Creation_SubFA import *  
import resource


def second_largest(numbers):
    count = 0
    m1 = m2 = float('-inf')
    for x in numbers:
        count += 1
        if x > m2:
            if x >= m1:
                m1, m2 = x, m1            
            else:
                m2 = x
    return m2 if count >= 2 else None

def insert_into_sql(results_subfa_final):
    counter=0
    a = 0    
    for index, row in results_subfa_final.iterrows():        
        subfa1_s_predicted = row[6]
        subfa2_s_predicted = row[7]
        subfa1_predicted = row[8]
        subfa2_predicted = row[9]
        subfa1_recruiter = row[17]
        subfa2_recruiter = row[18]
        subfa1_s_recruiter = row[4]
        subfa2_s_recruiter = row[5]
        jobid = row[12]
        try:
            if int(subfa1_predicted) != 0:
                query1 = "insert into recruiter_jobattribute (jobid_id, AttType, AttValue, groupid, AttValueCustom) VALUES ({jobid}, {type}, {subfa}, {gid}, {vcustom})".format(jobid=int(jobid), type=21, subfa=int(subfa1_predicted), gid=1, vcustom=subfa1_s_predicted)
                cursor.cursor().execute(query1)
                #print "inserted predicted sfa1"
            if int(subfa2_predicted) != 0:
                query2 = "insert into recruiter_jobattribute (jobid_id, AttType, AttValue, groupid, AttValueCustom) VALUES ({jobid}, {type}, {subfa}, {gid}, {vcustom})".format(jobid=int(jobid), type=21, subfa=int(subfa2_predicted), gid=2, vcustom=subfa2_s_predicted)
                cursor.cursor().execute(query2)
                #print "inserted predicted sfa2"
            if int(subfa1_recruiter) != 0:
                query3 = "insert into recruiter_jobattribute (jobid_id, AttType, AttValue, groupid, AttValueCustom) VALUES ({jobid}, {type}, {subfa}, {gid}, {vcustom})".format(jobid=int(jobid), type=21, subfa=int(subfa1_recruiter), gid=3, vcustom=subfa1_s_recruiter)
                cursor.cursor().execute(query3)
                #print "inserted recruiter sfa1"
            if int(subfa2_recruiter) != 0:
                query4 = "insert into recruiter_jobattribute (jobid_id, AttType, AttValue, groupid, AttValueCustom) VALUES ({jobid}, {type}, {subfa}, {gid}, {vcustom})".format(jobid=int(jobid), type=21, subfa=int(subfa2_recruiter), gid=4, vcustom=subfa2_s_recruiter)
                cursor.cursor().execute(query4)
                #print "inserted recruiter sfa2"
        except Exception as e:
            print "Error in Jobid : {}, {}".format(jobid, e)
    # print counter

stemmer = SnowballStemmer("english")

if __name__ == '__main__':
    print datetime.now()
    print "Predicting Functional Area of the job"
    cursor = connect_mysql('recruiter') 
    target = {0: "Customer Service / Back Office Operations", 1: "Sales / BD", 2: "IT - Software", 3: "Marketing / Advertising / MR / PR / Events", 4: "IT - Hardware / Networking / Telecom Engineering", 5: "HR / Recruitment", 6: "Finance / Accounts / Investment Banking", 7: "Quality / Testing (QA-QC)"}
    target_id_dict = {0: 10014, 1: 10033, 2: 10013, 3: 10020, 4: 10012, 5: 10010, 6: 10007, 7: 10028}

    # For complete jobs    
    # query = "select A.jobid as jobid, A.jobtitle as jobtitle, A.description as description,GROUP_CONCAT(DISTINCT(lk.field_id) SEPARATOR '|') as Recruiter_FA,GROUP_CONCAT(DISTINCT(lk.sub_field_id) SEPARATOR '|') as Subfa_Recruiter,GROUP_CONCAT(B.AttValueCustom SEPARATOR '|') as skills from recruiter_job as A left join recruiter_jobattribute as B on A.jobid=B.jobid_id left join lookup_subfunctionalarea as lk on B.AttValue = lk.sub_field_id where ((B.Atttype = 3 and B.groupid=400) or B.Atttype = 12) and A.jobstatus in (2,3,9) and A.companyid_id=461594 group by jobid LIMIT 50"
    # For periodic jobs from crawler
    query = "select A.jobid as jobid, A.jobtitle as jobtitle, A.description as description,GROUP_CONCAT(DISTINCT(lk.field_id) SEPARATOR '|') as Recruiter_FA,GROUP_CONCAT(DISTINCT(lk.sub_field_id) SEPARATOR '|') as Subfa_Recruiter,GROUP_CONCAT(B.AttValueCustom SEPARATOR '|') as skills from recruiter_job as A left join recruiter_jobattribute as B on A.jobid=B.jobid_id left join lookup_subfunctionalarea as lk on B.AttValue = lk.sub_field_id where ((B.Atttype = 3 and B.groupid=400) or B.Atttype = 12) and A.jobstatus in (2,3,9) and A.jobcreationdate BETWEEN '{start}' AND '{end}' group by jobid".format(start=datetime.now() - timedelta(minutes=35), end=datetime.now())
    t1 = datetime.now()
    raw_data = pd.read_sql(query, con=cursor)
    print 'time taken in query: {}'.format(datetime.now() - t1)
    if raw_data.empty:
        print "No Jobs published in the given duration"
        sys.exit()

    print "Number of Jobs loaded",raw_data.shape[0]
    if raw_data.shape[0] > 50000:
        print "Excess Jobs warning"
    
    data = raw_data.copy()
    
    ##########---------- Imputing missing value
    data['description'].fillna("nan", inplace=True)
    data['jobtitle'].fillna("nan", inplace=True)
    
    ##########---------- Data Cleaning
    data['description'] = data['description'].apply(remove_HtmlTags)  #######---------- Removing HTML Tags from JD
    data['description'] =  data['description'].apply(replace_abbr)    #######---------- Replacing Abbreviations
    data['jobtitle']    =  data['jobtitle'].apply(replace_abbr)       #######---------- Replacing Abbreviations
    data['description'] = data['description'].apply(remove_URLs)      #######---------- Removing URLs & Email Address
    data['jobtitle']    = data['jobtitle'].apply(remove_URLs)         #######---------- Removing URLs & Email Address
    data['description'] =  data['description'].apply(spell_corrector) #######---------- Spelling Corrector
    data['jobtitle']    =  data['jobtitle'].apply(spell_corrector)    #######---------- Spelling Corrector
    data['description'] = data['description'].apply(remove_StopWords) #######---------- Removing Stop Words from JD
    data['jobtitle']    = data['jobtitle'].apply(remove_StopWords)    #######---------- Removing Stop Words from JT
    data['description'] = data['description'].apply(lambda text: ' '.join([i for i in text.split() if not i.isdigit()]))  #######---------- Removing Number
    data['jobtitle']    = data['jobtitle'].apply(lambda text: ' '.join([i for i in text.split() if not i.isdigit()]))     #######---------- Removing Number
    
    ##########---------- Feature Creation
    data["original_jobtitle"]  = data["jobtitle"]
    data["description"] = data["description"].apply(lambda x: ' '.join([str(stemmer.stem(y)) for y in x.split()]))
    data["jobtitle"]    = data["jobtitle"].apply(lambda x: ' '.join([str(stemmer.stem(y)) for y in x.split()]))    
    extract_features(data)

    ##########---------- Predicting Functional Area
    pipeline = joblib.load('../Model/RandomForest_Trees_200.pkl')
    prediction = pipeline.predict(data)
    results = pd.DataFrame({"jobid": data["jobid"], "Functional_Area": prediction})
    results["Functional_Area"] = results["Functional_Area"].apply(lambda x: target_id_dict[x])
    fa_data = pd.merge(raw_data, results, on='jobid', how='left') 
    
    ##########---------- Imputing missing value
    fa_data['description'].fillna("nan", inplace=True)
    fa_data['jobtitle'].fillna("nan", inplace=True)
    fa_data['skills'].fillna("nan",inplace = True)
    fa_data['skills'] = fa_data['skills'].apply(repeat_feature_SubFA)
    
    ##########---------- subfa_data Cleaning
    fa_data['description'] = fa_data['description'].apply(remove_HtmlTags_SubFA)  #######---------- Removing HTML Tags from JD
    fa_data['description'] =  fa_data['description'].apply(replace_abbr_SubFA)    #######---------- Replacing Abbreviations
    fa_data['jobtitle']    =  fa_data['jobtitle'].apply(replace_abbr_SubFA)    #######---------- Replacing Abbreviations
    fa_data['skills'] = fa_data['skills'].apply(replace_abbr_SubFA)
    fa_data['description'] = fa_data['description'].apply(remove_URLs_SubFA)      #######---------- Removing URLs & Email Address
    fa_data['jobtitle']    = fa_data['jobtitle'].apply(remove_URLs_SubFA)         #######---------- Removing URLs & Email Address
    fa_data['skills']    = fa_data['skills'].apply(remove_URLs_SubFA)             #######---------- Removing URLs & Email Address
    fa_data['original_jobtitle'] = fa_data['jobtitle']
    fa_data['jobtitle'] = fa_data.jobtitle.astype(str).str.cat(fa_data.skills.astype(str), sep=' ')    ##########-------- concatenating jobtitle and skill columns 
    fa_data['skills'] = fa_data.skills.astype(str).str.cat(fa_data.description.astype(str), sep=' ')    ##########-------- concatenating skill and description
    fa_data['jobtitle_skills_description'] = fa_data[['skills','jobtitle','description']].apply(lambda x: ' '.join(x.dropna().astype(str).astype(str)),axis=1)
    
    Binary_Classifier = joblib.load('../Model/Binary_Classifier/FA_Binary_model_v2.pkl') 
    vocabulary = joblib.load('../Model/Binary_Classifier/FA_Binary_vocabulary_v2.pkl')
    vect = TfidfVectorizer(ngram_range=(1,3), stop_words = 'english',vocabulary = vocabulary)
    X = vect.fit_transform(fa_data['jobtitle_skills_description'])
    
    if resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1000 > 14000:
        print "Memory Warning"
        print "Try taking less number of jobs"
        sys.exit()

    Binary_Prediction_1 = Binary_Classifier.predict(X)
    Binary_Prediction = [int(x) for x in Binary_Prediction_1]
    Prediction_series = pd.Series(Binary_Prediction)
    fa_data['Binary_Prediction'] = Prediction_series.values ##################################----------------------Adding Binary Classifier predictions to the dataframe
    
    Binary_Probability = Binary_Classifier.predict_proba(X)
    #print Binary_Probability, "Binary Probability"
    
    prob_score = [float(max(a)) for a in Binary_Probability]
    Binary_prob = pd.Series(prob_score)
    fa_data['Binary_Probability'] = Binary_prob.values ##################################----------------------Adding Binary Classifier probability to the dataframe
     
    ##########---------- Feature Creation
    
    extract_features_SubFA(fa_data)
    fa_data['jobtitle'] = fa_data.jobtitle.astype(str).str.cat(fa_data.Intersection_Tokens.astype(str), sep=' ')    ##########-------- adding intersecting features to job title 
    
    #fa_data.to_csv('../InputFiles/binary_classifier_new.csv',index = False)

    #---------------------------SubFA_wise DataFrame creation 
    #print fa_data.head(),"Merged dataframe" 
        
  
    ################ IT Prediction ########################
    target_id_dict_new = [10013,10014,10033,10020,10012,10010,10007,10028]
    sfa_dict = {10013 : {0:1312,1:1313,2:4528,3:4529,4:4530,5:4531,6:4533,7:4557,8:4558,9:4559,10:4560} , 
                10014 : {0:1411,1:1407,2:4519,3:1401,4:1405},
                10033 : {0:3302,1:3301,2:3309},
                10020 : {0:2006,1:4554,2:2001,3:2005,4:4426,5:2007},
                10012 : {0:4561,1:1201,2:4429,3:4464},
                10010 : {0:1001,1:1003,2:4565},
                10007 : {0:4556,1:724,2:701,3:718,4:703,5:721},
                10028 : {0:2801,1:2803}}
    
    software_dict = {0:1312,1:1313,2:4528,3:4529,4:4530,5:4531,6:4533,7:4557,8:4558,9:4559,10:4560}
    dict_10012 = {0:4561,1:1201,2:4429,3:4464} ######## Hardware 
    dict_10014 = {0:1411,1:1407,2:4519,3:1401,4:1405} ####### Customer
    dict_10020 = {0:2006,1:4454,2:2001,3:2005,4:4426,5:2007} ####### Mark
    dict_10033 = {0:3302,1:3301,2:3309} ###### Sales
    dict_10010 = {0:1001,1:1003,2:4565} ###### hr
    dict_10028 = {0:2801,1:2803} #######quality
    dict_10007 = {0:4556,1:724,2:701,3:718,4:703,5:721} #######Finance
    
    for faId in target_id_dict_new:
        SubFa_1 = 0
        SubFa_2 = 0
        subFA_dataframe = fa_data.loc[fa_data.Functional_Area == faId]
        
        if resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1000 > 14000:
            print "Memory Warning"
            print "Try taking less number of jobs"
            sys.exit()
            
        if subFA_dataframe.empty:
            print "Empty Dataframe"
            continue
        
        if faId == 10013:
            base_model_directory = '../Model/Model_10013/200/RandomForest_Trees_'
            model = base_model_directory + '10013' + ".pkl"
        
        else :
            base_model_directory = '../Model/Model_%s/RandomForest_Trees_' % str(faId)
            model = base_model_directory + str(faId) + ".pkl"            
        
        sfa_dict_relevant = sfa_dict[faId]
        pipeline = joblib.load(model)
        prediction_numpy = pipeline.predict_proba(subFA_dataframe)    ######################### Class Probability List 
        prediction_1 = prediction_numpy.tolist() 
            
        SubFa_1 = [int(a.index(max(a))) for a in prediction_1]
        SubFa_2 = [int(a.index(second_largest(a))) for a in prediction_1]
    
        prediction = [sorted(a,reverse = True) for a in prediction_1]
        max_score = [float(max(a)) for a in prediction]
        second_max_score = [float(second_largest(a)) for a in prediction]
        fourth_max_score = [float(float(a[1])/5) for a in prediction]
        default_recruiter_score = [1 for a in prediction]
        results_subfa = pd.DataFrame({
            "jobid": subFA_dataframe["jobid"],"jobtitle":subFA_dataframe["original_jobtitle"],
            "skills":subFA_dataframe["skills"],"Predicted_Functional_Area":subFA_dataframe["Functional_Area"],"Recruiter_FA":subFA_dataframe["Recruiter_FA"],
            "SubFa_1": SubFa_1, "SubFa_2":SubFa_2,"Score_SubFA_1":max_score,"Score_SubFA_2" :second_max_score ,
            "Subfa_Recruiter_original":subFA_dataframe["Subfa_Recruiter"],
            "Subfa_Recruiter":subFA_dataframe["Subfa_Recruiter"],"Recruiter_Subfa_1_Score":default_recruiter_score,"Recruiter_Subfa_2_Score":default_recruiter_score,
            "Binary_Prediction":subFA_dataframe["Binary_Prediction"],"Binary_Probability":subFA_dataframe["Binary_Probability"]})
        results_subfa["SubFa_1"] = results_subfa["SubFa_1"].apply(lambda x: sfa_dict_relevant[x])
        results_subfa["SubFa_2"] = results_subfa["SubFa_2"].apply(lambda x: sfa_dict_relevant[x])
        if faId == 10013 : 
            results_subfa.ix[results_subfa.Score_SubFA_2 < 0.2, 'SubFa_2'] = 1301
            results_subfa.ix[results_subfa.Score_SubFA_1 < 0.35, 'SubFa_2'] = 1301
        results_subfa = results_subfa.reset_index(drop=True)
        try:
            Recruiter_fa_df = pd.DataFrame(results_subfa.Recruiter_FA.str.split('|',1).tolist(),columns = ['Recruiter_fa_1','Recruiter_fa_2'])
        except:
            Recruiter_fa_df = pd.DataFrame({'Recruiter_fa_1':results_subfa['Recruiter_FA'],'Recruiter_fa_2':0})
        results_subfa_final_1 = pd.concat([results_subfa,Recruiter_fa_df],axis=1, join='outer')
        results_subfa_final_1.Binary_Prediction = results_subfa_final_1.Binary_Prediction.astype(int)
        results_subfa_final_1.Binary_Probability = results_subfa_final_1.Binary_Probability.astype(float)
        results_subfa_final_1.Recruiter_fa_1 = results_subfa_final_1.Recruiter_fa_1.astype(str)
        results_subfa_final_1.Recruiter_fa_2 = results_subfa_final_1.Recruiter_fa_2.astype(str)                
        results_subfa_final_1.ix[((results_subfa_final_1.Recruiter_fa_1 != str(faId)) & (results_subfa_final_1.Recruiter_fa_2 != str(faId)) & ((results_subfa_final_1.Binary_Prediction == 0) | ((results_subfa_final_1.Binary_Prediction != 0) & (results_subfa_final_1.Binary_Probability < 0.6) ) )), 'SubFa_1'] = 0 ################ handling cases where the subfa's entered by recruiter is different from the 8 predicted FA's
        results_subfa_final_1.ix[((results_subfa_final_1.Recruiter_fa_1 != str(faId)) & (results_subfa_final_1.Recruiter_fa_2 != str(faId)) & ((results_subfa_final_1.Binary_Prediction == 0) | ((results_subfa_final_1.Binary_Prediction != 0) & (results_subfa_final_1.Binary_Probability < 0.6) ) )), 'SubFa_2'] = 0 ################ handling cases where the subfa's entered by recruiter is different from the 8 predicted FA's
        
        try:
            results_subfa_final_1[['Recruiter_Subfa_1', 'Recruiter_Subfa_2', 'Recruiter_Subfa_3','Recruiter_Subfa_4']] = pd.DataFrame([ x.split('|') for x in results_subfa_final_1['Subfa_Recruiter'].tolist() ])
        except:
            try:
                results_subfa_final_1[['Recruiter_Subfa_1', 'Recruiter_Subfa_2', 'Recruiter_Subfa_3']] = pd.DataFrame([ x.split('|') for x in results_subfa_final_1['Subfa_Recruiter'].tolist() ])
            except:
                try:
                    results_subfa_final_1[['Recruiter_Subfa_1', 'Recruiter_Subfa_2']] = pd.DataFrame([ x.split('|') for x in results_subfa_final_1['Subfa_Recruiter'].tolist() ])
                except:
                    results_subfa_final_1['Recruiter_Subfa_1'] = pd.DataFrame([ x.split('|') for x in results_subfa_final_1['Subfa_Recruiter'].tolist() ])
                    results_subfa_final_1['Recruiter_Subfa_2'] = 0 
                
        results_subfa_final = results_subfa_final_1
        results_subfa_final.fillna(0,inplace=True)
        results_subfa_final.Recruiter_Subfa_1 = results_subfa_final.Recruiter_Subfa_1.astype(str)
        results_subfa_final.Recruiter_Subfa_2 = results_subfa_final.Recruiter_Subfa_2.astype(str)
        results_subfa_final.SubFa_1 = results_subfa_final.SubFa_1.astype(str)
        results_subfa_final.SubFa_2 = results_subfa_final.SubFa_2.astype(str)
        results_subfa_final.ix[(results_subfa_final.SubFa_1 == results_subfa_final.Recruiter_Subfa_1) |(results_subfa_final.SubFa_1 == results_subfa_final.Recruiter_Subfa_2) , 'SubFa_1'] = 0
        results_subfa_final.ix[(results_subfa_final.SubFa_2 == results_subfa_final.Recruiter_Subfa_1) |(results_subfa_final.SubFa_2 == results_subfa_final.Recruiter_Subfa_2) , 'SubFa_2'] = 0
        insert_into_sql(results_subfa_final)
        
        print 'Memory used till now',(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1000)
        if resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1000 > 14000:
            print "Memory Warning"
            print "Try taking less number of jobs"
            sys.exit()
    print datetime.now()
