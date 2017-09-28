import csv
from Utils.Cleaning import *

def LoadFA_lookup(filename):
    fa_lookup = {}
    ifile = open(filename,'r')
    reader = csv.reader(ifile)
    reader.next()
    for row in reader:
        fa_lookup[int(float(row[0]))] = int(float(row[1]))
    return fa_lookup


def LoadSimilarFA(filename):
    similar_fa = {}
    ifile = open('/data/Projects/JobAlerts_Improvements/Data/Similar_FA.csv','r')
    reader = csv.reader(ifile)
    reader.next()
    for row in reader:
        #print row
        fa1 = int(row[0])
        fa2 = int(row[1])
        score = float(row[6])
        if fa1 in similar_fa.keys():
            similar_fa[fa1][fa2] = score
        else:
            similar_fa[fa1] = {fa2:score}
        
    return similar_fa

def find_important_skill(skills,jobtitle,profiletitle,resumeJT):
    try:
        skills_list = skills.split(',')
    except:
        skills_list = []
    skills_list = [x.strip() for x in skills_list]
    result = []
    text = cleanText(jobtitle) + ' ' +cleanText(profiletitle) + ' ' + cleanText(resumeJT)
    #print skills_list 
    for skill in skills_list :
        if skill in text:
            result.append(skill)
    return result




def findFA_Score(intersection,similar_fa_dict):
    fa_score = 0
    for fa in intersection:
        if similar_fa_dict[fa] > fa_score:
            fa_score = similar_fa_dict[fa]
        else:
            pass
    return fa_score
        
    
    
    
    
    
        
