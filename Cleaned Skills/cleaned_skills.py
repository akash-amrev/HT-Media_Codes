import os, re, gc
from nltk.util import ngrams
import pandas as pd

###################
#### Skill Cleaning
###################

# create n-grams for skills
def ngrams_search(text,n=6):
    skills_file = '/data/Shine/JobMatcherSOLR/Input/Skill_Mappping.csv'
    skills_lookup = pd.read_csv(skills_file)                
    skills = []
    skills_df = []
    while(n>0):
        ngram = ngrams(text.split(), n)
        skills = [' '.join(gram).strip().lower() for gram in ngram]
        df = pd.DataFrame(skills_lookup[skills_lookup['skill_final'].isin(skills)])
        if (n==6):
            skills_df = df[['skill_final_group']]
        else:
            temp = df[['skill_final_group']]
            skills_df = pd.concat([skills_df,temp], ignore_index=True)
            del temp
        n = n-1            
    skills_df = skills_df['skill_final_group'].drop_duplicates().values.tolist()
    del skills_file, skills_lookup, skills
    gc.collect()
    skills_df_temp = []
    for item in skills_df:
        for i1 in skills_df:
            if (i1==item):
                continue
            else:
                if (i1 in item):
                    skills_df_temp.append(i1)
    skills_df = list(set(skills_df) - set(skills_df_temp))
    del skills_df_temp
    return skills_df

# get cleaned skill vector 
def getCleanedSkills(text):
    text = str(text).replace(',',' ').replace('/',' ').replace('\"','')
    skills_list = ngrams_search(text,n=6)
    return skills_list
