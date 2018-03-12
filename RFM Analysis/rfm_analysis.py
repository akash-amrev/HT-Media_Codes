import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sys

##### Defining Color of Graphs to be Plotted #######
####################################################
color = sns.color_palette()

########## Scoring of Recency #######
#####################################

def r(row):
    if row['date'] > 201110:
        val = 5
    elif row['date'] <= 201110 and row['date'] > 201108:
        val = 4
    elif row['date'] <= 201108 and row['date'] > 201106:
        val = 3
    elif row['date'] <= 201106 and row['date'] > 201104:
        val = 2
    else:
        val = 1
    return val

###### Scoring of Frequency #######
###################################
    
def f(row):
    if row['InvoiceNo'] <= 13:
        val = 1
    elif row['InvoiceNo'] > 13 and row['InvoiceNo'] <= 25:
        val = 2
    elif row['InvoiceNo'] > 25 and row['InvoiceNo'] <= 38:
        val = 3
    elif row['InvoiceNo'] > 38 and row['InvoiceNo'] <= 55:
        val = 4
    else:
        val = 5
    return val

##### Scoring of Monetary #########
###################################    

def m(row):
    if row['Total_Price'] <= 243:
        val = 1
    elif row['Total_Price'] > 243 and row['Total_Price'] <= 463:
        val = 2
    elif row['Total_Price'] > 463 and row['Total_Price'] <= 892:
        val = 3
    elif row['Total_Price'] > 892 and row['Total_Price'] <= 1932:
        val = 4
    else:
        val = 5
    return val

def rfm_analysis():
    ########### Loading Data ###############
    ########################################

    data = pd.read_csv('D:\\Python_Codes\\RFM Analysis\\Online_Retail.csv')
    data['Total_Price'] = data['Quantity'] * data['UnitPrice']
    data['date'] = data['InvoiceDate'].str.extract('(.*)/').str.extract('(.*)/')
    data['date']=data.date.astype(str).str.zfill(2)
    data['date']=data['InvoiceDate'].str.extract('/(.*) ').str.extract('/(.*)') + data['date']
    data.date = pd.to_numeric(data.date, errors='coerce')
    
    ##### Checking Country Wise Distribution #######
    ################################################
    
    Cust_country=data[['Country','CustomerID']].drop_duplicates()
    Cust_country_count = Cust_country.groupby(['Country'],as_index = False).agg({'CustomerID':len})
    Cust_country_count = Cust_country_count.sort_values(['CustomerID'],ascending = False)
    country = list(Cust_country_count['Country'])
    Cust_id = list(Cust_country_count['CustomerID'])
    plt.figure(figsize=(12,8))
    sns.barplot(country, Cust_id, alpha=0.8, color=color[2])
    plt.xticks(rotation='60')
    plt.show()
    
    ########### Performing RFM Analysis ################
    ####################################################
    
    Cust_date_UK = data[data['Country']=='United Kingdom']
    Cust_date_UK=Cust_date_UK[['CustomerID','date']].drop_duplicates()
    Cust_date_UK['Recency_Flag'] = Cust_date_UK.apply(r, axis=1)
    Cust_date_UK = Cust_date_UK.groupby('CustomerID',as_index=False)['Recency_Flag'].max()
    plt.figure(figsize=(12,8))
    sns.countplot(x='Recency_Flag', data=Cust_date_UK, color=color[1])
    
    '''plt.ylabel('Count', fontsize=12)
    plt.xlabel('Recency_Flag', fontsize=12)
    plt.xticks(rotation='vertical')
    plt.title('Frequency of Recency_Flag', fontsize=15)
    plt.show()'''
    
    Cust_freq = data[['Country','InvoiceNo','CustomerID']].drop_duplicates()
    Cust_freq_count = Cust_freq.groupby(['Country','CustomerID'],as_index = False).agg({'InvoiceNo':len})
    Cust_freq_count_UK = Cust_freq_count[Cust_freq_count['Country']=="United Kingdom"]
    unique_invoice=Cust_freq_count_UK[["InvoiceNo"]].drop_duplicates()
    unique_invoice["Freqency_Band"] = pd.qcut(unique_invoice["InvoiceNo"], 5)
    Cust_freq_count_UK["Freq_Flag"] = Cust_freq_count_UK.apply(f, axis=1)
    Cust_monetary  = data.groupby(['Country','CustomerID'],as_index = False).agg({'Total_Price':sum})
    Cust_monetary_UK = Cust_monetary[Cust_monetary["Country"]=="United Kingdom"]
    unique_price = Cust_monetary_UK[['Total_Price']].drop_duplicates()
    unique_price = unique_price[unique_price['Total_Price'] > 0]
    unique_price["monetary_Band"] = pd.qcut(unique_price["Total_Price"], 5)
    Cust_monetary_UK["Monetary_Flag"] = Cust_monetary_UK.apply(m, axis=1).reset_index()
    
    ####### Merging R, F & M Dataframes to create RFM Dataframe ###############
    ###########################################################################
    
    Cust_UK_All = pd.merge(Cust_date_UK,Cust_freq_count_UK[["CustomerID","Freq_Flag"]],on=["CustomerID"],how="left")
    Cust_UK_All = pd.merge(Cust_UK_All,Cust_monetary_UK[["CustomerID","Monetary_Flag"]],on=["CustomerID"],how="left")
    
    print Cust_date_UK.head()
    print Cust_freq_count_UK.head()
    print Cust_monetary_UK.head()
    Cust_UK_All['RFM_Score'] = Cust_UK_All['Recency_Flag'] * Cust_UK_All['Freq_Flag'] * Cust_UK_All['Monetary_Flag'] 
    print Cust_UK_All.head()

def main():
    rfm_analysis()
    
if __name__ == '__main__':
    main()

