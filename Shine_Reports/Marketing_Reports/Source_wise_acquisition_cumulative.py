import sys
from _sqlite3 import Row
sys.path.append('/data/Projects/JobAlerts')
from pprint import pprint
from DataConnections.MySQLConnect.MySQLConnect import MySQLConnect
from DataConnections.MongoConnect.MongoConnect import MongoConnect
import pdb
import csv
import time
from multiprocessing import Pool
import os
import datetime
from datetime import timedelta
from datetime import timedelta
from datetime import date
import datetime
import re
import pandas as pd 
import numpy as np
import os, os.path
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders
import smtplib

sender = {-1:['analytics-no-reply','',''], 0:['analytics', '', '']}
server="172.22.65.51"
    
def send_mail(send_to, send_cc, send_bcc, subject, text, files=[], senderId=0):
    send_from = sender[senderId][0]+'@hindustantimes.com'
    #Reading authentication from file
    '''authenticationFile = "G:\shailk\Codes\SendMailUtil\\login.dat"
    fp = open(authenticationFile, "r")
    login = fp.read()
    fp.close()
    login1 = login.split("\n")'''

    assert type(send_to)==list
    assert type(files)==list

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Cc'] = COMMASPACE.join(send_cc)
    #msg['Bcc'] = COMMASPACE.join(send_bcc)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach( MIMEText(text) )

    for f in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload( open(f,"rb").read() )
        Encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
        msg.attach(part)

    smtp = smtplib.SMTP(server, 587)
    #login = ['analytics','An@lyt1cS']
    login = [sender[senderId][1],sender[senderId][2]]
    #print login
    #smtp.login(login[0], login[1])
    smtp.sendmail(send_from, send_to + send_cc + send_bcc, msg.as_string())
    #print "mail Sent"
    smtp.close()

def mail_daily_files(*args):
    mailing_list=['prateek.agarwal1@hindustantimes.com']
    cc_list = ['jitender.kumar@hindustantimes.com','dhruv.khatkar@hindustantimes.com']
    bcc_list = ['himanshu.solanki@hindustantimes.com','akash.verma@hindustantimes.com']
    subject = "Source Wise Acquisition(Daily Report from Mongo)"
    content = "PFA.\n\nRegards,\nHimanshu\n\n\n\n*system generated email*"
    attachment = []
    for arg in args:
        attachment.append(arg)
    print 'Mailing file......'
    send_mail(mailing_list,cc_list,bcc_list,subject, content,attachment,0)

def mail_cumulative_files(*args):
    mailing_list=['prateek.agarwal1@hindustantimes.com']
    cc_list = ['jitender.kumar@hindustantimes.com','dhruv.khatkar@hindustantimes.com']
    bcc_list = ['himanshu.solanki@hindustantimes.com','akash.verma@hindustantimes.com']
    subject = "Source Wise Acquisition(Cumulative Report from Mongo)"
    content = "PFA.\n\nRegards,\nHimanshu\n\n\n\n*system generated email*"
    attachment = []
    for arg in args:
        attachment.append(arg)
    print 'Mailing file......'
    send_mail(mailing_list,cc_list,bcc_list,subject, content,attachment,0)
    
def fileProcessing():
    source_file = open('/data/Projects/Shine_Reports/Marketing_Reports/Data/sources_list.csv','rb+')
    source_file2 = open('/data/Projects/Shine_Reports/Marketing_Reports/Data/sources_list_v3.csv','wb+')
    for line in source_file:
        if (str(line).startswith('#')):
            continue
        source = line.split(',')[0]
        name = line.split(',')[1]
        vendorids = line.split(',')[2].strip()
        vendorids = vendorids.split(';')
        for vendor in vendorids:
            if (str(vendor).startswith('-')):
                svi = evi = vendor
            else:
                svi = vendor.split('-')[0].strip()
                try:
                    evi = vendor.split('-')[1].strip()
                except:
                    evi = svi
            source_file2.write(str(source)+','+str(name)+','+str(svi)+','+str(evi)+'\n')
    source_file2.close()
    source_file.close()

if __name__ == '__main__':
    """
    lookup_file = "/data/Projects/Shine_Reports/Marketing_Reports/Data/lookup_subfa.csv"
    fa_file = open(lookup_file,"rb")
    reader_subfa = csv.reader(fa_file)
    
    subfa_to_fa_dict = {}
    for row in reader_subfa:
        subfa_to_fa_dict[str(row[1])] = str(row[3])
    
    fileProcessing()
    
    username = 'analytics'
    password = 'aN*lyt!cs@321'
    date_start_month = datetime.datetime.now() - datetime.timedelta(days=58)
    month_current =  datetime.datetime.now().month
    date_first = datetime.date(year = 2017,day = 01 , month = month_current).isoformat()
    date_object = datetime.datetime.strptime(date_first,"%Y-%m-%d")
    current_date = datetime.datetime.now().date().isoformat()
    current_date_object = datetime.datetime.strptime(current_date,"%Y-%m-%d")
    
    print date_object,"Start_Date"
    print current_date_object,"End_Date"
   
    monconn_users_static = MongoConnect('CandidateStatic', host = '172.22.65.88', port = 27018, database = 'sumoplus', username = username, password = password, authenticate = True).getCursor()
    #data_new_registrations = monconn_users_static.find({'$or': [{'red':{'$gt':date_object,'$lt':current_date_object}}, {'rsd':{'$gt':date_object,'$lt':current_date_object}}]})
    data_new_registrations = monconn_users_static.find({'$or': [{'red':{'$gt':date_object}}, {'rsd':{'$gt':date_object}}]})
    monconn_users_local = MongoConnect('candidates_processed', host = '172.22.66.253', database = 'Candidates').getCursor()
    
    cumulative_registration_file = open("/data/Projects/Shine_Reports/Marketing_Reports/Data/cumulative_registration_file.csv","wb")
    writer = csv.writer(cumulative_registration_file)
    
    writer.writerow(["user_id","user_email","total_experience","user_registration_end_date","user_ctc_min","user_last_applied_date","user_fa","user_end_vendor","user_start_vendor","user_reg_end_date","user_reg_start_date","user_cell_phone_verified","user_email_verified","user_rm","user_mo"])
    
    for data in data_new_registrations:
        user_end_vendor = data.get("evi","")
        user_start_vendor = data.get("svi","")
        user_reg_end_date = data.get("red","")
        user_reg_start_date = data.get("rsd","")
        user_cell_phone_verified = data.get("cpv","")
        user_email_verified = data.get("ev","")
        user_rm = data.get("rm","")
        user_mo = data.get("mo","")
        user_id = data.get("_id","")
        user_last_applied_date = data.get("lad","")
        user_email = data.get("e","")
        user_registration_end_datetime = str(data.get("red",""))
        if user_registration_end_datetime != "":
            try:
                user_registration_end_date_full = datetime.datetime.strptime(user_registration_end_datetime,"%Y-%m-%d %H:%M:%S.%f")
            except:
                user_registration_end_date_full = datetime.datetime.strptime(user_registration_end_datetime,"%Y-%m-%d %H:%M:%S")
            format = "%Y-%m-%d" 
            user_registration_end_date = user_registration_end_date_full.strftime(format)  
        else:
            user_registration_end_date = "N/A"
        data_fa  = list(monconn_users_local.find({"user_email":user_email},{"user_functionalarea_id":1,"user_edom":1,"user_experience":1,"user_ctc":1}))
        try:
            user_fa = subfa_to_fa_dict[data_fa[0].get("user_functionalarea_id","")]
        except:
            user_fa = ""        
        try:
            user_experience = data_fa[0].get("user_experience","")
        except:
            user_experience = ""
        try:
            user_salary = data_fa[0].get("user_ctc","")
        except:
            user_salary = ""
        try:
            user_ctc_min =  float((re.findall("[-+]?\d+[\.]?\d*", user_salary.replace(',','')))[0])
            if user_ctc_min == 50000:
                user_ctc_min = 0.5
        except:
            user_ctc_min = -10
        try:
            #total_experience = round(float(re.findall("[-+]?\d+[\.]?\d*", user_experience)[0]) + float(float(re.findall("[-+]?\d+[\.]?\d*", user_experience)[1])/12),2)
            total_experience = round(float(re.findall("[-+]?\d+[\.]?\d*", user_experience)[0])) 
        except:
            total_experience = ""
        writer.writerow([user_id,user_email,total_experience,user_registration_end_date,user_ctc_min,user_last_applied_date,user_fa,user_end_vendor,user_start_vendor,user_reg_end_date,user_reg_start_date,user_cell_phone_verified,user_email_verified,user_rm,user_mo])
            
    cumulative_registration_file.close()
    """        
    input_file = "/data/Projects/Shine_Reports/Marketing_Reports/Data/cumulative_registration_file.csv" 
    input = pd.read_csv(input_file)
    input = input.fillna("-1")
    input["calculated_experience"] = "a"
    input["calculated_fa"] = "a"
    input["calculated_missing_date"] = "a"
    input["calculated_status"] = "a"
    input["calculated_cell_phone"] = "a"
    input["calculated_email"] = "a"
    input["user_rm"] = input["user_rm"].astype(str)
    input["user_mo"] = input["user_mo"].astype(str)
    input["user_end_vendor"] = input["user_end_vendor"].astype(str)
    input["user_start_vendor"] = input["user_start_vendor"].astype(str)
    input["user_cell_phone_verified"] = input["user_cell_phone_verified"].astype(str)
    input["user_email_verified"] = input["user_email_verified"].astype(str)
    input["calculated_status"] = input["calculated_status"].astype(str)
    input["calculated_status"] = input["calculated_status"].astype(str)
    
    input.ix[((input.total_experience < 1) & (input.total_experience >= 0) & (input.user_rm == "0") & (input.user_mo == "0")) , 'calculated_experience'] = "0_1 Yr"
    input.ix[((input.total_experience < 4) & (input.total_experience >= 1) & (input.user_rm == "0") & (input.user_mo == "0")) , 'calculated_experience'] = "1_3 Yr"    
    input.ix[((input.total_experience >= 4 ) & (input.user_rm == "0") & (input.user_mo == "0")), 'calculated_experience'] = "3 Yr+"   
    
    input.user_fa = input.user_fa.astype(str)
    input.ix[(((input.user_fa == str(10013.0)) | (input.user_fa == str(10012.0)) |(input.user_fa == str(10028.0))) & (input.user_rm == "0") & (input.user_mo == "0")), "calculated_fa"] = "IT"
    input.ix[(((input.user_fa != str(10013.0)) & (input.user_fa != str(10012.0)) & (input.user_fa != str(10028.0))) & (input.user_rm == "0") & (input.user_mo == "0")), "calculated_fa"] = "Non-IT"   
    
    input.ix[((input.user_last_applied_date != "-1") & (input.user_rm == "0") & (input.user_mo == "0")) , 'calculated_missing_date'] = "Applied"
    input.ix[((input.user_last_applied_date == "-1") & (input.user_rm == "0") & (input.user_mo == "0")), 'calculated_missing_date'] = "Not_Applied"
    
#   input.ix[((input.user_rm == 0) & (input.user_mo == 0)),'calculated_status'] = "Activated"
    input.ix[((input.user_rm == "0") & (input.user_mo == "0")),'calculated_status'] = "Activated"
    input.ix[((input.user_rm != "0")|(input.user_mo != "0")|(input.user_end_vendor == "-1")),'calculated_status'] = "Midouts"
    
    #input.ix[((input.user_cell_phone_verified == 1) & (input.user_rm == 0) & (input.user_mo == 0)) , 'calculated_cell_phone'] = "Cell_Phone_Verified"
    input.ix[(input.user_cell_phone_verified == "1") , 'calculated_cell_phone'] = "Cell_Phone_Verified"
    input.ix[(input.user_cell_phone_verified != "1") , 'calculated_cell_phone'] = "Cell_Phone_Not_Verified"
    
    input.ix[(input.user_email_verified == "1")  , 'calculated_email'] = "Email_Verified"
    input.ix[(input.user_email_verified != "1")  , 'calculated_email'] = "Email_Not_Verified"
    
    #input.to_csv("/data/Projects/Shine_Reports/Marketing_Reports/Data/test.csv")
    pivot_experience = input.pivot_table(index="user_end_vendor",values="user_id",columns = ["calculated_experience"],aggfunc= len)
    pivot_fa = input.pivot_table(index="user_end_vendor",values="user_id",columns = ["calculated_fa"],aggfunc= len)
    pivot_apply = input.pivot_table(index="user_end_vendor",values="user_id",columns = ["calculated_missing_date"],aggfunc= len)
    pivot_status = input.pivot_table(index="user_start_vendor",values="user_id",columns = ["calculated_status"],aggfunc= len)
    pivot_cp = input.pivot_table(index="user_end_vendor",values="user_id",columns = ["calculated_cell_phone"],aggfunc= len)
    pivot_email  = input.pivot_table(index="user_end_vendor",values="user_id",columns = ["calculated_email"],aggfunc= len)
    pivot_registration_date = input.pivot_table(index="user_end_vendor",values="user_id",columns = ["user_registration_end_date"],aggfunc= len)
    registration_end_date_pdf = pd.DataFrame(pivot_registration_date)
    registration_end_date_pdf["user_end_vendor"] = registration_end_date_pdf.index
    
    ##print pivot_experience.head()
    ##print "############################################################################"
    #3print pivot_fa.head()
    ##print "############################################################################"
    ##print pivot_apply.head()
    ##print "############################################################################"
    ##print pivot_status.head()
    ##print "############################################################################"
    
    exp_df = pd.DataFrame(pivot_experience)
    exp_df = exp_df.drop('a', 1)
    fa_df = pd.DataFrame(pivot_fa)
    fa_df = fa_df.drop('a', 1)
    apply_df = pd.DataFrame(pivot_apply)
    apply_df = apply_df.drop('a', 1)
    
    status_df = pd.DataFrame(pivot_status)
    
    cp_df = pd.DataFrame(pivot_cp)
    email_df = pd.DataFrame(pivot_email)
    
    final_df = exp_df.join(fa_df, how='outer',lsuffix='_exp_df', rsuffix='_fa_df') 
    final_df_1 = final_df.join(apply_df, how='outer',lsuffix='_final_df', rsuffix='_apply_df') 
    final_df_2 = final_df_1.join(status_df, how='outer',lsuffix='_final_df_1', rsuffix='_status_df') 
    final_df_3 = final_df_2.join(cp_df, how='outer',lsuffix='_final_df_2', rsuffix='_cp_df')
    final_df_4 = final_df_3.join(email_df, how='outer',lsuffix='_final_df_3', rsuffix='_email_df')

    source_reader = open('/data/Projects/Shine_Reports/Marketing_Reports/Data/sources_list_v3.csv','rb')
    reader = csv.reader(source_reader)
    reader.next()
    
    final_df_4["user_end_vendor"] = final_df_4.index
    final_df_4 = final_df_4.fillna("0")
    registration_end_date_pdf = registration_end_date_pdf.fillna("0")

    #cumulative_list = ['user_end_vendor','0_1 Yr', '1_3 Yr', '3 Yr+', 'IT','Non-IT', 'Applied', 'Activated', 'Midouts', 'Cell_Phone_Not_Verified', 'Cell_Phone_Verified', 'Email_Not_Verified', 'Email_Verified']
    cumulative_list = list(final_df_4.columns.values)
    for column in cumulative_list:
        final_df_4[str(column)] = final_df_4[str(column)].astype(float)

    daily_list = list(registration_end_date_pdf.columns.values)
    for column in daily_list:
        registration_end_date_pdf[str(column)] = registration_end_date_pdf[str(column)].astype(float)
    
    final_df_4["Vendor_name"] = "Other"
    final_df_4["Source"] = "Other"
    registration_end_date_pdf["Vendor_name"] = "Other"
    registration_end_date_pdf["Source"] = "Other" 
    
    for row in reader :
        Source = row[0]
        Name = row[1]
        try:
            initial_vendor = int(row[2])
            final_vendor = int(row[3])
            final_df_4.ix[((final_df_4.user_end_vendor >= initial_vendor) & (final_df_4.user_end_vendor <= final_vendor)),'Vendor_name'] = Name
            final_df_4.ix[((final_df_4.user_end_vendor >= initial_vendor) & (final_df_4.user_end_vendor <= final_vendor)),'Source'] = Source
            registration_end_date_pdf.ix[((registration_end_date_pdf.user_end_vendor >= initial_vendor) & (registration_end_date_pdf.user_end_vendor <= final_vendor)),'Vendor_name'] = Name
            registration_end_date_pdf.ix[((registration_end_date_pdf.user_end_vendor >= initial_vendor) & (registration_end_date_pdf.user_end_vendor <= final_vendor)),'Source'] = Source     
        except:
            continue
    
    final_df_4 = final_df_4.drop('user_end_vendor', 1)
    registration_end_date_pdf = registration_end_date_pdf.drop('user_end_vendor', 1)
    result = final_df_4.groupby(['Source','Vendor_name']).sum()
    result_daily = registration_end_date_pdf.groupby(['Source','Vendor_name']).sum()
    result_df = pd.DataFrame(result)
    result_df['Activated'] = result_df["IT"] + result_df["Non-IT"] 
    print result_df.head()
    result_df.to_csv("/data/Projects/Shine_Reports/Marketing_Reports/Data/source_wise_cumulative.csv")    
    #mail_daily_files("/data/Projects/Shine_Reports/Marketing_Reports/Data/source_wise_cumulative.csv")
    
    result_registration_df = pd.DataFrame(result_daily)  
    print result_registration_df.head()
    result_registration_df.to_csv("/data/Projects/Shine_Reports/Marketing_Reports/Data/source_wise_daily.csv")
    #mail_cumulative_files("/data/Projects/Shine_Reports/Marketing_Reports/Data/source_wise_daily.csv") 
    
    os.system('mutt -s "Source_wise_daily_file_mongo!!! Please Validate." himanshu.solanki@hindustantimes.com ,prateek.agarwal1@hindustantimes.com ,jitender.kumar@hindustantimes.com ,dhruv.khatkar@hindustantimes.com ,akash.verma@hindustantimes.com -a /data/Projects/Shine_Reports/Marketing_Reports/Data/source_wise_daily.csv')
    os.system('mutt -s "Source_wise_cumulative_file_mongo!!! Please Validate." himanshu.solanki@hindustantimes.com ,prateek.agarwal1@hindustantimes.com ,jitender.kumar@hindustantimes.com ,dhruv.khatkar@hindustantimes.com ,akash.verma@hindustantimes.com -a /data/Projects/Shine_Reports/Marketing_Reports/Data/source_wise_cumulative.csv')
        
       
               

        
    
    
    
    
    
    
    
     
    