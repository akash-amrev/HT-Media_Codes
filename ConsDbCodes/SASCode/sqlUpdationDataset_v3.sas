
/*************************        
Author: Shailendra
Description: Data Export to csv to be imported in mySql
Date: Sat Apr 10 11:37:04 2013
***************************/

libname sasTemp "/data/Analytics/Utils/consolidatedDB/Model/SASTemp";
libname FlatLib '/data/Analytics/Utils/dataTransfer/Model/SASTemp/CandidateDataSets';
libname FIncLib '/data/Analytics/Utils/dataTransfer/Model/SASTemp/CandidateDataSets/Incrementals';
libname CQSLib '/data/Shine/CQS/Model/SASTemp';

option mprint; option spool; 
%include "/data/Analytics/Utils/consolidatedDB/Model/SASCode/DateConversionMacro.sas";
%include "/data/Analytics/Utils/consolidatedDB/Model/SASCode/DateConversionMacro_v3.sas";

/*PROC SORT DATA = FIncLib.FLATFILE1; BY UserId; RUN;*/
/*PROC SORT DATA = CQSLib.CandidateDB; BY UserId; RUN;*/

PROC SORT DATA = FIncLib.FLATFILE1; BY UserId; RUN;
DATA FlatLib.FLATFILE1(SORTEDBY = UserId);
        UPDATE FlatLib.FLATFILE1 (IN = A) FIncLib.FLATFILE1 (IN = B);
        BY UserId;
RUN;

DATA FIncLib.FLATFILE1Inc;
	SET FlatLib.FLATFILE1;
	*IF DatePart(LastLogin) >= INTNX('day', TODAY(), -9) AND DatePart(LastLogin) < INTNX('day', TODAY(), -5);
	IF DatePart(LastLogin) = INTNX('day', TODAY(), -1);
RUN;

PROC SORT DATA = FIncLib.FLATFILE1Inc; BY UserId; RUN;
DATA sasTemp.sqlUpdation;
	MERGE FIncLib.FLATFILE1Inc(IN=A) CQSLib.CandidateDB(KEEP = UserId S);
	BY UserId;
	IF A;
RUN;

PROC SORT DATA = sasTemp.CandidatesTokenInc; By Email; RUN;
DATA sasTemp.CandidatesToken(SORTEDBY = Email);
        SET sasTemp.CandidatesToken sasTemp.CandidatesTokenInc;
        BY Email;
RUN;

PROC SORT DATA = sasTemp.CandidatesToken; BY Email; RUN;
PROC SORT DATA = sasTemp.sqlUpdation; BY Email; RUN;
DATA sasTemp.sqlUpdation;
	MERGE sasTemp.sqlUpdation(IN=A) sasTemp.CandidatesToken;
	BY Email;
	IF A;
	S = ROUND(S, .1);
RUN;

DATA sqlUpdation;
	RETAIN UserId Email FirstName LastName CellPhone Gender City State StartVendorId EndVendorId Status
	RegistrationDate CompletionDate LastLoginDate LastUpdatedDate LatestAppliedJobDate
	TotalExperience Experience_Month Salary JobTitle1 SubFunction1 Function1 Industry1 Experience1 CompanyName1 
	Institute1 EducationalQualification1 Specialization1 Stream1
	IsEmailVerified IsCellPhoneVerified NotificationFrequency IsReceiveSMS 
	AutoLoginToken LastCellPhoneVerificationDate S LastCVUpdationDate no_skills;
	SET sasTemp.sqlUpdation;
	%global RegistrationDate CompletionDate LastLoginDate LastUpdatedDate LatestAppliedJobDate LastCellPhoneVerificationDate S LastCVUpdationDate;
	%DateConversion1(RegistrationStartDate,RegistrationDate);
	%DateConversion1(RegistrationEndDate,CompletionDate);
	%DateConversion1(LastLogin,LastLoginDate);
	%DateConversion1(LastModified,LastUpdatedDate);
	%DateConversion1(LastAppliedDate,LatestAppliedJobDate);
	%DateConversion1(LastCellPhoneVerifiedDate,LastCellPhoneVerificationDate);
	%DateConversion1(LastCVUpdateDate,LastCVUpdationDate);
	DROP RegistrationStartDate RegistrationEndDate LastLogin LastModified LastAppliedDate LastCellPhoneVerifiedDate LastCVUpdateDate;
RUN;

DATA sqlUpdation1;
	SET sqlUpdation(DROP=AutoLoginToken LastCellPhoneVerificationDate S LastCVUpdationDate no_skills);
RUN;

DATA sqlUpdation2;
	SET sqlUpdation(KEEP=UserId Email AutoLoginToken LastCellPhoneVerificationDate S LastCVUpdationDate no_skills);
RUN;

PROC EXPORT DATA = sqlUpdation1
	FILE = '/data/Analytics/Utils/consolidatedDB/Output/IncExtract/CandidatesInc1.csv' DBMS = CSV REPLACE;
RUN;
PROC EXPORT DATA = sqlUpdation2
	FILE = '/data/Analytics/Utils/consolidatedDB/Output/IncExtract/CandidatesInc2.csv' DBMS = CSV REPLACE;
RUN;
/*
PROC EXPORT DATA = sqlUpdation
	FILE = '/data/Analytics/Utils/consolidatedDB/Output/IncExtract/CandidatesInc.csv' DBMS = CSV REPLACE;
RUN;
*/

DATA LastJASentInc;
	SET sasTemp.LastJASentInc;
	%global LastJASentDate;
	%DateConversion3(da_LastJASentDate, LastJASentDate);
	DROP da_LastJASentDate;
RUN;

PROC EXPORT DATA = LastJASentInc
	FILE = '/data/Analytics/Utils/consolidatedDB/Output/IncExtract/LastJASentDateInc.csv' DBMS = CSV REPLACE;
RUN;
