
/*************************        
Author: Shailendra
Description: UPDATE consolidated DB
Date: Wed Feb 27 13:08:57 IST 2013
***************************/
%let PATH1 = /data/Analytics/Utils/consolidatedDB;
%let lookupdir = &PATH1./Model/Lookups;

libname lulib  "&PATH1./Model/Lookups";
libname sasTemp  "&PATH1./Model/SASTemp";
libname FlatLib '/data/Analytics/Utils/dataTransfer/Model/SASTemp/CandidateDataSets';
libname FIncLib '/data/Analytics/Utils/dataTransfer/Model/SASTemp/CandidateDataSets/Incrementals/';
libname MarkLib  "/data/Analytics/Utils/MarketingReports/Model/SASTemp";

/*
PROC SORT DATA = sasTemp.CandidatesTokenInc; By Email; RUN;
DATA sasTemp.CandidatesToken(SORTEDBY = Email);
        SET sasTemp.CandidatesToken sasTemp.CandidatesTokenInc;
	BY Email;
RUN;
*/

DATA sasTemp.ConsolidatedDB(SORTEDBY = sa_UserId);
	DROP ca_LoginsInc ca_ModifiesInc ca_AppliesInc ca_JAClicksInc ya_JASentInc ya_LatestMatchesInc ya_OtherMatchesInc ya_WVSentInc;
	DROP ra_ProfileViewsInc ra_ExcelDownloadsInc ra_CVDownloadsInc ra_EmailsInc ra_smsInc ra_starInc ra_folderInc ra_mobileInc ra_reported_candInc ra_noteInc;
        UPDATE sasTemp.ConsolidatedDB sasTemp.ConsolidatedDBInc;
        BY sa_UserId;
	ca_Logins = SUM(ca_Logins, ca_LoginsInc);
	ca_Modifies = SUM(ca_Modifies, ca_ModifiesInc);
	ca_Applies = SUM(ca_Applies, ca_AppliesInc);
	ca_JAClicks = SUM(ca_JAClicks, ca_JAClicksInc);
	ya_JASent = SUM(ya_JASent, ya_JASentInc);
	ya_LatestMatches = SUM(ya_LatestMatches, ya_LatestMatchesInc);
	ya_OtherMatches = SUM(ya_OtherMatches, ya_OtherMatchesInc);
	ya_WVSent = SUM(ya_WVSent, ya_WVSentInc);
	ra_ProfileViews = SUM(ra_ProfileViews, ra_ProfileViewsInc);
	ra_ExcelDownloads = SUM(ra_ExcelDownloads, ra_ExcelDownloadsInc);
	ra_CVDownloads = SUM(ra_CVDownloads, ra_CVDownloadsInc);
	ra_Emails = SUM(ra_Emails, ra_EmailsInc);
	ra_sms = SUM(ra_sms, ra_smsInc);
	ra_star = SUM(ra_star, ra_starInc);
	ra_folder = SUM(ra_folder, ra_folderInc);
	ra_mobile = SUM(ra_mobile, ra_mobileInc);
	ra_reported_cand = SUM(ra_reported_cand, ra_reported_candInc);
	ra_note = SUM(ra_note, ra_noteInc);
RUN;

DATA sasTemp.ConsolidatedDBIncBack;
	SET sasTemp.ConsolidatedDBInc;
RUN;

DATA sasTemp.ConsolidatedDBInc;
	SET sasTemp.ConsolidatedDB;
	IF DatePart(da_LastLogin) = INTNX('day', TODAY(), -1);
	*IF DatePart(da_LastLogin) >= INTNX('day', TODAY(), -9) AND DatePart(da_LastLogin) < INTNX('day', TODAY(), -5);
RUN;

PROC SORT DATA = FIncLib.FLATFILE; BY UserId; RUN;
PROC SORT DATA = FlatLib.FLATFILE; BY UserId; RUN;
DATA FlatLib.FLATFILE(SORTEDBY = UserId);
        UPDATE FlatLib.FLATFILE (IN = A) FIncLib.FLATFILE (IN = B);
        BY UserId; 
RUN;
/*
*/

PROC SORT DATA = sasTemp.no_skillsInc; BY UserId; RUN;
/*PROC SORT DATA = sasTemp.no_skills; BY UserId; RUN;*/
DATA sasTemp.no_skills(SORTEDBY = UserId);
        MERGE sasTemp.no_skills sasTemp.no_skillsInc;
        BY UserId;
RUN;

PROC SORT DATA = sasTemp.CandPrefFAInc; BY UserId; RUN;
PROC SORT DATA = sasTemp.CandPrefFAInc OUT = sasTemp.CandPrefFAIncUnique(KEEP = UserId) NODUPKEY; BY UserId; RUN;
/*PROC SORT DATA = sasTemp.CandPrefFA; BY UserId; RUN;*/
DATA sasTemp.CandPrefFA(SORTEDBY = UserId);
        MERGE sasTemp.CandPrefFA(IN=A) sasTemp.CandPrefFAIncUnique(IN=B);
        BY UserId;
        IF A AND NOT B;
RUN;

DATA sasTemp.CandPrefFA(SORTEDBY = UserId);
        SET sasTemp.CandPrefFA sasTemp.CandPrefFAInc;
        BY UserId;
RUN;

/*PROC SORT DATA = sasTemp.CandidateStaticInc; BY UserId; RUN;*/
DATA sasTemp.CandidateStatic(SORTEDBY = UserId);
        MERGE sasTemp.CandidateStatic sasTemp.CandidateStaticInc;
        BY UserId;
RUN;

PROC SORT DATA = sasTemp.RegistrationEndDateInc; BY UserId; RUN;
DATA sasTemp.RegistrationEndDate(SORTEDBY = UserId);
        MERGE sasTemp.RegistrationEndDate sasTemp.RegistrationEndDateInc;
        BY UserId;
RUN;

/*PROC SORT DATA = sasTemp.LoginHistory; BY UserId DESCENDING LastLogin; RUN;*/
PROC SORT DATA = sasTemp.LoginHistoryInc; BY UserId DESCENDING LastLogin; RUN;
DATA sasTemp.LoginHistory(SORTEDBY = UserId DESCENDING LastLogin);
        SET sasTemp.LoginHistory sasTemp.LoginHistoryInc;
        BY UserId DESCENDING LastLogin;
RUN;

/*PROC SORT DATA = sasTemp.ModificationHistory; BY UserId DESCENDING LastModified; RUN;*/
PROC SORT DATA = sasTemp.ModificationHistoryInc; BY UserId DESCENDING LastModified; RUN;
DATA sasTemp.ModificationHistory(SORTEDBY = UserId DESCENDING LastModified);
        SET sasTemp.ModificationHistory sasTemp.ModificationHistoryInc;
        BY UserId DESCENDING LastModified;
RUN;

/*PROC SORT DATA = sasTemp.PureLoginHistory; BY UserId DESCENDING LastLogin; RUN;*/
PROC SORT DATA = sasTemp.PureLoginHistoryInc; BY UserId DESCENDING LastLogin; RUN;
DATA sasTemp.PureLoginHistory(SORTEDBY = UserId DESCENDING LastLogin);
        SET sasTemp.PureLoginHistory sasTemp.PureLoginHistoryInc;
        BY UserId DESCENDING LastLogin;
RUN;

/*PROC SORT DATA = sasTemp.PureModificationHistory; BY UserId DESCENDING LastModified; RUN;*/
PROC SORT DATA = sasTemp.PureModificationHistoryInc; BY UserId DESCENDING LastModified; RUN;
DATA sasTemp.PureModificationHistory(SORTEDBY = UserId DESCENDING LastModified);
        SET sasTemp.PureModificationHistory sasTemp.PureModificationHistoryInc;
        BY UserId DESCENDING LastModified;
RUN;

PROC SORT DATA = sasTemp.Mob_Verify_LogsInc; BY Email; RUN;
DATA sasTemp.Mob_Verify_Logs(SORTEDBY=Email); 
	MERGE sasTemp.Mob_Verify_Logs sasTemp.Mob_Verify_LogsInc; 
	BY Email;	
RUN;

PROC SORT DATA = sasTemp.LastJASentInc; BY sa_UserId; RUN;
/*PROC SORT DATA = sasTemp.LastJASent; BY sa_UserId; RUN;*/
DATA sasTemp.LastJASent;
	MERGE sasTemp.LastJASent sasTemp.LastJASentInc;
	BY sa_UserId;
RUN;

/*	Stopped Maintaining	Start*/
/*
PROC SORT DATA = sasTemp.CandidateSkillsInc; BY UserId; RUN;
PROC SORT DATA = sasTemp.CandidateSkillsInc OUT = sasTemp.CandidateSkillsIncUnique(KEEP = UserId) NODUPKEY; BY UserId; RUN;
PROC SORT DATA = sasTemp.CandidateSkills; BY UserId; RUN;
DATA sasTemp.CandidateSkills(SORTEDBY = UserId);
        MERGE sasTemp.CandidateSkills(IN=A) sasTemp.CandidateSkillsIncUnique(IN=B);
        BY UserId;
        IF A AND NOT B;
RUN;

DATA sasTemp.CandidateSkills(SORTEDBY = UserId);
        SET sasTemp.CandidateSkills sasTemp.CandidateSkillsInc;
        BY UserId;
RUN;

PROC SORT DATA = sasTemp.CandPrefIndInc; BY UserId; RUN;
PROC SORT DATA = sasTemp.CandPrefIndInc OUT = sasTemp.CandPrefIndIncUnique(KEEP = UserId) NODUPKEY; BY UserId; RUN;
PROC SORT DATA = sasTemp.CandPrefInd; BY UserId; RUN;
DATA sasTemp.CandPrefInd(SORTEDBY = UserId);
        MERGE sasTemp.CandPrefInd(IN=A) sasTemp.CandPrefIndIncUnique(IN=B);
        BY UserId;
        IF A AND NOT B;
RUN;

DATA sasTemp.CandPrefInd(SORTEDBY = UserId);
        SET sasTemp.CandPrefInd sasTemp.CandPrefIndInc;
        BY UserId;
RUN;
*/
/*	Stopped Maintaining	End*/

PROC SORT DATA = sasTemp.CandidateJobsInc; BY UserId; RUN;
PROC SORT DATA = sasTemp.CandidateJobsInc OUT = sasTemp.CandidateJobsIncUnique(KEEP = UserId) NODUPKEY; BY UserId; RUN;
/*PROC SORT DATA = sasTemp.CandidateJobs; BY UserId; RUN;*/
DATA sasTemp.CandidateJobs(SORTEDBY = UserId);
        MERGE sasTemp.CandidateJobs(IN=A) sasTemp.CandidateJobsIncUnique(IN=B);
        BY UserId;
	IF A AND NOT B;
RUN;
DATA sasTemp.CandidateJobs(SORTEDBY = UserId);
	SET sasTemp.CandidateJobs sasTemp.CandidateJobsInc;
	BY UserId;
RUN;

PROC SORT DATA = sasTemp.CandidateEducationInc; BY UserId; RUN;
PROC SORT DATA = sasTemp.CandidateEducationInc OUT = sasTemp.CandidateEducationIncUnique(KEEP = UserId) NODUPKEY; BY UserId; RUN;
/*PROC SORT DATA = sasTemp.CandidateEducation; BY UserId; RUN;*/
DATA sasTemp.CandidateEducation(SORTEDBY = UserId);
        MERGE sasTemp.CandidateEducation(IN=A) sasTemp.CandidateEducationIncUnique(IN=B);
        BY UserId;
	IF A AND NOT B;
RUN;
DATA sasTemp.CandidateEducation(SORTEDBY = UserId);
        SET sasTemp.CandidateEducation sasTemp.CandidateEducationInc;
        BY UserId;
RUN;

DATA sasTemp.CandidateResumes;
	SET sasTemp.CandidateResumes sasTemp.CandidateResumesInc;
RUN;

PROC SORT DATA = sasTemp.CandidatePreferencesInc; BY UserId; RUN;
PROC SORT DATA = sasTemp.CandidatePreferencesInc OUT = sasTemp.CandidatePreferencesIncUnique(KEEP = UserId) NODUPKEY; BY UserId; RUN;
/*PROC SORT DATA = sasTemp.CandidatePreferences; BY UserId; RUN;*/
DATA sasTemp.CandidatePreferences(SORTEDBY = UserId);
	MERGE sasTemp.CandidatePreferences(IN=A) sasTemp.CandidatePreferencesIncUnique(IN=B);
	BY UserId;
	IF A AND NOT B;
RUN;

DATA sasTemp.CandidatePreferences(SORTEDBY = UserId);
	SET sasTemp.CandidatePreferences sasTemp.CandidatePreferencesInc;
	BY UserId;
RUN;

PROC SORT DATA = sasTemp.My_ParichayInc; BY Email; RUN;
PROC SORT DATA = sasTemp.My_ParichayInc OUT = My_ParichayIncUnique(KEEP = Email) NODUPKEY; BY Email; RUN;
/*PROC SORT DATA = sasTemp.My_Parichay; BY Email; RUN;*/
DATA sasTemp.My_Parichay(SORTEDBY = Email);
        MERGE sasTemp.My_Parichay(IN=A) My_ParichayIncUnique(IN=B);
        BY Email;
        IF A AND NOT B;
RUN; 

DATA sasTemp.My_Parichay(SORTEDBY = Email);
        SET sasTemp.My_Parichay sasTemp.My_ParichayInc;
        BY Email;
RUN; 

PROC SORT DATA = sasTemp.CandidateAPITokenInc; BY Email; RUN;
PROC SORT DATA = sasTemp.CandidateAPITokenInc OUT = CandidateAPITokenIncUnique(KEEP = Email) NODUPKEY; BY Email; RUN;
/*PROC SORT DATA = sasTemp.CandidateAPIToken; BY Email; RUN;*/
DATA sasTemp.CandidateAPIToken(SORTEDBY = Email);
        MERGE sasTemp.CandidateAPIToken(IN=A) CandidateAPITokenIncUnique(IN=B);
        BY Email;
        IF A AND NOT B;
RUN; 

DATA sasTemp.CandidateAPIToken(SORTEDBY = Email);
        SET sasTemp.CandidateAPIToken sasTemp.CandidateAPITokenInc;
        BY Email;
RUN; 

DATA sasTemp.CandidateMatch;
	SET sasTemp.CandidateMatch sasTemp.CandidateMatchInc;
RUN;
/*
DATA sasTemp.Activity;
	SET sasTemp.Activity sasTemp.ActivityInc;
RUN;
*/
DATA sasTemp.JASent;
	SET sasTemp.JASent sasTemp.JASentInc;
RUN;

DATA sasTemp.WVSent;
	SET sasTemp.WVSent sasTemp.WVSentInc;
RUN;

DATA sasTemp.JAClick;
	SET sasTemp.JAClick sasTemp.JAClickInc;
RUN;

