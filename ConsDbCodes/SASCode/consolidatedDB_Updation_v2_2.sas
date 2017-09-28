
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


%let days = 1;
/*	Filter	*/
/*
DATA sasTemp.PureLoginHistoryInc;
	SET sasTemp.PureLoginHistory;
	IF LastLogin >= INTNX('day', today(), -&days);
	*IF LastLogin >= INTNX('day', today(), -&days) AND LastLogin < INTNX('day', today(), -1);
RUN;

DATA sasTemp.PureModificationHistoryInc;
	SET sasTemp.PureModificationHistory;
	IF LastModified >= INTNX('day', today(), -&days);
	*IF LastModified >= INTNX('day', today(), -&days) AND LastModified < INTNX('day', today(), -1);
RUN;

DATA sasTemp.CandidateMatchInc;
	SET sasTemp.CandidateMatch;
	IF DatePart(ApplicationDate) >= INTNX('day', today(), -&days);
RUN;

DATA sasTemp.JAClickInc;
	SET sasTemp.JAClickInc;
	IF DATEPART(ClickDate) >= INTNX('day', today(), -&days);
RUN;

DATA sasTemp.JASentInc;
	SET sasTemp.JASentInc;
	IF SentDate >= INTNX('day', today(), -&days);
RUN;

DATA sasTemp.WVSentInc;
	SET sasTemp.WVSentInc;
	IF SentDate >= INTNX('day', today(), -&days);
RUN;
*/

DATA ProfileViewActivityInc(KEEP = UserId Type ActivityDate);
    SET sasTemp.ActivityInc;
    IF Type = 2;
	IF DATEPART(ActivityDate) >= INTNX('day', today(), -&days);
RUN;

DATA ExcelDownloadsActivityInc(KEEP = UserId Type ActivityDate);
    SET sasTemp.ActivityInc;
    IF Type = 3;
	IF DATEPART(ActivityDate) >= INTNX('day', today(), -&days);
RUN;

DATA CVDownloadsActivityInc(KEEP = UserId Type ActivityDate);
    SET sasTemp.ActivityInc;
    IF Type = 4;
	IF DATEPART(ActivityDate) >= INTNX('day', today(), -&days);
RUN;

DATA EmailsActivityInc(KEEP = UserId Type ActivityDate);
    SET sasTemp.ActivityInc;
    IF Type = 5;
	IF DATEPART(ActivityDate) >= INTNX('day', today(), -&days);
RUN;

DATA smsActivityInc(KEEP = UserId Type ActivityDate);
    SET sasTemp.ActivityInc;
    IF Type = 6;
	IF DATEPART(ActivityDate) >= INTNX('day', today(), -&days);
RUN;

DATA noteActivityInc(KEEP = UserId Type ActivityDate);
    SET sasTemp.ActivityInc;
    IF Type = 7;
	IF DATEPART(ActivityDate) >= INTNX('day', today(), -&days);
RUN;

DATA starActivityInc(KEEP = UserId Type ActivityDate);
    SET sasTemp.ActivityInc;
    IF Type = 8;
	IF DATEPART(ActivityDate) >= INTNX('day', today(), -&days);
RUN;

DATA folderActivityInc(KEEP = UserId Type ActivityDate);
    SET sasTemp.ActivityInc;
    IF Type = 9;
	IF DATEPART(ActivityDate) >= INTNX('day', today(), -&days);
RUN;

DATA mobileActivityInc(KEEP = UserId Type ActivityDate);
    SET sasTemp.ActivityInc;
    IF Type = 10;
	IF DATEPART(ActivityDate) >= INTNX('day', today(), -&days);
RUN;

DATA reported_candActivityInc(KEEP = UserId Type ActivityDate);
    SET sasTemp.ActivityInc;
    IF Type = 11;
	IF DATEPART(ActivityDate) >= INTNX('day', today(), -&days);
RUN;

/*	Roll-Up	*/
PROC SORT DATA = sasTemp.PureLoginHistoryInc; BY UserId; RUN;
PROC MEANS N DATA = sasTemp.PureLoginHistoryInc NOPRINT;
	BY UserId;
	VAR LastLogin;
	OUTPUT OUT = sasTemp.LoginsInc(DROP = _TYPE_ _FREQ_ RENAME = (UserId = sa_UserId)) N(LastLogin) = ca_LoginsInc;
RUN;

PROC SORT DATA = sasTemp.PureModificationHistoryInc; BY UserId;
PROC MEANS N DATA = sasTemp.PureModificationHistoryInc NOPRINT;
    BY UserId;
    VAR LastModified;
    OUTPUT OUT = sasTemp.ModifiesInc(DROP = _TYPE_ _FREQ_ RENAME = (UserId = sa_UserId)) N(LastModified) = ca_ModifiesInc;
RUN;

PROC SORT DATA = sasTemp.CandidateMatchInc; BY UserId;
PROC MEANS N DATA = sasTemp.CandidateMatchInc NOPRINT;
    BY UserId;
    VAR ApplicationDate;
    OUTPUT OUT = sasTemp.AppliesInc(DROP = _TYPE_ _FREQ_ RENAME = (UserId = sa_UserId)) N(ApplicationDate) = ca_AppliesInc;
RUN;

PROC SORT DATA = sasTemp.JAClickInc; BY UserId;
PROC MEANS N DATA = sasTemp.JAClickInc NOPRINT;
    BY UserId;
    VAR ClickDate;
    OUTPUT OUT = sasTemp.JAClicksInc(DROP = _TYPE_ _FREQ_ RENAME = (UserId = sa_UserId)) N(ClickDate) = ca_JAClicksInc;
RUN;

PROC SORT DATA = sasTemp.JASentInc; BY UserId; RUN;
PROC MEANS N DATA = sasTemp.JASentInc NOPRINT;
    BY UserId;
    VAR SentDate MatchedJobs OtherSugJobs;
    OUTPUT OUT = sasTemp.JASentToCandidateInc(DROP = _TYPE_ _FREQ_ RENAME = (UserId = sa_UserId))
    N(SentDate) = ya_JASentInc SUM(MatchedJobs) = ya_LatestMatchesInc SUM(OtherSugJobs) = ya_OtherMatchesInc;
RUN;

PROC SORT DATA = sasTemp.WVSentInc; BY UserId; RUN;
PROC MEANS N DATA = sasTemp.WVSentInc NOPRINT;
    BY UserId;
    VAR SentDate;
    OUTPUT OUT = sasTemp.WVSentToCandidateInc(DROP = _TYPE_ _FREQ_ RENAME = (UserId = sa_UserId))
    N(SentDate) = ya_WVSentInc;
RUN;

PROC SORT DATA = ProfileViewActivityInc; BY UserId; RUN;
PROC MEANS N DATA = ProfileViewActivityInc NOPRINT;
    BY UserId;
    VAR ActivityDate;
    OUTPUT OUT = sasTemp.ProfileViewsInc(DROP = _TYPE_ _FREQ_ RENAME = (UserId = sa_UserId)) N(ActivityDate) = ra_ProfileViewsInc;
RUN;

PROC SORT DATA = ExcelDownloadsActivityInc; BY UserId; RUN;
PROC MEANS N DATA = ExcelDownloadsActivityInc NOPRINT;
    BY UserId;
    VAR ActivityDate;
    OUTPUT OUT = sasTemp.ExcelDownloadsInc(DROP = _TYPE_ _FREQ_ RENAME = (UserId = sa_UserId)) N(ActivityDate) = ra_ExcelDownloadsInc;
RUN;

PROC SORT DATA = CVDownloadsActivityInc; BY UserId; RUN;
PROC MEANS N DATA = CVDownloadsActivityInc NOPRINT;
    BY UserId;
    VAR ActivityDate;
    OUTPUT OUT = sasTemp.CVDownloadsInc(DROP = _TYPE_ _FREQ_ RENAME = (UserId = sa_UserId)) N(ActivityDate) = ra_CVDownloadsInc;
RUN;

PROC SORT DATA = EmailsActivityInc; BY UserId; RUN;
PROC MEANS N DATA = EmailsActivityInc NOPRINT;
    BY UserId;
    VAR ActivityDate;
    OUTPUT OUT = sasTemp.EmailsInc(DROP = _TYPE_ _FREQ_ RENAME = (UserId = sa_UserId)) N(ActivityDate) = ra_EmailsInc;
RUN;

PROC SORT DATA = smsActivityInc; BY UserId; RUN;
PROC MEANS N DATA = smsActivityInc NOPRINT;
    BY UserId;
    VAR ActivityDate;
    OUTPUT OUT = sasTemp.smsInc(DROP = _TYPE_ _FREQ_ RENAME = (UserId = sa_UserId)) N(ActivityDate) = ra_smsInc;
RUN;

PROC SORT DATA = noteActivityInc; BY UserId; RUN;
PROC MEANS N DATA = noteActivityInc NOPRINT;
    BY UserId;
    VAR ActivityDate;
    OUTPUT OUT = sasTemp.noteInc(DROP = _TYPE_ _FREQ_ RENAME = (UserId = sa_UserId)) N(ActivityDate) = ra_noteInc;
RUN;

PROC SORT DATA = starActivityInc; BY UserId; RUN;
PROC MEANS N DATA = starActivityInc NOPRINT;
    BY UserId;
    VAR ActivityDate;
    OUTPUT OUT = sasTemp.starInc(DROP = _TYPE_ _FREQ_ RENAME = (UserId = sa_UserId)) N(ActivityDate) = ra_starInc;
RUN;

PROC SORT DATA = folderActivityInc; BY UserId; RUN;
PROC MEANS N DATA = folderActivityInc NOPRINT;
    BY UserId;
    VAR ActivityDate;
    OUTPUT OUT = sasTemp.folderInc(DROP = _TYPE_ _FREQ_ RENAME = (UserId = sa_UserId)) N(ActivityDate) = ra_folderInc;
RUN;

PROC SORT DATA = mobileActivityInc; BY UserId; RUN;
PROC MEANS N DATA = mobileActivityInc NOPRINT;
    BY UserId;
    VAR ActivityDate;
    OUTPUT OUT = sasTemp.mobileInc(DROP = _TYPE_ _FREQ_ RENAME = (UserId = sa_UserId)) N(ActivityDate) = ra_mobileInc;
RUN;

PROC SORT DATA = reported_candActivityInc; BY UserId; RUN;
PROC MEANS N DATA = reported_candActivityInc NOPRINT;
    BY UserId;
    VAR ActivityDate;
    OUTPUT OUT = sasTemp.reported_candInc(DROP = _TYPE_ _FREQ_ RENAME = (UserId = sa_UserId)) N(ActivityDate) = ra_reported_candInc;
RUN;

/*	Combining	*/
PROC SORT DATA = sasTemp.ConsolidatedDBInc; BY sa_UserId; RUN;
PROC SORT DATA = sasTemp.LoginsInc; BY sa_UserId; RUN;
PROC SORT DATA = sasTemp.ModifiesInc; BY sa_UserId; RUN;
PROC SORT DATA = sasTemp.AppliesInc; BY sa_UserId; RUN;
PROC SORT DATA = sasTemp.JAClicksInc; BY sa_UserId; RUN;
PROC SORT DATA = sasTemp.JASentToCandidateInc; BY sa_UserId; RUN;
PROC SORT DATA = sasTemp.WVSentToCandidateInc; BY sa_UserId; RUN;
PROC SORT DATA = sasTemp.ProfileViewsInc; BY sa_UserId; RUN;
PROC SORT DATA = sasTemp.ExcelDownloadsInc; BY sa_UserId; RUN;
PROC SORT DATA = sasTemp.CVDownloadsInc; BY sa_UserId; RUN;
PROC SORT DATA = sasTemp.EmailsInc; BY sa_UserId; RUN;
PROC SORT DATA = sasTemp.smsInc; BY sa_UserId; RUN;
PROC SORT DATA = sasTemp.noteInc; BY sa_UserId; RUN;
PROC SORT DATA = sasTemp.starInc; BY sa_UserId; RUN;
PROC SORT DATA = sasTemp.folderInc; BY sa_UserId; RUN;
PROC SORT DATA = sasTemp.mobileInc; BY sa_UserId; RUN;
PROC SORT DATA = sasTemp.reported_candInc; BY sa_UserId; RUN;
DATA sasTemp.ConsolidatedDBInc;
    MERGE sasTemp.ConsolidatedDBInc(IN=A) sasTemp.LoginsInc sasTemp.ModifiesInc sasTemp.AppliesInc
	sasTemp.JAClicksInc sasTemp.JASentToCandidateInc sasTemp.WVSentToCandidateInc 
	sasTemp.ProfileViewsInc sasTemp.ExcelDownloadsInc sasTemp.CVDownloadsInc sasTemp.EmailsInc sasTemp.smsInc
	sasTemp.noteInc sasTemp.starInc sasTemp.folderInc sasTemp.mobileInc sasTemp.reported_candInc;
    BY sa_UserId;
    IF A;
RUN;

DATA sasTemp.ConsolidatedDBInc;
    SET sasTemp.ConsolidatedDBInc;
    IF ca_LoginsInc = . THEN ca_LoginsInc = 0;
    IF ca_ModifiesInc = . THEN ca_ModifiesInc = 0;
    IF ca_AppliesInc = . THEN ca_AppliesInc = 0;
    IF ca_JAClicksInc = . THEN ca_JAClicksInc = 0;
    IF ya_JASentInc = . THEN ya_JASentInc = 0;
    IF ya_LatestMatchesInc = . THEN ya_LatestMatchesInc = 0;
    IF ya_OtherMatchesInc = . THEN ya_OtherMatchesInc = 0;
    IF ya_WVSentInc = . THEN ya_WVSentInc = 0;
    IF ra_ProfileViewsInc = . THEN ra_ProfileViewsInc = 0;
    IF ra_ExcelDownloadsInc = . THEN ra_ExcelDownloadsInc = 0;
    IF ra_CVDownloadsInc = . THEN ra_CVDownloadsInc = 0;
    IF ra_EmailsInc = . THEN ra_EmailsInc = 0;
    IF ra_smsInc = . THEN ra_smsInc = 0;
    IF ra_noteInc = . THEN ra_noteInc = 0;
    IF ra_starInc = . THEN ra_starInc = 0;
    IF ra_folderInc = . THEN ra_folderInc = 0;
    IF ra_mobileInc = . THEN ra_mobileInc = 0;
    IF ra_reported_candInc = . THEN ra_reported_candInc = 0;
RUN;

