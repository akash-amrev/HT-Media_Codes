
/*************************	
Author: Shailendra
Description: Import incremental Datasets
Date: Thu Apr 26 16:38:10 2012
***************************/
%let PATH1 = /data/Analytics/Utils/consolidatedDB;
%let lookupdir = &PATH1./Model/Lookups;

libname lulib  "&PATH1./Model/Lookups";
libname sasTemp  "&PATH1./Model/SASTemp";

DATA sasTemp.CandidateStaticInc;
	INFILE "/data/Analytics/Utils/consolidatedDB/Input/dataFromMongo/Incrementals/CandidateStaticInc.csv"
	DLM=',' MISSOVER DSD LRECL=3326 FIRSTOBS=2 DSD termstr=lf;

	INFORMAT CandidateLocation BEST4.;
	INFORMAT CountryCode $10.;
	INFORMAT SMSAlertFlag BEST1.;
	INFORMAT Experience BEST2.;
	INFORMAT ReceiveOtherProductInfo $10.;
	INFORMAT CellPhone $50.;
	INFORMAT EmailQuality $10.;
	INFORMAT PrivacyStatus $10.;
	INFORMAT IsEmailVerified $5.;
	INFORMAT Password $200.;
	INFORMAT AdminUserId $10.;
	INFORMAT YearsOfExpLastUpdated anydtdtm20.0;
	INFORMAT LastLogin anydtdtm20.0;
	INFORMAT LastModified anydtdtm20.0;
	INFORMAT RegistrationStartDate anydtdtm20.0;
	INFORMAT StartIpAddress $50.;
	INFORMAT IsResumeMidOut BEST1.;
	INFORMAT StartVendorId BEST8.;
	INFORMAT RegistrationEndDate anydtdtm20.0;
	INFORMAT PreferenceUpdate anydtdtm20.0;
	INFORMAT LastAppliedDate anydtdtm20.0;
	INFORMAT TeamSizeManaged BEST1.;
	INFORMAT EmailAlertStatus BEST1.;
	INFORMAT FirstName $250.;
	INFORMAT IsActive BEST1.;
	INFORMAT EndVendorId BEST8.;
	INFORMAT QualityScore BEST2.;
	INFORMAT Email $100.;
	INFORMAT Gender BEST1.;
	INFORMAT IsMidOut BEST1.;
	INFORMAT Experience_Month BEST2.;
	INFORMAT EndIpAddress $50.;
	INFORMAT LastName $250.;
	INFORMAT Salary BEST2.;
	INFORMAT MobileQuality BEST2.;
	INFORMAT UserId $50.;
	INFORMAT BOUpdateDate anydtdtm20.0;
	INFORMAT IsCellPhoneVerified BEST1.;
	INFORMAT SpamStatus BEST1.;
	INFORMAT BounceStatus BEST1.;
	INFORMAT LinkedInToken $255.;
	INFORMAT LinkedInExpiryDate anydtdtm20.0;
	INFORMAT ReceiveRecruiterMails BEST1.;
	INFORMAT DateOfBirth anydtdtm20.0;
	INFORMAT NoticePeriod BEST2.;
	INFORMAT ProfileTitle $100.;
	INFORMAT IsMobileLead BEST1.;
	INFORMAT Certifications $250.;

	FORMAT CandidateLocation BEST4.;
	FORMAT CountryCode $10.;
	FORMAT SMSAlertFlag BEST1.;
	FORMAT Experience BEST2.;
	FORMAT ReceiveOtherProductInfo $10.;
	FORMAT CellPhone $50.;
	FORMAT EmailQuality $50.;
	FORMAT PrivacyStatus $10.;
	FORMAT IsEmailVerified $5.;
	FORMAT Password $200.;
	FORMAT AdminUserId $10.;
	FORMAT YearsOfExpLastUpdated DateTime19.;
	FORMAT LastLogin DateTime19.;
	FORMAT LastModified DateTime19.;
	FORMAT RegistrationStartDate DateTime19.;
	FORMAT StartIpAddress $50.;
	FORMAT IsResumeMidOut BEST1.;
	FORMAT StartVendorId BEST8.;
	FORMAT RegistrationEndDate DateTime19.;
	FORMAT PreferenceUpdate DateTime19.;
	FORMAT LastAppliedDate DateTime19.;
	FORMAT TeamSizeManaged BEST1.;
	FORMAT EmailAlertStatus BEST1.;
	FORMAT FirstName $250.;
	FORMAT IsActive BEST1.;
	FORMAT EndVendorId BEST8.;
	FORMAT QualityScore BEST2.;
	FORMAT Email $100.;
	FORMAT Gender BEST1.;
	FORMAT IsMidOut BEST1.;
	FORMAT Experience_Month BEST2.;
	FORMAT EndIpAddress $50.;
	FORMAT LastName $250.;
	FORMAT Salary BEST2.;
	FORMAT MobileQuality BEST2.;
	FORMAT UserId $50.;
	FORMAT BOUpdateDate DateTime19.;
	FORMAT IsCellPhoneVerified BEST1.;
	FORMAT SpamStatus BEST1.;
	FORMAT BounceStatus BEST1.;
	FORMAT LinkedInToken $255.;
	FORMAT LinkedInExpiryDate DateTime19.;
	FORMAT ReceiveRecruiterMails BEST1.;
	FORMAT DateOfBirth DateTime19.;
	FORMAT NoticePeriod BEST2.;
	FORMAT ProfileTitle $100.;
	FORMAT IsMobileLead BEST1.;
	FORMAT Certifications $250.;

	INPUT
	CandidateLocation CountryCode SMSAlertFlag Experience ReceiveOtherProductInfo CellPhone EmailQuality
	PrivacyStatus IsEmailVerified Password AdminUserId YearsOfExpLastUpdated LastLogin  LastModified
	RegistrationStartDate StartIpAddress IsResumeMidOut StartVendorId RegistrationEndDate PreferenceUpdate
	LastAppliedDate TeamSizeManaged EmailAlertStatus FirstName IsActive EndVendorId QualityScore Email
	Gender IsMidOut Experience_Month EndIpAddress LastName Salary MobileQuality UserId BOUpdateDate
	IsCellPhoneVerified SpamStatus BounceStatus LinkedInToken LinkedInExpiryDate ReceiveRecruiterMails
	DateOfBirth NoticePeriod ProfileTitle IsMobileLead Certifications $;
RUN;

DATA sasTemp.CandidateJobsInc;
	INFILE "/data/Analytics/Utils/consolidatedDB/Input/dataFromMongo/Incrementals/CandidateJobsInc.csv"
	DLM=',' MISSOVER DSD LRECL=3326 FIRSTOBS=2 DSD termstr=lf;

	INFORMAT Experience_Month BEST2.;
	INFORMAT UserId $50.;
	INFORMAT Industry BEST2.;
	INFORMAT LinkedInCount BEST4.;
	INFORMAT CompanyNameCustom $250.;
	INFORMAT JobTitleId BEST3.;
	INFORMAT Experience BEST2.;
	INFORMAT IsMostRecent BEST3.;
	INFORMAT CandidateJobId $50.; 
	INFORMAT SubField BEST4.;
	INFORMAT JobTitleCustom $250.;
	INFORMAT CompanyName BEST4.;
	
	FORMAT Experience_Month BEST2.;
	FORMAT UserId $50.;
	FORMAT Industry BEST2.;
	FORMAT LinkedInCount BEST4.;
	FORMAT CompanyNameCustom $250.;
	FORMAT JobTitleId BEST3.;
	FORMAT Experience BEST2.;
	FORMAT IsMostRecent BEST3.;
	FORMAT CandidateJobId $50.;
	FORMAT SubField BEST4.;
	FORMAT JobTitleCustom $250.;
	FORMAT CompanyName BEST4.;

	INPUT Experience_Month UserId Industry LinkedInCount CompanyNameCustom JobTitleId Experience 
	IsMostRecent CandidateJobId SubField JobTitleCustom CompanyName $;
RUN;

DATA sasTemp.CandidateEducationInc;
	INFILE "/data/Analytics/Utils/consolidatedDB/Input/dataFromMongo/Incrementals/CandidateEducationInc.csv"
	DLM=',' MISSOVER DSD LRECL=3326 FIRSTOBS=2 DSD termstr=lf;

	INFORMAT Education_Level BEST3.;
	INFORMAT InstituteNameCustom $250.;
	INFORMAT UserId $50.;
	INFORMAT LinkedInCount BEST4.;
	INFORMAT InstituteName BEST3.;
	INFORMAT IsMostRecent BEST2.;
	INFORMAT CandidateEducationId $50.; 
	INFORMAT StudyField BEST4.;
	INFORMAT CourseType BEST1.;
	INFORMAT YearOfPassout BEST4.;

	FORMAT Education_Level BEST3.;
	FORMAT InstituteNameCustom $250.;
	FORMAT UserId $50.;
	FORMAT LinkedInCount BEST4.;
	FORMAT InstituteName BEST3.;
	FORMAT IsMostRecent BEST2.;
	FORMAT CandidateEducationId $50.; 
	FORMAT StudyField BEST4.;
	FORMAT CourseType BEST1.;
	FORMAT YearOfPassout BEST4.;
	
	INPUT Education_Level InstituteNameCustom UserId LinkedInCount InstituteName IsMostRecent 
	CandidateEducationId StudyField CourseType YearOfPassout $;
RUN;

DATA sasTemp.CandidateResumesInc;
	INFILE "/data/Analytics/Utils/consolidatedDB/Input/dataFromMongo/Incrementals/CandidateResumesInc.csv"
	DLM=',' MISSOVER DSD LRECL=3326 FIRSTOBS=2 DSD termstr=lf;
	
	INFORMAT CandidateResumeId $50.;
	INFORMAT CreationDate anydtdtm20.0;
	INFORMAT DefaultStatus BEST1.;
	INFORMAT DeletionStatus $1.;
	INFORMAT Extension $10.;
	INFORMAT UserId $50.;
	INFORMAT IsParsed BEST1.;
	INFORMAT ResumeName $50.;
	INFORMAT ResumeTitle $50.;
	INFORMAT TypeId BEST1.;

	FORMAT CandidateResumeId $50.;
	FORMAT CreationDate DateTime19.;
	FORMAT DefaultStatus BEST1.;
	FORMAT DeletionStatus $1.;
	FORMAT Extension $10.;
	FORMAT UserId $50.;
	FORMAT IsParsed BEST1.;
	FORMAT ResumeName $50.;
	FORMAT ResumeTitle $50.;
	FORMAT TypeId BEST1.;

	INPUT CandidateResumeId CreationDate DefaultStatus DeletionStatus Extension UserId IsParsed ResumeName ResumeTitle TypeId $;
RUN;

DATA sasTemp.CandidatePreferencesInc;
	INFILE "/data/Analytics/Utils/consolidatedDB/Input/dataFromMongo/Incrementals/CandidatePreferencesInc.csv"
	DLM=',' MISSOVER DSD LRECL=3326 FIRSTOBS=2 DSD termstr=lf;

	INFORMAT CandidatePreferencesId $50.;
	INFORMAT UserId $50.;
	INFORMAT Type BEST2.;
	INFORMAT Value BEST9.;
	INFORMAT ValueCustom $20.;
	INFORMAT Level BEST4.;
	INFORMAT IsMostRecent BEST1.;

	FORMAT CandidatePreferencesId $50.;
	FORMAT UserId $50.;
	FORMAT Type BEST2.;
	FORMAT Value BEST9.;
	FORMAT ValueCustom $20.;
	FORMAT Level BEST4.;
	FORMAT IsMostRecent BEST1.;

	INPUT CandidatePreferencesId UserId Type Value ValueCustom Level IsMostRecent $;
RUN;

DATA sasTemp.CandidateMatchInc;
	INFILE "/data/Analytics/Utils/consolidatedDB/Input/dataFromMongo/Incrementals/CandidateMatchInc.csv" 
	DLM=',' MISSOVER DSD LRECL=3326 FIRSTOBS=2 DSD termstr=lf;

	INFORMAT IsApply BEST3.;
	INFORMAT JobId BEST8.;
	INFORMAT UserId $50.;
	INFORMAT MatchDate anydtdtm20.0;
	INFORMAT ResumeId $50.;
	INFORMAT IsMatch $5.;
	INFORMAT CandidateMatchId $50.;
	INFORMAT DeletionStatus BEST1.;
	INFORMAT ApplicationDate anydtdtm20.0;
	INFORMAT sId $32.;

	FORMAT IsApply BEST3.;
	FORMAT JobId BEST8.;
	FORMAT UserId $50.;
	FORMAT MatchDate DateTime19.;
	FORMAT ResumeId $50.;
	FORMAT IsMatch $5.;
	FORMAT CandidateMatchId $50.;
	FORMAT DeletionStatus BEST1.;
	FORMAT ApplicationDate DateTime19.;
	FORMAT sId $32.;

	INPUT IsApply JobId UserId MatchDate ResumeId IsMatch CandidateMatchId DeletionStatus ApplicationDate sId $;
RUN;

DATA sasTemp.ActivityInc;
	INFILE "/data/Analytics/Utils/consolidatedDB/Input/dataFromMongo/Incrementals/ActivityInc.csv" 
	DLM=',' MISSOVER DSD LRECL=3326 FIRSTOBS=2 DSD termstr=lf;

	INFORMAT UserId $50.;
	INFORMAT ActivityDate anydtdtm20.0;
	INFORMAT CompanyId $10.;
	INFORMAT Type BEST2.;
	INFORMAT RecruiterName $100.;
	INFORMAT ActivityId $10.;
	INFORMAT RecruiterId $10.;
	INFORMAT CompanyName $50.;

	FORMAT UserId $50.;
	FORMAT ActivityDate DateTime19.;
	FORMAT CompanyId $10.;
	FORMAT Type BEST2.;
	FORMAT RecruiterName $100.;
	FORMAT ActivityId $10.;
	FORMAT RecruiterId $10.;
	FORMAT CompanyName $50.;

	INPUT UserId ActivityDate CompanyId Type RecruiterName ActivityId RecruiterId CompanyName $;
RUN;

DATA sasTemp.Mob_Verify_LogsInc;
	INFILE "/data/Analytics/Utils/consolidatedDB/Input/dataFromMongo/Incrementals/mobverifylogsInc.csv" 
	DLM=',' MISSOVER DSD LRECL=3326 FIRSTOBS=2 DSD termstr=lf;

	INFORMAT Email $100.;
	INFORMAT LastCellPhoneVerifiedDate anydtdtm20.0;
	INFORMAT MobileNo $100.;

	FORMAT Email $100.;
	FORMAT LastCellPhoneVerifiedDate DateTime19.;
	FORMAT MobileNo $100.;
	
	INPUT Email LastCellPhoneVerifiedDate MobileNo;
RUN;

DATA sasTemp.My_ParichayInc;
	INFILE "/data/Analytics/Utils/consolidatedDB/Input/dataFromMongo/Incrementals/my_parichayInc.csv" 
	DLM=',' MISSOVER DSD LRECL=3326 FIRSTOBS=2 DSD termstr=lf;
	
	INFORMAT Id $50.;
	INFORMAT CandidateLocation BEST4.;
	INFORMAT Email $100.;
	INFORMAT Experience BEST2.;
	INFORMAT ExperienceMonth BEST2.;
	INFORMAT FA BEST4.;
	INFORMAT Industry BEST2.;
	INFORMAT LastAppliedDate anydtdtm20.0;
	INFORMAT LastModified anydtdtm20.0;
	INFORMAT Mobile $50.;
	INFORMAT Name $250.;
	INFORMAT Resume $100.;
	INFORMAT RegistrationEndDate anydtdtm20.0;
	INFORMAT Salary BEST2.;

	FORMAT Id $50.;
	FORMAT CandidateLocation BEST4.;
	FORMAT Email $100.;
	FORMAT Experience BEST2.;
	FORMAT ExperienceMonth BEST2.;
	FORMAT FA BEST4.;
	FORMAT Industry BEST2.;
	FORMAT LastAppliedDate DateTime19.;
	FORMAT LastModified DateTime19.;
	FORMAT Mobile $50.;
	FORMAT Name $250.;
	FORMAT Resume $100.;
	FORMAT RegistrationEndDate DateTime19.;
	FORMAT Salary BEST2.;

	INPUT Id CandidateLocation Email Experience ExperienceMonth FA Industry LastAppliedDate LastModified Mobile Name Resume RegistrationEndDate Salary;
RUN;

DATA sasTemp.CandidateAPITokenInc;
        INFILE "/data/Analytics/Utils/consolidatedDB/Input/dataFromMongo/Incrementals/CandidateAPITokenInc.csv"
        DLM=',' MISSOVER DSD LRECL=3326 FIRSTOBS=2 DSD termstr=lf;

        INFORMAT Id $50.;
        INFORMAT Email $100.;
        INFORMAT UserId $50.;
        INFORMAT ED anydtdtm20.0;
        INFORMAT CD anydtdtm20.0;
        INFORMAT t BEST1.;

        FORMAT Id $50.;
        FORMAT Email $100.;
        FORMAT UserId $50.;
        FORMAT ED DateTime19.;
        FORMAT CD DateTime19.;
        FORMAT t BEST1.;

        INPUT Id Email UserId ED CD t;
RUN;

/*
DATA sasTemp.MinResumeUploadDate_RM;
	INFILE "/data/Analytics/Utils/consolidatedDB/Input/RegistrationEndDateNullResumeUploadDate_RM.csv"
	DLM=',' MISSOVER DSD LRECL=3326 FIRSTOBS=2 DSD termstr=lf;

	INFORMAT UserId $50.;
	INFORMAT RegistrationStartDate anydtdtm20.0;
	INFORMAT RegistrationEndDate anydtdtm20.0;
	INFORMAT MinResumeUploadDate anydtdtm20.0;
	FORMAT UserId $50.;
	FORMAT RegistrationStartDate DateTime19.;
	FORMAT RegistrationEndDate DateTime19.;
	FORMAT MinResumeUploadDate DateTime19.;
	INPUT UserId RegistrationStartDate RegistrationEndDate MinResumeUploadDate$;
RUN;
*/


DATA sasTemp.JAClickInc1;
	INFILE "/data/Analytics/Utils/consolidatedDB/Input/JAClickInc.csv"
	DLM=',' MISSOVER DSD LRECL=3326 FIRSTOBS=1 DSD termstr=lf;

	INFORMAT UserId $50.;
	INFORMAT Email $100.;
	INFORMAT Temp $10.;
	INFORMAT JobId BEST7.;
	INFORMAT SentDate anydtdtm19.;
	INFORMAT ClickDate anydtdtm19.;
	INFORMAT Type $10.;
	INFORMAT LinkText $30.;
	INFORMAT LinkPosition $10.;
	INFORMAT Domain $20.;
	INFORMAT Browser $20.;
	INFORMAT OS $20.;

	FORMAT UserId $50.;
	FORMAT Email $100.;
	FORMAT Temp $10.; 
	FORMAT JobId BEST7.; 
	FORMAT SentDate DateTime19.;
	FORMAT ClickDate DateTime19.;
	FORMAT Type $10.;
	FORMAT LinkText $30.;
	FORMAT LinkPosition $10.;
	FORMAT Domain $20.;
	FORMAT Browser $20.;
	FORMAT OS $20.;

	INPUT UserId Email Temp JobId SentDate ClickDate Type LinkText LinkPosition Domain Browser OS $;
RUN;

DATA sasTemp.JASentInc;
	INFILE "/data/Analytics/Utils/consolidatedDB/Input/JASentInc.csv" 
	DLM=',' MISSOVER DSD LRECL=3326 FIRSTOBS=1 DSD termstr=lf;

	INFORMAT UserId $50.;
	INFORMAT SentDate ddmmyy10.;
	INFORMAT MatchedJobs BEST3.;
	INFORMAT OtherSugJobs BEST3.;
	INFORMAT Type $10.;

	FORMAT UserId $50.;
	FORMAT SentDate Date9.;
	FORMAT MatchedJobs BEST3.;
	FORMAT OtherSugJobs BEST3.;
	FORMAT Type $10.;

	INPUT UserId SentDate MatchedJobs OtherSugJobs Type $;
RUN;

DATA sasTemp.WVSentInc;
	INFILE "/data/Analytics/Utils/consolidatedDB/Input/WhoViewedInc.csv" 
	DLM=',' MISSOVER DSD LRECL=3326 FIRSTOBS=1 DSD termstr=lf;

	INFORMAT UserId $50.;
	INFORMAT Email $100.;
	INFORMAT RecCount BEST3.;
	INFORMAT ViewCount BEST3.;
	INFORMAT SentDate yymmdd10.;

	FORMAT UserId $50.;
	FORMAT Email $100.;
	FORMAT RecCount BEST3.;
	FORMAT ViewCount BEST3.;
	FORMAT SentDate Date9.;

	INPUT UserId Email RecCount ViewCount SentDate$;
RUN;

DATA EmailInc(KEEP = Email);
	SET sasTemp.CandidateStaticInc(KEEP = Email RegistrationStartDate);
	IF DatePart(RegistrationStartDate) = INTNX('day', TODAY(), -1);
	*IF DatePart(RegistrationStartDate) >= INTNX('day', TODAY(), -9) AND DatePart(RegistrationStartDate) < INTNX('day', TODAY(), -5);
	*SET sasTemp.CandidateStaticInc(KEEP = Email LastLogin);
	*IF DatePart(LastLogin) = INTNX('day', TODAY(), -1);
RUN;
PROC EXPORT DATA = EmailInc OUTFILE = '/data/Analytics/Utils/consolidatedDB/Input/EmailInc.csv' DBMS = CSV REPLACE; RUN;
