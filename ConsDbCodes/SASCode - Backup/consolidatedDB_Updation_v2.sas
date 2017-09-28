
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

option mprint;	option spool;	%include "&PATH1./Model/SASCode/DateConversionMacro.sas";

/*	Preferences	*/
DATA sasTemp.SkillsInc(KEEP = UserId Value ValueCustom);
        SET sasTemp.CandidatePreferencesInc;
        IF Type = 3;
RUN;

PROC SORT DATA = sasTemp.SkillsInc; BY Value; RUN;
PROC SORT DATA = lulib.lookups_skill; BY skill_id; RUN;
DATA SkillsTempInc(drop = value rename = (skill_desc = value));
        MERGE sasTemp.SkillsInc(IN=A) lulib.lookups_skill(IN=B KEEP = skill_id skill_desc RENAME = (skill_id = value));
        BY Value;
RUN;

DATA sasTemp.CandidateSkillsInc(KEEP = UserId Skill);
        SET SkillsTempInc;
        Skill = STRIP(COMPBL(CATX('',Value, ValueCustom)));
RUN;

PROC SORT DATA = sasTemp.CandidateSkillsInc; BY UserId; RUN;
PROC SUMMARY DATA = sasTemp.CandidateSkillsInc;
        BY UserId;
        OUTPUT OUT = sasTemp.no_skillsInc(DROP = _TYPE_ RENAME = (_FREQ_ = No_Skills));
RUN;

DATA PrefFAInc(KEEP = UserId Value);
        SET sasTemp.CandidatePreferencesInc;
        IF Type = 10;
RUN;

PROC SORT DATA = PrefFAInc; BY Value; RUN;
PROC SORT DATA = lulib.lookups_subfunctionalarea; BY sub_field_id; RUN;
DATA sasTemp.CandPrefFAInc(drop = value rename = (sub_field_enu = DesiredSubFunction field_enu = DesiredFunction));
        MERGE PrefFAInc(IN=A) lulib.lookups_subfunctionalarea(IN=B KEEP = sub_field_id sub_field_enu field_enu RENAME = (sub_field_id = value));
        BY Value;
RUN;

DATA PrefIndInc(KEEP = UserId Value);
        SET sasTemp.CandidatePreferencesInc;
        IF Type = 9;
RUN;

PROC SORT DATA = PrefIndInc; BY Value; RUN;
PROC SORT DATA = lulib.lookups_industry; BY industry_id; RUN;
DATA sasTemp.CandPrefIndInc(drop = value rename = (industry_desc = DesiredIndustry));
        MERGE PrefIndInc(IN=A) lulib.lookups_industry(IN=B KEEP = industry_id industry_desc RENAME = (industry_id = value));
        BY Value;
RUN;

/*	*/
PROC SORT DATA = sasTemp.CandidateMatchInc OUT = CandidateMatchInc(KEEP = UserId ApplicationDate); BY UserId DESCENDING ApplicationDate; RUN;
PROC SORT DATA = CandidateMatchInc(RENAME = (UserId = sa_UserId ApplicationDate = da_LastAppliedDate)) NODUPKEY; BY sa_UserId; RUN;

PROC SORT DATA = sasTemp.CandidateResumesInc OUT = CandidateResumesInc(KEEP = UserId CreationDate); BY UserId DESCENDING CreationDate; RUN;
PROC SORT DATA = CandidateResumesInc(RENAME = (UserId = sa_UserId CreationDate = da_LastCVUpdateDate)) NODUPKEY; BY sa_UserId; RUN;

PROC SORT DATA = sasTemp.JASentInc OUT = sasTemp.LastJASentInc(KEEP = UserId SentDate); BY UserId DESCENDING SentDate; RUN;
PROC SORT DATA = sasTemp.LastJASentInc(RENAME = (UserId = sa_UserId SentDate = da_LastJASentDate)) NODUPKEY; BY sa_UserId; RUN;
/*	*/

/*	Part 1 	*/
PROC SORT DATA = sasTemp.CandidateJobsInc; BY UserId DESCENDING IsMostRecent; RUN;
PROC SORT DATA = sasTemp.CandidateJobsInc OUT = sasTemp.CandidateJobsInc1 NODUPKEY; BY UserId; RUN;

/*PROC SORT DATA = sasTemp.CandidateEducationInc; BY UserId DESCENDING IsMostRecent; RUN;*/
PROC SORT DATA = sasTemp.CandidateEducationInc; BY UserId DESCENDING Education_Level; RUN;
PROC SORT DATA = sasTemp.CandidateEducationInc OUT = sasTemp.CandidateEducationInc1 NODUPKEY; BY UserId; RUN;

/*
DATA sasTemp.CandidateStaticInc;
        SET sasTemp.CandidateStaticInc;
        IF Email ~= '';
RUN;
*/

proc sort data = sasTemp.CandidateStaticInc; by UserId; run;
DATA sasTemp.ConsolidatedDBInc(SORTEDBY = UserId);
	MERGE sasTemp.CandidateStaticInc (IN = A) sasTemp.CandidateJobsInc1 (IN = B 
	DROP = LinkedInCount IsMostRecent CandidateJobId RENAME = (Experience = Experience1 Experience_Month = Experience_Month1));
	BY UserId;
	IF A;
RUN;

DATA sasTemp.ConsolidatedDBInc(SORTEDBY = UserId);
	MERGE sasTemp.ConsolidatedDBInc (IN = A) sasTemp.CandidateEducationInc1 (IN = B DROP = IsMostRecent LinkedInCount CandidateEducationId);
	BY UserId;
	IF A;
RUN;

PROC SORT Data = sasTemp.ConsolidatedDBInc; BY CandidateLocation; RUN;
DATA sasTemp.ConsolidatedDBInc(DROP = city_id RENAME = (city = sa_City state = sa_state state2 = sa_state2 Region = sa_zone Country = sa_Country IsTopCity = fa_IsTopCity));
	MERGE sasTemp.ConsolidatedDBInc (IN = A RENAME = (CandidateLocation = city_id)) lulib.Lookups_CandidateLocation (IN = B);
	BY City_Id;
	IF A;
RUN;

PROC SORT Data = sasTemp.ConsolidatedDBInc; BY StartVendorId; RUN;
DATA sasTemp.ConsolidatedDBInc (RENAME = (VendorName = sa_StartSource));
        MERGE sasTemp.ConsolidatedDBInc (IN = A) MarkLib.VendorLook(IN = B RENAME = (EndVendorId = StartVendorId));
        BY StartVendorId;
        IF A;
RUN;

PROC SORT Data = sasTemp.ConsolidatedDBInc; BY EndVendorId; RUN;
DATA sasTemp.ConsolidatedDBInc (RENAME = (VendorName = sa_Source));
        MERGE sasTemp.ConsolidatedDBInc (IN = A) MarkLib.VendorLook (IN = B);
        BY EndVendorId;
        IF A;
RUN;

PROC SORT Data = sasTemp.ConsolidatedDBInc; BY Salary; RUN;
DATA sasTemp.ConsolidatedDBInc (DROP = Salary RENAME = (text_value = sa_Salary SalaryMin = sa_SalaryMin SalaryMax = sa_SalaryMax));
	MERGE sasTemp.ConsolidatedDBInc (IN = A) 
	lulib.Lookups_Salary (IN = B DROP = text_value_hr text_value_MIN text_value_MAX RENAME = (Salary_Id = Salary));
	BY Salary;
	IF A;
RUN;

PROC SORT Data = sasTemp.ConsolidatedDBInc; BY Industry; RUN;
DATA sasTemp.ConsolidatedDBInc (DROP = Industry);
	MERGE sasTemp.ConsolidatedDBInc (IN = A) 
	lulib.Lookups_Industry (IN = B Drop = sort_order RENAME = (Industry_id = Industry industry_desc = sa_Industry));
	BY Industry;
	IF A;
RUN;

PROC SORT Data = sasTemp.ConsolidatedDBInc; BY SubField; RUN;
DATA sasTemp.ConsolidatedDBInc (DROP = SubField);
	MERGE sasTemp.ConsolidatedDBInc (IN = A) lulib.Lookups_subfunctionalarea (IN = B 
	DROP = field_id field_sort_order sub_field_sort_order RENAME = (sub_field_id = SubField sub_field_enu = sa_SubFunction field_enu = sa_Function));
	BY SubField;
	IF A;
RUN;

PROC SORT Data = sasTemp.ConsolidatedDBInc; BY JobTitleId; RUN;
DATA sasTemp.ConsolidatedDBInc (DROP = JobTitleId);
	MERGE sasTemp.ConsolidatedDBInc (IN = A) 
	lulib.Lookups_JobTitleId (IN = B RENAME = (Job_Title_Id = JobTitleId job_title_desc = sa_JobTitle));
	BY JobTitleId;
	IF A;
RUN;

PROC SORT Data = sasTemp.ConsolidatedDBInc; BY CompanyName; RUN;
DATA sasTemp.ConsolidatedDBInc (DROP = CompanyName);
        MERGE sasTemp.ConsolidatedDBInc (IN = A) 
	lulib.Lookups_CompanyName (IN = B RENAME = (value = CompanyName display = sa_lastCompany));
        BY CompanyName;
        IF A;
RUN;

PROC SORT Data = sasTemp.ConsolidatedDBInc; BY Education_Level; RUN;
DATA sasTemp.ConsolidatedDBInc (DROP = Education_Level);
	MERGE sasTemp.ConsolidatedDBInc (IN = A) lulib.Lookups_eduqualilevel (IN = B 
	drop =  sort_order RENAME = (Education_Level_Id = Education_Level text_value = sa_lastEducationalQualification));
	BY Education_Level;
	IF A;
RUN;

PROC SORT Data = sasTemp.ConsolidatedDBInc; BY StudyField; RUN;
DATA sasTemp.ConsolidatedDBInc (DROP = StudyField);
        MERGE sasTemp.ConsolidatedDBInc (IN = A) lulib.Lookups_educationstream (IN = B 
	drop = study_field_grouping_id RENAME = (study_id = StudyField study_field_grouping_desc = sa_Stream study_desc = sa_Specialization));
        BY StudyField;
        IF A;
RUN;

PROC SORT Data = sasTemp.ConsolidatedDBInc; BY InstituteName; RUN;
DATA sasTemp.ConsolidatedDBInc (DROP = InstituteName);
        MERGE sasTemp.ConsolidatedDBInc (IN = A) lulib.Lookups_educationInstitute (IN = B 
	DROP = academic_school_type_id academic_school_type_desc RENAME = (Academic_School_Id = InstituteName Academic_School_desc = sa_lastInstitute));
        BY InstituteName;
        IF A;
RUN;

PROC SORT Data = sasTemp.ConsolidatedDBInc; BY CourseType; RUN;
DATA sasTemp.ConsolidatedDBInc (DROP = CourseType);
        MERGE sasTemp.ConsolidatedDBInc (IN = A) lulib.Lookups_coursetype (IN = B
		RENAME = (value = CourseType display = sa_lastCourseType));
        BY CourseType;
        IF A;
RUN;

/*PROC SORT DATA = Lulib.Lookups_Exp; BY value; RUN;*/
PROC SORT DATA = sasTemp.ConsolidatedDBInc; BY Experience; RUN;
DATA sasTemp.ConsolidatedDBInc;
	MERGE sasTemp.ConsolidatedDBInc(IN=A) Lulib.Lookups_Exp(drop = id id_nosql RENAME = (value = Experience display = TotalExperience));
	BY Experience;
	IF A;
RUN;
PROC SORT DATA = sasTemp.ConsolidatedDBInc; BY Experience1; RUN;
DATA sasTemp.ConsolidatedDBInc(drop = Experience1 RENAME = (display = Experience1));
	MERGE sasTemp.ConsolidatedDBInc(IN=A) Lulib.Lookups_Exp(drop = id id_nosql RENAME = (value = Experience1 exp = exp1));
	BY Experience1;
	IF A;
RUN;

DATA sasTemp.ConsolidatedDBInc (DROP = JobTitleCustom InstituteNameCustom CompanyNameCustom Gender Experience Experience_Month1 exp exp1);
        SET sasTemp.ConsolidatedDBInc;
	IF sa_JobTitle = '' THEN sa_JobTitle = JobTitleCustom;
        IF sa_lastInstitute = '' THEN sa_lastInstitute = InstituteNameCustom;
        IF sa_lastCompany = '' THEN sa_lastCompany = CompanyNameCustom;

	sa_JobTitle = PROPCASE(sa_JobTitle);
	sa_lastInstitute = PROPCASE(sa_lastInstitute);
	sa_lastCompany = PROPCASE(sa_lastCompany);

        IF IsResumeMidOut = 1 AND IsMidOut = 0 THEN sa_Status = 'ResumeMidOut';
        ELSE IF IsResumeMidOut = 0 AND IsMidOut = 0 THEN sa_Status = 'Activated';
        /*ELSE IF IsResumeMidOut = 1 AND IsMidOut = 1 THEN sa_Status = 'MidOut';*/
	ELSE IF IsMidOut = 1 THEN sa_Status = 'MidOut';

	IF gender = 1 THEN sa_gender = 'M';
	ELSE IF gender = 2 THEN sa_gender = 'F';
	ELSE sa_gender = 'Not Set';

	sa_experience = exp + Experience_Month / 12;
	sa_lastexperience = exp1 + Experience_Month1 / 12;

	FORMAT sa_Status $15.;
        FORMAT sa_experience pvalue4.2;
        FORMAT sa_lastexperience pvalue4.2;
RUN;

/*
<-- Applying Exception On RegistrationEndDate
*/
PROC SORT DATA = sasTemp.MinResumeUploadDate_RM; BY UserId; RUN;
PROC SORT DATA = sasTemp.ConsolidatedDBInc; BY UserId; RUN;
DATA sasTemp.ConsolidatedDBInc;
	MERGE sasTemp.ConsolidatedDBInc(IN=A) sasTemp.MinResumeUploadDate_RM(IN=B KEEP = UserId MinResumeUploadDate);
	BY UserId;
	IF A;
RUN;

DATA sasTemp.ConsolidatedDBInc(DROP = MinResumeUploadDate RegistrationEndDate);
	SET sasTemp.ConsolidatedDBInc;
	da_RegistrationEndDate = RegistrationEndDate;
	IF sa_Status = 'Activated' AND RegistrationEndDate = . THEN da_RegistrationEndDate = MinResumeUploadDate; 
	FORMAT da_RegistrationEndDate DATETIME19.;
RUN;

/*
  Applying Exception On RegistrationEndDate -->
*/

/*
<-- Getting rid of rest of the lookups
*/
DATA sasTemp.ConsolidatedDBInc (DROP = SpamStatus BounceStatus LinkedInToken IsEmailVerified IsCellPhoneVerified SMSAlertFlag EmailAlertStatus ReceiveRecruiterMails PrivacyStatus 
	RENAME = (RegistrationStartDate = da_RegistrationStartDate LastLogin = da_LastLogin 
	LastModified = da_LastModified YearsOfExpLastUpdated = da_YearsOfExpLastUpdated PreferenceUpdate = da_PreferenceUpdate LastAppliedDate = da_LastAppliedDate 
	BOUpdateDate = da_BOUpdateDate LinkedInExpiryDate = da_LinkedInExpiryDate DateOfBirth = da_DateOfBirth YearOfPassout = sa_YearOfPassout 
	UserId = sa_UserId Email = sa_Email CellPhone = sa_CellPhone FirstName = sa_FirstName LastName = sa_LastName TeamSizeManaged = sa_TeamSizeManaged
	NoticePeriod = sa_NoticePeriod ProfileTitle = sa_ProfileTitle Certifications = sa_Certifications));
        SET sasTemp.ConsolidatedDBInc;
	IF SpamStatus = 1 THEN fa_SpamStatus = 'Y'; 
	ELSE IF SpamStatus = 0 THEN fa_SpamStatus = 'N'; 
	ELSE IF SpamStatus = . THEN fa_SpamStatus = 'Not Set';

	IF BounceStatus = 1 THEN fa_BounceStatus = 'Y'; 
	ELSE IF BounceStatus = 0 THEN fa_BounceStatus = 'N'; 
	ELSE IF BounceStatus = . THEN fa_BounceStatus = 'Not Set';

	IF LinkedInToken ~= '' AND DATEPART(LinkedInExpiryDate) >= TODAY() THEN fa_LinkedInConnectivity = 'Y'; 
	ELSE fa_LinkedInConnectivity = 'N';

        IF IsEmailVerified = 'True' OR IsEmailVerified = '1' THEN fa_IsEmailVerified = 'Y'; 
	ELSE IF IsEmailVerified = 'False' OR IsEmailVerified = '0' THEN fa_IsEmailVerified = 'N'; 
	ELSE IF IsEmailVerified = '' THEN fa_IsEmailVerified = 'Not Set';

	IF IsCellPhoneVerified = 1 THEN fa_IsCellPhoneVerified = 'Y'; 
	ELSE IF IsCellPhoneVerified = 0 THEN fa_IsCellPhoneVerified = 'N'; 
	ELSE IF IsCellPhoneVerified = . THEN fa_IsCellPhoneVerified = 'Not Set';

	IF SMSAlertFlag = 1 THEN fa_SMSAlertFlag = 'Y'; 
	ELSE IF SMSAlertFlag = 0 THEN fa_SMSAlertFlag = 'N'; 
	ELSE IF SMSAlertFlag = . THEN fa_SMSAlertFlag = 'Not Set';

	IF EmailAlertStatus = 0 THEN fa_EmailAlertStatus = 'NO_JA,NO_OtherPdt';
	ELSE IF EmailAlertStatus = 1 THEN fa_EmailAlertStatus = 'Yes_JA,Yes_OtherPdt'; 
	ELSE IF EmailAlertStatus = 2 THEN fa_EmailAlertStatus = 'Yes_JA,NO_OtherPdt'; 
	ELSE IF EmailAlertStatus = 3 THEN fa_EmailAlertStatus = 'NO_JA,Yes_OtherPdt'; 
	ELSE IF EmailAlertStatus = . THEN fa_EmailAlertStatus = 'Not Set';

	IF ReceiveRecruiterMails = 1 THEN fa_ReceiveRecruiterMails = 'Y'; 
	ELSE IF ReceiveRecruiterMails = 0 THEN fa_ReceiveRecruiterMails = 'N'; 
	ELSE IF ReceiveRecruiterMails = . THEN fa_ReceiveRecruiterMails = 'Not Set';

        IF PrivacyStatus = 'True' OR PrivacyStatus = '1' THEN fa_PrivacyStatus = 'Private';
	ELSE IF PrivacyStatus = 'False'  OR PrivacyStatus = '0' THEN fa_PrivacyStatus = 'Public';
	ELSE IF PrivacyStatus = '' THEN fa_PrivacyStatus = 'Not Set';
RUN;

/*
    Getting rid of rest of the lookups -->
*/

DATA sasTemp.ConsolidatedDBInc;
	UPDATE sasTemp.ConsolidatedDBInc(IN=A) CandidateMatchInc;
	BY sa_UserId;
	IF A;
RUN;

DATA sasTemp.ConsolidatedDBInc;
	UPDATE sasTemp.ConsolidatedDBInc(IN=A) CandidateResumesInc;
	BY sa_UserId;
	IF A;
RUN;

PROC SORT DATA = sasTemp.Mob_Verify_LogsInc; BY Email; RUN;
PROC SORT DATA = sasTemp.UserIdEmail; BY Email; RUN;
DATA sasTemp.Mob_Verify_LogsInc;
	MERGE sasTemp.Mob_Verify_LogsInc(IN=A) sasTemp.UserIdEmail(RENAME = (UserId1 = sa_UserId));
	BY Email;
	IF A;
RUN;

PROC SORT DATA = sasTemp.Mob_Verify_LogsInc; BY sa_UserId; RUN;
DATA sasTemp.ConsolidatedDBInc;
	UPDATE sasTemp.ConsolidatedDBInc(IN=A) 
	sasTemp.Mob_Verify_LogsInc(KEEP = sa_UserId LastCellPhoneVerifiedDate RENAME = (LastCellPhoneVerifiedDate = da_LastCellPhoneVerifiedDate));
	BY sa_UserId;
	IF A;
RUN;

PROC SORT DATA = sasTemp.no_skillsInc; BY UserId; RUN;
DATA sasTemp.ConsolidatedDBInc;
        UPDATE sasTemp.ConsolidatedDBInc(IN=A)
	sasTemp.no_skillsInc(RENAME = (UserId = sa_UserId));
        BY sa_UserId;
        IF A;
RUN;

DATA sasTemp.ConsolidatedDBInc;
	RETAIN sa_UserId sa_Email sa_FirstName sa_LastName sa_CellPhone sa_gender 
	IsResumeMidOut IsMidOut sa_Status StartVendorId sa_StartSource EndVendorId sa_Source 
	da_DateOfBirth sa_City sa_State sa_State2 sa_Zone sa_Country fa_IsTopCity
	sa_ProfileTitle sa_NoticePeriod sa_Certifications sa_experience sa_Salary sa_SalaryMin sa_SalaryMax
	sa_JobTitle sa_lastCompany sa_Industry sa_SubFunction sa_Function sa_lastexperience  sa_TeamSizeManaged 
	sa_lastEducationalQualification sa_Stream sa_Specialization sa_lastInstitute sa_lastCourseType sa_YearOfPassout
	da_RegistrationStartDate da_RegistrationEndDate da_LastLogin da_LastModified da_LastAppliedDate da_PreferenceUpdate da_YearsOfExpLastUpdated da_BOUpdateDate da_LinkedInExpiryDate 
	fa_SpamStatus fa_BounceStatus fa_LinkedInConnectivity fa_IsEmailVerified fa_IsCellPhoneVerified fa_SMSAlertFlag fa_EmailAlertStatus fa_ReceiveRecruiterMails fa_PrivacyStatus;
	*ca_Logins ca_Modifies ca_Applies ca_JAClicks ra_ProfileViews ra_ExcelDownloads ra_CVDownloads ra_Emails ra_sms ra_note ra_star ra_folder ra_mobile ra_reported_cand ya_JASent ya_LatestMatches ya_OtherMatches ya_WVSent;
	SET sasTemp.ConsolidatedDBInc;
	IF TotalExperience = '0 Yr' AND Experience_Month = 0 THEN DO;
		IF sa_Industry = '' THEN sa_Industry = 'Fresher (No Industry)';
		IF sa_SubFunction = '' THEN DO;
                        sa_Function = 'Fresher (No Experience)';	sa_SubFunction = 'Fresher (No Experience)';
                END;
	END;
RUN;

/*	Make FlatFile1	*/
DATA FIncLib.FlatFile1(SORTEDBY = UserId);
	SET sasTemp.ConsolidatedDBInc;
	IF sa_City = 'NA' THEN DO;
		sa_City = sa_Country;
		sa_State = sa_Zone;
	END;
        IF fa_IsEmailVerified = 'Y' THEN fa_IsEmailVerified = '1';
        ELSE IF fa_IsEmailVerified = 'N' THEN fa_IsEmailVerified = '0';

        IF sa_gender = 'M' THEN Gender = 1;
        ELSE IF sa_gender = 'F' THEN Gender = 2;
        ELSE IF sa_gender = 'Not Set' THEN Gender = 0;

        IF fa_IsCellPhoneVerified = 'Y' THEN IsCellPhoneVerified = 1; 
        ELSE IF fa_IsCellPhoneVerified = 'N' THEN IsCellPhoneVerified = 0;
        ELSE IF fa_IsCellPhoneVerified = 'Not Set' THEN IsCellPhoneVerified = .; 

        IF fa_SMSAlertFlag = 'Y' THEN IsReceiveSMS = 1;
        ELSE IF fa_SMSAlertFlag = 'N' THEN IsReceiveSMS = 0;
        ELSE IF fa_SMSAlertFlag = 'Not Set' THEN IsReceiveSMS = .;

        IF fa_EmailAlertStatus = 'NO_JA,NO_OtherPdt' THEN NotificationFrequency = 0;
        ELSE IF fa_EmailAlertStatus = 'Yes_JA,Yes_OtherPdt' THEN NotificationFrequency = 1;
        ELSE IF fa_EmailAlertStatus = 'Yes_JA,NO_OtherPdt' THEN NotificationFrequency = 2;
        ELSE IF fa_EmailAlertStatus = 'NO_JA,Yes_OtherPdt' THEN NotificationFrequency = 3;
        ELSE IF fa_EmailAlertStatus = 'Not Set' THEN NotificationFrequency = .;

	KEEP sa_UserId sa_Email sa_FirstName sa_LastName sa_CellPhone Gender sa_City sa_State StartVendorId EndVendorId sa_Status 
	da_RegistrationStartDate da_RegistrationEndDate da_LastLogin da_LastModified da_LastAppliedDate da_LastCVUpdateDate da_LastCellPhoneVerifiedDate
	TotalExperience Experience_Month sa_Salary sa_JobTitle sa_SubFunction sa_Function sa_Industry Experience1 sa_lastCompany 
	sa_lastInstitute sa_lastEducationalQualification sa_Specialization sa_Stream 
	fa_IsEmailVerified IsCellPhoneVerified NotificationFrequency IsReceiveSMS No_Skills;
	RENAME sa_UserId = UserId sa_Email = Email sa_FirstName = FirstName sa_LastName = LastName sa_CellPhone = CellPhone
	sa_City = City sa_State = State sa_Status = Status sa_Salary = Salary sa_JobTitle = JobTitle1 sa_SubFunction = SubFunction1
	sa_Function = Function1 sa_Industry = Industry1 sa_lastCompany = CompanyName1 sa_lastInstitute = Institute1 
	sa_lastEducationalQualification = EducationalQualification1 sa_Specialization = Specialization1 sa_Stream = Stream1 
	fa_IsEmailVerified = IsEmailVerified da_RegistrationStartDate = RegistrationStartDate 
	da_RegistrationEndDate = RegistrationEndDate da_LastAppliedDate = LastAppliedDate da_LastLogin = LastLogin da_LastModified = LastModified
	da_LastCVUpdateDate = LastCVUpdateDate da_LastCellPhoneVerifiedDate = LastCellPhoneVerifiedDate;
RUN;

DATA FIncLib.FLATFILE(SORTEDBY = UserId);
	RETAIN UserId Email FirstName LastName CellPhone Gender City State StartVendorId EndVendorId Status 
	RegistrationDate CompletionDate LastLoginDate LastUpdatedDate LatestAppliedJobDate 
	TotalExperience Experience_Month Salary JobTitle1 SubFunction1 Function1 Industry1 Experience1 CompanyName1 
	Institute1 EducationalQualification1 Specialization1 Stream1 
	IsEmailVerified IsCellPhoneVerified NotificationFrequency IsReceiveSMS
	LastCellPhoneVerificationDate LastCVUpdationDate No_Skills;
        SET FIncLib.FLATFILE1;
	%global RegistrationDate CompletionDate LastLoginDate LastUpdatedDate LatestAppliedJobDate LastCellPhoneVerificationDate LastCVUpdationDate;
	%DateConversion1(RegistrationStartDate, RegistrationDate);
	%DateConversion1(RegistrationEndDate, CompletionDate);
	%DateConversion1(LastLogin, LastLoginDate);
	%DateConversion1(LastModified, LastUpdatedDate);
	%DateConversion1(LastAppliedDate, LatestAppliedJobDate);
	%DateConversion1(LastCellPhoneVerifiedDate, LastCellPhoneVerificationDate);
	%DateConversion1(LastCVUpdateDate, LastCVUpdationDate);
	DROP RegistrationStartDate RegistrationEndDate LastLogin LastModified LastAppliedDate LastCellPhoneVerifiedDate LastCVUpdateDate;
RUN;

/*
PROC EXPORT DATA = FIncLib.FLATFILE
	FILE = '/data/Analytics/Utils/consolidatedDB/Output/IncExtract/CandidatesInc.csv' DBMS = CSV REPLACE;
RUN;
*/
/*	Part 2		*/

DATA sastemp.UserIdEmailInc(KEEP = UserId1 Email);
        SET sasTemp.CandidateStaticInc(KEEP = UserId Email IsResumeMidOut IsMidOut);
        IF IsResumeMidOut = 0 AND IsMidOut = 0;
        RENAME UserId = UserId1;
RUN;

PROC SORT DATA = sastemp.UserIdEmailInc;BY UserId1;RUN;
PROC SORT DATA = sastemp.UserIdEmail;BY UserId1;RUN;
DATA sastemp.UserIdEmail;
	UPDATE sastemp.UserIdEmail sastemp.UserIdEmailInc;
	BY UserId1;
RUN;

PROC SORT DATA = sastemp.UserIdEmail; BY Email; RUN;
PROC SORT DATA = sasTemp.JAClickInc1; BY Email; RUN;
DATA JAClickInc2;
        MERGE sastemp.UserIdEmail(IN=A) sasTemp.JAClickInc1(IN=B);
        BY Email;
        IF B;
RUN;

DATA sasTemp.JAClickInc(DROP = UserId1);
        SET JAClickInc2;
        IF UserId = '' AND Email ~= '' THEN UserId = UserId1;
RUN;

DATA sasTemp.LoginHistoryInc(RENAME = (LastLoginDate = LastLogin));
        SET sasTemp.CandidateStaticInc;
        IF DatePart(LastLogin) = INTNX('day', today(), -1);
	*IF DatePart(LastLogin) >= INTNX('day', today(), -9) AND  DatePart(LastLogin) < INTNX('day', today(), -5);
        LastLoginDate = DatePart(LastLogin);
        FORMAT LastLoginDate Date9.;
        KEEP UserId LastLoginDate;
RUN;

DATA sasTemp.ModificationHistoryInc(RENAME = (LastModifiedDate = LastModified));
        SET sasTemp.CandidateStaticInc;
        IF DatePart(LastModified) = INTNX('day', today(), -1);
	*IF DatePart(LastModified) >= INTNX('day', today(), -9) AND  DatePart(LastModified) < INTNX('day', today(), -5);
        LastModifiedDate = DatePart(LastModified);
        FORMAT LastModifiedDate Date9.;
        KEEP UserId LastModifiedDate;
RUN;

/*      Pure    */
PROC SORT DATA = sasTemp.CandidateStaticInc; BY UserId; RUN;
DATA sasTemp.RegistrationEndDateInc(KEEP = UserId RegistrationEndDate );
        SET sasTemp.CandidateStaticInc(KEEP = UserId RegistrationEndDate IsResumeMidOut IsMidOut); 
        IF IsResumeMidOut = 0 AND IsMidOut = 0;
RUN;

DATA sasTemp.PureLoginHistoryInc(DROP = RegistrationEndDate SORTEDBY = UserId LastLogin);
        MERGE sasTemp.LoginHistoryInc(IN=A) sasTemp.RegistrationEndDateInc(IN=B);
        BY UserId;
        IF A AND B AND DATEPART(RegistrationEndDate) < LastLogin;
RUN;

DATA sasTemp.PureModificationHistoryInc(DROP = RegistrationEndDate SORTEDBY = UserId LastModified);
        MERGE sasTemp.ModificationHistoryInc(IN=A) sasTemp.RegistrationEndDateInc(IN=B);
        BY UserId;
        IF A AND B AND DATEPART(RegistrationEndDate) < LastModified;
RUN;

DATA sasTemp.CandidatesTokenInc;
        INFILE "/data/Analytics/Utils/consolidatedDB/Input/CandidatesTokenInc.csv"
        DLM=',' MISSOVER DSD LRECL=3326 FIRSTOBS=2 DSD termstr=lf;

        INFORMAT Email $100.;
        INFORMAT AutoLoginToken $200.;
  
        FORMAT Email $100.;
        FORMAT AutoLoginToken $200.;
  
	INPUT Email AutoLoginToken;
RUN;

