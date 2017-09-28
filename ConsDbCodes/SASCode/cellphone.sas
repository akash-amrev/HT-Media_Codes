LIBNAME MYLIB '/data/Shine/Shine_AdHoc/Model/SASTemp';
LIBNAME CONSDB '/data/Analytics/Utils/consolidatedDB/Model/SASTemp';

DATA MYLIB.cellphone_details;
	INFILE "/tmp/CS_CPV_IdNew.csv" DLM=',' MISSOVER DSD LRECL=3326 DSD termstr=lf;
	INFORMAT sa_UserId $50.;
	FORMAT sa_UserId $50.;
	INPUT sa_UserId $;
RUN;

DATA MYLIB.consdb_backup_V1;
	SET consdb.consolidateddb;
RUN;
/*
PROC SORT DATA = MYLIB.cellphone_details ; BY sa_UserId ; RUN;
PROC SORT DATA = MYLIB.consdb_backup_V1 ; BY sa_UserId ; RUN;

DATA MYLIB.consdb_cellphone;
	MERGE MYLIB.cellphone_details(IN = A) MYLIB.consdb_backup_V1;
	BY sa_UserId;
	IF A;
RUN;

DATA MYLIB.consdb_cellphone;
	SET MYLIB.consdb_cellphone;
	fa_IsCellPhoneVerified = 'N';
RUN;

DATA MYLIB.consdb_cellphone_1;
	MERGE MYLIB.cellphone_details(IN = A) MYLIB.consdb_backup_V1;
	BY sa_UserId;
	IF NOT A;
RUN;

PROC APPEND BASE = MYLIB.consdb_cellphone_1 DATA = MYLIB.consdb_cellphone; RUN;
*/



	
	