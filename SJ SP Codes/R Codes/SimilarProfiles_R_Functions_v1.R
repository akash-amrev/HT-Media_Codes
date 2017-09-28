##################################			                            ################################
##################################   SimilarProfiles R Functions file   ################################
##################################			                            ################################

tim = function(){as.character(Sys.time())}

logger =function(...,multi_line=FALSE){
	if(multi_line){
		print(sprintf("%s:%s",tim(),paste(...,sep=" ")))
	} else {	
		cat(tim(),":",...,"\n")
	}
}

## function to trim white spaces in the start and end
trim = function(z) gsub("^\\s+|\\s+$","", z)
## function to combine spaces
combBL = function(z) gsub("[ \t\n\r]+"," ",z)

change_mysql_settings = function(mysql_variable_list){
	logger("Starting change_mysql_settings")
	for(i in 1:length(mysql_variable_list)){
		dump = executeQuery(sprintf("set global %s = %s ",names(mysql_variable_list)[i],mysql_variable_list[[i]]))
		logger(sprintf("Changed %s to %s",names(mysql_variable_list)[i],mysql_variable_list[[i]]))
	}
	logger("Dropping Linux cache")
	dump = system("echo 1 > /proc/sys/vm/drop_caches")
	logger("change_mysql_settings function : Completed")
	return(TRUE)
}

get_from_to_run_dates = function(run_log_table,from_path = raw_input_dir,suffix = "dailyModifies_",date_format = "%d-%m-%Y"){
	logger("get_from_to_run_dates function : Started")
	last_run = executeQuery(sprintf("select * from %s where RunCompleted = 1 order by DataTakenTillDate desc limit 1",run_log_table))
	from_date = as.Date(last_run[1,"DataTakenTillDate"]) + 1
	all_files = list.files(raw_input_dir)
	prof_files = grep(sprintf("%s\\d+.*",suffix),all_files,value=T)
	prof_files_dates = as.Date(gsub(sprintf("%s|[.]csv",suffix),"",prof_files),format = date_format)
	after_from_dates = sort(prof_files_dates[which(prof_files_dates>=from_date)])
	if(length(after_from_dates)==0){
		logger("Data not generated since last run !!!")
		stop(simpleError("Data not generated consistently"))
	}
	if(all(as.numeric(diff(after_from_dates)))!=1){
		logger("Inconsistency in the data !!!")
		stop(simpleError("Data not generated consistently"))
	}
	to_date = max(after_from_dates)
	return_list = list("from_date" = from_date,"to_date" = to_date)
	logger("get_from_to_run_dates function : Completed")
	return(return_list)
}

fetch_update_data = function(from_path = raw_input_dir, from_date ,to_date,suffix = "dailyModifies_",date_format = "%d-%m-%Y"){
	logger("fetch_update_data function : Started")
	logger("from_date :",as.character(from_date))
	logger("to_date :",as.character(to_date))
	all_dates = format(as.Date(as.Date(from_date,format = date_format):as.Date(to_date,format = date_format),origin="1970-1-1"),"%d-%m-%Y")
	profile_update_files = paste(suffix,all_dates,".csv",sep="")
	skills_update_files = paste(suffix,"skills_",all_dates,".csv",sep="")
	prof_data = data.frame()
	for(update_file in profile_update_files){
		tmp_data = read.csv(file=sprintf("%s%s",from_path,update_file),as.is=T)
		if(nrow(prof_data)==0){
			prof_data = tmp_data
		} else {
			prof_data = rbind(prof_data,tmp_data)
		}
	}
	prof_skill_data = data.frame()
	for(skill_update_file in skills_update_files){
		tmp_data = read.csv(file=sprintf("%s%s",from_path,skill_update_file),as.is=T)
		if(nrow(prof_skill_data)==0){
			prof_skill_data = tmp_data
		} else {
			prof_skill_data = rbind(prof_skill_data,tmp_data)
		}
	}
	logger("fetch_update_data function : Completed")
	return(list(prof_data = prof_data,prof_skill_data = prof_skill_data))
}

get_latest_update_file = function(path = raw_input_dir,suffix = "NewProfiles_",date_format = "%d-%m-%Y"){
	all_files = list.files(path = path)
	all_files = grep(suffix,all_files,value=T)
	date_files = gsub(sprintf("%s|[.]csv|[.]txt|[.]tsv",suffix),"",all_files)
	date_files = as.Date(date_files,format = date_format)
	return(all_files[which.max(date_files)])
}

dataPrep = function(data = update_data){
	logger("dataPrep function : Started")
	logger("Unique users in update_data :", length(unique(data$UserId)))
	data = data[!duplicated(data$UserId),]
        data = data[which(data$UserId!=""),]
        rem_rows = which(is.na(as.numeric(data$UserId)) & nchar(data$UserId) !=24)
	if(length(rem_rows)>0){
		data = data[-rem_rows,]
	}
	data[is.na(data$Experience_years),"Experience_years"] = 0
	data[is.na(data$Exp_months),"Exp_months"] = 0
	data$Total_exp = as.numeric(trim(gsub("Yrs|Yr|<|>","",data$Experience_years))) + as.numeric(data$Exp_months)/12
	data[is.na(data$Total_exp),"Total_exp"] = -1 
	data$Salary = as.numeric(data$Salary_Lakhs) + as.numeric(data$Salary_Thousands)/100
	data[is.na(data$Salary),"Salary"] = as.numeric(data[is.na(data$Salary),"AvgSal"])
	data[is.na(data$Salary),"Salary"] = -1
	data$Salary = ifelse(data$Salary_Lakhs==-1 && data$Salary_Thousands ==-1,-1,data$Salary)
	data$Edu = sprintf("%s|%s|%s|%s",data$EducationLevel,data$CourseType,data$subject,data$specialization)
	data[which(!(data$CourseType %in% valid_CourseType & data$specialization %in% valid_Specialization & data$subject %in% valid_Stream)),"Edu"] = -1
	data$Last_Modified = strptime(data$Last_Modified,format = "%Y-%m-%d %H:%M:%S",  tz = "")
	job_title_rows = which(data$JobTitle %in% c("","None") | is.na(data$JobTitle))
	if(length(job_title_rows)>0){
		data[job_title_rows,"JobTitle"] = NA
	}
	data[is.na(data$JobTitle),"JobTitle"] = -1
	data$JobTitle = tolower(data$JobTitle)
	data = data[,c("UserId","JobTitle","FA","Industry","SubFA","Total_exp","Salary","Edu","Last_Modified")]
	logger("Rows in update_data :" , nrow(data))
	logger("dataPrep function : Completed")
	return(data)
}

get_lookups = function(tables){
	logger("get_lookups function : Started")
	ans = lapply(tables,FUN = function(z){executeQuery(sprintf("select * from %s.%s",mysql_dbname,z))})
	logger("get_lookups function : Completed")
	return(ans)
}

short_hands_correction = function(jts){
	for(i in names(JT_short_hands)){
		jts = gsub(paste("(\\s+|[.]+|[-]+|[/]+|[\\]+|^)",paste(JT_short_hands[[i]],collapse="(\\s+|[.]+|[-]+|[/]+|[\\]+|$)|(\\s+|[.]+|[-]+|[/]+|[\\]+|^)"),"(\\s+|[.]+|[-]+|[/]+|[\\]+|$)",sep=""),paste(" ",i," ",sep=""),jts)
	}
	jts = trim(combBL(jts))
	return(jts)
}

correct_jts = function(jts,correction_df,split_vec = c("-",";","/"),remove_brackets = TRUE){
	if(remove_brackets){
		jts = trim(gsub("\\(([^)]+)\\)","",jts))
		jts = gsub("\\[([^]]+)\\]|\\[.+$","",jts,perl=T)
		jts = gsub("<([^>]+)>","",jts)
	}
	jts = tolower(combBL(trim(jts)))
	correction_df[,1] = tolower(combBL(trim(correction_df[,1])));correction_df[,2] = tolower(combBL(trim(correction_df[,2])))
	# to handle dots and spl characters in regex
	correction_df[,1] = gsub("[[:punct:]]","",correction_df[,1],perl=T)
	correction_df = correction_df[!duplicated(correction_df),] 
	correction_df = correction_df[which(correction_df[,1]!=correction_df[,2]),] 
	jts = trim(combBL(gsub("[[:punct:]]"," ",jts,perl=T)))
	affected_jts = c()
	i_min = 1;i_max = ifelse((length(correction_df[,1])>=2000),2000,length(correction_df[,1]))
	repeat{
		affected_jts = grep(paste("\\b",paste(correction_df[i_min:i_max,1],collapse="\\b|\\b"),"\\b",sep=""),jts)
		if(length(affected_jts)>0){
			for(i in i_min:i_max){
				jts[affected_jts] = gsub(paste("\\b",correction_df[i,1],"\\b",sep=""),paste(" ",correction_df[i,2]," ",sep=""),jts[affected_jts])
				#if(i%%500 ==0) cat(i,"|")
			}
		}
		if(i_max>=length(correction_df[,1])){
			break
		} else {
			i_min = i_max+1
			i_max = ifelse((i_max+2001)>=length(correction_df[,1]),length(correction_df[,1]),(i_max+2001))
			affected_jts = c()
		}
	}
	jts = trim(combBL(jts))
	return(jts)
}

cleanJT = function(jts){
	logger("cleanJT function : Started")
	jt_correction_df = executeQuery(sprintf("select * from %s",Corrected_JT_table))
	jts_corrected = jt_correction_df[match(jts,jt_correction_df$JobTitle),"Corrected_JT"]
	new_jts = jts[which(is.na(jts_corrected))]
	if(length(new_jts)>0){
		jts_df = read.csv(file=JT_corrected_words,as.is=T)
		new_jts_corrected = correct_jts(jts = new_jts,correction_df = jts_df)
		new_jts_corrected = short_hands_correction(new_jts_corrected)
		jts_corrected[which(is.na(jts_corrected))] = jt_correction_df[match(new_jts_corrected,jt_correction_df$JobTitle),"Corrected_JT"]
		still_na_jts = jts[which(is.na(jts_corrected))]
		still_na_jts = short_hands_correction(still_na_jts)
		if(length(still_na_jts)){
			source(spell_corrector)
			most_used_terms = table(read.csv(most_used_terms_file,as.is=T)[,1]) #333
			correction_df = data.frame(word = character(0),corrected_word = character(0))
			jt_corrected2 = combBL(trim(sapply(still_na_jts,FUN = function(z) CorrectDocument(z, dtm = most_used_terms,split_vec = c(" ",";","-","(",")","/"),min_nchar = 6)))) #3-5 mins
			jts_corrected[which(is.na(jts_corrected))] = jt_corrected2
		}
	}
	logger("cleanJT function : Completed")
	return(jts_corrected)
}

split_skills_func = function(skills_data,userid_col="userid",skill_col="skill_col",separators = c(";"),extra_col = "Skill"){
		split_skills = strsplit(skills_data[,skill_col],paste(separators,collapse="|"))
		skill_length_vec = sapply(split_skills, length)
		new_userid_vec = rep(skills_data[,userid_col],skill_length_vec )
		extra_col_vec = rep(skills_data[,extra_col], skill_length_vec)
		new_skills_vec = unlist(split_skills)
		return(data.frame(userid_col = new_userid_vec,skill_col = new_skills_vec,extra_col =extra_col_vec ))
}

adhoc_correction = function(to_be_corrected,adhoc_correction_file,bycol=NULL){
	to_be_corrected = tolower(combBL(trim(to_be_corrected)))
	adhoc_correction_list = read.csv(file=adhoc_correction_file,as.is=T); adhoc_correction_list[,1] = tolower(combBL(trim(adhoc_correction_list[,1])));adhoc_correction_list[,2] = tolower(combBL(trim(adhoc_correction_list[,2])))
	to_be_corrected[which(to_be_corrected %in% adhoc_correction_list[,1])]  = adhoc_correction_list[na.omit(match(to_be_corrected,adhoc_correction_list[,1])),2]
	return(to_be_corrected)
}
	
cleanSkills = function(skill_tmp){
	logger("cleanSkills function : Started")
	colnames(skill_tmp) = c("CandidateID","skill_final")
	skill_col = "skill_col"
	initial_skill_col = "skill_final"
	userid_col = "CandidateID"
	skill_col2 = "skill_col_wo_version"
	skill_tmp[,skill_col] = combBL(tolower(trim(iconv(skill_tmp[,initial_skill_col],"UTF-8", "ASCII","byte"))))
	skill_tmp[,skill_col] = gsub("\\(([^)]+)\\)|\\(.+$","",skill_tmp[,skill_col],perl=T) ## removing everything between brackets () or []
	skill_tmp[,skill_col] = trim(skill_tmp[,skill_col])
	new_skills_data = split_skills_func(skill_tmp,userid_col=userid_col,skill_col=skill_col,separators = c(";","\t"),extra_col = "skill_final")
	colnames(new_skills_data) = c(userid_col,skill_col,initial_skill_col)
	rem_rows = which(is.na(new_skills_data[,skill_col])|new_skills_data[,skill_col]%in%c("","na","n/a","no","nill","all","no","nil","abc","xyz","xxx","none","nothing","n.a.","n.a","aaa","not applicable","test","xx","non","any","abcd","n","aa","a"," ","x"))
	if(length(rem_rows)>0){
		new_skills_data = new_skills_data[-rem_rows,]
	}
	new_skills_data[,skill_col] = gsub("\\(([^)]+)\\)|\\(.+$","",new_skills_data[,skill_col],perl=T) 
	new_skills_data[,skill_col2] = combBL(trim(gsub("([.-]\\s*|\\s+)([a-z]\\d+|\\d+).*","",new_skills_data[,skill_col]))) ## correcting for version numbers and years
	null_rows = grep("^[[:punct:]]*\\d+[[:punct:]]*$|^[[:punct:]]*$",new_skills_data[,skill_col2]) #[1] 116089 ## containing only digits or punctuations or there combination or just ""
	if(length(null_rows)>0){
		new_skills_data[null_rows,skill_col2] = NA
	}
	new_skills_data[,skill_col2] = combBL(trim(gsub("[-]"," ",new_skills_data[,skill_col2]))) #hypen to space
	new_skills_data[,skill_col2] = combBL(trim(gsub("[.]+$","",new_skills_data[,skill_col2]))) #remove the trailing dot (.)
	new_skills_data[,skill_col2] = trim(gsub("&amp$","",new_skills_data[,skill_col2])) #replacing the &amp with ''
	new_skills_data[which(new_skills_data[,skill_col2]==""),skill_col2] = NA
	new_skills_data[,skill_col2] = adhoc_correction(to_be_corrected = new_skills_data[,skill_col2],adhoc_correction_file = adhoc_skill_correction_file)
	skill_data = new_skills_data[,c("CandidateID","skill_col_wo_version")]
	colnames(skill_data) = c("CandidateID","skill_final")
	skill_data = skill_data[!duplicated(skill_data),]
	skill_data = na.omit(skill_data)
	logger("cleanSkills function : Completed")
	return(skill_data)
}

applyLookups = function(data,lookup_list,na_vec,today_date){
	logger("applyLookups function : Started")
	data[which((data$Industry %in% na_vec) | is.na(data$Industry)),"Industry"] = missing_replacement
	data[which((data$SubFA %in% na_vec) | is.na(data$SubFA)),"SubFA"] = missing_replacement
	data$ind_lookup = lookup_list[["ind"]][match(data$Industry,lookup_list[["ind"]]$Value),"Lookup"]
	data$subfa_lookup = lookup_list[["subfa"]][match(data$SubFA,lookup_list[["subfa"]]$Value),"Lookup"]
	data$insti_lookup = lookup_list[["edu"]][match(data$Edu,lookup_list[["edu"]]$Value),"Lookup"]
	data$jt_lookup = lookup_list[["jt"]][match(data$JobTitle,lookup_list[["jt"]]$Value),"Lookup"]
	data$fa_lookup = lookup_list[["fa"]][match(data$FA,lookup_list[["fa"]]$Value),"Lookup"]
	data[,c("ind_lookup","subfa_lookup","insti_lookup","jt_lookup","fa_lookup")] = sapply(data[,c("ind_lookup","subfa_lookup","insti_lookup","jt_lookup","fa_lookup")],FUN=function(z){z[is.na(z)] = -1;return(z)})
	data = data[!duplicated(data$UserId),]
	data$user_lookup = lookup_list[["userid"]][match(data$UserId,lookup_list[["userid"]]$Value),"Lookup"]
	# The new users
	new_users = data[which(is.na(data$user_lookup)),c("UserId")]
	if(length(new_users)>0){
		new_users_lookup = data.frame(Value=new_users,Lookup = (max(lookup_list[["userid"]]$Lookup)+1):(max(lookup_list[["userid"]]$Lookup+length(new_users))))
		data[which(is.na(data$user_lookup)),"user_lookup"] = new_users_lookup[match(data[which(is.na(data$user_lookup)),"UserId"],new_users_lookup$Value),"Lookup"]
		new_users_lookup$FA_Lookup = data[match(new_users_lookup$Value,data$UserId),"fa_lookup"]
		write.csv(new_users_lookup,file=sprintf("%s/new_users_lookup_%s.csv",lookup_folder,today_date),row.names=F,quote=F)
		new_users_count <<- nrow(new_users_lookup)
		logger(sprintf("Inserting New users into Lookup table : %s",nrow(new_users_lookup)))
		dump = executeQuery(sprintf("drop table if exists %s",tmp_table))
		dbDisconnect(con);con=dbConnect('MySQL',username=mysql_username,password=mysql_password,host=mysql_host, dbname=mysql_dbname)
		dump = dbWriteTable(con, name=tmp_table, value=new_users_lookup, field.types=list(Value="varchar(100)",Lookup="int(11)",FA_Lookup="int(5)"),row.names=FALSE)
		backup_table = paste(lookup_tables["userid"],"backup",format(today_date,"%Y%m%d"),sep="_")
		dump = executeQuery(sprintf("drop table if exists %s",backup_table))
		dump = executeQuery(sprintf("create table %s as  select * from %s;",backup_table,lookup_tables["userid"]))
		logger("Backup taken for User Lookup table : ",backup_table)
		dump = executeQuery(sprintf("insert into %s select * from %s",lookup_tables["userid"],tmp_table))
	}
	logger("applyLookups function : Completed")
	return(data)
}

get_login_data = function(login_csv){
	logger("get_login_data function : Started")
	dump = system(sprintf("/usr/bin/python %s",python_login_data_file),intern=T)
	logger(dump,multi_line=T)
	data_login = read.csv(file=login_csv,as.is=T,header=T)
	colnames(data_login) = c("X_id","ll")
	data_login$ll = strptime(data_login$ll,format="%Y-%m-%d %H:%M:%OS")
	logger("get_login_data function : Completed")
	return(data_login)
}

create_exp_sal_bins = function(data){
	logger("create_exp_sal_bins function : Started")
	exp_check_df = executeQuery(sprintf("select * from %s.%s",mysql_dbname,"Exp_Check"))
	sal_check_df = executeQuery(sprintf("select * from %s.%s",mysql_dbname,"Sal_Check"))
	data$exp_bin = cut(data[,"Total_exp"],breaks = c(exp_check_df$exp_start,Inf),labels = exp_check_df$exp_bin)
	data$exp_bin = paste("_",data$exp_bin,"_",sep="")
	data$sal_bin = cut(data[,"Salary"],breaks = c(sal_check_df$sal_start,Inf),labels = sal_check_df$sal_bin)
	data$sal_bin = paste("_",data$sal_bin,"_",sep="")
	data$exp_bin_compare = "_"
	data$sal_bin_compare = "_"
	for(i in 1:nrow(exp_check_df)){
		data$exp_bin_compare = ifelse(data[,"Total_exp"]>=exp_check_df[i,"range_start"] & data[,"Total_exp"]<exp_check_df[i,"range_end"],paste(data$exp_bin_compare,exp_check_df[i,"exp_bin"],"_",sep=""),data$exp_bin_compare)
	}
	for(i in 1:nrow(sal_check_df)){
		data$sal_bin_compare = ifelse(data[,"Salary"]>=sal_check_df[i,"range_start"] & data[,"Salary"]<sal_check_df[i,"range_end"],paste(data$sal_bin_compare,sal_check_df[i,"sal_bin"],"_",sep=""),data$sal_bin_compare)
	}
	logger("create_exp_sal_bins function : Completed")
	return(data)
}

testConnection = function(){
	tryCatch({
			test = dbListTables(con)
			return(TRUE)},error= function(ex){
			if(ex$message %in% c("RS-DBI driver: (could not run statement: MySQL server has gone away)","expired MySQLConnection")){
				dbDisconnect(con)
				con<<-dbConnect('MySQL',username=mysql_username,password=mysql_password,host=mysql_host, dbname=mysql_dbname)
				logger("refreshed connection!")
				return(TRUE)
			}else{
				logger(ex$message)
				logger("connection could not be refreshed !")
				return(FALSE)
				}
			}
		)
}

#This function will stop the code if the query does not get executed...done intentionally as all the queries were sequential.To change make "stop" to "return"
executeQuery = function(query,isInsert=FALSE,attempt=1){
	tryCatch({
		if(attempt>max_query_reattempts){logger("max reattempts reached ... giving up");stop(simpleError("max reattempts reached"))}
		if(testConnection()){
			result = dbGetQuery(con,query)
			return(result)
		} else {
			logger("could not execute query...check connection!")
			stop(simpleError("could not execute query...check connection!"))
		}},error = function(ex){
			if(isInsert==FALSE){
				if(ex$message %in% c("RS-DBI driver: (could not run statement: Lost connection to MySQL server during query)","RS-DBI driver: (could not run statement: MySQL server has gone away)")){
					logger("error:",ex$message)
					attempt = attempt+1
					logger("dont worry...trying again!")
					result = executeQuery(query,isInsert,attempt)
					return(result)
				} else {
					logger("error:",ex$message)
					stop(ex)
				}
			} else {
				logger("its an insert query.. stopping")
				logger("error:",ex$message)
				stop(simpleError("insertError"))
			}
		}
	)
}

create_skill_vector = function(skill_data,lookup_list,knn_data,today_date){
	logger("create_skill_vector function : Started")
	load(file=skills_dist_mat_file) #cos_dist_sparse
	load(file=skill_table_file) #skill_table
	total_allowed_skills = names(skill_table)
	skill_data = skill_data[which(skill_data$skill_final %in% total_allowed_skills),]
	load(file=skill_fa_wide_file) ##skill_fa_wide
	fa_lookup = lookup_list[["fa"]]
	user_lookup = lookup_list[["userid"]]
	fa_vec = setdiff(sort(unique(knn_data$fa_lookup)),c(0,-1))
	dataR_file = sprintf("%s_%s.csv",outputR_file,today_date)
	top_skills = 1000
	max_users  = 100000
	for(fa_test in fa_vec){
		lookup_fa = fa_test
		fa_test = fa_lookup[match(fa_test,fa_lookup[,"Lookup"]),"Value"]
		skill_fa_specific = skill_fa_wide[,c("skill_final",paste("V1.",fa_test,"_percent",sep=""), paste("V1.",fa_test,sep=""), "skill_sum")]
		#skill_fa_specific$count_score = 10*tanh(skill_fa_specific$skill_sum/2000)
		skill_fa_specific$count_score = 10*tanh(skill_fa_specific$skill_sum/250)
		skill_fa_specific$rel_imp = skill_fa_specific[,paste("V1.",fa_test,"_percent",sep="")] * skill_fa_specific$count_score
		skill_fa_specific = skill_fa_specific[order(skill_fa_specific$rel_imp,decreasing=T)[1:min(top_skills,nrow(skill_fa_specific))],]
		skill_fa_specific$skill_final_lookup = 1:nrow(skill_fa_specific)
		fa_users_lookup = knn_data[which(knn_data$fa_lookup==lookup_fa),"user_lookup"]
		fa_users = user_lookup[match(fa_users_lookup,user_lookup$Lookup),"Value"]
		i_min = 1
		if(length(fa_users)<=max_users){
			i_max  = length(fa_users)
		} else {
			i_max = max_users
		}
		repeat{
			skill_data_specific = skill_data[which(skill_data$CandidateID %in% fa_users[i_min:i_max] & skill_data$skill_final %in% skill_fa_specific$skill_final),c("CandidateID","skill_final")]
			skill_data_specific = skill_data_specific[!duplicated(skill_data_specific),]
			skill_data_specific$skill_final_lookup = skill_fa_specific[match(skill_data_specific$skill_final,skill_fa_specific[,"skill_final"]),"skill_final_lookup"]
			skill_data_specific$user_lookup = match(skill_data_specific$CandidateID,fa_users[i_min:i_max])
			skill_data_specific$score = 1
			skill_data_specific = na.omit(skill_data_specific)
			Y <- sparseMatrix(skill_data_specific$skill_final_lookup,skill_data_specific$user_lookup, x=skill_data_specific$score, dims = c(top_skills,length(fa_users[i_min:i_max])))
			fa_dist_sparse = cos_dist_sparse[match(skill_fa_specific$skill_final,names(skill_table)),match(skill_fa_specific$skill_final,names(skill_table))]
			Z = Y*skill_fa_specific[,"rel_imp"]
			Z= t(Z)
			Z_rowsum = rowSums(Z)
			Z_rowsum[which(Z_rowsum==0)] = 1
			Z = Z/Z_rowsum
			K = Z %*% fa_dist_sparse
			K@x  = round(K@x,3)
			tmp_user_lookup = user_lookup[match(fa_users[i_min:i_max],user_lookup$Value),"Lookup"]
			#tmp_user_lookup = user_lookup[match(fa_users,user_lookup$Value),"Lookup"]
			fa_knn_data = knn_data[match(tmp_user_lookup,knn_data[,1]),]
			K_mat = as.matrix(K)
			Y = as.matrix(summary(t(Y)))
			K_mat[Y[,1:2]] =1
			skill_mat = apply(K_mat,1,FUN = function(z)paste(z,collapse=","))
			K_mat = cbind(fa_knn_data,skill_mat);rm(K,Y,Z,fa_knn_data,tmp_user_lookup,skill_mat);gc()	
			write.table(K_mat,file=dataR_file,append=T,sep="|",row.names=F,col.names=F,quote=F)			
			if(i_max==length(fa_users)){
				logger("Done:" ,fa_test,": length :",i_max)
				break
			} else {
				i_min = i_max + 1
				i_max = min(i_max+max_users,length(fa_users))
			}
		}
	}
	logger("new_dataR_file : ",dataR_file)
	logger("create_skill_vector function : Completed")
	return(dataR_file)
}

update_user_profile_table = function(new_user_lookup,dataR_file,outputS_file,full_outputR_file,data_login,all_user_lookup,today_date){
	require(foreach)
	require(doMC)
	#registerDoMC(cores=2)
	registerDoMC(cores=1) # Not doing it in parallel
	logger("update_user_profile_table function : Started")
	outputS_file_new = sprintf("%s_%s.csv",outputS_file,today_date)
	full_outputR_file_new = sprintf("%s_%s.csv",full_outputR_file,today_date)
	users_in_table = executeQuery(sprintf("select Lookup from %s",user_profile_table))
	users_in_table_12m = executeQuery(sprintf("select Lookup from %s",user_profile_table_12m))
	min_login_date_6m =  today_date - 180; min_login_date_12m =  today_date - 365
	users_in_table$login_date = data_login[match(all_user_lookup[match(users_in_table$Lookup,all_user_lookup$Lookup),"Value"],data_login$X_id),"ll"]
	to_delete_users_6m = setdiff(users_in_table[which(as.Date(users_in_table$login_date)<min_login_date_6m),"Lookup"],new_user_lookup)
	users_in_table_12m$login_date = data_login[match(all_user_lookup[match(users_in_table_12m$Lookup,all_user_lookup$Lookup),"Value"],data_login$X_id),"ll"]
	to_delete_users_12m = setdiff(users_in_table_12m[which(as.Date(users_in_table_12m$login_date)<min_login_date_12m),"Lookup"],new_user_lookup)
	table_details = list()
	table_details[["S"]] = list("user_profile_table" = user_profile_table,"to_delete_users" = to_delete_users_6m,"output_file_new" = outputS_file_new,"data_S3_location" = dataS_S3_location,"tmp_table" = paste(tmp_table,"_S",sep=""))
	table_details[["R"]] = list("user_profile_table" = user_profile_table_12m,"to_delete_users" = to_delete_users_12m,"output_file_new" = full_outputR_file_new,"data_S3_location" = full_dataR_S3_location,"tmp_table" = paste(tmp_table,"_R",sep=""))
	foreach(i = c("R","S"),.verbose=FALSE,.combine=c)%dopar%{
		tmp_details = table_details[[i]]
		logger("Starting to alter the user profile table : ",tmp_details[["user_profile_table"]])
		dump = executeQuery(sprintf("drop table if exists %s",tmp_details[["tmp_table"]]))
		dump = executeQuery(sprintf("create table %s (Lookup int,fa tinyint,exp_bin varchar(10),exp_bin_compare varchar(20),sal_bin varchar(10),sal_bin_compare varchar(20),Total_exp float,Salary float,ind_lookup mediumint,subfa_lookup mediumint, insti_lookup mediumint,jt_lookup mediumint,skills_vec mediumtext) ENGINE=MyISAM;",tmp_details[["tmp_table"]]))
		dump = executeQuery(sprintf("LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';",dataR_file,tmp_details[["tmp_table"]]))
		dump = executeQuery(sprintf("alter table %s add index Lookup(Lookup)",tmp_details[["tmp_table"]]))
		dump = executeQuery("START TRANSACTION;")
		dump = executeQuery(sprintf("LOCK TABLES %s WRITE,%s  WRITE;",tmp_details[["user_profile_table"]],tmp_details[["tmp_table"]]))
		dump = executeQuery(sprintf("ALTER TABLE %s DISABLE KEYS;",tmp_details[["user_profile_table"]]))
		dataR_user_lookup = executeQuery(sprintf("select Lookup from %s",tmp_details[["tmp_table"]]))[,1]
		dump = executeQuery(sprintf("delete from %s where Lookup in (%s)",tmp_details[["user_profile_table"]],paste(unique(c(dataR_user_lookup,tmp_details[["to_delete_users"]])),collapse=",")))
		dump = executeQuery(sprintf("insert into %s select * from %s",tmp_details[["user_profile_table"]],tmp_details[["tmp_table"]]),isInsert=TRUE)
		dump = executeQuery(sprintf("ALTER TABLE %s ENABLE KEYS;",tmp_details[["user_profile_table"]]))
		dump = executeQuery("UNLOCK TABLES;")
		dump = executeQuery("COMMIT;")
		dump = executeQuery(sprintf("drop table %s",tmp_details[["tmp_table"]]))
		logger(sprintf("Completed for table %s",tmp_details[["user_profile_table"]]))
		#logger("output_file_new : ",tmp_details[["output_file_new"]])
		#dump = system(sprintf("mysql -u %s -p'%s' -e 'select * from %s.%s' | sed 's/\t/,/g' | tail -n +2 > %s",mysql_username,mysql_password,mysql_dbname,tmp_details[["user_profile_table"]],tmp_details[["output_file_new"]]))
		dump = export_table_in_parts(n = num_parts_in_export_table,output_file_new = tmp_details[["output_file_new"]] ,user_profile_table = tmp_details[["user_profile_table"]])
		logger("Uploading data to S3 : ",tmp_details[["data_S3_location"]])
		start_time_upload = Sys.time()
		dump = system(sprintf("/usr/local/bin/s3cmd put %s %s --multipart-chunk-size-mb=200 ",tmp_details[["output_file_new"]],tmp_details[["data_S3_location"]]))
		time_taken_S3_upload <- as.numeric(difftime(Sys.time(),start_time_upload,units="mins"))
		logger(sprintf("Time taken to upload file : %s : %s mins",tmp_details[["output_file_new"]],time_taken_S3_upload))
		i
	}
	logger("Uploading data to S3 : ",dataR_file)
	dump = system(sprintf("/usr/local/bin/s3cmd put %s %s --multipart-chunk-size-mb=200 ",dataR_file,dataR_S3_location))
	dataR_file = paste(dataR_S3_location,strsplit(dataR_file,"/")[[1]][length(strsplit(dataR_file,"/")[[1]])],sep="")
	dataS_file = paste(table_details[["S"]][["data_S3_location"]],strsplit(table_details[["S"]][["output_file_new"]],"/")[[1]][length(strsplit(table_details[["S"]][["output_file_new"]],"/")[[1]])],sep="")
	full_dataR_file = paste(table_details[["R"]][["data_S3_location"]],strsplit(table_details[["R"]][["output_file_new"]],"/")[[1]][length(strsplit(table_details[["R"]][["output_file_new"]],"/")[[1]])],sep="")
	logger("update_user_profile_table function : Completed")
	return(list(dataR = dataR_file ,dataS = dataS_file,full_dataR = full_dataR_file))
}

export_table_in_parts = function(n = 5,output_file_new,user_profile_table){
	logger("export_table_in_parts function : Started")
	total_count = executeQuery(sprintf("select count(*) from %s",user_profile_table))[1,1]
	split_row_range = lapply(split(1:total_count,ceiling(seq_along(1:total_count)/(length(1:total_count)/n))),range)
	split_row_range = lapply(split_row_range,FUN= function(z){z[1] = z[1]-1;z[2] = z[2]-z[1];z}) # for the limit query
	final_file_name = gsub("[.]csv","",tail(strsplit(output_file_new,"/")[[1]],1))
	export_part_file_vec = c()
	for(i in 1:length(split_row_range)){
		export_part_file_vec[i] = sprintf("%s/%s_%s.csv",export_table_temp,final_file_name,i)
		dump = system(sprintf("rm -f %s",export_part_file_vec[i])) #remove if exists
		dump = system(sprintf("mysql -u %s -p'%s' -e 'select * from %s.%s limit %s,%s' | sed 's/\t/,/g' | tail -n +2 > %s",mysql_username,mysql_password,mysql_dbname,user_profile_table,split_row_range[[i]][1],split_row_range[[i]][2],export_part_file_vec[i]))
	}
	dump = system(sprintf("cat %s > %s",paste(export_part_file_vec,collapse=" "),output_file_new))
	dump = system(sprintf("rm %s",paste(export_part_file_vec,collapse=" ")))
	logger("export_table_in_parts function : Completed")
	return(TRUE)
}

run_EMR_cluster = function(dataR_file,dataS_file,full_dataR,final_S3_destination_folder,today_date){
	logger("run_EMR_cluster function : Started")
	inner_dataR_file = gsub("outer","inner",dataR_file)
	dump = system(sprintf("/usr/local/bin/s3cmd cp %s %s",dataR_file,inner_dataR_file))	
	dataR_file = gsub("^s3:","s3n:",dataR_file)
	dataS_file = gsub("^s3:","s3n:",dataS_file)
	inner_dataR_file = gsub("^s3:","s3n:",inner_dataR_file)	
	jobid_output = system(sprintf("/root/elastic-mapreduce-cli/elastic-mapreduce --create --alive --visible-to-all-users --name Analytics-KnnRun_%s --ami-version 2.4.2 --hadoop-version 1.0.3 --instance-group master --instance-type %s --instance-count 1 --instance-group core --instance-type %s --instance-count %s --subnet subnet-bbc0e6d2 --bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-hadoop --args '-s,io.file.buffer.size=65536,-m,mapred.tasktracker.reduce.tasks.maximum=%s,-m,mapred.task.timeout=%s,-m,mapred.tasktracker.map.tasks.maximum=%s,-h,dfs.block.size=%s' --bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-daemons --args  --namenode-opts=-XX:GCTimeRatio=19",today_date,master_instance,core_instance,core_nodes_number,reducers_per_node,task_timeout,mappers_per_node,dfs_block_size),intern=T)
	job_id = gsub(".*(j-)([^ ]*)","\\1\\2",jobid_output)
	logger("EMR_cluster job_id :",job_id)
	dump = system(sprintf("/root/elastic-mapreduce-cli/elastic-mapreduce -j %s --wait-for-steps",job_id))
	dump = system(sprintf("/root/elastic-mapreduce-cli/elastic-mapreduce -j %s --jar s3://elasticmapreduce/libs/script-runner/script-runner.jar --arg s3://elasticmapreduce/libs/state-pusher/0.1/fetch --step-name Setup-hadoop-debugging --step-action TERMINATE_JOB_FLOW",job_id))	
	#dump = system(sprintf("/root/elastic-mapreduce-cli/elastic-mapreduce -j %s --jar s3://analytics.shine.com/similarprofiles/KnnInput/simprofknnFinal-0.0.8-jar-with-dependencies.jar --main-class org.htmedia.shine.simprofileknn.NPhase1 --args  '-part,%s,-dim,1006,-knn,501,-wc,0.6,-sc,0.05,-sjw,0.3,-scsp,2,-block,%s,-buffer,16' --arg s3n://analytics.shine.com/similarprofiles/KnnInput/ --arg %s --arg %s --arg hdfs:///output1 --step-name Knn1 --step-action TERMINATE_JOB_FLOW",job_id,partition,block_size,dataS_file,dataR_file))	
	dump = system(sprintf("/root/elastic-mapreduce-cli/elastic-mapreduce -j %s --jar s3://analytics.shine.com/similarprofiles/KnnInput/simprofknnFinal-0.0.9-jar-with-dependencies.jar --main-class org.htmedia.shine.simprofileknn.NPhase1 --args '-part,%s,-dim,1006,-knn,501,-wc,0.6,-sc,0.05,-sjw,0.3,-scsp,2,-block,%s,-buffer,16' --arg s3n://analytics.shine.com/similarprofiles/KnnInput/ --arg %s --arg %s --arg hdfs:///outputNPhase1 --step-name NPhase1 --step-action TERMINATE_JOB_FLOW",job_id,partition,block_size,dataS_file,dataR_file))	
	dump = system(sprintf("/root/elastic-mapreduce-cli/elastic-mapreduce -j %s --jar s3://analytics.shine.com/similarprofiles/KnnInput/simprofknnFinal-0.0.9-jar-with-dependencies.jar --main-class org.htmedia.shine.simprofileknn.NPhase2 --args  '-part,%s,-dim,7,-knn,501' --arg hdfs:///outputNPhase1 --arg  hdfs:///outputNPhase2 --step-name NPhase2 --step-action TERMINATE_JOB_FLOW",job_id,partition))	
	#dump = system(sprintf("/root/elastic-mapreduce-cli/elastic-mapreduce -j %s --jar s3://analytics.shine.com/similarprofiles/KnnInput/simprofknnFinal-0.0.9-jar-with-dependencies.jar --main-class org.htmedia.shine.simprofileknn.NPhase1ModifiedProfilesSimilarityUpdate --args '-part,%s,-dim,1006,-knn,501,-wc,0.6,-sc,0.05,-sjw,0.3,-scsp,2,-block,%s,-buffer,16' --arg s3n://analytics.shine.com/similarprofiles/KnnInput/ --arg %s --arg %s --arg hdfs:///outputModifiedPhase1 --step-name NPhase1Modified --step-action TERMINATE_JOB_FLOW ",job_id,partition,block_size,inner_dataR_file,full_dataR))	
	#dump = system(sprintf("/root/elastic-mapreduce-cli/elastic-mapreduce -j %s --jar s3://analytics.shine.com/similarprofiles/KnnInput/simprofknnFinal-0.0.9-jar-with-dependencies.jar --main-class org.htmedia.shine.simprofileknn.NPhase2 --args  '-part,%s,-dim,7,-knn,501' --arg hdfs:///outputModifiedPhase1 --arg  hdfs:///outputModifiedPhase2 --step-name NPhase2Modified --step-action TERMINATE_JOB_FLOW",job_id,partition))
	dump = system(sprintf("/root/elastic-mapreduce-cli/elastic-mapreduce -j %s --jar s3://analytics.shine.com/similarprofiles/KnnInput/simprofknnFinal-0.0.9-jar-with-dependencies.jar --main-class org.htmedia.shine.simprofileknn.NPhase1NewProfilesSimilarityUpdate --args '-part,%s,-dim,1006,-knn,501,-wc,0.6,-sc,0.05,-sjw,0.3,-scsp,2,-block,%s,-buffer,16' --arg s3n://analytics.shine.com/similarprofiles/KnnInput/ --arg %s --arg %s --arg hdfs:///outputNewNPhase1 --step-name NPhase1New --step-action TERMINATE_JOB_FLOW",job_id,partition,block_size,inner_dataR_file,full_dataR))
	dump = system(sprintf("/root/elastic-mapreduce-cli/elastic-mapreduce -j %s --jar s3://analytics.shine.com/similarprofiles/KnnInput/simprofknnFinal-0.0.9-jar-with-dependencies.jar --main-class org.htmedia.shine.simprofileknn.NPhase2 --args  '-part,%s,-dim,7,-knn,501' --arg hdfs:///outputNewNPhase1 --arg  hdfs:///outputNewNPhase2 --step-name NPhase2New --step-action TERMINATE_JOB_FLOW",job_id,partition))
	dump = system(sprintf("/root/elastic-mapreduce-cli/elastic-mapreduce -j %s --jar s3://analytics.shine.com/similarprofiles/KnnInput/simprofknnFinal-0.0.9-jar-with-dependencies.jar --main-class org.htmedia.shine.simprofileknn.NPhase2Partitioner --args '-map,30,-red,34,-dim,7' --arg  hdfs:///outputNPhase2,hdfs:///outputNewNPhase2 --arg  hdfs:///outputNPhase2Partition --step-name NPhase2Partition --step-action TERMINATE_JOB_FLOW",job_id))
	dump = system(sprintf("/root/elastic-mapreduce-cli/elastic-mapreduce -j %s --jar s3://analytics.shine.com/similarprofiles/KnnInput/s3distcp.jar --args '--src,hdfs:///outputNPhase2Partition,--dest,%s' --step-name S3Distcp --step-action TERMINATE_JOB_FLOW",job_id,final_S3_destination_folder))
	logger("run_EMR_cluster function : Completed")
	return(job_id)
}

check_if_Job_finished = function(job_id){
	logger("check_if_Job_finished function : Started")
	start_time = Sys.time()
	repeat{
		current_status = get_current_status(job_id)
		if(current_status=="TERMINATED"){
			logger("Cluster Terminated by itself... Check Steps!")
			logger("Final Cluster Output : ",system(sprintf("/root/elastic-mapreduce-cli/elastic-mapreduce -j %s --list",job_id),intern=T),multi_line=T)
			cluster_run_time <<- as.numeric(difftime(Sys.time(),start_time,units="mins"))
			logger("Run time on Cluster (mins) : ",cluster_run_time)
			break
		}
		if(current_status=="WAITING"){
			Sys.sleep(check_sleep_time)
			current_status = get_current_status(job_id)
			if(current_status=="WAITING"){
				logger("Final Cluster Output : ",system(sprintf("/root/elastic-mapreduce-cli/elastic-mapreduce -j %s --list",job_id),intern=T),multi_line=T)
				dump = system(sprintf("/root/elastic-mapreduce-cli/elastic-mapreduce --terminate --jobflow %s",job_id))
				cluster_run_time <<- as.numeric(difftime(Sys.time(),start_time,units="mins"))
				logger("Run time on Cluster (mins) : ",cluster_run_time)
				break
			}
		}
		if(as.numeric(difftime(Sys.time(),start_time,units="mins")) > cluster_cutoff_time){
			logger("Final Cluster Output : ",system(sprintf("/root/elastic-mapreduce-cli/elastic-mapreduce -j %s --list",job_id),intern=T),multi_line=T)
			logger("Closing Cluster as max allocated time exceeded (mins) : ",cluster_cutoff_time)
			dump = system(sprintf("/root/elastic-mapreduce-cli/elastic-mapreduce --terminate --jobflow %s",job_id))
			cluster_run_time <<- as.numeric(difftime(Sys.time(),start_time,units="mins"))
			logger("Run time on Cluster (mins) : ",cluster_run_time)
			break
		}
		Sys.sleep(check_sleep_time)
	}
	logger("check_if_Job_finished Function : Completed")
	return(TRUE)
}
	
get_current_status = function(job_id){
	check_output = system(sprintf("/root/elastic-mapreduce-cli/elastic-mapreduce -j %s --list",job_id),intern=T)
	current_status = gsub(sprintf("(.*)\\b(%s)\\s+(\\w+)\\s+(.*)",job_id),"\\3",check_output[1])
	return(current_status)
}

make_User_score_cutoff_table =  function(min_os_table = "User_min_os"){
	logger("make_User_score_cutoff_table function : Started")
	dump = executeQuery(sprintf("drop table if exists %s",min_os_table))
	for(fa in 1:32){
		logger("Starting for FA : ",fa)
		if(fa==1){
			dump = executeQuery(sprintf("create table %s as select U1,FA,min(Overall_Sim) as min_os from SimilarProfiles_FA%s group by 1;",min_os_table,fa))
		} else {
			dump = executeQuery(sprintf("insert into %s select U1,FA,min(Overall_Sim) as min_os from SimilarProfiles_FA%s group by 1;",min_os_table,fa),isInsert=TRUE)
		}
	}
	dump = executeQuery(sprintf("alter table %s add index u1(U1),add index fa(FA);",min_os_table))
	logger("make_User_score_cutoff_table function : Completed")
	return(TRUE)
}

make_User_max_score_table =  function(max_os_table = "User_max_os"){
	logger("make_User_max_score_table function : Started")
	dump = executeQuery(sprintf("drop table if exists %s",max_os_table))
	dump = executeQuery(sprintf("drop table if exists tmp_%s",max_os_table))
	for(fa in 32:1){
		logger("Starting for FA : ",fa)
		if(fa==32){
			dump = executeQuery(sprintf("create table %s as select U1,FA,max(Overall_Sim) as max_os from SimilarProfiles_FA%s group by 1;",max_os_table,fa))
			dump = executeQuery(sprintf("create table tmp_%s as select U1,U2,FA,Overall_Sim as max_os from SimilarProfiles_FA%s where U1=U2;",max_os_table,fa))
		} else {
			dump = executeQuery(sprintf("insert into %s select U1,FA,max(Overall_Sim) as max_os from SimilarProfiles_FA%s group by 1;",max_os_table,fa),isInsert=TRUE)
			dump = executeQuery(sprintf("insert into tmp_%s select U1,U2,FA,Overall_Sim as max_os from SimilarProfiles_FA%s where U1=U2;",max_os_table,fa),isInsert=TRUE)
		}
	}
	dump = executeQuery(sprintf("alter table %s add index u1(U1),add index fa(FA);",max_os_table))
	dump = executeQuery(sprintf("alter table tmp_%s add index u1(U1)",max_os_table))
	logger("Alter max_os_table where U1=U2")
	dump = executeQuery(sprintf("alter table %s add column withSelf tinyint default 0;",max_os_table))
	dump = executeQuery(sprintf("update %s a join tmp_%s b using(U1) set withSelf =1 where b.U1 is not null",max_os_table,max_os_table))
	dump = executeQuery(sprintf("drop table if exists tmp_%s",max_os_table))
	logger("make_User_max_score_table function : Completed")
	return(TRUE)
}

insert_data_into_tables = function(data_lookup,data_login,all_user_lookup,final_S3_destination_folder,final_partition_folder,remove_old_FA_users=TRUE,remove_old_users = TRUE,today_date){
	logger("insert_data_into_tables function : Started")
	final_S3_destination_folder = gsub("s3n:","s3:",final_S3_destination_folder)
	if (!file.exists(final_partition_folder)){
		dir.create(file.path(final_partition_folder))
	}
	dump = system(sprintf("/usr/local/bin/s3cmd get %s/* %s ",final_S3_destination_folder,final_partition_folder))
	logger("/usr/local/bin/s3cmd get completed : final_partition_folder : ",final_partition_folder)
	if(!(double_check_S3_copy(final_S3_destination_folder,final_partition_folder))){
		#Delete the partially copied files
		dump = system(sprintf("rm %s/* ",final_partition_folder))
		logger("/usr/local/bin/s3cmd get not fully completed ... trying again")
		dump = system(sprintf("/usr/local/bin/s3cmd get %s/* %s ",final_S3_destination_folder,final_partition_folder))
		logger("/usr/local/bin/s3cmd get completed : final_partition_folder : ",final_partition_folder)
		if(!(double_check_S3_copy(final_S3_destination_folder,final_partition_folder))){
			logger("/usr/local/bin/s3cmd get failed twice ... Terminating the code ")
			stop(simpleError("S3cmd Copy Error"))
		}
	}
	original_wd = getwd()
	setwd(final_partition_folder)
	user_fa_old = executeQuery(sprintf("select * from %s",lookup_tables[["userid"]]))
	data_lookup$old_fa = user_fa_old[match(data_lookup$user_lookup,user_fa_old$Lookup),"FA_Lookup"]
	fa_modifiers = data_lookup[which((data_lookup$fa_lookup != data_lookup$old_fa) & !is.na(data_lookup$old_fa)),]
	min_login_date_12m =  today_date - 365
	start_time = Sys.time()
	for(fa in 1:32){
		logger("Starting for FA : ",fa)
		part_fa = ifelse(nchar(fa)<2,sprintf("0%s",fa),fa)
		dump = executeQuery(sprintf("drop table if exists %s%s",tmp_table,fa))
		dump = executeQuery(sprintf("create table %s%s (U1 int unsigned NOT NULL,	U2 int unsigned NOT NULL,	Overall_Sim SMALLINT NOT NULL,	FA TINYINT,	Exp_Sim SMALLINT,	Sal_Sim SMALLINT,	Ind_Sim SMALLINT,	SubFA_Sim SMALLINT,	Edu_Sim SMALLINT,	JT_Sim SMALLINT,	Skill_Sim SMALLINT , PRIMARY KEY (U1,U2)) ENGINE=MyISAM ;",tmp_table,fa))
		dump = executeQuery(sprintf("LOAD DATA LOCAL INFILE '%s/part-000%s' INTO TABLE %s%s FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';",final_partition_folder,part_fa,tmp_table,fa))
		fa_modified_users = data_lookup[which(data_lookup$fa_lookup==fa),"user_lookup"]
		if(remove_old_FA_users){
			fa_changed_users = fa_modifiers[which(fa_modifiers$old_fa==fa),"user_lookup"]
			fa_modified_users = c(fa_modified_users,fa_changed_users)
			logger("Users who changed the FA (Added to delete list):", length(fa_changed_users))
		}
		if(remove_old_users){
			u1_users = executeQuery(sprintf("select distinct U1 from SimilarProfiles_FA%s;",fa))[,1]
			u2_users = executeQuery(sprintf("select distinct U2 from SimilarProfiles_FA%s;",fa))[,1]
			users_in_table = data.frame(Lookup = unique(c(u1_users,u2_users)))
			users_in_table$login_date = data_login[match(all_user_lookup[match(users_in_table$Lookup,all_user_lookup$Lookup),"Value"],data_login$X_id),"ll"]
			to_delete_users_12m = setdiff(users_in_table[which(as.Date(users_in_table$login_date)<min_login_date_12m),"Lookup"],data_lookup$user_lookup)
			fa_modified_users = c(fa_modified_users,to_delete_users_12m)
			logger("Users whose LastLogin is older than 1 year (Added to delete list):", length(to_delete_users_12m))
		}
		fa_modified_users = unique(fa_modified_users)
		logger("Users in delete list):", length(fa_modified_users))
		fa_table_count_start = executeQuery(sprintf("select count(*) from SimilarProfiles_FA%s;",fa))
		logger(sprintf("FA: %s : fa_table_count_start : %s",fa,fa_table_count_start[1,1]))
		dump = executeQuery("START TRANSACTION;")
		dump = executeQuery(sprintf("LOCK TABLES SimilarProfiles_FA%s WRITE,%s%s  WRITE;",fa,tmp_table,fa))
		fa_modified_users_flat = paste(fa_modified_users,collapse=",")
		dump = executeQuery(sprintf("ALTER TABLE SimilarProfiles_FA%s DISABLE KEYS;",fa))
		dump = executeQuery(sprintf("delete from SimilarProfiles_FA%s where U1 in (%s) or U2 in (%s)",fa,fa_modified_users_flat,fa_modified_users_flat))
		dump = executeQuery(sprintf("insert into SimilarProfiles_FA%s select * from %s%s",fa,tmp_table,fa),isInsert=TRUE)
		dump = executeQuery(sprintf("ALTER TABLE SimilarProfiles_FA%s ENABLE KEYS;",fa))
		dump = executeQuery("UNLOCK TABLES;")
		dump = executeQuery("COMMIT;")
		fa_table_count_end = executeQuery(sprintf("select count(*) from SimilarProfiles_FA%s;",fa))
		logger(sprintf("FA: %s : fa_table_count_end : %s",fa,fa_table_count_end[1,1]))
		logger(sprintf("FA: %s : Difference in count : %s",fa,as.numeric(fa_table_count_end[1,1] - fa_table_count_start[1,1])))
		dump = executeQuery(sprintf("DROP table if exists %s%s",tmp_table,fa))
		logger("Insert Completed for FA : ",fa)
	}
	table_modify_time <<- as.numeric(difftime(Sys.time(),start_time,units="mins"))
	logger("insert_data_into_tables Function : Completed")
	return(TRUE)
}

update_Lookup_FA = function(data_lookup){
	logger("update_Lookup_FA function : Started")
	userid_fa = data_lookup[,c("user_lookup","fa_lookup")]
	colnames(userid_fa) = c("Lookup","FA_Lookup")
	dump = executeQuery(sprintf("drop table if exists %s",tmp_table))
	dump = dbDisconnect(con);con=dbConnect('MySQL',username=mysql_username,password=mysql_password,host=mysql_host, dbname=mysql_dbname)
	dump = dbWriteTable(con, name=tmp_table, value=userid_fa, field.types=list(Lookup="int",FA_Lookup="tinyint"),row.names=FALSE)
	dump = executeQuery(sprintf("alter table %s add index Lookup(Lookup)",tmp_table))
	dump = executeQuery(sprintf("UPDATE %s a join %s b using(Lookup) SET a.FA_Lookup = b.FA_Lookup;",lookup_tables["userid"],tmp_table))
	logger("update_Lookup_FA Function : Completed")
	return(TRUE)
}

make_U2_U1_data = function(data_lookup,U2_U1_file,U2_U1_S3_location){
	logger("make_U2_U1_data function : Started")
	dump = system(sprintf("rm -f %s",U2_U1_file))
	require(data.table)
	for(fa in 1:32){
		logger("Starting for FA : ",fa)
		fa_users = data_lookup[which(data_lookup$fa_lookup==fa),"user_lookup"]
		u1_u2_fa = executeQuery(sprintf("select U1,U2 from SimilarProfiles_FA%s where U2 in (%s)",fa,paste(fa_users,collapse=",")))
		u1_u2_fa = u1_u2_fa[which(!(u1_u2_fa$U1 %in% data_lookup$user_lookup)),]
		u1_u2_fa = data.table(u1_u2_fa)
		u2_u1_flat = u1_u2_fa[,paste(U1,collapse=","),by="U2"]
		write.table(u2_u1_flat,file=U2_U1_file,append=T,sep=",",row.names=F,col.names=F,quote=F)
		logger("Completed for FA : ",fa)
	}
	dump = system(sprintf("/usr/local/bin/s3cmd put %s %s --multipart-chunk-size-mb=200 ",U2_U1_file,U2_U1_S3_location))
	logger("make_U2_U1_data Function : Completed")
	return(paste(U2_U1_S3_location,tail(strsplit(U2_U1_file,"/")[[1]],1),sep=""))
}

get_User_min_os = function(min_overall_Sim_table,User_min_os_file,User_min_os_S3_location){
	logger("get_User_min_os function : Started")
	dump = system(sprintf("rm -f %s",User_min_os_file))
	s3_location_User_min_os_data = paste(User_min_os_S3_location,tail(strsplit(User_min_os_file,"/")[[1]],1),sep="")
	dump = system(sprintf("/usr/local/bin/s3cmd del %s",s3_location_User_min_os_data))
	dump = system(sprintf("mysql -u %s -p'%s' -e 'select U1,min_os from %s.%s' | sed 's/\t/,/g' | tail -n +2 > %s",mysql_username,mysql_password,mysql_dbname,min_overall_Sim_table,User_min_os_file))
	dump = system(sprintf("/usr/local/bin/s3cmd put %s %s --multipart-chunk-size-mb=200 ",User_min_os_file,User_min_os_S3_location))
	logger("get_User_min_os Function : Completed")
	return(s3_location_User_min_os_data)
}

update_RunLog_SimilarProfiles = function(run_log_table,RunDate,DataTakenFromDate ,DataTakenTillDate, TotalUsers, NewUsers,Revivals,EMRRunTime,TotalRunTime,TableModificationTime,RunCompleted = 0){
	dump = executeQuery(sprintf("insert into %s values ('%s','%s','%s',%s,%s,%s,%s,%s,%s,%s);",run_log_table,RunDate,DataTakenFromDate ,DataTakenTillDate, TotalUsers, NewUsers,Revivals,EMRRunTime,TotalRunTime,TableModificationTime,RunCompleted),isInsert=TRUE)
	logger("update_RunLog_SimilarProfiles Function : Completed")
	return(dump)
}

send_email = function(today_date,subject,msg,to_users,attachments = c()){
	subject = sprintf("*IMP* SimilarProfiles Run %s : %s" ,today_date,subject)
	to_users = paste(to_users,collapse = ",")
	msg = paste("Time : ",tim(),"\n",msg,"\n\n","----- System Generated Email -----",sep="")
	if(length(attachments)>0){
		attach_files = paste("-a",paste(attachments,collapse=" -a "),sep=" ")
		dump = writeLines(msg,p<-pipe(sprintf("mutt -s '%s' %s -- %s",subject,attach_files,to_users)));close(p)
	} else {
		dump = writeLines(msg,p<-pipe(sprintf("mutt -s '%s' -- %s",subject,to_users)));close(p)
	}
	return(TRUE)
}

cleanS3_folders = function(){
	dump = system(sprintf("/usr/local/bin/s3cmd ls %s",dataR_S3_location),intern=T)
	
	
	
	
	
}

cleanLocal_folders = function(){
	
	
	
	
	
	
	
}

double_check_S3_copy = function(final_S3_destination_folder,final_partition_folder){
	logger("double_check_S3_copy function : Started")
	dump = system(sprintf("/usr/local/bin/s3cmd ls %s/",final_S3_destination_folder),intern=T)
	s3_files = gsub(sprintf("(.*)\\b(%s)/(.*$)",final_S3_destination_folder),"\\3",dump)
	for(s3file in s3_files){
		if(!(file.exists(sprintf("%s/%s",final_partition_folder,s3file)))){
			logger("double_check_S3_copy Function : Completed")
			return(FALSE)
		}
	}
	logger("double_check_S3_copy Function : Completed")
	return(TRUE)
}

remove_extra_simprofiles = function(per_query_users){
	logger("remove_extra_simprofiles function : Started")
	for(fa in 1:32){
		logger("Starting For FA :",fa)
		fa_table_count_start = executeQuery(sprintf("select count(*) from SimilarProfiles_FA%s;",fa))
		logger(sprintf("FA: %s : fa_table_count_start : %s",fa,fa_table_count_start[1,1]))
		fa_total_u1 = executeQuery(sprintf("select distinct U1 from SimilarProfiles_FA%s",fa))[,1]
		counter=0 
		to_delete_user_pairs = data.frame(U1=numeric(0),U2=numeric(0))
		repeat{
			if(counter >= length(fa_total_u1)){
				break
			} else {
				user_range = (counter+1):min((counter+per_query_users),length(fa_total_u1))
			}
			loop_users = fa_total_u1[user_range]
			u1_u2_fa = executeQuery(sprintf("select U1,U2,Overall_Sim from SimilarProfiles_FA%s where U1 in (%s)",fa,paste(loop_users,collapse=",")))
			u1_u2_fa = u1_u2_fa[order(u1_u2_fa$U1,u1_u2_fa$Overall_Sim,decreasing=T),]
			u1_u2_fa$Sim_rank = sequence(rle(u1_u2_fa$U1)$lengths)
			tmp_to_delete_users = u1_u2_fa[which(u1_u2_fa$Sim_rank>501),c("U1","U2")]
			to_delete_user_pairs = rbind(to_delete_user_pairs,tmp_to_delete_users)
			counter = max(user_range)
		}
		if(nrow(to_delete_user_pairs)>0){
			#Delete users from table
			#dump = executeQuery("START TRANSACTION;")
			#dump = executeQuery(sprintf("LOCK TABLES SimilarProfiles_FA%s WRITE",fa))
			#dump = executeQuery(sprintf("ALTER TABLE SimilarProfiles_FA%s DISABLE KEYS;",fa))
			delete_users_pairs = by(to_delete_user_pairs$U2,to_delete_user_pairs$U1,FUN = function(z) paste("(",paste(z,collapse=","),")",sep=""))
			delete_condition = paste(paste("( U1 = ",names(delete_users_pairs)," and U2 in ",delete_users_pairs,")",sep=""),collapse=" or ")
			#delete_condition = paste(paste("( U1 = ",to_delete_user_pairs$U1," and "," U2 = ",to_delete_user_pairs$U2,")",sep=""),collapse=" or ")
			dump = executeQuery(sprintf("delete from SimilarProfiles_FA%s where %s ",fa,delete_condition))[,1]
			#dump = executeQuery(sprintf("ALTER TABLE SimilarProfiles_FA%s ENABLE KEYS;",fa))
			#dump = executeQuery("UNLOCK TABLES;")
			#dump = executeQuery("COMMIT;")
		}
	fa_table_count_end = executeQuery(sprintf("select count(*) from SimilarProfiles_FA%s;",fa))
	logger(sprintf("FA: %s : fa_table_count_end : %s",fa,fa_table_count_end[1,1]))
	logger(sprintf("FA: %s : Difference in count : %s",fa,as.numeric(fa_table_count_end[1,1] - fa_table_count_start[1,1])))
	}
	logger("remove_extra_simprofiles Function : Completed")
	return(TRUE)
}

###################   In case of code failure   #########################
# Delete the folder in /data/Projects/SimilarProfiles/Output/SimilarProfiles_Master corresponding to the run
# Delete the file /data/Projects/SimilarProfiles/Input/outputS/
# Delete the file /data/Projects/SimilarProfiles/Input/outputR/
# Delete the folder from /data/Projects/SimilarProfiles/Input/OutputPartitions/
# Rename log file and messages file in the folder /data/Projects/SimilarProfiles/Output/SimilarProfiles_Logs/
# Rename the new users file in the folder /data/Projects/SimilarProfiles/Input/LookupFiles/
# Run the R code below

if(FALSE){
	#Load New users lookups
	new_user_data = read.csv(file="/data/Projects/SimilarProfiles/Input/LookupFiles/new_users_lookup_2014-11-21_error.csv") # change according the the date
	#Delete these lookups from the Lookup table 
	dump = executeQuery(sprintf("delete from Lookup_UserID where Lookup in (%s)",paste(new_user_data$Lookup,collapse=",")))
}








