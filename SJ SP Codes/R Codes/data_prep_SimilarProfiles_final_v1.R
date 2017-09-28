##!/usr/bin/Rscript

##################################			                            ################################
##################################    SimilarProfiles DataPrep RCode    ################################
##################################			                            ################################

tryCatch({
	run_start_time = Sys.time()
	today_date = Sys.Date()
	source("/data/Projects/SimilarProfiles/Model/Rcode/SimilarProfiles_Rconfig_final_v1.R")
	source(functions_file)
	output_path = paste(wd_location,"SimilarProfiles_run_",format(Sys.time(),"%m-%d"),sep="")
	if (!file.exists(output_path)){
			dir.create(file.path(output_path))
	}
	setwd(output_path)
	logfile = paste(log_location,"SimilarProfiles_logger_",format(Sys.time(),"%m-%d"),".log",sep="")
	messagesfile = paste(log_location,"SimilarProfiles_messages_",format(Sys.time(),"%m-%d"),".log",sep="")
	logger("Log File :",logfile)
	messagesfile_con = file(messagesfile, open="wt")
	sink(logfile)
	sink(messagesfile_con,type="message")
	logger("Output Path :",output_path)
	options(width=150)
	options(scipen=999)
	require(data.table)
	require(Matrix)
	require(RMySQL)
	require(foreach)
	require(doMC)
	registerDoMC(cores=cores_to_use)
	con=dbConnect('MySQL',username=mysql_username,password=mysql_password,host=mysql_host, dbname=mysql_dbname)
	from_to_date_list = get_from_to_run_dates(run_log_table = run_log_table)
	update_data_list = fetch_update_data(from_path = raw_input_dir, from_to_date_list[["from_date"]] ,from_to_date_list[["to_date"]],suffix = raw_input_suffix,date_format = "%d-%m-%Y")
	update_data = update_data_list[[1]]
	update_data_skills = update_data_list[[2]];rm(update_data_list);gc()
	update_data = dataPrep(update_data)
	#Change MySQL settings
	dump = change_mysql_settings(mysql_variable_list = new_mysql_variable_list)
	
	#Cleaning JT and Skills
	update_data$cleaned_JT = cleanJT(update_data$JobTitle)
	update_data_skills = cleanSkills(update_data_skills)

	#Lookups	
	lookup_list = get_lookups(lookup_tables)
	data_lookup = applyLookups(update_data,lookup_list = lookup_list,na_vec=na_vec,today_date = today_date)

	#Login Details
	data_login = get_login_data(login_mongo_export_file)

	#Dropping freshers and login based users
	data_lookup$LastLogin = data_login[match(data_lookup$UserId,data_login$X_id),"ll"]
	data_lookup = data_lookup[which(!is.na(data_lookup$LastLogin)),] 
	non_fresher_rows = which(data_lookup$Total_exp>0 & data_lookup$fa_lookup %in% 1:32)
	freshers_count = nrow(data_lookup) - length(non_fresher_rows)
	total_candidates_count = nrow(data_lookup)
	data_lookup = data_lookup[non_fresher_rows,]
	
	data_lookup = create_exp_sal_bins(data_lookup)
	data_lookup = data_lookup[,c("user_lookup","fa_lookup","exp_bin","exp_bin_compare","sal_bin","sal_bin_compare","Total_exp","Salary","ind_lookup","subfa_lookup","insti_lookup","jt_lookup")]

	#Generating Skills vec
	lookup_list = get_lookups(lookup_tables)
	new_dataR_file = create_skill_vector(update_data_skills,lookup_list,data_lookup,today_date=today_date)
	
	#Update & Export User_Profile table
	s3data_files = update_user_profile_table(new_user_lookup = data_lookup$user_lookup,dataR_file = new_dataR_file,outputS_file = outputS_file ,full_outputR_file = full_outputR_file , data_login = data_login,all_user_lookup = lookup_list[["userid"]],today_date =today_date)
	
	#Make data to update old users lists
	#U2_U1_file = sprintf("%s/%s",U2_U1_folder,U2_U1_filename)
	#logger("U2_U1_file : ", U2_U1_file)
	#s3_location_U2_U1_data = make_U2_U1_data(data_lookup,U2_U1_file,U2_U1_S3_location)
	User_min_os_file = sprintf("%s/%s",User_min_os_folder,User_min_os_filename)
	s3_location_User_min_os_data = get_User_min_os(min_overall_Sim_table = min_overall_Sim_table,User_min_os_file = User_min_os_file,User_min_os_S3_location = User_min_os_S3_location)
	
	#Run EMR
	if(length(s3data_files)>1){
		final_S3_destination_folder = sprintf("%sNPhasePartition-%s",S3_destination_folder,today_date)
		job_id = run_EMR_cluster(s3data_files$dataR,s3data_files$dataS,s3data_files$full_dataR,final_S3_destination_folder,today_date = today_date)
		dump = check_if_Job_finished(job_id)
		final_partition_folder = sprintf("%sNPhasePartition-%s",output_partition_folder,today_date)
		dump = insert_data_into_tables(data_lookup = data_lookup,data_login,all_user_lookup = lookup_list[["userid"]],final_S3_destination_folder,final_partition_folder,remove_old_FA_users  = TRUE,remove_old_users = TRUE, today_date = today_date)
	}
	#Updating FA in lookup table 
	dump = update_Lookup_FA(data_lookup)
	# Min & Max overall similarity for every User
	dump = make_User_score_cutoff_table(min_os_table = min_overall_Sim_table) # 1 hour from scratch
	#Removing greater than 500 similar users
	#dump = remove_extra_simprofiles(per_query_users = 20000)
	#dump = make_User_max_score_table(max_os_table = max_overall_Sim_table) # 1 hour from scratch	
	#dump = clean_SimProf_tables_lookups(all_user_lookup = lookup_list[["userid"]]) # To keep only the lookups present in the lookup table for all the tables.
	dump = change_mysql_settings(mysql_variable_list = old_mysql_variable_list)
	total_run_time = as.numeric(difftime(Sys.time(),run_start_time,units="mins"))
	logger("SimilarProfiles Run Completed !!!","Sending Email")
	dump = update_RunLog_SimilarProfiles(run_log_table = run_log_table,RunDate = today_date,DataTakenFromDate = from_to_date_list[["from_date"]] ,DataTakenTillDate = from_to_date_list[["to_date"]] , TotalUsers = total_candidates_count , NewUsers = new_users_count,Revivals = revivals_count,EMRRunTime = cluster_run_time,TotalRunTime = total_run_time,TableModificationTime = table_modify_time,RunCompleted = 1)
	dump = send_email(today_date=today_date,subject = "And Now His Watch Is Ended",msg = sprintf("RunDate = %s\nDataTakenFromDate = %s\nDataTakenTillDate = %s\nTotalUsers = %s\nNewUsers = %s\nRevivals = %s\nEMRRunTime = %s\nTotalRunTime = %s\nTableModificationTime = %s",today_date,from_to_date_list[["from_date"]],from_to_date_list[["to_date"]],total_candidates_count,new_users_count,revivals_count,cluster_run_time,total_run_time,table_modify_time),to_users = email_to)
	sink()
	sink(type="message")
},error = function(ex){
		logger("Error occurred in SimilarProfiles Run !!!","Sending Email")
		dump = send_email(today_date=today_date,subject = "Valar Morghulis",msg = paste(": Error :",ex$message,"\n","Traceback: ",traceback(),sep=""),to_users = error_email_to,attachments = c(logfile,messagesfile))
		logger(ex$message,multi_line=TRUE)
		dump = change_mysql_settings(mysql_variable_list = old_mysql_variable_list)
		sink()
		sink(type="message")
		stop(ex)
})
