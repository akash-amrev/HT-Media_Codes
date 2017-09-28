##################################			                         ################################
##################################   SimilarProfiles R config file   ################################
##################################			                         ################################

#MySQL credentials
mysql_username="root"
mysql_password="mysql@123"
mysql_host="localhost"
mysql_dbname="SimilarProfiles" # a db where you have write permission

#Mongo credentials
mongo_username=""
mongo_password=""
mongo_host="localhost"
mongo_login_dbname="sumoplus" 
mongo_login_collection = "CandidateStatic"

#Email config
email_from = "htcampus.analytics@gmail.com" #using mutt change will have to be made in mutt setting also
email_to = c("parul.agarwal@hindustantimes.com","akash.verma@hindustantimes.com")
error_email_to =  c("parul.agarwal@hindustantimes.com","akash.verma@hindustantimes.com","akash.it.manit@gmail.com","parulagarwal1989@gmail.com")

#Master working directory - where everything will be saved(date wise folders will be created)
wd_location = "/data/Projects/SimilarProfiles/Output/SimilarProfiles_Master/"
#Logs present inside the log folder
log_location = "/data/Projects/SimilarProfiles/Output/SimilarProfiles_Logs/"
raw_input_dir = "/data/Projects/SimilarProfiles/Input/DailyCandInput/"
raw_input_suffix = "dailyModifies_"
raw_input_date_format = "%d-%m-%Y"

functions_file = "/data/Projects/SimilarProfiles/Model/Rcode/SimilarProfiles_R_Functions_v1.R"
python_login_data_file = "/data/Projects/SimilarProfiles/Model/Pycode/LoginData.py"

cores_to_use = 5 # Number of cores to use where parallel is present in the code
max_query_reattempts = 2
num_parts_in_export_table = 5
report_irregularity = TRUE #Report where the lookups failed

#Tables
lookup_tables = c(userid = "Lookup_UserID",fa = "Lookup_FA",subfa = "Lookup_SubFA",ind= "Lookup_Ind",edu = "Lookup_Edu",jt = "Lookup_JT",skills = "Lookup_Skills")
user_profile_table = "User_Profile_6m"
user_profile_table_12m = "User_Profile_12m"
run_log_table = "RunLog_SimilarProfiles"
tmp_table ="tmp_table_to_delete"
Corrected_JT_table = "Corrected_JT"
min_overall_Sim_table = "User_min_os"
max_overall_Sim_table = "User_max_os"

#MySQL settings
new_mysql_variable_list = list("key_buffer_size" = 42949672960,"sort_buffer_size" = 1073741824,"read_rnd_buffer_size" = 1073741824,"read_buffer_size" = 1073741824)
old_mysql_variable_list = list("key_buffer_size" = 5368709120,"sort_buffer_size" = 67108864,"read_rnd_buffer_size" = 67108864,"read_buffer_size" = 67108864)

#Corrections
JT_corrected_words = "/data/Projects/SimilarProfiles/Input/JT_corrected_Words_24Aug_corrected.csv"
spell_corrector = "/data/Projects/SimilarProfiles/Codes/spelling_corrector_12May.R"
most_used_terms_file = "/data/Projects/SimilarProfiles/Input/most_used_terms_JTs_8Sep.csv"

adhoc_skill_correction_file = "/data/Projects/SimilarProfiles/Input/adhoc_skill_correction_list.csv"
missing_replacement = "NULL"
na_vec = c("","NA","Null","None")
lookup_folder = "/data/Projects/SimilarProfiles/Input/LookupFiles"
login_mongo_export_file = "/data/Projects/SimilarProfiles/Input/LoginData/CandidateStatic.csv"
skills_dist_mat_file = "/data/Projects/SimilarProfiles/Input/FinalInput/skills_cosine_dist_mat_min_sim_05_21Aug.Rdata"
skill_table_file = "/data/Projects/SimilarProfiles/Input/FinalInput/skills_table_21Aug.Rdata"
skill_fa_wide_file = "/data/Projects/SimilarProfiles/Input/FinalInput/skill_fa_wide_21Aug.R"
outputR_file = "/data/Projects/SimilarProfiles/Input/outputR/outer_dataR_weekly"
full_outputR_file = "/data/Projects/SimilarProfiles/Input/outputR/outer_dataR_full"
dataR_S3_location = "s3://analytics.shine.com/similarprofiles/OutputR/"
full_dataR_S3_location = "s3://analytics.shine.com/similarprofiles/Full_OutputR/"
outputS_file = "/data/Projects/SimilarProfiles/Input/outputS/inner_dataS"
dataS_S3_location = "s3://analytics.shine.com/similarprofiles/OutputS/"
S3_destination_folder = "s3n://analytics.shine.com/similarprofiles/WeeklyRun/"
output_partition_folder = "/data/Projects/SimilarProfiles/Input/OutputPartitions/"
U2_U1_filename = "u2_u1_upd.csv"
U2_U1_folder = "/data/Projects/SimilarProfiles/Input/FinalInput/U2_U1_folder"
U2_U1_S3_location = "s3://analytics.shine.com/similarprofiles/KnnInput/"
User_min_os_filename = "u1_min_os.csv"
User_min_os_S3_location = "s3://analytics.shine.com/similarprofiles/KnnInput/"
User_min_os_folder = "/data/Projects/SimilarProfiles/Input/FinalInput/User_min_os"
export_table_temp = "/data/Projects/SimilarProfiles/Input/Export_table_tmp"

#EMR config
master_instance = "m1.large"
core_instance = "r3.8xlarge"
core_nodes_number = 4
reducers_per_node = 40
mappers_per_node = 15
task_timeout = 2500000
dfs_block_size = 536870912
partition = 12
block_size = 5000

cluster_cutoff_time = 240 #mins
check_sleep_time = 20 #secs
cluster_run_time = 0

table_modify_time = 0

new_users_count = 0
revivals_count = 0
freshers_count = 0
total_candidates_count = 0
time_taken_S3_upload = 0 #mins

#Valid lists
valid_CourseType = c('Full Time','Part Time','Correspondance')
valid_Stream = c('Arts','Science','Engineering','Medical','Management','Computer Application','Education','Commerce','Other','Design','Pharmacy','Chartered Accountant  / CFA / CWA','Company Secretary','Law','Journalism / Communication','Hospitality and Hotel Management','Veterinary Science','Aviation')
valid_Specialization = c('Other','Biology - Molecular Biology','Computer / IT / Software Engineering','Surgery','Biology - Microbiology','HR','Biology - Zoology','Marketing','Biology','BCA /MCA','Operations Management','Other Management','Language & Literature - Spanish','Economics / Econometrics','Computers / IT / Software - Science','Education','History','Maths','Opthalmology','Metallurgy Engineering','Biology - Biochemistry','Chemistry - Organic','Commerce','Industrial','Biotechnology Engineering','Finance','Public Administration','Electrical Engineering','Language & Literature - English','Other Medical','Dentistry','Biology - Botany','Chemistry - Inorganic','Chemistry','Political Science','Biology - Genetics','Electronics and Communication Engineering','Civil Engineering','Language & Literature - Sanskrit','Pharmacy','Mechanical Engineering','CA','Language & Literature - Hindi','Communication - Journalism','Psyciatry','Instrumentation Engineering','Other Science','Company Secretary','Language & Literature - French','Agriculture & Dairy','Chemical Engineering','Biomedical Engineering','Law','Home Science','Environmental Engineering','Aeronautics Engineering','Communication - Advertising & PR','Hospitality and Hotel Management','Sociology','Performing Arts - Music','CFA','CWA','Other Engineering','Physics - Mechanics','Medicine','MIS Management','Maths - Statistics','Biology - Physiology','Physics','Maths - Algebra','Geography','Graphics','Neurosurgery','Veterinary Science','Environment','Anthropology','Agriculture / Dairy Engineering','General','Performing Arts - Theatre','Psychology','Communication - Tech. Writing','Chemistry - Physical','Mining Engineering','Biology - Forensics','Philosophy','Maths - Number Theory','Industrial and Production Engineering','Food Technology','Fashion','ALTP','Materials Engineering','International Trade','Geology','Marine Engineering','Cardiology','CPL','Physics - Optics','Nuclear Engineering','Archaeology','Textile Engineering','Orthopedics','Anaesthesia','Electronics / Communication / Instrumentation','Physics - Nuclear Physics','Biology - Pathology','Physics - Applied Physics','Food Technology Engineering','Pathology','Biology - Ecology','Performing Arts - Dance','Neurology','Public Policy','Physics - Astrophysics','Med. Allied - Nursing','Physics - Astronomy','Performing Arts - Film & Television','Gynaecology','Dentistry - Surgery','Architecture Engineering','Visual Arts - Fine Arts','Radiology','Pediatrics','Med. Allied - Physiotherapy','Optics Engineering','Dentistry - Orthodontics','Med. Allied - Nutrition & Dietetics','Biology - Biophysics','Forestry','Visual Arts - Photography','Physics - Molecular Physics','Language & Literature - Japanese','Biology - Neuroscience','Language & Literature - Mandarin','Maths - Logic and Set Theory','Language & Literature - German','Military Science','Sports Medicine','Biology - Anatomy','Physics - Thermodynamics','Dentistry - Periodontics','Physics - Dynamics','Forestry Engineering','Textile Design','Maths - Geometry','Med. Allied - Optometry','Maths - Probability','Med. Allied - Speech Therapy')


JT_short_hands = list(
	 "senior" = c("sr.","sr","s.r.","s.r"),
	 "senior manager" = c("sr.manager","seniormanager"),
	 "junior"= c("jr.","jr","j.r.","j.r","jnr","jnr."),
	 "executive" = c("exec.","exec","exe."),
	 "trainee" = c("tr.","tr","train","train."),
	 "assistant" = c("asst.","asst","ass.","ast.","ass","ast","assit.","asstt.","astt","astt.","asstt"),
	 "general manager" = c("gm","g.m.","g.m"),
	 "assistant manager" = c("am","a.m.","a.mgr","a.manager","asst.manager"),
	 "assistant general manager" = c("agm","a.g.m.","a.g.mgr","ag.manager"),
	 "software" = c("soft.","soft","sw","s/w"),
	 "programmer" = c("progmr","progmr"),
	 "engineer" = c("engg.","engg","eng.","enggr"),
	 "manager" = c("mgr","mgr."),
	 "quality control" = c("qc","q.c.","qc.","q.c"),
	 "quality assurance" = c("qa","qa.","q.a.","q.a"),
	 "vice president" = c("vp","v.p.","v.p","avp / vp"),
	 "assistant vice president" = c("avp","a.v.p.","a.vp","avp."),
	 ".net" = c("dotnet","dot net",". net"),
	 "department" = c("deptt.","deptt","dept"),
	 "deputy"= c("dy.","dy"),
	 "deputy general manager" = c("dgm","d.gm","d.g.m.","dgm."),
	 "service delivery manager" = c("sdm","sdm.","s.d.m.","s.dm"),
	 "junior engineer" = c("je","j.e.","j.engineer","je."),
	 "human resources"  = c("hr.","h/r","h.r.","h.r","hr","human recruiter","h. r.","h r"),
	 "administrator" = c("admin","admin."),
	 "sap" = c("s.a.p.","s.a.p","sap."),
	 "hardware & network" = c("hwn","hw/n","h/n","h/w n","hn","hardware network","hardware/networking","hardware & networking", "hardware&networking"),
	 "embedded software developer" = c("esd","e.s.d."),
	 "zonal sales manager" = c("zsm","z.s.m.","z.s.m"),
	 "coordinator" = c("coordi.","co-ordinator"),
	 "superintendent" = c("supdt.","superintend","supdt")
)

