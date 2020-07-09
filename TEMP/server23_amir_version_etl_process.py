#!/usr/bin/python3
import sys
from sys import getsizeof
import csv
try:
	ee = int(sys.argv[1])
except:
	raise Exception("Sorry, give number 1 for email:yes, and 0 for email:no")

error_email = ee
if	not error_email in [0,1]:
	raise Exception("Sorry, give number 1 for email:yes, and 0 for email:no")

import time
import os
import shutil
from text_files_to_dataframes_V4 import *
import pandas as pd
pd.set_option("display.max_rows", None, "display.max_columns", None)
import glob
from datetime import datetime
from distutils.dir_util import copy_tree
import numpy as np
import traceback

today__ = datetime.today().strftime('%Y-%m-%d')
directory = 'raw_synced_data/'
actualFiles = []
Id_df = pd.read_csv("proceced_IDs.csv")
Id_df.sort_values('createdtimestamp', inplace=True)
start = time.time()

arr_get_new_df = []
arr_get_old_df = []
arr_merge_pickles = []
arr_push_data_to_db = []
arr_convert_txt_to_csv = []
arr_generate_db_queries = []
arr_get_id_and_date_time = []
arr_calculate_difference = []

cmd_print = False

# when inserting into temporary folder  this variable allows us to have different count for each table. for eg: call_log_1.pkl, call_log_2.pkl ....
temporaryFileCount = 0

# it is used to solve a special case where last file in first folder has empty data, so in that case oldFolderName was being returned as None, which then doesnot allow us to delete the old folder and therefore the old folder starts having multiple folders
tmpOldFolderName = None

columns_for_matcing_dict  = {
	"accounts_list"     : ["name", "type", "ID"],
	"app_install_log"   : ["Package", "ID"],
	"calendar_events"   : ['ID', 'title', 'description', 'allowReminders', 'dTStart', 'dTEnd'],
	"call_log"          : ['ID', 'number', 'Type', 'Duration'],
	"contacts_list"     : ["displayName", "phone", "ID"],
	"ext_storage_files" : ["Downloads", "Documents", "Images", "Videos", "ID"],
	"filter_app_log"    : ["social", "dangerous", "darkweb", "wallet", "banking", "lending", "rosca", "ID"],
	"location"          : ["latitude", "longitude", "ID"],
	"gallery_data"      : ["ID", "title", "path"],
	"outgoing_call_log" : ['ID', 'Date', 'Count'],
	"sms_log"           : ['ID', 'Address', 'Body', 'Type', 'sendDate', 'senderName'],
	"sms_sent_log"      : ['ID', 'date', 'count'],
	"master"            : ["ID", "ipaddress", "battery_level", "storage_size", "brand", "deviceSoftware", "manufacturer", "networkOperator", "ram", "model", 'deviceWifiDataInBytes', 'deviceMobileDataInBytes', 'numberOfWifiStored', 'connectedConnectionName']
}

function_ = {
	"contacts_list"     : contacts_list_func,
	"ext_storage_files" : ext_storage_files_func,
	"location"          : location_func,
	"outgoing_call_log" : outgoing_call_log_func,
	"sms_log"           : sms_log_func,
	"sms_sent_log"      : sms_sent_log_func,
	"accounts_list"     : accounts_list_func,
	"app_install_log"   : app_install_log_func,
	"gallery_data"      : df_gallery_data_func,
	"call_log"          : call_log_func,
	"filter_app_log"    : filter_app_log_func,
	"calendar_events"   : calendar_events_func,
	"master"            : Master_func
}

def download_process_IDs():
	import pymysql
	import warnings
	import pandas as pd
	global Id_df
	warnings.filterwarnings("ignore")

	try:
		mysql_ai = pymysql.connect(host= '192.168.61.15', port= 3306, user= 'readonly_user_lfd', passwd= 'K@sh@!$3aDo#@3R', db = 'kashegypt_core')

		d = pd.read_sql("SELECT * FROM userDevice", con = mysql_ai)
		f = pd.read_sql("SELECT * FROM userFile", con = mysql_ai)

		user_table = d.merge(f, right_on="customerDeviceId", left_on="id")
		user_table = user_table[['createdTimestamp_y', 'customerAccountId', 'filePath', 'customerDeviceId']]
		user_table.columns = ['createdtimestamp', 'AccountId', 'filePath', 'DeviceId']
		user_table["PK"] = user_table["AccountId"].astype(int).astype(str) + "-" + user_table["DeviceId"].astype(int).astype(str)
		Id_df = user_table[["filePath", "PK", "createdtimestamp"]]
		Id_df.to_csv("proceced_IDs.csv", index=False)
		print("********************************************************************************Successfully Downloaded proceced_IDs.csv\n\n")
	except:
		return "Error"
	return None

def CONNECT():
	import sqlalchemy
	import os
	user = 'db_user'  if(os.path.exists('/home/amir')) else 'root' #'newuser'
	passw = 'passw0rd'  if(os.path.exists('/home/amir')) else '' #'password'
	host =  '127.0.0.1'
	port = 3306
	database = 'kashat' #'algo'
	conn = sqlalchemy.create_engine(f'mysql+pymysql://{user}:{passw}@{host}/{database}')
	conn = conn.connect()
	return conn

def unzip(filename):
	filename = filename.strip("/raw_synced_data").replace("_unzip", ".zip")
	zipfile = directory + filename
	unzipfile = directory + filename.replace('.zip','_unzip')
	try:
		shutil.unpack_archive(zipfile,extract_dir=unzipfile)
		return False
	except Exception as e:
		folder_Corrupt(filename)
		return True

def get_id_and_date_time(folder_name):
	try:
		ID = list(Id_df[Id_df.filePath == (folder_name + ".zip")]["PK"].values)[0]
		date_time = (Id_df[Id_df.filePath == (folder_name + ".zip")]["createdtimestamp"].values)[0]
		return (ID, date_time)
	except:
		if not folder_name + ".zip" in Id_df["filePath"]:
			open("logs/ID-not-found-in-user-table.txt", "a").write(folder_name + "\n")
	return None

def get_new_df (eachFile, file_name, id_, datetime_):
	if(file_name == 'master'):
		df = function_[file_name](eachFile)
	else:
		df = function_[file_name](eachFile, file_name+".txt")
	try:
		df.columns
	except:
		return None

	if(len(df)> 0): 
		df['ID'] = id_
		df['timestamp']=datetime_
		df["Epoch"] = str(time.time())
		# ye loop pehly neechy tha, magar masla ye aa raha h k 5/6 lines bad jo drop_duplicates jab chalta h to case diffrence k bajay same ho.
		for i in df.dtypes[df.dtypes == "object"].index:
			try:
				df[i] = df[i].str.lower()
			except:
				pass

		return df.drop_duplicates(subset=columns_for_matcing_dict[file_name], keep = 'first')
	else:
		return df

def get_old_df(id_, file_name):
	Id_df = pd.read_csv("proceced_IDs.csv")
	oldFileIDs = []
	oldFolderName =  None
	old_files = os.listdir("old/")
	if(len(old_files)):
		try:
			old_files = [i.replace("_unzip", '')+".zip" for i in old_files]
			Id_df = Id_df[Id_df.filePath.isin(old_files)]
			# this catered the problem when the file against a particular id is not avaialble in old folder
			if(len(Id_df[Id_df.PK == id_]) == 0):
				return None, None
			oldFolderName = list(Id_df[Id_df.PK == id_]["filePath"])[0].replace(".zip", "_unzip")

			if(oldFolderName):
				old_filE = 'old/'+oldFolderName+'/'+file_name + ".pkl"
				dfOld = pickle.load(open(old_filE, "rb"))
				return dfOld, oldFolderName
		except Exception as e:
			pass
	return None, None

def concatinate_pk(df, PK):
	try:
		df["Count"] = df["Count"].astype(str)
	except:
		pass
	command = ""
	for i in range(len(PK)):
		command += f"+ '|' + df['{PK[i]}']"
	command = command.strip("+ '|' + ")
	df["db_matching_variable"] = eval(command)
	return df

def calculate_difference(eachFile, file_name, id_, datetime_):
	if(cmd_print):
		print("__________ calculate_difference() _________", file_name)
	global temporaryFileCount
	global new_folderPath
	global old_folderPath
	global oldFolderName
	global tmpOldFolderName

	# eachFile + file_name = "raw_synced_data/123_unzip/contacts_list"
	time_get_new_df = time.time()
	newDf = get_new_df(eachFile, file_name, id_, datetime_)
	arr_get_new_df.append(str(time.time() - time_get_new_df))
	if(newDf is None):
		return True

	if len(newDf) == 0:
		oldFileIDs = []
		oldFolderName =  None
		old_files = os.listdir("old/")
		if(len(old_files)):
			try:
				old_files = [i.replace("_unzip", '')+".zip" for i in old_files]
				Id_df_tmp = Id_df[Id_df.filePath.isin(old_files)]
				# this catered the problem when the file against a particular id is not avaialble in old folder
				if(len(Id_df_tmp[Id_df_tmp.PK == id_]) == 0):
					raise Exception('No old file avaialble for this ID')
				oldFolderName = list(Id_df_tmp[Id_df_tmp.PK == id_]["filePath"])[0].replace(".zip", "_unzip")
				if(oldFolderName):
					old_filE = 'old/'+oldFolderName+'/'+file_name + ".pkl"
					if not os.path.exists(eachFile.replace('raw_synced_data/', 'new/')):
						os.mkdir(eachFile.replace('raw_synced_data/', 'new/'))
					new_temp_path = eachFile.replace('raw_synced_data/', 'new/') + '/'+file_name + '.pkl'
					shutil.move(old_filE, new_temp_path)                    
			except Exception as e:
				pass
		return None

	if not file_name in ["sms_sent_log", "outgoing_call_log"]:
		# newDf['process_date'] = str(datetime.now().date())
		newDf['process_date'] = pd.to_datetime(newDf.timestamp).dt.date.astype(str)
		newDf['process_hour'] = str(datetime.now().hour)
		newDf['isActive'] = 1
		if file_name != "location":
			newDf['end_date'] = "2099-12-31"

	# isay bahar isliyey nikala tha q k sms and out may db_match var nahi ban rha tha
	newDf = concatinate_pk(newDf,columns_for_matcing_dict[file_name])
	
	new_folderPath = eachFile.replace('raw_synced_data/', 'new/')
	# new_folderPath = "new/123_unzip"

	old_folderPath = new_folderPath.replace('new','old')
	# old_folderPath = "old/123_unzip/"

	if not os.path.exists(new_folderPath):
		os.mkdir(new_folderPath)
	new_filePath = new_folderPath +'/' + file_name + '.pkl'
	# new_filePath = "new/123_unzip/contacts_list.pkl"

	pickle.dump(newDf, open(new_folderPath+'/'+file_name+'.pkl', 'wb'))

	time_get_old_df = time.time()
	oldDf, oldFolderName = get_old_df(id_, file_name)

	if(oldFolderName is not None):
		# it is used to solve a special case where last file in first folder has empty data, so in that case oldFolderName was being returned as None, which then doesnot allow us to delete the old folder and therefore the old folder starts having multiple folders
		# No matter whether the next file has data or not, if oldfoldername is updated once, it must have to be deleted, that's why this assignment was done,  to avoid updation when a txt file is empty []
		tmpOldFolderName = oldFolderName

	arr_get_old_df.append(str(time.time() - time_get_old_df))

	# if((cmd_print) and (not data)):
	# 	print("\nNew DF :  ==================================== :")
	# 	print(newDf[['db_matching_variable','Epoch']])
	# 	print("\nOld Directory : \n", os.listdir('old/'))

	if(oldDf is None):
		pickle.dump(newDf, open('temporary/'+file_name+"_"+str(temporaryFileCount)+'.pkl', 'wb'))
		temporaryFileCount+=1
	else:
		df = pd.concat([oldDf, newDf])
		df.reset_index(drop=True, inplace=True)
		df = df.drop_duplicates(subset=columns_for_matcing_dict[file_name], keep = False)

		newDf = newDf.reset_index().drop("index", axis=1)
		oldDf = oldDf.reset_index().drop("index", axis=1)

		# if((cmd_print) and (not data)):
		# 	print("\nOld DF :  ==================================== :")
		# 	print(oldDf[['db_matching_variable','Epoch']])

		# if((cmd_print) and (not data)):
		# 	print("\nAfter drop duplicates :  ==================================== :")
		# 	print(df[['db_matching_variable','Epoch']])

		n = newDf.copy()
		# jb old or new may 2 rows same hon, too old ka epoch new may lejao (df and file both)
		df__ = newDf[["db_matching_variable", "Epoch"]].merge(oldDf[["db_matching_variable", "Epoch"]], 
				   on = "db_matching_variable", how="inner")
		df__ = df__.rename(columns={"Epoch_x" :  "new_Epoch", "Epoch_y" : "old_Epoch"})
		x = df__.apply(lambda x: (np.where(x.db_matching_variable == newDf.db_matching_variable)[0][0]), axis=1)
		newDf.loc[x, "Epoch"] = list(df__.old_Epoch)

		pickle.dump(newDf, open(new_folderPath+'/'+file_name+'.pkl', 'wb'))

		if(len(df)):
			pickle.dump(df, open('temporary/'+file_name+"_"+str(temporaryFileCount)+'.pkl', 'wb'))
			temporaryFileCount+=1
		elif(len(df) == 0):
			source = 'old/'+oldFolderName+'/'+file_name+'.pkl'
			os.remove(new_folderPath+'/'+file_name+'.pkl')
			if(oldFolderName):
				shutil.move(source, new_folderPath)

	# if(cmd_print):
		# print(".....................MERGE............................")
		# merge_pickle_one_table_files(file_name)
		# print(".....................MERGE............................")
	return None

def convert_txt_to_csv():
	print("____________convert_txt_to_csv____________()")
	global cmd_print
	global oldFolderName
	global new_folderPath
	global old_folderPath
	global tmpOldFolderName

	try:
		shutil.rmtree(data+"temporary/")
	except: 
		pass
	os.mkdir(data+"temporary/")

	try:
		shutil.rmtree(data+"dbpush/")
	except:
		pass
	os.mkdir(data+"dbpush/")
	count_ = 0
	M__ = 0

	for eachFile in Id_df['filePath']:
		#print("M__ : ", M__)
		#if M__ == 5:
			#break
		count_ += 1
		if count_ % 500 == 0:
			print("\n\n *******************",  count_ , "files processes Successfully\n\n")
		loopBreaker = False
		second_loop_break = False
		oldFolderName = ""
		new_folderPath = ""
		old_folderPath = ""
		if not os.path.exists('raw_synced_data/'+eachFile): 
			open('logs/unavailable_zip_folders.txt','a').write(eachFile.replace("_unzip",".zip").replace("raw_synced_data/","") + '\n')
			continue
		eachFile = "raw_synced_data/" + eachFile.replace(".zip", "_unzip")
		
		if(os.path.exists("logs/processed_files.pkl")):
			files_processed = pickle.load(open("logs/processed_files.pkl", "rb"))
		else:
			files_processed = []
		if(eachFile in files_processed):
			continue
		else:
			files_processed += [eachFile]
		corruptFolderCheck = unzip(eachFile)
		if(corruptFolderCheck):
			continue
		time_start_get_id_and_date = time.time()
		zip_file_name = eachFile.split('/')[-1].replace("_unzip", "")
		try:
			id_, datetime = get_id_and_date_time(zip_file_name)
		except:
			open("logs/zipfile_exist_but_no_entry_in_processIDs.csv", "a").write(eachFile.replace("_unzip", ".zip"))
			continue
		arr_get_id_and_date_time.append(str(time.time() - time_start_get_id_and_date))
		if os.path.exists('logs/error_logs.txt'):
			errorLogsList = open('logs/error_logs.txt','r').readlines()
			for error in errorLogsList:
				if((id_ + ',') in error):
					open('logs/unprocessed_file_due_to_error.txt','a').write(id_ + ',' + eachFile.replace("_unzip",".zip").replace("raw_synced_data/","") + '\n')
					loopBreaker = True
					break
		if(loopBreaker):
			shutil.rmtree(eachFile)
			continue

		print("======================================================================================")
		print(eachFile)
		print("======================================================================================")

		tmpOldFolderName = None

		# for file_name in ["location","contacts_list"]:
		for file_name in ["contacts_list", "ext_storage_files", "location", "outgoing_call_log", "sms_log", "sms_sent_log", "accounts_list", "app_install_log", "call_log", "filter_app_log", "calendar_events", "master", "gallery_data"]:
			cmd_print = True
			# if(file_name == 'contacts_list'):
				# cmd_print = True
			# else:
				# cmd_print = False
			if second_loop_break:
				continue
			time_calculate_difference = time.time()
			errorCheck = calculate_difference(eachFile, file_name, id_, datetime)
			arr_calculate_difference.append(str(time.time() - time_calculate_difference))
			if(errorCheck):
				open('logs/error_logs.txt', 'a').write(id_ + ',' + file_name + ',' + eachFile.replace("_unzip",".zip").replace("raw_synced_data/","") + '\n')
				second_loop_break = True
				continue
		shutil.rmtree(eachFile)
		if second_loop_break:
			continue
		pickle.dump(files_processed, open('logs/processed_files.pkl','wb'))

		if ((oldFolderName) or (tmpOldFolderName)):
			shutil.rmtree('old/'+tmpOldFolderName)
		if (not new_folderPath) and (not old_folderPath):
			open("logs/empty_all_files.txt", "a").write(eachFile.replace("_unzip", ".zip").lstrip("raw_synced_data/"))
		else:
			shutil.move(new_folderPath, old_folderPath)
		M__ += 1
	try:
		shutil.rmtree(data+"dbpush")
	except:
		pass

	shutil.move(data+"temporary/", data+"dbpush/")
	try:
		shutil.rmtree(data+"temporary")
	except OSError as e:
		pass
	os.mkdir(data+"temporary")

def merge_pickles():
	print("\n\n\n____________________merge_pickles()_______________________________________\n\n")
	import os
	files = os.listdir(data + "dbpush/")
	tables_names = ['accounts_list', 'app_install_log', 'calendar_events', 'call_log',\
			 'contacts_list', 'ext_storage_files', 'filter_app_log', 'gallery_data',\
			 'location', 'master', 'outgoing_call_log', 'sms_log', 'sms_sent_log']
	for table_name in tables_names:
		list_of_dfs = []
		L = [file for file in files if file.startswith(table_name)]
		if not L:
			continue
		print(table_name)
		LL = [int(i.split("_")[-1].replace(".pkl", "")) for i in L]
		L = list(zip(LL, L))
		L.sort(key = lambda x: x[0])
		L = [i[1] for i in L]
		for file in L:
			list_of_dfs.append(
				pickle.load(open(data + 'dbpush/'+file, "rb"))
			)
		df = pd.concat(list_of_dfs)
		pickle.dump(
			df, open(data + "dbpush/"+table_name + ".pkl", 'wb')
		)
		# jab choti files ko use kar bari 1 file ban jay tab sari choti files ko remove kar den
		for i in L:
			os.remove(data + "dbpush/" + i)

def GetQery(df, table_name):
	if(table_name in ['sms_sent_log','outgoing_call_log']):
		query = f"""
		INSERT INTO {table_name} {str(tuple(df.columns)).replace("'", "")}
			VALUES
			{str(df.values.tolist()).replace("[", "(").replace("]", ")")}""".replace("\n", "").replace("((", "(").replace("))", ")").strip()
	elif (table_name == 'location'):
		query = f"""
				INSERT INTO {table_name} {str(tuple(df.columns)).replace("'", "")}
					VALUES
					{str(df.values.tolist()).replace("[", "(").replace("]", ")")}
					ON DUPLICATE KEY UPDATE isActive = '0'""".replace("\n", "").replace("((", "(").replace("))", ")").strip()
	else:
		query = f"""
		INSERT INTO {table_name} {str(tuple(df.columns)).replace("'", "")}
			VALUES
			{str(df.values.tolist()).replace("[", "(").replace("]", ")")}
			ON DUPLICATE KEY UPDATE isActive = '0', end_date='{today__}'""".replace("\n", "").replace("((", "(").replace("))", ")").strip()	
	return query

def push_data_to_db():
	print("__________________push_data_to_db()_________________________")
	filePaths = os.listdir("dbpush/")
	conn = CONNECT()
	for file in	filePaths:
		df = pickle.load(open(data + "dbpush/" + file, "rb"))
		LEN_DF = len(df)
		df.index = list(range(len(df)))
		for i in df.columns:
			df[i] = df[i].astype(str).str.replace("'",'').str.replace("\\","").str.replace("\n","").str.replace('%','').str.replace('\r','')
		print("\n\nfile : ", file)
		table_name = file.replace(".pkl", "")
		if(table_name in ['sms_sent_log','outgoing_call_log']):
			df = df.drop_duplicates(keep = 'first', subset=columns_for_matcing_dict[table_name]+["Epoch"])
		chunc_size = 100000
		chuncs_completed = 0
		if len(df) > chunc_size:
			for i in range(0, len(df), chunc_size):
				df_temp = df.iloc[i:i+chunc_size]
				query = GetQery(df_temp, table_name)
				print("df_temp Query size (MB) : ", getsizeof(query)/1000000, 
					"completed from long df: ", chuncs_completed * chunc_size,
					"df_temp shape : ", df_temp.shape,
					"df length : ", LEN_DF)
				conn.execute(query)
				chuncs_completed += 1

		else:
			query = GetQery(df, table_name)
			print("df Query size (MB): ", getsizeof(query)/1000000, "df shape : ", df.shape)
			conn.execute(query)
	# to be uncommented
	shutil.move(data+"dbpush/", data+"History/" + str(round(time.time())))

#====================================== just for debugging purposes ======================================= 
#==========================================================================================================

def merge_pickle_one_table_files(table_name, print_only_specifit_columns='all'):
	import os
	files = os.listdir("temporary/")
	# print("iles : ", files)
	# # print()
	# print(table_name)

	list_of_dfs = []
	L = [file for file in files if file.startswith(table_name)]
	if not L:
		if(cmd_print):
			print("There no .pkl file for "+ table_name)
		return 
	LL = [int(i.split("_")[-1].replace(".pkl", "")) for i in L]
	L = list(zip(LL, L))
	L.sort(key = lambda x: x[0])
	L = [i[1] for i in L]
	for file in L:
		list_of_dfs.append(
			pickle.load(open('temporary/'+file, "rb"))
		)
	df = pd.concat(list_of_dfs)
	# if(cmd_print):
	# 	print("\nFinal DB Push DF :  ==================================== :")
	# 	print(df[['db_matching_variable','Epoch']])



def read_table_into_df(table_name):
	conn = CONNECT()
	res = conn.execute('select * from {}'.format(table_name))
	df = pd.DataFrame(res.fetchall())
	df.columns = res.keys()
	print(len(df))
	a = df.groupby('db_matching_variable').agg('count')['ID']
	b = a[a > 1]
	for i in b.index:
		print(df[df.db_matching_variable == i][['db_matching_variable','Epoch','isActive']])
		print("***************************************************")


#if ((os.path.exists("/home/amir/github")) or (os.path.exists("/home/lfd/project"))): # for debuggin, amir local par contition True ho gi, taky exeception catch na ho or error a jay, taky usy solve kya jay
#	data = ''
# 	# read_table_into_df('call_log')
# 	convert_txt_to_csv()
# 	merge_pickles()
# 	push_data_to_db()
# else:
data = '/data/'
#d = download_process_IDs()
#if d == "Error":
	#print("\n\nError occured while downloading proceced_IDs.csv file\n")
	#sys.exit()
#-------------------------------------------------------------------------------------
time_convert_txt_to_csv = time.time()
try:
	#convert_txt_to_csv()
	pass
except:
	# error_ = '"' + traceback.format_exc().replace('"', "'") + '"'.strip()
	# command = "./email convert_txt_to_csv-------------\n" + error_
	error_ = traceback.format_exc().replace('"', "'").strip()
	command = """./email convert_txt_to_csv -------------
	
	
	
	""" + error_
	command = command[:8] + '"' + command[8:] + '"'
	if error_email:
		os.system(command)
	else:
		print(command.replace("./email", ""))
	sys.exit()
arr_convert_txt_to_csv.append(str(time.time() - time_convert_txt_to_csv))
#-------------------------------------------------------------------------------------
time_merge_pickles = time.time()
try:
	#merge_pickles()
	pass
except:
	# error_ = '"' + traceback.format_exc().replace('"', "'") + '"'.strip()
	# command = "./email merge_pickles-------------\n" + error_
	error_ = traceback.format_exc().replace('"', "'").strip()
	command = """./email merge_pickles -------------
	
	
	
	""" + error_
	command = command[:8] + '"' + command[8:] + '"'

	if error_email:
		os.system(command)
	else:
		print(command.replace("./email", ""))
	sys.exit()
arr_merge_pickles.append(str(time.time() - time_merge_pickles))
#-------------------------------------------------------------------------------------
time_push_data_to_db = time.time()
try:
	push_data_to_db()
	pass
except:
	error_ = '"' + traceback.format_exc().replace('"', "'") + '"'.strip()
	e_start = error_[:8000]
	e_end =  error_[-8000:]
	error_ = "-------Error Start----------\n" + e_start + "\n--------Error End----------\n" + e_end
	# command = "./email push_data_to_db-------------\n" + error_
	command = """./email push_data_to_db -------------
	
	
	
	""" + error_
	command = command[:8] + '"' + command[8:] + '"'
	if error_email:
		os.system(command)
	else:
		print(command.replace("./email", ""))
	sys.exit()
arr_push_data_to_db.append(str(time.time() - time_push_data_to_db))




#-------------------------------------------------------------------------------------

# print("arr_get_new_df : ",          sum([float(i) for i in arr_get_new_df]))
# print("arr_get_old_df : ",          sum([float(i) for i in arr_get_old_df]))
# print("arr_get_id_and_date_time : ",sum([float(i) for i in arr_get_id_and_date_time]))
# print("arr_calculate_difference : ",sum([float(i) for i in arr_calculate_difference]))
# print("arr_convert_txt_to_csv : ",  sum([float(i) for i in arr_convert_txt_to_csv]))
# print("arr_merge_pickles : ",       sum([float(i) for i in arr_merge_pickles]))
# print("arr_push_data_to_db : ",     sum([float(i) for i in arr_push_data_to_db]))

# open("logs/arr_get_new_df.txt",'w').write("\n".join(arr_get_new_df))
# open("logs/arr_get_old_df.txt",'w').write("\n".join(arr_get_old_df))
# open("logs/arr_get_id_and_date_time.txt",'w').write("\n".join(arr_get_id_and_date_time))
# open("logs/arr_calculate_difference.txt",'w').write("\n".join(arr_calculate_difference))
# open("logs/arr_convert_txt_to_csv.txt",'w').write("\n".join(arr_convert_txt_to_csv))
# open("logs/arr_merge_pickles.txt",'w').write("\n".join(arr_merge_pickles))
# open("logs/arr_push_data_to_db.txt",'w').write("\n".join(arr_push_data_to_db))


print("Total runtime : ", time.time()-start)
print("_________________________________\n")


