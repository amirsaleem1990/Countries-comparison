#!/usr/bin/python3
import time
import pandas as pd
import pickle
import os
import sys

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

conn = CONNECT()

def TEST1(table_name):
	global mainDf
	# SELECT db_matching_variable, process_date, process_hour, count(*)<br>
	# from kashat.{table}<br>
	# group by db_matching_variable, process_date , process_hour<br>
	# order by count(*) desc;
	start = time.time()
	mainDf["MINUTE"] = pd.to_datetime(mainDf.timestamp).dt.minute
	test1_actual = (mainDf[['db_matching_variable', 'process_date' , 'process_hour', "MINUTE"]] .duplicated()).sum()
	test1_expected = 0
	print("Expected: ", test1_expected)
	print("Actual  : ", test1_actual)
	if test1_actual == test1_expected:
		print("TEST 1 >>>>>>>>>>>> PASS")
	else:
		(mainDf[['db_matching_variable', 'process_date' , 'process_hour', 'MINUTE']] .duplicated()).to_csv("test1-"+table_name+".csv")
		print("TEST 1 >>>>>>>>>>>> FAIL")
	print("Test 1 should return count = 1 for all rows")
	print("time consumed: ", time.time() - start)
	print("---------------------------------\n")
	mainDf = mainDf.drop("MINUTE", axis=1)
def TEST2(table_name):
	# SELECT end_date, count(*)<br>
	# from kashat.{table}<br>
	# group by end_date<br>
	# order by count(*) desc;

	start = time.time()
	test2 = mainDf.end_date.value_counts()
	test2_check_1 = len(test2) > 1
	test2_check_2 = "2099-12-31" in test2.index

	print("Check 1: ", "Pass" if test2_check_1 else "Fail")
	print("Check 2: ", "Pass" if test2_check_2 else "Fail")

	if test2_check_1 and test2_check_2:
		print("TEST 2 >>>>>>>>>>>> PASS")
	else:
		(mainDf.end_date.value_counts()).to_csv("test2-"+table_name+".csv")
		print("TEST 2 >>>>>>>>>>>> FAIL")
		sys.exit()
	print("Test 2 should have multiple rows including row for '2099-12-31'")
	print("time consumed: ", time.time() - start)
	print("---------------------------------\n")

def TEST3(table_name):
	#select db_matching_variable , count(distinct concat(process_date,'-',process_hour))<br>
	#from {table}<br>
	#where isActive = 1<br>
	#group by db_matching_variable<br>
	#order by count(distinct concat(process_date,'-',process_hour)) desc

	start = time.time()
	test3 = mainDf[mainDf.isActive == "1"][['db_matching_variable', 'process_date' , 'process_hour']]
	test3["concat"] = test3["process_date"] + '-' + test3["process_hour"]
	test3 = test3[['db_matching_variable','concat']]
	test3 = test3.groupby('db_matching_variable').agg(pd.Series.nunique)
	expected_test_3 = 0
	actual_test3 = sum(test3.concat != 1)
	print("Expected: ", expected_test_3)
	print("Actual  : ", actual_test3)
	if expected_test_3 == actual_test3:
		print("TEST 3 >>>>>>>>>>>> PASS")
	else:
		(test3.concat != 1).to_csv("test3-"+table_name+".csv")
		print("TEST 3 >>>>>>>>>>>> FAIL")
		sys.exit()
	print("Expected: ", expected_test_3)
	print("Actual  : ", actual_test3)
	print("Test 3 should return count of 1 for all row")
	print("time consumed: ", time.time() - start)
	print("---------------------------------\n")

def TEST4(table_name):
	# drop temporary table if exists tempdb.{table}_test; create temporary table tempdb.{table}_test select db_matching_variable , max(timestamp) as max_ts from kashat.{table} group by db_matching_variable; drop temporary table if exists tempdb.{table}_test1; create temporary table tempdb.{table}_test1 select db_matching_variable , timestamp ts from kashat.{table} where isActive = 1; select a.* from tempdb.{table}_test a left join tempdb.{table}_test1 b on a.db_matching_variable = b.db_matching_variable where a.max_ts <> b.ts;
	start = time.time()
	t1 = mainDf[['db_matching_variable','timestamp']].\
			groupby(['db_matching_variable'], as_index=False)['timestamp'].max() 
	t1 = t1.rename(columns={"timestamp" : "max_ts"})
	
	t2 = mainDf[mainDf.isActive == 1][["db_matching_variable", "timestamp"]]
	t2 = t2.rename(columns={"timestamp" : "ts"})
	
	t3 = t2.merge(t1, how="left", on="db_matching_variable")
	t3 = t3[t3.max_ts != t3.ts]
	test4_expected = 0
	test4_actual = len(t3)
	print("Expected: ", test4_expected)
	print("Actual  : ", test4_actual)
	if test4_actual == test4_expected:
		print("TEST 4 >>>>>>>>>>>> PASS")
	else:
		t3.to_csv("test4-"+table_name+".csv")
		print("TEST 4 >>>>>>>>>>>> FAIL")
		sys.exit()
	print("time consumed: ", time.time() - start)
	print("---------------------------------\n")

for table in ["filter_app_log", "ext_storage_files", "master", "location", "accounts_list", "calendar_events", "app_install_log", "contacts_list", "sms_log", "call_log", "gallery_data"]:

	print("\n\n**********************************************")
	print("Table name : -------------------------------------------", table, '\n')
	
	SQL_Query = pd.read_sql_query(
		f"""SELECT * from kashat.{table}""", conn)
	mainDf = pd.DataFrame(SQL_Query)

	# ye is lye kya:
	
	# In [17]: df[df.db_matching_variable == '("whatsapp","instagram","messenger","imo")|()|()|("ana vodafone")|()|()|()|16112-17585'][["timestamp", "process_date", "process_hour", "MINUTE"]]
	# Out[17]:
	#				 timestamp process_date  process_hour  MINUTE
	# 7314  2020-03-17 13:54:32   2020-03-17			 5	  54
	# 7315  2020-03-17 21:54:20   2020-03-17			 5	  54

	mainDf["process_hour"] = pd.to_datetime(mainDf.timestamp).dt.hour.astype(str)


	print("len(mainDf): ", len(mainDf))

	print("\n------------TEST 1---------------")
	TEST1(table)

	if not table == "location":
		print("\n------------TEST 2---------------")
		TEST2(table)

	print("\n------------TEST 3---------------")
	TEST3(table)

	print("\n------------TEST 4---------------")
	TEST4(table)
