#!/usr/bin/python3
import os

import pandas as pd
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

print("outgoing_call_log :", pd.read_sql_query("SELECT * FROM outgoing_call_log" , conn)["ID"].nunique())
print("accounts_list     :", pd.read_sql_query("SELECT * FROM accounts_list" , conn)["ID"].nunique())
print("filter_app_log    :", pd.read_sql_query("SELECT * FROM filter_app_log" , conn)["ID"].nunique())
print("location          :", pd.read_sql_query("SELECT * FROM location" , conn)["ID"].nunique())
print("call_log          :", pd.read_sql_query("SELECT * FROM call_log" , conn)["ID"].nunique())
print("contacts_list     :", pd.read_sql_query("SELECT * FROM contacts_list" , conn)["ID"].nunique())
print("sms_sent_log      :", pd.read_sql_query("SELECT * FROM sms_sent_log" , conn)["ID"].nunique())
print("app_install_log   :", pd.read_sql_query("SELECT * FROM app_install_log" , conn)["ID"].nunique())
print("ext_storage_files :", pd.read_sql_query("SELECT * FROM ext_storage_files" , conn)["ID"].nunique())
print("calendar_events   :", pd.read_sql_query("SELECT * FROM calendar_events" , conn)["ID"].nunique())
print("master            :", pd.read_sql_query("SELECT * FROM master" , conn)["ID"].nunique())
print("sms_log           :", pd.read_sql_query("SELECT * FROM sms_log" , conn)["ID"].nunique())
print("gallery_data      :", pd.read_sql_query("SELECT * FROM gallery_data" , conn)["ID"].nunique())
