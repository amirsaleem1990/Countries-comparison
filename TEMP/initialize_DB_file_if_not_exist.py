#!/usr/bin/python3

import warnings
warnings.filterwarnings("ignore")
import os
import pandas as pd
import time
from sqlalchemy.dialects.mysql import LONGTEXT


def CONNECT():
	import sqlalchemy
	user = 'db_user'  #'newuser'
	passw = 'passw0rd' if os.getcwd().split("/")[2] == "amir" else "" #'password'
	host =  '127.0.0.1'
	port = 3306
	database = 'kashat' #'algo'
	conn = sqlalchemy.create_engine(f'mysql+pymysql://{user}:{passw}@{host}/{database}')
	conn = conn.connect()
	return conn


conn = CONNECT()

# import sys
# folder_name = sys.argv[1]

# import pickle
# import os

# dfList = []
# dbPushPKL = os.listdir("dbpush/")
# for eachFile in dbPushPKL:
#     if eachFile.endswith(".pkl"):
#         print("file name : ", eachFile)
#         # print("file name : ", eachFile)
#         with open("dbpush/"+eachFile, 'rb') as file:
#             a = pickle.load(file)
#             dfList.append(a.columns)
#             print(a.columns)
#             print("______________________")



# cols = ['Address', 'Body', 'Type', 'sendDate', 'senderName', 'ID', 'timestamp', 'process_date', 'process_hour', 'isActive', 'end_date', 'db_matching_variable', "Epoch"]
cols = ['Address', 'Body', 'Type', 'sendDate', 'senderName', 'ID', 'timestamp', 'Epoch', 'process_date', 'process_hour', 'isActive', 'end_date', 'db_matching_variable']
df_sms_log = pd.DataFrame(columns = cols)
dtypes = {} 
df_sms_log.to_sql(name="sms_log", con=conn, index=False, if_exists="replace", dtype=dtypes)

# cols = ['Date', 'Count', 'ID', 'timestamp', "Epoch", "db_matching_variable"]
cols = ['Date', 'Count', 'ID', 'timestamp', 'Epoch', "db_matching_variable"]
df_outgoing_call_log = pd.DataFrame(columns = cols)
dtypes = {} 
df_outgoing_call_log.to_sql(name="outgoing_call_log", con=conn, index=False, if_exists="replace", dtype=dtypes)

# cols = ['name', 'type', 'ID', 'timestamp', 'process_date', 'process_hour', 'isActive', 'end_date', 'db_matching_variable', "Epoch"]
cols = ['name', 'type', 'ID', 'timestamp', 'Epoch', 'process_date', 'process_hour', 'isActive', 'end_date', 'db_matching_variable']
df_accounts_list = pd.DataFrame(columns = cols)
dtypes = {} 
df_accounts_list.to_sql(name="accounts_list", con=conn, index=False, if_exists="replace", dtype=dtypes)

# cols =  ['social', 'dangerous', 'darkweb', 'wallet', 'banking', 'lending', 'rosca', 'ID', 'timestamp', 'process_date', 'process_hour', 'isActive', 'end_date', 'db_matching_variable', "Epoch"]
cols = ['social', 'dangerous', 'darkweb', 'wallet', 'banking', 'lending', 'rosca', 'ID', 'timestamp', 'Epoch', 'process_date', 'process_hour', 'isActive', 'end_date', 'db_matching_variable']
df_filter_app_log = pd.DataFrame(columns = cols)
dtypes = {} 
df_filter_app_log.to_sql(name="filter_app_log", con=conn, index=False, if_exists="replace", dtype=dtypes)

# cols = ['TimeStamp_', 'latitude', 'longitude', 'ID', 'timestamp', 'process_date', 'process_hour', 'isActive', 'db_matching_variable', "Epoch"]
cols = ['TimeStamp_', 'latitude', 'longitude', 'ID', 'timestamp', 'Epoch', 'process_date', 'process_hour', 'isActive', 'db_matching_variable']
df_location = pd.DataFrame(columns = cols)
dtypes = {} 
df_location.to_sql(name="location", con=conn, index=False, if_exists="replace", dtype=dtypes)

# cols = ['number', 'Type', 'Duration', 'ID', 'timestamp', 'process_date', 'process_hour', 'isActive', 'end_date', 'db_matching_variable', "Epoch"]
cols = ['number', 'Type', 'Duration', 'ID', 'timestamp', 'Epoch', 'process_date', 'process_hour', 'isActive', 'end_date', 'db_matching_variable']

df_call_log = pd.DataFrame(columns = cols)
dtypes = {} 
df_call_log.to_sql(name="call_log", con=conn, index=False, if_exists="replace", dtype=dtypes)

# cols = ['displayName', 'phone', 'ID', 'timestamp', 'process_date', 'process_hour', 'isActive', 'end_date', 'db_matching_variable', "Epoch"]
cols = ['displayName', 'phone', 'ID', 'timestamp', 'Epoch', 'process_date', 'process_hour', 'isActive', 'end_date', 'db_matching_variable']
df_contacts_list = pd.DataFrame(columns = cols)
dtypes = {} 
df_contacts_list.to_sql(name="contacts_list", con=conn, index=False, if_exists="replace", dtype=dtypes)

# cols = ['db_matching_variable', 'ID','date','count','timestamp', "Epoch"]
cols = ['date', 'count', 'ID', 'timestamp', 'Epoch', "db_matching_variable"]
df_sms_sent_log = pd.DataFrame(columns = cols)
dtypes = {} 
df_sms_sent_log.to_sql(name="sms_sent_log", con=conn, index=False, if_exists="replace", dtype=dtypes)

# cols = ['Package', 'ID', 'timestamp', 'process_date', 'process_hour', 'isActive', 'end_date', 'db_matching_variable', "Epoch"]
cols = ['Package', 'ID', 'timestamp', 'Epoch', 'process_date', 'process_hour', 'isActive', 'end_date', 'db_matching_variable']
df_app_install_log = pd.DataFrame(columns = cols)
dtypes = {} 
df_app_install_log.to_sql(name="app_install_log", con=conn, index=False, if_exists="replace", dtype=dtypes)


# cols = ['Downloads', 'Documents', 'Images', 'Videos', 'ID', 'timestamp', 'process_date', 'process_hour', 'isActive', 'end_date', 'db_matching_variable', "Epoch"]
cols = ['Downloads', 'Documents', 'Images', 'Videos', 'ID', 'timestamp', 'Epoch', 'process_date', 'process_hour', 'isActive', 'end_date', 'db_matching_variable']
df_ext_storage_files = pd.DataFrame(columns = cols)
dtypes = {i:LONGTEXT for i in df_ext_storage_files.columns} 
df_ext_storage_files.to_sql(name="ext_storage_files", con=conn, index=False, if_exists="replace", dtype=dtypes)

# cols = ['title', 'description', 'allowReminders', 'dTStart', 'dTEnd', 'allDay', 'ID', 'timestamp', 'process_date', 'process_hour', 'isActive', 'end_date', 'db_matching_variable', "Epoch"]
cols = ['title', 'description', 'allowReminders', 'dTStart', 'dTEnd', 'allDay', 'ID', 'timestamp', 'Epoch', 'process_date', 'process_hour', 'isActive', 'end_date', 'db_matching_variable']
df_calendar_events = pd.DataFrame(columns = cols)
dtypes = {} 
df_calendar_events.to_sql(name="calendar_events", con=conn, index=False, if_exists="replace", dtype=dtypes)

# cols = ['battery_level', 'ipaddress', 'model', 'manufacturer', 'brand', 'deviceSoftware', 'networkOperator', 'ram', 'storage_size', 'deviceWifiDataInBytes', 'deviceMobileDataInBytes', 'numberOfWifiStored', 'connectedConnectionName', 'timestamp', 'ID', 'process_date', 'process_hour', 'isActive', 'end_date', 'db_matching_variable', "Epoch"]
cols = ['battery_level', 'ipaddress', 'model', 'manufacturer', 'brand', 'deviceSoftware', 'networkOperator', 'ram', 'storage_size', 'deviceWifiDataInBytes', 'deviceMobileDataInBytes', 'numberOfWifiStored', 'connectedConnectionName', 'ID', 'timestamp', 'Epoch', 'process_date', 'process_hour', 'isActive', 'end_date', 'db_matching_variable']
df_master = pd.DataFrame(columns = cols)
dtypes = {} 
df_master.to_sql(name="master", con=conn, index=False, if_exists="replace", dtype=dtypes)


# cols = ["title", "timestamp", "path", "Epoch", 'ID', 'process_date', 'process_hour', 'isActive', 'end_date', 'db_matching_variable']
cols = ['title', 'timestamp', 'path', 'ID', 'Epoch', 'process_date', 'process_hour', 'isActive', 'end_date', 'db_matching_variable']
df_gallery_data = pd.DataFrame(columns = cols)
dtypes = {} 
df_gallery_data.to_sql(name="gallery_data", con=conn, index=False, if_exists="replace", dtype=dtypes)
