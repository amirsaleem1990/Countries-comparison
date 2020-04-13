import pandas as pd
import sqlite3
import pandas as pd
from datetime import datetime
import math
import os

def initialize_ETL_DB_file():

		conn = sqlite3.connect('/home/amir/github/LFD-projects/ETL-cash-egypt/23-oct-2019/ETL.db') 

		df_sms_log = pd.DataFrame(columns = ['ID','Address','Body','Type','sendDate','senderName','Date & time'])
		df_outgoing_call_log = pd.DataFrame(columns = ['ID','Date','Count','Date & time'])
		df_accounts_list = pd.DataFrame(columns = ['ID','name','type','Date & time'])
		df_filter_app_log = pd.DataFrame(columns = ['ID','social','dangerous','darkweb','wallet','banking','lending', 'rosca','Date & time'])
		df_location = pd.DataFrame(columns = ['ID','TimeStamp','latitude','longitude','Date & time'])
		df_call_log = pd.DataFrame(columns = ['ID','number','Type','Duration','Date & time'])
		df_contacts_list = pd.DataFrame(columns = ['ID','displayName','phone','Date & time'])
		df_sms_sent_log = pd.DataFrame(columns = ['ID','date','count','Date & time'])
		df_app_install_log = pd.DataFrame(columns = ['ID','Package','Date & time'])
		df_ext_storage_files = pd.DataFrame(columns = ['ID','Downloads','Documents','Images','Videos','Date & time'])
		df_calendar_events = pd.DataFrame(columns = ['ID','title','description','allowReminders','dTStart','dTEnd','allDay','Date & time'])

		df_sms_log.to_sql("sms_log", conn, index=False)
		df_outgoing_call_log.to_sql("outgoing_call_log", conn, index=False)
		df_accounts_list.to_sql("accounts_list", conn, index=False)
		df_filter_app_log.to_sql("filter_app_log", conn, index=False)
		df_location.to_sql("location", conn, index=False)
		df_call_log.to_sql("call_log", conn, index=False)
		df_contacts_list.to_sql("contacts_list", conn, index=False)
		df_sms_sent_log.to_sql("sms_sent_log", conn, index=False)
		df_app_install_log.to_sql("app_install_log", conn, index=False)
		df_ext_storage_files.to_sql("ext_storage_files", conn, index=False)
		df_calendar_events.to_sql("calendar_events", conn, index=False)

if not "ETL.db" in os.listdir():
	initialize_ETL_DB_file()


conn = sqlite3.connect('/home/amir/github/LFD-projects/ETL-cash-egypt/23-oct-2019/ETL.db')  
c = conn.cursor()
c.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = c.fetchall()
tables_names = [table[0] for table in tables]

def only_new_records(df, table_name, ID, Time):
	if len(df) > 0:
		if "Date & time" in df.columns:
			df.drop("Date & time", axis=1, inplace=True)
		if table_name in tables_names:
			df_SQL_exist = pd.read_sql_query("SELECT * FROM " + table_name, conn)
			df_SQL_exist.drop("Date & time", axis=1, inplace=True)
			# srif wo records jo <df> me hen lekin <df_SQL_exist> me nahi
			only_new = df[~df.isin(df_SQL_exist.to_dict('l')).all(1)]
			if len(only_new) > 0:
				only_new['ID'] = ID
				only_new = only_new[["ID"] + only_new.columns[:-1].to_list()]
				only_new["Date & time"] = Time
				only_new.columns = only_new.columns.map(str.strip)
				only_new.to_sql(table_name, conn, index=False, if_exists='append')
		else:
			df['ID'] = ID
			df = df[["ID"] + [column for column in df.columns if column != "ID"]]
			df["Date & time"] = Time
			df.columns = df.columns.map(str.strip)
			df.to_sql(table_name, conn, index=False, if_exists='append')

def Push_to_sqlite(folder_name, df_sms_log, 
					df_outgoing_call_log, 
					df_accounts_list, 
					df_filter_app_log, 
					df_calendar_events, 
					df_location, 
					df_galary_data, 
					df_call_log, 
					df_contacts_list, 
					df_app_install_log, 
					df_ext_storage_files, 
					df_sms_sent_log, 
					S_phone_battery_level, 
					S_ip_address, 
					S_device_info, 
					S_storage_size):

	
	ab = folder_name.replace("phoneRawDataFile-", "").replace(".zip", "").split("-")
	ID = ab[0]
	# Time = str(datetime.now())

	Time = datetime.fromtimestamp(math.floor(int(ab[1])/1000))	

	only_new_records(df_sms_log, "sms_log", ID, Time)
	only_new_records(df_outgoing_call_log, "outgoing_call_log", ID, Time)
	only_new_records(df_accounts_list, "accounts_list", ID, Time)
	only_new_records(df_filter_app_log, "filter_app_log", ID, Time)
	only_new_records(df_calendar_events, "calendar_events", ID, Time)
	only_new_records(df_location, "location", ID, Time)
	only_new_records(df_galary_data, "galary_data", ID, Time)
	only_new_records(df_call_log, "call_log", ID, Time)
	only_new_records(df_contacts_list, "contacts_list", ID, Time)
	only_new_records(df_app_install_log, "app_install_log", ID, Time)
	only_new_records(df_ext_storage_files, "ext_storage_files", ID, Time)
	only_new_records(df_sms_sent_log, "sms_sent_log", ID, Time)
	
	conn = sqlite3.connect('/home/amir/github/LFD-projects/ETL-cash-egypt/23-oct-2019/Master.db')
	c = conn.cursor()
	df_master = pd.DataFrame(pd.concat([S_phone_battery_level, S_ip_address, S_device_info, S_storage_size])).T
	df_master["ID"] = [ID]
	df_master.columns = df_master.columns.map(str.strip).sort_values()
	df_master.to_sql('Master', conn, index=False, if_exists='append')