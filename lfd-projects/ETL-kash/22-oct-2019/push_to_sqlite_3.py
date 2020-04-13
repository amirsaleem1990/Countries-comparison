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
	import sqlite3
	import pandas as pd
	import datetime
	
	current_time = str(datetime.datetime.now())

	conn = sqlite3.connect('/home/amir/github/LFD-projects/ETL-cash-egypt/22-oct-2019/ETL.db')  
	c = conn.cursor()
	c.execute("SELECT name FROM sqlite_master WHERE type='table';")
	tables = c.fetchall()
	tables_names = [table[0] for table in tables]

	ID = folder_name.replace("phoneRawDataFile-", "").replace(".zip", "")

	if len(df_sms_log) > 0:
		if "ID" in df_sms_log.columns:
			df_sms_log.drop("ID", axis=1, inplace=True)
		table_name = "sms_log"
		# # print(folder_name, table_name)
		if table_name in tables_names:
			df_SQL_exist = pd.read_sql_query("SELECT * FROM " + table_name, conn)
			df_SQL_exist.drop(["Date & time", "ID"], axis=1, inplace=True)
			# srif wo records jo <df_sms_log> me hen lekin <df_SQL_exist> me nahi
			only_new = df_sms_log[~df_sms_log.isin(df_SQL_exist.to_dict('l')).all(1)]
			if len(only_new) > 0:
				only_new['ID'] = ID
				only_new = only_new[["ID"] + only_new.columns[:-1].to_list()]
				only_new["Date & time"] = current_time
				# # print(len(only_new))
				only_new.to_sql(table_name, conn, index=False, if_exists='append')
		else:
			df_sms_log['ID'] = ID
			df_sms_log = df_sms_log[["ID"] + [column for column in df_sms_log.columns if column != "ID"]]
			df_sms_log["Date & time"] = current_time
			df_sms_log.to_sql(table_name, conn, index=False, if_exists='append')

		# df_sms_log['ID'] = ID
		# df_sms_log = df_sms_log[["ID"] + [column for column in df_sms_log.columns if column != "ID"]]
		# df_sms_log.to_sql('sms_log', conn, index=False, if_exists='append')
	if len(df_outgoing_call_log) > 0:
		if "ID" in df_outgoing_call_log.columns:
			df_outgoing_call_log.drop("ID", axis=1, inplace=True)
		table_name = "outgoing_call_log"
		# # print(folder_name, table_name)
		if table_name in tables_names:
			df_SQL_exist = pd.read_sql_query("SELECT * FROM " + table_name, conn)
			df_SQL_exist.drop(["Date & time", "ID"], axis=1, inplace=True)
			# srif wo records jo <df_outgoing_call_log> me hen lekin <df_SQL_exist> me nahi
			only_new = df_outgoing_call_log[~df_outgoing_call_log.isin(df_SQL_exist.to_dict('l')).all(1)]
			if len(only_new) > 0:
				only_new['ID'] = ID
				only_new = only_new[["ID"] + only_new.columns[:-1].to_list()]
				only_new["Date & time"] = current_time
				# print(len(only_new))
				only_new.to_sql(table_name, conn, index=False, if_exists='append')
			# df_outgoing_call_log['ID'] = ID
			# df_outgoing_call_log = df_outgoing_call_log[["ID"] + [column for column in df_outgoing_call_log.columns if column != "ID"]]
			# df_outgoing_call_log.to_sql('outgoing_call_log', conn, index=False, if_exists='append')
		else:
			df_outgoing_call_log['ID'] = ID
			df_outgoing_call_log = df_outgoing_call_log[["ID"] + [column for column in df_outgoing_call_log.columns if column != "ID"]]
			df_outgoing_call_log["Date & time"] = current_time
			df_outgoing_call_log.to_sql(table_name, conn, index=False, if_exists='append')
	
	if len(df_accounts_list) > 0:
		if "ID" in df_accounts_list.columns:
			df_accounts_list.drop("ID", axis=1, inplace=True)
		table_name = "accounts_list"
		# print(folder_name, table_name)
		if table_name in tables_names:
			df_SQL_exist = pd.read_sql_query("SELECT * FROM " + table_name, conn)
			df_SQL_exist.drop(["Date & time", "ID"], axis=1, inplace=True)
			# srif wo records jo <df_accounts_list> me hen lekin <df_SQL_exist> me nahi
			only_new = df_accounts_list[~df_accounts_list.isin(df_SQL_exist.to_dict('l')).all(1)]
			if len(only_new) > 0:
				only_new['ID'] = ID
				only_new = only_new[["ID"] + only_new.columns[:-1].to_list()]
				only_new["Date & time"] = current_time
				# print(len(only_new))
				only_new.to_sql(table_name, conn, index=False, if_exists='append')
			# df_accounts_list['ID'] = ID
			# df_accounts_list = df_accounts_list[["ID"] + [column for column in df_accounts_list.columns if column != "ID"]]
			# df_accounts_list.to_sql('accounts_list', conn, index=False, if_exists='append')
		else:
			df_accounts_list['ID'] = ID
			df_accounts_list = df_accounts_list[["ID"] + [column for column in df_accounts_list.columns if column != "ID"]]
			df_accounts_list["Date & time"] = current_time
			df_accounts_list.to_sql(table_name, conn, index=False, if_exists='append')
	if len(df_filter_app_log) > 0:
		if "ID" in df_filter_app_log.columns:
			df_filter_app_log.drop("ID", axis=1, inplace=True)
		table_name = "filter_app_log"
		# print(folder_name, table_name)
		if table_name in tables_names:
			df_SQL_exist = pd.read_sql_query("SELECT * FROM " + table_name, conn)
			df_SQL_exist.drop(["Date & time", "ID"], axis=1, inplace=True)
			# srif wo records jo <df_filter_app_log> me hen lekin <df_SQL_exist> me nahi
			only_new = df_filter_app_log[~df_filter_app_log.isin(df_SQL_exist.to_dict('l')).all(1)]
			if len(only_new) > 0:
				only_new['ID'] = ID
				only_new = only_new[["ID"] + only_new.columns[:-1].to_list()]
				only_new["Date & time"] = current_time
				# print(len(only_new))
				only_new.to_sql(table_name, conn, index=False, if_exists='append')
			# df_filter_app_log['ID'] = ID
			# df_filter_app_log = df_filter_app_log[["ID"] + [column for column in df_filter_app_log.columns if column != "ID"]]
			# df_filter_app_log.to_sql('filter_app_log', conn, index=False, if_exists='append')
		else:
			df_filter_app_log['ID'] = ID
			df_filter_app_log = df_filter_app_log[["ID"] + [column for column in df_filter_app_log.columns if column != "ID"]]
			df_filter_app_log["Date & time"] = current_time
			df_filter_app_log.to_sql(table_name, conn, index=False, if_exists='append')

	if len(df_calendar_events) > 0:
		if "ID" in df_calendar_events.columns:
			df_calendar_events.drop("ID", axis=1, inplace=True)
		table_name = "calendar_events"
		# print(folder_name, table_name)
		if table_name in tables_names:
			df_SQL_exist = pd.read_sql_query("SELECT * FROM " + table_name, conn)
			df_SQL_exist.drop(["Date & time", "ID"], axis=1, inplace=True)
			# srif wo records jo <df_calendar_events> me hen lekin <df_SQL_exist> me nahi
			only_new = df_calendar_events[~df_calendar_events.isin(df_SQL_exist.to_dict('l')).all(1)]
			if len(only_new) > 0:
				only_new['ID'] = ID
				only_new = only_new[["ID"] + only_new.columns[:-1].to_list()]
				only_new["Date & time"] = current_time
				# print(len(only_new))
				only_new.to_sql(table_name, conn, index=False, if_exists='append')
			# df_calendar_events['ID'] = ID
			# df_calendar_events = df_calendar_events[["ID"] + [column for column in df_calendar_events.columns if column != "ID"]]
			# df_calendar_events.to_sql('calendar_events', conn, index=False, if_exists='append')
		else:
			df_calendar_events['ID'] = ID
			df_calendar_events = df_calendar_events[["ID"] + [column for column in df_calendar_events.columns if column != "ID"]]
			df_calendar_events["Date & time"] = current_time
			df_calendar_events.to_sql(table_name, conn, index=False, if_exists='append')

	if len(df_location) > 0:
		if "ID" in df_location.columns:
			df_location.drop("ID", axis=1, inplace=True)
		table_name = "location"
		# print(folder_name, table_name)
		if table_name in tables_names:
			df_SQL_exist = pd.read_sql_query("SELECT * FROM " + table_name, conn)
			df_SQL_exist.drop(["Date & time", "ID"], axis=1, inplace=True)
			# srif wo records jo <df_location> me hen lekin <df_SQL_exist> me nahi
			only_new = df_location[~df_location.isin(df_SQL_exist.to_dict('l')).all(1)]
			if len(only_new) > 0:
				only_new['ID'] = ID
				only_new = only_new[["ID"] + only_new.columns[:-1].to_list()]
				only_new["Date & time"] = current_time
				# print(len(only_new))
				only_new.to_sql(table_name, conn, index=False, if_exists='append')
			# df_location['ID'] = ID
			# df_location = df_location[["ID"] + [column for column in df_location.columns if column != "ID"]]
			# df_location.to_sql('location', conn, index=False, if_exists='append')
		else:
			df_location['ID'] = ID
			df_location = df_location[["ID"] + [column for column in df_location.columns if column != "ID"]]
			df_location["Date & time"] = current_time
			df_location.to_sql(table_name, conn, index=False, if_exists='append')

	if len(df_galary_data) > 0:
		if "ID" in df_galary_data.columns:
			df_galary_data.drop("ID", axis=1, inplace=True)
		table_name = "galary_data"
		# print(folder_name, table_name)
		if table_name in tables_names:
			df_SQL_exist = pd.read_sql_query("SELECT * FROM " + table_name, conn)
			df_SQL_exist.drop(["Date & time", "ID"], axis=1, inplace=True)
			# srif wo records jo <df_galary_data> me hen lekin <df_SQL_exist> me nahi
			only_new = df_galary_data[~df_galary_data.isin(df_SQL_exist.to_dict('l')).all(1)]
			if len(only_new) > 0:
				only_new['ID'] = ID
				only_new = only_new[["ID"] + only_new.columns[:-1].to_list()]
				only_new["Date & time"] = current_time
				# print(len(only_new))
				only_new.to_sql(table_name, conn, index=False, if_exists='append')
			# df_galary_data['ID'] = ID
			# df_galary_data = df_galary_data[["ID"] + [column for column in df_galary_data.columns if column != "ID"]]
			# df_galary_data.to_sql('galary_data', conn, index=False, if_exists='append')
		else:
			df_galary_data['ID'] = ID
			df_galary_data = df_galary_data[["ID"] + [column for column in df_galary_data.columns if column != "ID"]]
			df_galary_data["Date & time"] = current_time
			df_galary_data.to_sql(table_name, conn, index=False, if_exists='append')

	if len(df_call_log) > 0:
		if "ID" in df_call_log.columns:
			df_call_log.drop("ID", axis=1, inplace=True)
		table_name = "call_log"
		# print(folder_name, table_name)
		if table_name in tables_names:
			df_SQL_exist = pd.read_sql_query("SELECT * FROM " + table_name, conn)
			df_SQL_exist.drop(["Date & time", "ID"], axis=1, inplace=True)
			# srif wo records jo <df_call_log> me hen lekin <df_SQL_exist> me nahi
			only_new = df_call_log[~df_call_log.isin(df_SQL_exist.to_dict('l')).all(1)]
			if len(only_new) > 0:
				only_new['ID'] = ID
				only_new = only_new[["ID"] + only_new.columns[:-1].to_list()]
				only_new["Date & time"] = current_time
				# print(len(only_new))
				only_new.to_sql(table_name, conn, index=False, if_exists='append')
			# df_call_log['ID'] = ID
			# df_call_log = df_call_log[["ID"] + [column for column in df_call_log.columns if column != "ID"]]
			# df_call_log.to_sql('call_log', conn, index=False, if_exists='append')
		else:
			df_call_log['ID'] = ID
			df_call_log = df_call_log[["ID"] + [column for column in df_call_log.columns if column != "ID"]]
			df_call_log["Date & time"] = current_time
			df_call_log.to_sql(table_name, conn, index=False, if_exists='append')

	if len(df_contacts_list) > 0:
		if "ID" in df_contacts_list.columns:
			df_contacts_list.drop("ID", axis=1, inplace=True)
		table_name = "contacts_list"
		# print(folder_name, table_name)
		if table_name in tables_names:
			df_SQL_exist = pd.read_sql_query("SELECT * FROM " + table_name, conn)
			df_SQL_exist.drop(["Date & time", "ID"], axis=1, inplace=True)
			# srif wo records jo <df_contacts_list> me hen lekin <df_SQL_exist> me nahi
			only_new = df_contacts_list[~df_contacts_list.isin(df_SQL_exist.to_dict('l')).all(1)]
			if len(only_new) > 0:
				only_new['ID'] = ID
				only_new = only_new[["ID"] + only_new.columns[:-1].to_list()]
				only_new["Date & time"] = current_time
				# print(len(only_new))
				only_new.to_sql(table_name, conn, index=False, if_exists='append')
			# df_contacts_list['ID'] = ID
			# df_contacts_list = df_contacts_list[["ID"] + [column for column in df_contacts_list.columns if column != "ID"]]
			# df_contacts_list.to_sql('contacts_list', conn, index=False, if_exists='append')
		else:
			df_contacts_list['ID'] = ID
			df_contacts_list = df_contacts_list[["ID"] + [column for column in df_contacts_list.columns if column != "ID"]]
			df_contacts_list["Date & time"] = current_time
			df_contacts_list.to_sql(table_name, conn, index=False, if_exists='append')

	if len(df_app_install_log) > 0:
		if "ID" in df_app_install_log.columns:
			df_app_install_log.drop("ID", axis=1, inplace=True)
		table_name = "app_install_log"
		# print(folder_name, table_name)
		if table_name in tables_names:
			df_SQL_exist = pd.read_sql_query("SELECT * FROM " + table_name, conn)
			df_SQL_exist.drop(["Date & time", "ID"], axis=1, inplace=True)
			# srif wo records jo <df_app_install_log> me hen lekin <df_SQL_exist> me nahi
			only_new = df_app_install_log[~df_app_install_log.isin(df_SQL_exist.to_dict('l')).all(1)]
			if len(only_new) > 0:
				only_new['ID'] = ID
				only_new = only_new[["ID"] + only_new.columns[:-1].to_list()]
				only_new["Date & time"] = current_time
				# print(len(only_new))
				only_new.to_sql(table_name, conn, index=False, if_exists='append')
			# df_app_install_log['ID'] = ID
			# df_app_install_log = df_app_install_log[["ID"] + [column for column in df_app_install_log.columns if column != "ID"]]
			# df_app_install_log.to_sql('app_install_log', conn, index=False, if_exists='append')
		else:
			df_app_install_log['ID'] = ID
			df_app_install_log = df_app_install_log[["ID"] + [column for column in df_app_install_log.columns if column != "ID"]]
			df_app_install_log["Date & time"] = current_time
			df_app_install_log.to_sql(table_name, conn, index=False, if_exists='append')

	if len(df_ext_storage_files) > 0:
		if "ID" in df_ext_storage_files.columns:
			df_ext_storage_files.drop("ID", axis=1, inplace=True)
		table_name = "ext_storage_files"
		# print(folder_name, table_name)
		if table_name in tables_names:
			df_SQL_exist = pd.read_sql_query("SELECT * FROM " + table_name, conn)
			df_SQL_exist.drop(["Date & time", "ID"], axis=1, inplace=True)
			# srif wo records jo <df_ext_storage_files> me hen lekin <df_SQL_exist> me nahi
			only_new = df_ext_storage_files[~df_ext_storage_files.isin(df_SQL_exist.to_dict('l')).all(1)]
			if len(only_new) > 0:
				only_new['ID'] = ID
				only_new = only_new[["ID"] + only_new.columns[:-1].to_list()]
				only_new["Date & time"] = current_time
				# print(len(only_new))
				only_new.to_sql(table_name, conn, index=False, if_exists='append')
			# df_ext_storage_files['ID'] = ID
			# df_ext_storage_files = df_ext_storage_files[["ID"] + [column for column in df_ext_storage_files.columns if column != "ID"]]
			# df_ext_storage_files.to_sql('ext_storage_files', conn, index=False, if_exists='append')
		else:
			df_ext_storage_files['ID'] = ID
			df_ext_storage_files = df_ext_storage_files[["ID"] + [column for column in df_ext_storage_files.columns if column != "ID"]]
			df_ext_storage_files["Date & time"] = current_time
			df_ext_storage_files.to_sql(table_name, conn, index=False, if_exists='append')

	if len(df_sms_sent_log) > 0:
		if "ID" in df_sms_sent_log.columns:
			df_sms_sent_log.drop("ID", axis=1, inplace=True)
		table_name = "sms_sent_log"
		# print(folder_name, table_name)
		if table_name in tables_names:
			df_SQL_exist = pd.read_sql_query("SELECT * FROM " + table_name, conn)
			df_SQL_exist.drop(["Date & time", "ID"], axis=1, inplace=True)
			# srif wo records jo <df_sms_sent_log> me hen lekin <df_SQL_exist> me nahi
			only_new = df_sms_sent_log[~df_sms_sent_log.isin(df_SQL_exist.to_dict('l')).all(1)]
			if len(only_new) > 0:
				only_new['ID'] = ID
				only_new = only_new[["ID"] + only_new.columns[:-1].to_list()]
				only_new["Date & time"] = current_time
				# print(len(only_new))
				only_new.to_sql(table_name, conn, index=False, if_exists='append')
			# df_sms_sent_log['ID'] = ID
			# df_sms_sent_log = df_sms_sent_log[["ID"] + [column for column in df_sms_sent_log.columns if column != "ID"]]
			# df_sms_sent_log.to_sql('sms_sent_log', conn, index=False, if_exists='append')
		else:
			df_sms_sent_log['ID'] = ID
			df_sms_sent_log = df_sms_sent_log[["ID"] + [column for column in df_sms_sent_log.columns if column != "ID"]]
			df_sms_sent_log["Date & time"] = current_time
			df_sms_sent_log.to_sql(table_name, conn, index=False, if_exists='append')

	
	conn = sqlite3.connect('/home/amir/github/LFD-projects/ETL-cash-egypt/22-oct-2019/Master.db')
	c = conn.cursor()
	df_master = pd.DataFrame(pd.concat([S_phone_battery_level, S_ip_address, S_device_info, S_storage_size])).T
	df_master["ID"] = [ID]
	df_master.columns = df_master.columns.map(str.strip).sort_values()
	print(df_master.columns)
	df_master.to_sql('Master', conn, index=False, if_exists='append')