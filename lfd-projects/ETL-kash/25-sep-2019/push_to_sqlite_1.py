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
	conn = sqlite3.connect('/home/amir/github/LFD-projects/ETL-cash-egypt/25-sep-2019/ETL.db')  
	c = conn.cursor()

	ID = folder_name.replace("phoneRawDataFile-", "").replace(".zip", "")

	if len(df_sms_log) > 0:
	    df_sms_log['ID'] = ID
	    df_sms_log = df_sms_log[["ID"] + [column for column in df_sms_log.columns if column != "ID"]]
	    df_sms_log.to_sql('sms_log', conn, index=False, if_exists='append')
	if len(df_outgoing_call_log) > 0:
	    df_outgoing_call_log['ID'] = ID
	    df_outgoing_call_log = df_outgoing_call_log[["ID"] + [column for column in df_outgoing_call_log.columns if column != "ID"]]
	    df_outgoing_call_log.to_sql('outgoing_call_log', conn, index=False, if_exists='append')
	if len(df_accounts_list) > 0:
	    df_accounts_list['ID'] = ID
	    df_accounts_list = df_accounts_list[["ID"] + [column for column in df_accounts_list.columns if column != "ID"]]
	    df_accounts_list.to_sql('accounts_list', conn, index=False, if_exists='append')
	if len(df_filter_app_log) > 0:
	    df_filter_app_log['ID'] = ID
	    df_filter_app_log = df_filter_app_log[["ID"] + [column for column in df_filter_app_log.columns if column != "ID"]]
	    df_filter_app_log.to_sql('filter_app_log', conn, index=False, if_exists='append')
	if len(df_calendar_events) > 0:
	    df_calendar_events['ID'] = ID
	    df_calendar_events = df_calendar_events[["ID"] + [column for column in df_calendar_events.columns if column != "ID"]]
	    df_calendar_events.to_sql('calendar_events', conn, index=False, if_exists='append')
	if len(df_location) > 0:
	    df_location['ID'] = ID
	    df_location = df_location[["ID"] + [column for column in df_location.columns if column != "ID"]]
	    df_location.to_sql('location', conn, index=False, if_exists='append')
	if len(df_galary_data) > 0:
	    df_galary_data['ID'] = ID
	    df_galary_data = df_galary_data[["ID"] + [column for column in df_galary_data.columns if column != "ID"]]
	    df_galary_data.to_sql('galary_data', conn, index=False, if_exists='append')
	if len(df_call_log) > 0:
	    df_call_log['ID'] = ID
	    df_call_log = df_call_log[["ID"] + [column for column in df_call_log.columns if column != "ID"]]
	    df_call_log.to_sql('call_log', conn, index=False, if_exists='append')
	if len(df_contacts_list) > 0:
	    df_contacts_list['ID'] = ID
	    df_contacts_list = df_contacts_list[["ID"] + [column for column in df_contacts_list.columns if column != "ID"]]
	    df_contacts_list.to_sql('contacts_list', conn, index=False, if_exists='append')
	if len(df_app_install_log) > 0:
	    df_app_install_log['ID'] = ID
	    df_app_install_log = df_app_install_log[["ID"] + [column for column in df_app_install_log.columns if column != "ID"]]
	    df_app_install_log.to_sql('app_install_log', conn, index=False, if_exists='append')
	if len(df_ext_storage_files) > 0:
	    df_ext_storage_files['ID'] = ID
	    df_ext_storage_files = df_ext_storage_files[["ID"] + [column for column in df_ext_storage_files.columns if column != "ID"]]
	    df_ext_storage_files.to_sql('ext_storage_files', conn, index=False, if_exists='append')
	if len(df_sms_sent_log) > 0:
	    df_sms_sent_log['ID'] = ID
	    df_sms_sent_log = df_sms_sent_log[["ID"] + [column for column in df_sms_sent_log.columns if column != "ID"]]
	    df_sms_sent_log.to_sql('sms_sent_log', conn, index=False, if_exists='append')

	    
	conn = sqlite3.connect('/home/amir/github/LFD-projects/ETL-cash-egypt/25-sep-2019/Master.db')  
	c = conn.cursor()
	df_master = pd.DataFrame(pd.concat([S_phone_battery_level, S_ip_address, S_device_info, S_storage_size])).T
	df_master["ID"] = [ID]
	df_master.to_sql('Master', conn, index=False, if_exists='append')