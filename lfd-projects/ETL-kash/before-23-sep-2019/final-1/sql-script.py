import pandas as pd
import sqlite3
import json
import zipfile
import os

os.chdir("/home/amir/github/LFD-projects/ETL-cash-egypt/final-1")
import text_files_to_dataframes
os.chdir("/home/amir/github/LFD-projects/ETL-cash-egypt/MongoDB")

df_sms_log = text_files_to_dataframes.sms_log_func()
df_outgoing_call_log = text_files_to_dataframes.outgoing_call_log_func()
df_accounts_list = text_files_to_dataframes.accounts_list_func()
df_filter_app_log = text_files_to_dataframes.filter_app_log_func()
df_calendar_events = text_files_to_dataframes.calendar_events_func()
df_location = text_files_to_dataframes.location_func()
df_galary_data = text_files_to_dataframes.df_gallery_data_func()
df_call_log = text_files_to_dataframes.call_log_func()
df_contacts_list = text_files_to_dataframes.contacts_list_func()
df_app_install_log = text_files_to_dataframes.app_install_log_func()
df_ext_storage_files = text_files_to_dataframes.ext_storage_files_func()
df_sms_sent_log = text_files_to_dataframes.sms_sent_log_func()
S_phone_battery_level = text_files_to_dataframes.phone_battery_level_func()
S_ip_address = text_files_to_dataframes.ip_address_func()
S_device_info = text_files_to_dataframes.device_info_func()
S_storage_size = text_files_to_dataframes.storage_size_func()


conn = sqlite3.connect('/home/amir/ETL.db')  
c = conn.cursor()
# if_exists='append'

ID = input("Enter ID: ")

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

    
conn = sqlite3.connect('/home/amir/Master.db')  
c = conn.cursor()
df_master = pd.DataFrame(pd.concat([S_phone_battery_level, S_ip_address, S_device_info, S_storage_size])).T
df_master["ID"] = [ID]
df_master.to_sql('Master', conn, index=False, if_exists='append')