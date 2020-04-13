import pandas as pd
import sqlite3
import json
import zipfile
import os
import numpy as np
import text_files_to_dataframes

os.chdir('/home/amir/github/LFD-projects/ETL-cash-egypt/4-sep-2019')
file_name = "Sample 2 - Sept 2.zip"
folder_name = file_name.replace(".zip", "")
if not folder_name in os.listdir():
    os.mkdir(folder_name)
    with zipfile.ZipFile(file_name, 'r') as zip_ref:
        zip_ref.extractall(folder_name)
os.chdir(folder_name)

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

for table_name, i in zip(["sms_log","outgoing_call_log","accounts_list","filter_app_log","calendar_events",
    "location","galary_data","call_log","contacts_list","app_install_log","ext_storage_files","sms_sent_log"], 
    [df_sms_log, df_outgoing_call_log, df_accounts_list, df_filter_app_log,df_calendar_events,df_location,
    df_galary_data,df_call_log,df_contacts_list,df_app_install_log,df_ext_storage_files,df_sms_sent_log]):
    if len(i) > 0:
        df_SQL_exist = pd.read_sql_query("SELECT * FROM " + table_name, conn)
        # srif wo records jo <i> me hen lekin <df_SQL_exist> me nahi
        only_new = i[~i.isin(df_SQL_exist.to_dict('l')).all(1)]
        only_new['ID'] = ["ID Sep 2(sample 2)" for z in range(len(only_new))]
        only_new = only_new[["ID"] + only_new.columns[:-1].to_list()]
        only_new.to_sql(table_name, conn, index=False, if_exists='append')
        
if len(S_phone_battery_level) == 0:
    S_phone_battery_level = pd.Series({"battery_level" : "NA"})

if len(S_ip_address) == 0:
    S_ip_address = pd.Series({"ipaddress" : "NA"})
    
if len(S_device_info) == 0:
    S_device_info = pd.Series({' brand': 'NA',
                             ' deviceSoftware': 'NA',
                             ' manufacturer': 'NA',
                             ' networkOperator': 'NA',
                             ' ram': 'NA',
                             'model': 'NA'})
    
if len(S_storage_size) == 0:
    S_storage_size = pd.Series({'storage_size': 'NA'})
    
conn = sqlite3.connect('/home/amir/Master.db')  
c = conn.cursor()

df_master = pd.DataFrame(pd.concat([S_phone_battery_level, S_ip_address, S_device_info, S_storage_size])).T
df_sql = pd.read_sql_query("SELECT * FROM Master", conn)

dm = df_master[[i for i in df_master.columns if i != "ID"]]
ds = df_sql[[i for i in df_sql.columns if i != "ID"]]

if not pd.merge(dm,ds).equals(dm):
    dm["ID"] = ["ID Sep 2(sample 2)"]
    dm.to_sql('Master', conn, index=False, if_exists='append')