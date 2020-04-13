import sqlite3
import pandas as pd
conn = sqlite3.connect('/home/amir/github/LFD-projects/ETL-kash/10-Dec-2019/db/ETL.db') 


df_sms_log = pd.read_sql_query("SELECT * FROM sms_log" , conn)
df_outgoing_call_log = pd.read_sql_query("SELECT * FROM outgoing_call_log" , conn)
df_accounts_list = pd.read_sql_query("SELECT * FROM accounts_list" , conn)
df_filter_app_log = pd.read_sql_query("SELECT * FROM filter_app_log" , conn)
df_location = pd.read_sql_query("SELECT * FROM location" , conn)
df_call_log = pd.read_sql_query("SELECT * FROM call_log" , conn)
df_contacts_list = pd.read_sql_query("SELECT * FROM contacts_list" , conn)
df_sms_sent_log = pd.read_sql_query("SELECT * FROM sms_sent_log" , conn)
df_app_install_log = pd.read_sql_query("SELECT * FROM app_install_log" , conn)
df_ext_storage_files = pd.read_sql_query("SELECT * FROM ext_storage_files" , conn)
df_calendar_events = pd.read_sql_query("SELECT * FROM calendar_events" , conn)

