import pandas as pd
import os
from pymongo import MongoClient

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

# first go to terminal and type <sudo mongod>
# https://api.mongodb.com/python/current/tutorial.html
#point the client
client = MongoClient('localhost', 27017)

#select database
db = client.temp5DB

# db.list_collection_names()
# db.tempTable.remove()

ID = input("Enter ID: ")

# -------------------------------------------------------------------------------------------------------
# DATAFRAMES
for collection in ['sms_log', 'outgoing_call_log', 'accounts_list', 'filter_app_log', 'calendar_events', 'location', 'galary_data', 'call_log', 'contacts_list', 'app_install_log', 'ext_storage_files', 'sms_sent_log']:
    mongoDB_exits = pd.DataFrame(list(db[collection].find()))
    if len(mongoDB_exits) > 0:
        mongoDB_exits = mongoDB_exits.drop(["ID", "_id"], axis=1)
    fresh_data = eval("df_" + collection)
#     fresh_data = [i[1].to_dict() for i in eval("df_" + collection).iterrows()]
    # srif wo records jo <fresh_data> me hen lekin <mongoDB_exits> me nahi
    only_new = fresh_data[~fresh_data.isin(mongoDB_exits.to_dict('l')).all(1)]
    if len(only_new) > 0:
        only_new["ID"] = ID 
        only_new_list_of_dicts = [i[1].to_dict() for i in only_new.iterrows()]
        db[collection].insert_many(only_new_list_of_dicts)
        

#---------------------------------------------------------------------------------------------------------
# SERIES
db = client.temp5DBMaster

mongoDB_exits = pd.DataFrame(list(db["Master"].find()))
if len(mongoDB_exits) > 0:
    mongoDB_exits = mongoDB_exits.drop(["ID", "_id"], axis=1)
fresh_data = pd.DataFrame(pd.concat([S_phone_battery_level, S_ip_address, S_device_info, S_storage_size])).T

# srif wo records jo <fresh_data> me hen lekin <mongoDB_exits> me nahi
only_new = fresh_data[~fresh_data.isin(mongoDB_exits.to_dict('l')).all(1)]
if len(only_new) > 0:
    only_new["ID"] = ID
    db["Master"].insert_one(only_new.loc[0].to_dict())