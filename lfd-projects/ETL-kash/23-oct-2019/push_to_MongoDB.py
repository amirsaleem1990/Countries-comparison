def Push_to_MongoDB(folder_name,
					df_sms_log, 
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
	from pymongo import MongoClient
	import pandas as pd
	# first go to terminal and type <sudo mongod>
	# https://api.mongodb.com/python/current/tutorial.html
	#point the client
	client = MongoClient('localhost', 27017)

	#select database
	db = client.temp5DB

	# db.list_collection_names()
	# db.tempTable.remove()

	ID = folder_name

	# -------------------------------------------------------------------------------------------------------
	# DATAFRAMES
	for collection in ['sms_log', 'outgoing_call_log', 'accounts_list', 'filter_app_log', 'calendar_events', 'location', 'galary_data', 'call_log', 'contacts_list', 'app_install_log', 'ext_storage_files', 'sms_sent_log']:
	    mongoDB_exits = pd.DataFrame(list(db[collection].find()))
	    if len(mongoDB_exits) > 0:
	        mongoDB_exits = mongoDB_exits.drop(["ID", "_id"], axis=1)
	    fresh_data = eval("df_" + collection)
		# fresh_data = [i[1].to_dict() for i in eval("df_" + collection).iterrows()]
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