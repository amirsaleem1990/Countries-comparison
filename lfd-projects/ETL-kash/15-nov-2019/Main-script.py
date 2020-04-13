import sys
sys.path.insert(0,'/home/amir/github/LFD-projects/ETL-cash-egypt/15-nov-2019')
import json
import zipfile
import os
from shutil import copyfile

from push_to_sqlite_4 import Push_to_sqlite
from push_to_sqlite_4 import Push_to_sqlite_Master_table
from push_to_MongoDB import Push_to_MongoDB


# os.system("rm *.db")
# if input("Empty trash [y\\n]   ") == "y":
	# os.system("del -rf ~/.local/share/Trash/files/*")
# os.system("rm $(ls -d */)")

os.chdir("/home/amir/github/LFD-projects/ETL-cash-egypt/raw_files")
import text_files_to_dataframes_2
zip_files_names = [i for i in os.listdir() if i.endswith(".zip")]

# zip_file = zip_files_names[0]
for zip_file in zip_files_names:
	folder_name = zip_file.replace(".zip", "")
	# print(folder_name +  ":   "  , "Starting ... ", end="   ")
	try:
		with zipfile.ZipFile(zip_file, 'r') as zip_ref:
		    zip_ref.extractall(folder_name)
	except:
		import os
		from text_files_to_dataframes_2 import folder_Corrupt
		folder_Corrupt(folder_name)
		continue
	# copyfile("text_files_to_dataframes_2.py", folder_name+"/text_files_to_dataframes_2.py")
	os.chdir(folder_name)
	# import text_files_to_dataframes
	# print(f"Starting {folder_name}", end="")
	
	df_sms_log = text_files_to_dataframes_2.sms_log_func(folder_name, "sms_log")
	df_outgoing_call_log = text_files_to_dataframes_2.outgoing_call_log_func(folder_name, "outgoing_call_log")
	df_accounts_list = text_files_to_dataframes_2.accounts_list_func(folder_name, "accounts_list")
	df_filter_app_log = text_files_to_dataframes_2.filter_app_log_func(folder_name, "filter_app_log")
	df_calendar_events = text_files_to_dataframes_2.calendar_events_func(folder_name, "calendar_events")
	df_location = text_files_to_dataframes_2.location_func(folder_name, "location")
	df_galary_data = text_files_to_dataframes_2.df_gallery_data_func(folder_name, "gallery_data")
	df_call_log = text_files_to_dataframes_2.call_log_func(folder_name, "call_log")
	df_contacts_list = text_files_to_dataframes_2.contacts_list_func(folder_name, "contacts_list")
	df_app_install_log = text_files_to_dataframes_2.app_install_log_func(folder_name, "app_install_log")
	df_ext_storage_files = text_files_to_dataframes_2.ext_storage_files_func(folder_name, "ext_storage_files")
	df_sms_sent_log = text_files_to_dataframes_2.sms_sent_log_func(folder_name, "sms_sent_log")

	S_phone_battery_level = text_files_to_dataframes_2.phone_battery_level_func(folder_name, "phone_battery_level")
	S_ip_address = text_files_to_dataframes_2.ip_address_func(folder_name, "ip_address")
	S_device_info = text_files_to_dataframes_2.device_info_func(folder_name, "device_info")
	S_storage_size = text_files_to_dataframes_2.storage_size_func(folder_name, "storage_size")

	Push_to_sqlite(folder_name, 
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
					df_sms_sent_log)
	Push_to_sqlite_Master_table(folder_name,
								S_phone_battery_level, 
								S_ip_address, 
								S_device_info, 
								S_storage_size)
	# Push_to_MongoDB(folder_name,
	# 				df_sms_log, 
	# 				df_outgoing_call_log, 
	# 				df_accounts_list, 
	# 				df_filter_app_log, 
	# 				df_calendar_events, 
	# 				df_location, 
	# 				df_galary_data, 
	# 				df_call_log, 
	# 				df_contacts_list, 
	# 				df_app_install_log, 
	# 				df_ext_storage_files, 
	# 				df_sms_sent_log, 
	# 				S_phone_battery_level, 
	# 				S_ip_address, 
	# 				S_device_info, 
	# 				S_storage_size)
	# print(".... OK")
	os.chdir("../")
	# print(", DONE:", folder_name)
os.system("del -rf $(ls -d */)")