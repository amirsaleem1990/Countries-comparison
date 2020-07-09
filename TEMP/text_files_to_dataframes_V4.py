import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import json
import zipfile
import os
import pickle
import datetime
import copy
import numpy as np

contacts_list_func_default = pd.DataFrame(columns=['displayName', 'phone'])
ext_storage_files_func_default = pd.DataFrame(columns=['Downloads', 'Documents', 'Images', 'Videos'])
location_func_default = pd.DataFrame(columns=['TimeStamp_', 'latitude', 'longitude'])
outgoing_call_log_func_default = pd.DataFrame(columns=['Date', 'Count'])
sms_log_func_default = pd.DataFrame(columns=['Address', 'Body', 'Type', 'sendDate', 'senderName'])
sms_sent_log_func_default = pd.DataFrame(columns=["date", "count"])
accounts_list_func_default = pd.DataFrame(columns=['name', 'type'])
app_install_log_func_default = pd.DataFrame(columns=['Package'])
df_gallery_data_func_default = pd.DataFrame(columns=['title', 'timestamp', 'path'])
call_log_func_default = pd.DataFrame(columns=['number', 'Type', 'date', 'Duration'])
filter_app_log_func_default = pd.DataFrame(columns=['social', 'dangerous', 'darkweb', 'wallet', 'banking', 'lending', 'rosca'])
calendar_events_func_default = pd.DataFrame(columns=['title', 'description', 'allowReminders', 'dTStart', 'dTEnd', 'allDay'])

ip_address_func_default = pd.Series({"ipaddress" : np.nan})
phone_battery_level_func_default = pd.Series({"battery_level" : np.nan})
storage_size_func_default = pd.Series({'storage_size': np.nan})
device_info_func_default = pd.Series({'brand': np.nan, 
									'deviceSoftware': np.nan, 
									'manufacturer': np.nan,
									'networkOperator': np.nan, 
									'ram': np.nan, 
									'model': np.nan})
network_data_func_default = pd.Series({'deviceWifiDataInBytes'   : np.nan, 
							   'deviceMobileDataInBytes' : np.nan,
							   'numberOfWifiStored'      : np.nan,
							   'connectedConnectionName' : np.nan})

master_func_default = pd.DataFrame(columns=['battery_level','ipaddress','brand','deviceSoftware','manufacturer',
				'networkOperator','ram','model','deviceWifiDataInBytes','deviceMobileDataInBytes','numberOfWifiStored',
				'connectedConnectionName'])

LOG_FILE = "logs/logs.pkl"

def folder_Corrupt(folder_name):
  try:
	  with open(LOG_FILE, "rb") as file:
		  logs = pickle.load(file)
  except:
	  logs = {}

  existing_logs = copy.deepcopy(logs)

  if folder_name in logs:
	  logs[folder_name].append(("Corrupt_zip_file", "Corrupt folder error"))
  else:
	  logs[folder_name] = [("Corrupt_zip_file", "Corrupt folder error")]

  if logs != existing_logs:
	  with open(LOG_FILE, "wb") as file:
		  pickle.dump(logs, file) 


# def push_error(table_name):
#   try:
#       with open(LOG_FILE, "rb") as file:
#           logs = pickle.load(file)
#   except:
#       logs = {}

#   existing_logs = copy.deepcopy(logs)

#   logs[table_name] = [("SQL push error", "SQL push error")]

#   if logs != existing_logs:
#       with open(LOG_FILE, "wb") as file:
#           pickle.dump(logs, file)


def parsing_error(folder_name, file_name):
	try:
		with open(LOG_FILE, "rb") as file:
			logs = pickle.load(file)
	except:
		logs = {}

	existing_logs = copy.deepcopy(logs)

	if folder_name in logs:
		logs[folder_name].append((file_name, "Parsing error"))
	else:
		logs[folder_name] = [(file_name, "Parsing error")]

	if logs != existing_logs:
		with open(LOG_FILE, "wb") as file:
			pickle.dump(logs, file) 

def file_not_found(folder_name, file_name):
	try:
		with open(LOG_FILE, "rb") as file:
			logs = pickle.load(file)
	except:
		logs = {}

	existing_logs = copy.deepcopy(logs)

	if folder_name in logs:
		logs[folder_name].append((file_name, "File not found"))
	else:
		logs[folder_name] = [(file_name, "File not found")]

	if logs != existing_logs:
		with open(LOG_FILE, "wb") as file:
			pickle.dump(logs, file) 

def check_empty(folder_name, file_name):
	try:
		with open(LOG_FILE, "rb") as file:
			logs = pickle.load(file)
	except:
		logs = {}

	existing_logs = copy.deepcopy(logs)

	if folder_name in logs:
		logs[folder_name].append((file_name, "Empty file"))
	else:
		logs[folder_name] = [(file_name, "Empty file")]
	
	if logs != existing_logs:
		with open(LOG_FILE, "wb") as file:
			pickle.dump(logs, file) 

def contacts_list_func(folder_name, file_name):
	# contacts_list.txt
	try:
		contacts_list = open(folder_name + '/' + file_name, "r").read()
	except Exception as e:
		#print("file issue : ",e)
		file_not_found(folder_name, file_name)
		return contacts_list_func_default

	if not "=" in contacts_list:
		check_empty(folder_name, file_name)
		return contacts_list_func_default
	try:
		# a = eval(contacts_list)
		# dd = {"displayName" : [],
		# "phone" : []}
		# for b in a:
		#   if b.count("=") > 2:
		#       continue
		#   c = b.replace('"', "")
		#   c = c.replace(",", "").replace("\n", "").replace("phone", ", phone")
		#   c = c.replace("®", "")
		#   c = c.replace('" |', "|")
		#   c = c.replace('"|', "|")
		#   c = c.replace(" ", "").\
		#   replace("=", '":"').\
		#   replace("{", '{"').\
		#   replace("}", '"}').\
		#   replace(",", '","')
		#   c = c.replace('""', '"')
		#   if "," in c[c.find("phone")+8:]:
		#       c = c.replace('phone":"', 'phone":["').replace('"}', '"]}')
		#   if c.split("phone")[-1] == '":"}':
		#       c = c[:-2] + '"NaN"}'
		#   d = eval(c)
		#   dd["displayName"].append(d["displayName"])
		#   dd["phone"].append(d["phone"])  
		# df_contacts_list = pd.DataFrame.from_dict(dd)
		# df_contacts_list.phone = ["{" + ", ".join(i) + "}" if isinstance(i, list) else i for i in df_contacts_list.phone.to_list()]
		# if len(df_contacts_list) == 0:
		#   return None

		a = eval(contacts_list)
		dd = {"displayName" : [],
		"phone" : []}
		for b in a:
			if b.count("=") > 2:
				continue
			c = b.strip("{").strip("}").split("displayName=")[1:][0].split("phone=")
			displayName, phone = (i.replace(",", "").strip()  for i in c)
			dm = {"displayName" : displayName,
					"phone" : phone}
			for i in dm:
				dm[i].replace('"', "").replace(",", "").replace("\n", "").replace("phone", ", phone").replace("®", "").\
				replace('" |', "|").replace('"|', "|").replace(" ", "").replace("=", '":"').replace("{", '{"').\
				replace("}", '"}').replace(",", '","').replace('""', '"')
			dd["displayName"].append(dm["displayName"])
			dd["phone"].append(dm["phone"]) 
		df_contacts_list = pd.DataFrame.from_dict(dd)
		df_contacts_list.phone = ["{" + ", ".join(i) + "}" if isinstance(i, list) else i for i in df_contacts_list.phone.to_list()]
		df_contacts_list.columns = df_contacts_list.columns.str.strip()
		if len(df_contacts_list) == 0:
			return contacts_list_func_default
		else:
			return df_contacts_list
	except:
		parsing_error(folder_name, file_name)
		return None
# def ext_storage_files_func(folder_name, file_name):
# 	# ext_storage_files.tx
# 	try:
# 		ext_storage_files = open(folder_name + '/' + file_name, "r").read()
# 	except Exception as e:
# 		#print("file issue : ",e)
# 		file_not_found(folder_name, file_name)
# 		return ext_storage_files_func_default
	
# 	if not "=" in ext_storage_files:
# 		check_empty(folder_name, file_name)
# 		return ext_storage_files_func_default
# 	try:
# 		a = eval(ext_storage_files)[0]
# 		b = a.replace(',,','').\
# 			replace(', ,', ',').\
# 			replace('\\"', "").\
# 			replace('\\"', "").\
# 			replace('\"', "").\
# 			replace(')"', "").\
# 			replace(" ", "").\
# 			replace("=[", '":["').\
# 			replace(",", '","').\
# 			replace('""', '"').\
# 			replace(']",', '"],').\
# 			replace(':"[', ':["').\
# 			replace('["]', "[]" ).\
# 			replace('[""]', '[]')
# 		if (b[-3:] != '[]}') and (b[-2:] == ']}'):
# 			b = b[:-2] + '"]}'
# 		b = '{"' + b.lstrip("{")
# 		d = eval(b)
# 		df_ext_storage_files = pd.DataFrame()
# 		for i in d:
# 			if len(d[i]) == 1:
# 				df_ext_storage_files[i] = d[i]
# 			else:
# 				df_ext_storage_files[i] = ['|'.join(d[i])]
# 		df_ext_storage_files.columns = df_ext_storage_files.columns.str.strip()
# 		if len(df_ext_storage_files) == 0:
# 			return ext_storage_files_func_default
# 		else:
# 			return df_ext_storage_files
# 	except:
# 		parsing_error(folder_name, file_name)
# 		return None

def ext_storage_files_func(folder_name, file_name):
	# ext_storage_files.tx
	try:
		ext_storage_files = open(folder_name + '/' + file_name, "r").read()
	except Exception as e:
		#print("file issue : ",e)
		file_not_found(folder_name, file_name)
		return ext_storage_files_func_default
	if not "=" in ext_storage_files:
		check_empty(folder_name, file_name)
		return ext_storage_files_func_default
	try:
		a = eval(ext_storage_files)[0]
		b = a.replace(',,','').\
			replace('\r', '').\
			replace('\n', '').\
			replace('\xa0',' ').\
			replace(', ,', ',').\
			replace('\\"', "").\
			replace('\"', "").\
			replace("\'",'').\
			replace(')"', "").\
			replace(" ", "").\
			replace("=[", '":["').\
			replace(",", '","').\
			replace('""', '"').\
			replace(']",', '"],').\
			replace(':"[', ':["').\
			replace('["]', "[]" ).\
			replace('[""]', '[]')
			
		if (b[-3:] != '[]}') and (b[-2:] == ']}'):
			b = b[:-2] + '"]}'
		b = '{"' + b.lstrip("{")
		try:
			d = eval(b)
		except:
			import re
			actual_list = []
			pattern_list = []
			pattern_list = re.findall('\[[a-zA-Z]+\"\]',b)
			actual_list = [a.replace('"]',']"') for a in pattern_list]
			for i in range(0,len(pattern_list)):
				if(pattern_list[i] in b):
					b = b.replace(pattern_list[i],actual_list[i])
			d = eval(b)

		df_ext_storage_files = pd.DataFrame()
		for i in d:
			if len(d[i]) == 1:
				df_ext_storage_files[i] = d[i]
			else:
				df_ext_storage_files[i] = ['|'.join(d[i])]
		df_ext_storage_files.columns = df_ext_storage_files.columns.str.strip()
		if len(df_ext_storage_files) == 0:
			return ext_storage_files_func_default
		else:
			return df_ext_storage_files
	except:
		parsing_error(folder_name, file_name)
		return None



def ip_address_func(folder_name, file_name):
	# ip_address.txt
	try:
		ip_address = open(folder_name + '/' + file_name, "r").read()
	except Exception as e:
		#print("file issue : ",e)
		file_not_found(folder_name, file_name)
		return ip_address_func_default

	if not "=" in ip_address:
		check_empty(folder_name, file_name)
		return ip_address_func_default
	try:
		d = dict([eval(ip_address)[0].\
			  replace("{", "").\
			  replace("}", "").split("=")])
		S_ip_address = pd.Series(d)
		if len(S_ip_address) == 0:
			return ip_address_func_default
		else:
			return S_ip_address
	except:
		parsing_error(folder_name, file_name)
		return None

def location_func(folder_name, file_name):
	# location.txt
	try:
		location = open(folder_name + '/' + file_name, "r").read().strip().splitlines()
	except Exception as e:
		#print("file issue : ",e)
		file_not_found(folder_name, file_name)
		return location_func_default
	if not "=" in str(location):
		check_empty(folder_name, file_name)
		return location_func_default
	try:
		d = {"TimeStamp_" : [],
			"latitude" : [],
			"longitude" : []}
		for a in location:
			b = a.replace("[", "").\
				replace("]", "")
			c = eval(b).split("=")
			d["TimeStamp_"].append(c[0].replace("{", ""))
			d["latitude"].append(c[2].split(",")[0])
			d["longitude"].append(c[3].replace(")", "").replace("}", ""))
		df_location = pd.DataFrame(d)
		df_location.columns = df_location.columns.str.strip()
		if len(df_location) == 0:
			return location_func_default
		else:
			# #print(df_location.head())
			# #print(df_location.dtypes)
			# #print("____________$$$$$$$$$$_____________")
			return df_location
	except:
		parsing_error(folder_name, file_name)
		return None

def outgoing_call_log_func(folder_name, file_name):
	# outgoing_call_log.txt
	try:
		# outgoing_call_log = open(folder_name + '/' + file_name, "r").read()
		with open(folder_name + '/' + file_name, "r") as file:
			outgoing_call_log = file.read()

		# if len(outgoing_call_log.split(",")) == 1:
		#   return None
	except Exception as e:
		#print("file issue : ",e)
		file_not_found(folder_name, file_name)
		return outgoing_call_log_func_default

	if not "=" in outgoing_call_log:
		check_empty(folder_name, file_name)
		return outgoing_call_log_func_default
	try:
		d = {}
		for i in outgoing_call_log.split(","):
			b = i.replace("[", "").replace("{", "").replace("}", "").replace("]", "").strip()
			c = b.split("=")
			d[c[0]]= int(c[1])
		df_outgoing_call_log = pd.DataFrame(pd.Series(d), columns=["outgoing_call_log"])
		df_outgoing_call_log = df_outgoing_call_log.reset_index()
		df_outgoing_call_log.columns = ["Date", "Count"]
		df_outgoing_call_log.columns = df_outgoing_call_log.columns.str.strip()
		df_outgoing_call_log["Count"] = df_outgoing_call_log["Count"].astype(int)
		if len(df_outgoing_call_log) == 0:
			return outgoing_call_log_func_default
		else:
			return df_outgoing_call_log
	except:
		parsing_error(folder_name, file_name)
		return None

def phone_battery_level_func(folder_name, file_name):
	# phone_battery_level.txt
	try:
		# phone_battery_level = open(folder_name + '/' + file_name, "r").read()
		with open(folder_name + '/' + file_name, "r") as file:
			phone_battery_level = file.read()   

	except Exception as e:
		#print("file issue : ",e)
		file_not_found(folder_name, file_name)
		return phone_battery_level_func_default

	if not "=" in phone_battery_level:
		check_empty(folder_name, file_name)
		return phone_battery_level_func_default
	try:
		a = eval(phone_battery_level)[0]
		d = {"battery_level" :  a.split("=")[-1].replace("}", "")}
		S_phone_battery_level = pd.Series(d)
		if len(S_phone_battery_level) == 0:
			return phone_battery_level_func_default
		else:
			return S_phone_battery_level
	except:
		parsing_error(folder_name, file_name)
		return None

def sms_log_func(folder_name, file_name):
	# sms_log.txt
	try:
		# sms_log = open(folder_name + '/' + file_name, "r").read()
		with open(folder_name + '/' + file_name, "r") as file:
			sms_log = eval(file.readlines()[0])

	except Exception as e:
		#print("file issue : ",e)
		file_not_found(folder_name, file_name)
		return sms_log_func_default

	if len(sms_log) == 0:
		check_empty(folder_name, file_name)
		return sms_log_func_default
	try:
		d = {"Address" : [],
			  "Body" : [],
			  "Type" : [],
			  "sendDate" : [],
			  "senderName" : []}
		for i in sms_log:
			# a = i.translate(str.maketrans({
			#   "=" : ":", 
			#   "Address:" : "Address=",
			#   "Body:" : "Body=",
			#   "Type:" :  "Type=",
			#   "sendDate:" : "sendDate=",
			#   "senderName:" :  "senderName=",
			#   ";" : ""}))
			# #print(a)
			a = i.replace("=", ":").replace("Address:", "Address=").replace("Body:", "Body=").\
				 replace("Type:", "Type=").replace("sendDate:", "sendDate=").replace("senderName:", "senderName=")
			a = a.split("=")
			d["Address"].append(''.join(a[1].split(", ")[:-1]))
			d["Body"].append(''.join(a[2].split(", ")[:-1]))
			d["Type"].append(''.join(a[3].split(", ")[:-1]))
			d["sendDate"].append(''.join(a[4].split(", ")[:-1]))
			d["senderName"].append(a[5].replace("}", ""))
		df_sms_log = pd.DataFrame(d)
		if len(df_sms_log) == 0:
			return sms_log_func_default
		else:
			return df_sms_log
	except:
		parsing_error(folder_name, file_name)
		return None

def sms_sent_log_func(folder_name, file_name):
	# sms_sent_log.txt
	try:
		with open(folder_name + '/' + file_name, "r") as file:
			sms_sent_log = file.read()
	except Exception as e:
		#print("file issue : ",e)
		file_not_found(folder_name, file_name)
		return sms_sent_log_func_default

	if not "=" in sms_sent_log:
		check_empty(folder_name, file_name)
		return sms_sent_log_func_default
	try:
		d = {"count" : [], "date" : []}
		for l in eval(sms_sent_log):
			a = l.replace(" ", "").replace(",", '","').replace("{", '{"').replace("}", '"}').replace("=", '":"')
			b = eval(a)
			for i in d:
				d[i].append(b[i])
		df_sms_sent_log = pd.DataFrame(d)[["date", "count"]]
		df_sms_sent_log.columns = df_sms_sent_log.columns.str.strip()
		if len(df_sms_sent_log) == 0:
			return sms_sent_log_func_default
		return df_sms_sent_log
	except:
		parsing_error(folder_name, file_name)
		return None

def storage_size_func(folder_name, file_name):
	# storage_size.txt
	try:
		with open(folder_name + '/' + file_name, "r") as file:
			storage_size = file.read()
	except Exception as e:
		#print("file issue : ",e)
		file_not_found(folder_name, file_name)
		return storage_size_func_default

	if storage_size.replace("\n", "").replace(" ", "") == "":
		check_empty(folder_name, file_name)
		return storage_size_func_default

	if not (("GB" in storage_size) or ("gb" in storage_size)):  
		parsing_error(folder_name, file_name)
		return None

	S_storage_size = pd.Series({"storage_size" : storage_size})
	if len(S_storage_size) == 0:
		return storage_size_func_default
	else:
		return S_storage_size


#******************************* from Here FARAZ work *******************************
def accounts_list_func(folder_name, file_name):
	# accounts_list.txt
	try:
		with open(folder_name + '/' + file_name,"r") as f:
			x = f.readline()
	except Exception as e:
		#print("file issue : ",e)
		file_not_found(folder_name, file_name)
		return accounts_list_func_default

	if not "=" in x:
		check_empty(folder_name, file_name)
		return accounts_list_func_default
	try:
		accounts = json.loads(x)
		key = list()
		sample = (accounts[0].strip('{}')).split(',')
		for s in sample:
			key.append(s.split('=')[0])
		accounts_list = dict()
		name = list()
		domain_type = list()
		for account in accounts:
			domains = (account.strip('{}')).split(',')
			for domain in domains:
				##print((domain.split('=')))
				if(str.lower(domain.strip().split('=')[0])=='name'):
					name.append(domain.split('=')[1])
				elif(str.lower(domain.strip().split('=')[0])=='type'):
					domain_type.append(domain.split('=')[1])
		value = [name, domain_type]
		accounts_list = dict(zip(key , value))
		df_accounts_list = pd.DataFrame(accounts_list)
		df_accounts_list.columns = df_accounts_list.columns.str.strip()
		if len(df_accounts_list) == 0:
			return accounts_list_func_default
		else:
			return df_accounts_list
	except:
		parsing_error(folder_name, file_name)
		return None

def app_install_log_func(folder_name, file_name):
	# app_install_log.txt
	try:
		with open(folder_name + '/' + file_name,"r") as f:
			x = f.readline()
	except Exception as e:
		#print("file issue : ",e)
		file_not_found(folder_name, file_name)
		return app_install_log_func_default

	if not "=" in x:
		check_empty(folder_name, file_name)
		return app_install_log_func_default
	try:
		app_install_log = json.loads(x)
		key = list()
		sample = (app_install_log[0].strip('{}')).split(',')
		for s in sample:
			key.append(s.split('=')[0])
		app_list = dict()
		package = list()
		for a in app_install_log:
			domains = (a.strip('{}')).split(',')
			for domain in domains:
				if(str.lower(domain.strip().split('=')[0])=='package'):
					package.append(domain.split('=')[1])
		value = [package]
		app_list = dict(zip(key , value))
		df_app_install_log = pd.DataFrame(app_list)
		df_app_install_log.columns = df_app_install_log.columns.str.strip()
		if len(df_app_install_log) == 0:
			return app_install_log_func_default
		else:
			return df_app_install_log
	except:
		parsing_error(folder_name, file_name)
		return None

def df_gallery_data_func(folder_name, file_name):
	#print("\n____________ GALLETY DATA________________\n")
	# gallery_data.txt
	try:
		with open(folder_name + '/' + file_name,"r") as f:
			x = f.readline()
	except Exception as e:
		#print("file issue : ",e)
		file_not_found(folder_name, file_name)
		return df_gallery_data_func_default

	if len(x.replace("[", "").replace("]", "").replace("{", "").replace("}", "").replace("\n", "").\
		replace('"', "").replace("'", "").strip()) == 0:
		check_empty(folder_name, file_name)
		return df_gallery_data_func_default


	try:
		gallery_data = json.loads(x)
		key = list()
		sample = (gallery_data[0].strip('{}')).split(',')
		for s in sample:
			key.append(s.split('=')[0]) 
		d = dict()
		title = list()
		timestamp = list()
		path = list()
		for a in gallery_data:
			domains = (a.strip('{}')).split(',')
			for domain in domains:
				if(str.lower(domain.strip().split('=')[0])=='title'):
					title.append(domain.split('=')[1])
				elif(str.lower(domain.strip().split('=')[0])=='timestamp'):
					timestamp.append(domain.split('=')[1])
				elif(str.lower(domain.strip().split('=')[0])=='path'):
					path.append(domain.split('=')[1])      
		df_galary_data = pd.DataFrame([title,timestamp,path]).T
		df_galary_data.columns=['title', 'timestamp', 'path']
		if len(df_galary_data) == 0:
			return df_gallery_data_func_default
		else:
			return df_galary_data
	except:
		try:
			with open(folder_name + '/' + file_name,"r") as f:
				gallery_data = f.readline().lstrip("[").rstrip("]")
			d = {"title" : [],
				"timestamp" : [],
				"path" : []}
			characters_to_remove = ['"', "'", "{", "}", "[", "]"]
			translationtable = str.maketrans({i:'' for i in characters_to_remove})

			gallery_data = [i.translate(translationtable) for i in x.split('","')]
			for row in gallery_data:
				if row.count(",") == 2:
					for i in row.split(","):
						z = i.split("=")
						d[z[0].strip()].append(z[1].strip())
				else:
					row = row[6:]
					title = row[:row.index("timestamp=")].strip().strip(",")
					row = row[row.index("timestamp="):].lstrip("timestamp=")
					timestamp = row[:row.index("path=")].strip().strip(",")
					row = row[row.index("path="):].strip("path=")
					path = row
					d["title"].append(title)
					d["timestamp"].append(timestamp)
					d["path"].append(path)

			df_galary_data = pd.DataFrame(d)
			df_galary_data.columns=['title', 'timestamp', 'path']
			if len(df_galary_data) == 0:
				return df_gallery_data_func_default

			return df_galary_data
		except:
			parsing_error(folder_name, file_name)
			print("__________________________________________")
			return None


def call_log_func(folder_name, file_name):
	# call_log.txt
	try:
		with open(folder_name + '/' + file_name,"r") as f:
			x = f.readline()
	except Exception as e:
		#print("file issue : ",e)
		file_not_found(folder_name, file_name)
		return call_log_func_default

	if not "=" in x:
		check_empty(folder_name, file_name)
		return call_log_func_default

	try:    
		call_log = json.loads(x)
		if not call_log:
			parsing_error(folder_name, file_name)
			return None
		key = list()
		sample = (call_log[0].strip('{}')).split(',')
		for s in sample:
			key.append(s.split('=')[0])
		d = dict()
		number = list()
		Type = list()
		DateTime = list()
		Duration= list()
		for a in call_log:
			domains = (a.strip('{}')).split(',')
			for domain in domains:
				##print((domain.split('=')))
				if(str.lower(domain.strip().split('=')[0])=='number'):
					number.append(domain.split('=')[1])
				elif(str.lower(domain.strip().split('=')[0])=='type'):
					Type.append(domain.split('=')[1])
				elif(str.lower(domain.strip().split('=')[0])=='date'):
					DateTime.append(domain.split('=')[1])
				elif(str.lower(domain.strip().split('=')[0])=='duration'):
					Duration.append(domain.split('=')[1])
		value = [number,Type,DateTime,Duration]
		call_log = dict(zip(key , value))
		keys_for_empty_values = [i for i in call_log if not call_log[i]]
		for i in keys_for_empty_values:
			call_log.pop(i)
		df_call_log = pd.DataFrame(call_log)
		df_call_log.columns = df_call_log.columns.str.strip()
		if len(df_call_log) == 0:
			return call_log_func_default
		else:
			return df_call_log
	except:
		parsing_error(folder_name, file_name)
		return None

def device_info_func(folder_name, file_name):
	# device_info.txt
	try:
		with open(folder_name + '/' + file_name,"r") as f:
			x = f.readline()
	except Exception as e:
		#print("file issue : ",e)
		file_not_found(folder_name, file_name)
		return device_info_func_default

	if not "=" in x:
		check_empty(folder_name, file_name)
		return device_info_func_default

	try:

		device_info = json.loads(x)
		key = list()
		sample = (device_info[0].strip('{}')).split(',')
		for s in sample:
			key.append(s.split('=')[0])
		d = dict()
		model = list()
		manufacturer = list()
		brand = list()
		deviceSoftware= list()
		networkOperator = list()
		ram = list()
		for a in device_info:
			domains = (a.strip('{}')).split(',')
			for domain in domains:
				if(str.lower(domain.strip().split('=')[0])=='model'):
					model.append(domain.split('=')[1])
				elif(str.lower(domain.strip().split('=')[0])=='manufacturer'):
					manufacturer.append(domain.split('=')[1])
				elif(str.lower(domain.strip().split('=')[0])=='brand'):
					brand.append(domain.split('=')[1])
				elif(str.lower(domain.strip().split('=')[0])=='devicesoftware'):
					deviceSoftware.append(domain.split('=')[1])
				elif(str.lower(domain.strip().split('=')[0])=='networkoperator'):
					networkOperator.append(domain.split('=')[1])
				elif(str.lower(domain.strip().split('=')[0])=='ram'):
					ram.append(domain.split('=')[1])            
		value = [model,manufacturer,brand,deviceSoftware,networkOperator,ram]
		value = [i[0] for i in value]
		device_info = dict(zip(key , value))
		S_device_info = pd.Series(device_info)
		S_device_info.index = S_device_info.index.str.strip() # tajarba
		if len(S_device_info) == 0:
			return device_info_func_default
		else:
			return S_device_info
	except:
		parsing_error(folder_name, file_name)
		return None

def filter_app_log_func(folder_name, file_name):
	# filter_app_log.txt
	try:
		# with open("filter_app_log.txt","r") as f:
			# x = f.readline()
		x = open(folder_name + '/' + file_name, "r").readline()
	except Exception as e:
		#print("file issue : ",e)
		file_not_found(folder_name, file_name)
		return filter_app_log_func_default

	if not "=" in x:
		check_empty(folder_name, file_name)
		return filter_app_log_func_default

	try:
		filter_app_log = json.loads(x)
		key = list()
		sample = (filter_app_log[0].strip('{}')).split(', ')
		for s in sample:
			key.append(s.split('=')[0])   
		d = dict()
		social = list()
		dangerous = list()
		darkweb = list()
		wallet= list()
		banking = list()
		lending = list()
		rosca = list()
		for a in filter_app_log:
			domains = (a.strip('{}')).split(', ')
			for domain in domains:
				if(str.lower(domain.strip().split('=')[0])=='social'):
					social.append(domain.split('=')[1])
				elif(str.lower(domain.strip().split('=')[0])=='dangerous'):
					dangerous.append(domain.split('=')[1])
				elif(str.lower(domain.strip().split('=')[0])=='darkweb'):
					darkweb.append(domain.split('=')[1])
				elif(str.lower(domain.strip().split('=')[0])=='wallet'):
					wallet.append(domain.split('=')[1])
				elif(str.lower(domain.strip().split('=')[0])=='banking'):
					banking.append(domain.split('=')[1])
				elif(str.lower(domain.strip().split('=')[0])=='lending'):
					lending.append(domain.split('=')[1])
				elif(str.lower(domain.strip().split('=')[0])=='rosca'):
					rosca.append(domain.split('=')[1])
		value = [social,dangerous,darkweb,wallet,banking,lending,rosca]
		filter_app_log = dict(zip(key , value))
		df_filter_app_log = pd.DataFrame(filter_app_log)
		df_filter_app_log.columns = df_filter_app_log.columns.str.strip()
		if len(df_filter_app_log) == 0:
			return filter_app_log_func_default
		else:
			return df_filter_app_log

	except:
		parsing_error(folder_name, file_name)
		return None

def calendar_events_func(folder_name, file_name):
	# calendar_events.txt
	try:
		with open(folder_name + '/' + file_name,"r") as f:
			x = f.readline()
	except Exception as e:
		#print("file issue : ",e)
		file_not_found(folder_name, file_name)
		return calendar_events_func_default

	if not "=" in x:
		check_empty(folder_name, file_name)
		return calendar_events_func_default
	try:
		if bool(eval(x)):
			lst_of_dicts = []
			calendar_events = json.loads(x)
			for a in calendar_events:
				domains = (a.strip('{}')).split(', ')
				keys = []
				values = []
				lst = []
				for i in domains:
					if not "=" in i:
						lst[-1] += "|" + i
					else:
						lst.append(i)
				for i in lst:
					keys.append(i.split("=")[0])
					values.append(i.split("=")[1])
				lst_of_dicts.append(dict(zip(keys, values)))
			df_calendar_events = pd.DataFrame(lst_of_dicts)
			df_calendar_events = df_calendar_events[["title", "description", "allowReminders", "dTStart", "dTEnd", "allDay"]]
			df_calendar_events.columns = df_calendar_events.columns.str.strip()
		else:
			df_calendar_events = pd.DataFrame()

		if len(df_calendar_events) == 0:
			return calendar_events_func_default
		else:
			return df_calendar_events
	except:
		parsing_error(folder_name, file_name)
		return None

def network_data_func(folder_name, file_name):
	try:
		try:
			# network_data = open("network_data.txt", "r").read()
			network_data = open(folder_name + '/' + file_name, "r").read()
		except Exception as  e:
			#print("----------------------------1st E : ", e)
			file_not_found(folder_name, file_name)
			return network_data_func_default

		if not "=" in network_data:
			check_empty(folder_name, file_name)
			return network_data_func_default
		try:
			s = network_data.translate(str.maketrans({'"': '',
												  '{': '',
												  '}' : '',
												  '[' : '',
												  ']' : '',
												  '>' : '',
												  '<' : ''})).split(",")
			S_network_data = pd.Series({i.split("=")[0] :i.split("=")[1] for i in s})
		except:
			key = []
			value = []
			for i in network_data.split('","'):
				z = i.translate(str.maketrans({
					'"': '',
					'{': '',
					'}' : '',
					'[' : '',
					']' : '',
					'>' : '',
					'<' : ''})).split("=")
				key.append(z[0].strip())
				value.append(z[1].strip())
			S_network_data = pd.Series(value, key)
		if len(S_network_data) == 0:
			return network_data_func_default
		else:
			return S_network_data
	except Exception as e:
		parsing_error(folder_name, file_name)
		return None

def Master_func(folder_name):
	S_phone_battery_level = phone_battery_level_func(folder_name, "phone_battery_level.txt")
	if S_phone_battery_level is None:
		return master_func_default
	elif (S_phone_battery_level.isna()).sum():
		print(": S_phone_battery_level", len(S_phone_battery_level))
		open('logs/master_special_case.txt', 'a').write(folder_name + ',' + 'phone_battery_level' + '\n')
		return master_func_default

	S_ip_address = ip_address_func(folder_name, "ip_address.txt")
	if S_ip_address is None:
	  return master_func_default
	elif(S_ip_address.isna()).sum():
		print("S_ip_address: ", len(S_ip_address))
		open('logs/master_special_case.txt', 'a').write(folder_name + ',' + 'ip_address' + '\n')
		return master_func_default

	S_device_info = device_info_func(folder_name, "device_info.txt")
	if S_device_info is None:
	  return master_func_default
	elif(S_device_info.isna()).sum():
		print("S_device_info: ", len(S_device_info))
		open('logs/master_special_case.txt', 'a').write(folder_name + ',' + 'device_info' + '\n')
		return master_func_default

	S_storage_size = storage_size_func(folder_name, "storage_size.txt")
	if S_storage_size is None:
	  return master_func_default
	elif(S_storage_size.isna()).sum():
		print("S_storage_size: ", len(S_storage_size))
		open('logs/master_special_case.txt', 'a').write(folder_name + ',' + 'storage_size' + '\n')
		return master_func_default

	S_network_data = network_data_func(folder_name, "network_data.txt")
	if S_network_data is None:
		return master_func_default
	elif(S_network_data.isna()).sum():
		print("S_network_data: ", len(S_network_data))
		open('logs/master_special_case.txt', 'a').write(folder_name + ',' + 'network_data' + '\n')
		return master_func_default

	df = pd.DataFrame(pd.concat([S_phone_battery_level, S_ip_address, S_device_info, S_storage_size, S_network_data])).T  

	if list(df.T.isna().sum())[0] == len(df.columns):
	  return master_func_default
	return df
