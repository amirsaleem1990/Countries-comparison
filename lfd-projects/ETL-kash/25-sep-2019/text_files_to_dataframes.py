import pandas as pd
import sqlite3
import json
import zipfile
import os
import numpy as np
import pickle
import datetime
import copy

now = str(datetime.datetime.now())

try:
	with open("/home/amir/github/LFD-projects/ETL-cash-egypt/25-sep-2019/logs.pkl", "rb") as file:
		logs = pickle.load(file)
except:
	logs = {}

existing_logs = copy.deepcopy(logs)

def logs_func(folder_name):
	if folder_name in logs:
		logs[folder_name].append(("FILE NOT FOUND", now, folder_name))
	else:
		logs[folder_name] = [("FILE NOT FOUND", now, folder_name)]

def contacts_list_func(folder_name):
	# contacts_list.txt
	try:
		with open("contacts_list.txt", "r") as file:
			contacts_list = file.read()
	except:
		logs_func(folder_name)
		return pd.DataFrame(columns=['displayName', 'phone'])
	a = eval(contacts_list)
	dd = {"displayName" : [],
	"phone" : []}
	for b in a:
		c = b.replace(" ", "").\
		replace("=", '":"').\
		replace("{", '{"').\
		replace("}", '"}').\
		replace(",", '","')
		if "," in c[c.find("phone")+8:]:
			c = c.replace('phone":"', 'phone":["').replace('"}', '"]}' )
		d = eval(c)
		dd["displayName"].append(d["displayName"])
		dd["phone"].append(d["phone"])
	df_contacts_list = pd.DataFrame.from_dict(dd)
	df_contacts_list.phone = ["{" + ", ".join(i) + "}" if isinstance(i, list) else i for i in df_contacts_list.phone.to_list()]
	if len(df_contacts_list) == 0:
		return pd.DataFrame(columns=['displayName', 'phone'])
	else:
		return df_contacts_list

def ext_storage_files_func(folder_name):
	# ext_storage_files.tx
	try:
		with open("ext_storage_files.txt", "r") as file:
			ext_storage_files = file.read()
	except:
		logs_func(folder_name)
		return pd.DataFrame(columns=['Downloads', 'Documents', 'Images', 'Videos'])
	
	a = eval(ext_storage_files)[0]
	b = a.replace(" ", "").\
		replace("{", '{"').\
		replace("=", '":"').\
		replace(",", '","').\
		replace(']",', '"],').\
		replace(':"[', ':["').\
		replace('["]', "[]" ).\
		replace('[""]', '[]')
	d = eval(b)
	df_ext_storage_files = pd.DataFrame()
	for i in d:
		if len(d[i]) == 1:
			df_ext_storage_files[i] = d[i]
		else:
			df_ext_storage_files[i] = ['|'.join(d[i])]
	if len(df_ext_storage_files) == 0:
		return pd.DataFrame(columns=['Downloads', 'Documents', 'Images', 'Videos'])
	else:
		return df_ext_storage_files

def ip_address_func(folder_name):
	# ip_address.txt
	try:
		with open("ip_address.txt", "r") as file:
			ip_address = file.read()
	except:
		logs_func(folder_name)
		return pd.Series({"ipaddress" : "NA"})
	d = dict([eval(ip_address)[0].\
		  replace("{", "").\
		  replace("}", "").split("=")])
	S_ip_address = pd.Series(d)
	if len(S_ip_address) == 0:
		return pd.Series({"ipaddress" : "NA"})
	else:
		return S_ip_address

def location_func(folder_name):
	# location.txt
	try:
		with open("location.txt", "r") as file:
			location = file.read().strip().splitlines()
	except:
		logs_func(folder_name)
		return pd.DataFrame(columns=['TimeStamp', 'latitude', 'longitude'])
	d = {"TimeStamp" : [],
		"latitude" : [],
		"longitude" : []}
	for a in location:
		b = a.replace("[", "").\
			replace("]", "")
		c = eval(b).split("=")
		d["TimeStamp"].append(c[0].replace("{", ""))
		d["latitude"].append(c[2].split(",")[0])
		d["longitude"].append(c[3].replace(")", "").replace("}", ""))
	df_location = pd.DataFrame(d)
	if len(df_location) == 0:
		return pd.DataFrame(columns=['TimeStamp', 'latitude', 'longitude'])
	else:
		return df_location

def outgoing_call_log_func(folder_name):
	# outgoing_call_log.txt
	try:
		with open("outgoing_call_log.txt", "r") as file:
			outgoing_call_log = file.read()
		if len(outgoing_call_log.split(",")) == 1:
			return pd.DataFrame(columns=['Date', 'Count'])
	except:
		logs_func(folder_name)
		return pd.DataFrame(columns=['Date', 'Count'])
	d = {}
	for i in outgoing_call_log.split(","):
		b = i.replace("[", "").replace("{", "").replace("}", "").replace("]", "").strip()
		c = b.split("=")
		d[c[0]]= int(c[1])
	df_outgoing_call_log = pd.DataFrame(pd.Series(d), columns=["outgoing_call_log"])
	df_outgoing_call_log = df_outgoing_call_log.reset_index()
	df_outgoing_call_log.columns = ["Date", "Count"]
	if len(df_outgoing_call_log) == 0:
		return pd.DataFrame(columns=['Date', 'Count'])
	else:
		return df_outgoing_call_log

def phone_battery_level_func(folder_name):
	# phone_battery_level.txt
	try:
		with open("phone_battery_level.txt", "r") as file:
			phone_battery_level = file.read()	
	except:
		logs_func(folder_name)
		return pd.Series({"battery_level" : "NA"})

	a = eval(phone_battery_level)[0]
	d = {"battery_level" :  a.split("=")[-1].replace("}", "")}
	S_phone_battery_level = pd.Series(d)
	if len(S_phone_battery_level) == 0:
		return pd.Series({"battery_level" : "NA"})
	else:
		return S_phone_battery_level

def sms_log_func(folder_name):
	# sms_log.txt
	try:
		with open("sms_log.txt", "r") as file:
			sms_log = eval(file.readlines()[0])
	except:
		logs_func(folder_name)
		return pd.DataFrame(columns=['Address', 'Body', 'Type', 'sendDate', 'senderName'])
	d = {"Address" : [],
		 "Body" : [],
		 "Type" : [],
		 "sendDate" : [],
		 "senderName" : []}

	for i in sms_log:
		a = i.split("=")
		d["Address"].append(' '.join(a[1].split(", ")[:-1]))
		d["Body"].append(' '.join(a[2].split(", ")[:-1]))
		d["Type"].append(' '.join(a[3].split(", ")[:-1]))
		d["sendDate"].append(' '.join(a[4].split(", ")[:-1]))
		d["senderName"].append(a[5].replace("}", ""))
	df_sms_log = pd.DataFrame(d)
	# with open("sms_log.txt", "r") as file:
	#	 sms_log = eval(file.read())
	# d = {"Address" : [],
	#	  "Body" : [],
	#	  "Type" : [],
	#	  "sendDate" : [],
	#	  "senderName" : []}
	# for e, a in enumerate(sms_log):
	#	 b = eval(
	#		 a.replace("}", '"}').\
	#		 replace("{", '{"').\
	#		 replace(" ", "").\
	#		 replace("=", '":"').\
	#		 replace("\r", "").\
	#		 replace("\n", "").\
	#		 replace(',Type":', '","Type":').\
	#		 replace(',senderName":', '","senderName":').\
	#		 replace(',Body"', '","Body"').\
	#		 replace(',sendDate"', '","sendDate"')
	#		 )
	#	 for i in d:
	#		 d[i].append(b[i]) 
	# df_sms_log = pd.DataFrame(d)
	if len(df_sms_log) == 0:
		return pd.DataFrame(columns=['Address', 'Body', 'Type', 'sendDate', 'senderName'])
	else:
		return df_sms_log

def sms_sent_log_func(folder_name):
	# sms_sent_log.txt
	try:
		with open("sms_sent_log.txt", "r") as file:
			sms_sent_log = file.read()
	except:
		logs_func(folder_name)
		return pd.DataFrame(columns=["date", "count"])

	d = {"count" : [], "date" : []}
	for l in eval(sms_sent_log):
		a = l.replace(" ", "").replace(",", '","').replace("{", '{"').replace("}", '"}').replace("=", '":"')
		b = eval(a)
		for i in d:
			d[i].append(b[i])
	df_sms_sent_log = pd.DataFrame(d)[["date", "count"]]
	return df_sms_sent_log


def storage_size_func(folder_name):
	# storage_size.txt
	try:
		with open("storage_size.txt", "r") as file:
			storage_size = file.read()
	except:
		logs_func(folder_name)
		return pd.Series({'storage_size': 'NA'})
	S_storage_size = pd.Series({"storage_size" : storage_size})
	if len(S_storage_size) == 0:
		return pd.Series({'storage_size': 'NA'})
	else:
		return S_storage_size


#******************************* from Here FARAZ work *******************************
def accounts_list_func(folder_name):
	# accounts_list.txt
	try:
		with open("accounts_list.txt","r") as f:
			x = f.readline()
	except:
		logs_func(folder_name)
		return pd.DataFrame(columns=['name', ' type'])
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
			#print((domain.split('=')))
			if(str.lower(domain.strip().split('=')[0])=='name'):
				name.append(domain.split('=')[1])
			elif(str.lower(domain.strip().split('=')[0])=='type'):
				domain_type.append(domain.split('=')[1])
	value = [name, domain_type]
	accounts_list = dict(zip(key , value))
	df_accounts_list = pd.DataFrame(accounts_list)
	if len(df_accounts_list) == 0:
		return pd.DataFrame(columns=['name', ' type'])
	else:
		return df_accounts_list

def app_install_log_func(folder_name):
	# app_install_log.txt
	try:
		with open("app_install_log.txt","r") as f:
			x = f.readline()
	except:
		logs_func(folder_name)
		return pd.DataFrame(columns=['Package'])

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
	if len(df_app_install_log) == 0:
		return pd.DataFrame(columns=['Package'])
	else:
			return df_app_install_log


def df_gallery_data_func(folder_name):
	# gallery_data.txt
	try:
		with open("gallery_data.txt","r") as f:
			x = f.readline()
	except:
		logs_func(folder_name)
		return pd.DataFrame(columns=['title', ' timestamp', ' path'])
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
			#print((domain.split('=')))
			if(str.lower(domain.strip().split('=')[0])=='title'):
				title.append(domain.split('=')[1])
			elif(str.lower(domain.strip().split('=')[0])=='timestamp'):
				timestamp.append(domain.split('=')[1])
			elif(str.lower(domain.strip().split('=')[0])=='path'):
				path.append(domain.split('=')[1])	   
	value = [title,timestamp,path]
	galary_data = dict(zip(key , value))
	df_galary_data = pd.DataFrame(galary_data)
	if len(df_galary_data) == 0:
		return pd.DataFrame(columns=['title', ' timestamp', ' path'])
	else:
		return df_galary_data

def call_log_func(folder_name):
	# call_log.txt
	try:
		with open("call_log.txt","r") as f:
			x = f.readline()
	except:
		logs_func(folder_name)
		return pd.DataFrame(columns=['number', ' Type', ' DateTime', ' Duration'])

	call_log = json.loads(x)
	if not call_log:
		return pd.DataFrame(columns=['number', ' Type', ' DateTime', ' Duration'])
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
			#print((domain.split('=')))
			if(str.lower(domain.strip().split('=')[0])=='number'):
				number.append(domain.split('=')[1])
			elif(str.lower(domain.strip().split('=')[0])=='type'):
				Type.append(domain.split('=')[1])
			elif(str.lower(domain.strip().split('=')[0])=='datetime'):
				DateTime.append(domain.split('=')[1])
			elif(str.lower(domain.strip().split('=')[0])=='duration'):
				Duration.append(domain.split('=')[1])
	value = [number,Type,DateTime,Duration]
	call_log = dict(zip(key , value))
	keys_for_empty_values = [i for i in call_log if not call_log[i]]
	for i in keys_for_empty_values:
		call_log.pop(i)
	df_call_log = pd.DataFrame(call_log)
	if len(df_call_log) == 0:
		return pd.DataFrame(columns=['number', ' Type', ' DateTime', ' Duration'])
	else:
		return df_call_log

def device_info_func(folder_name):
	# device_info.txt
	try:
		with open("device_info.txt","r") as f:
			x = f.readline()
	except:
		logs_func(folder_name)
		return pd.Series({'brand': 'NA',
			'deviceSoftware': 'NA',
			'manufacturer': 'NA',
			'networkOperator': 'NA',
			'ram': 'NA',
			'model': 'NA'})
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
	if len(S_device_info) == 0:
		return pd.Series({'brand': 'NA',
			'deviceSoftware': 'NA',
			'manufacturer': 'NA',
			'networkOperator': 'NA',
			'ram': 'NA',
			'model': 'NA'})
	else:
		return S_device_info


def filter_app_log_func(folder_name):
	# filter_app_log.txt
	try:
		with open("filter_app_log.txt","r") as f:
			x = f.readline()
	except:
		logs_func(folder_name)
		return pd.DataFrame(columns=['social', 'dangerous', 'darkweb', 'wallet', 'banking', 'lending', 'rosca'])

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
	if len(df_filter_app_log) == 0:
		return pd.DataFrame(columns=['social', 'dangerous', 'darkweb', 'wallet', 'banking', 'lending', 'rosca'])
	else:
		return df_filter_app_log

def calendar_events_func(folder_name):
	# calendar_events.txt
	try:
		with open("calendar_events.txt","r") as f:
			x = f.readline()
	except:
		logs_func(folder_name)
		return pd.DataFrame(columns=['title', 'description', 'allowReminders', 'dTStart', 'dTEnd', 'allDay'])

	if bool(eval(x)):
		calendar_events = json.loads(x)
		key = list()
		sample = (calendar_events[0].strip('{}')).split(', ')
		for s in sample:
			key.append(s.split('=')[0])
		d = dict()
		title = list()
		description = list()
		allowReminders = list()
		dTStart= list()
		dTEnd = list()
		allDay = list()
		for a in calendar_events:
			domains = (a.strip('{}')).split(', ')
			for domain in domains:
				if(str.lower(domain.strip().split('=')[0])=='title'):
					title.append(domain.split('=')[1])
				elif(str.lower(domain.strip().split('=')[0])=='description'):
					description.append(domain.split('=')[1])
				elif(str.lower(domain.strip().split('=')[0])=='allowreminders'):
					allowReminders.append(domain.split('=')[1])
				elif(str.lower(domain.strip().split('=')[0])=='dtstart'):
					dTStart.append(domain.split('=')[1])
				elif(str.lower(domain.strip().split('=')[0])=='dtend'):
					dTEnd.append(domain.split('=')[1])
				elif(str.lower(domain.strip().split('=')[0])=='allday'):
					allDay.append(domain.split('=')[1])
		value = [title,description,allowReminders,dTStart,dTEnd,allDay]
		calendar_events = dict(zip(key , value))
		df_calendar_events = pd.DataFrame(calendar_events)
	else:
		df_calendar_events = pd.DataFrame()

	if len(df_calendar_events) == 0:
		return pd.DataFrame(columns=['title', 'description', 'allowReminders', 'dTStart', 'dTEnd', 'allDay'])
	else:
		return df_calendar_events


if logs != existing_logs:
	with open("/home/amir/github/LFD-projects/ETL-cash-egypt/25-sep-2019/logs.pkl", "wb") as file:
		pickle.dump(logs, file)	