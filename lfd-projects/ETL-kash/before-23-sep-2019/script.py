import pandas as pd
import sqlite3
import json

# ext_storage_files.txt
with open("ext_storage_files.txt", "r") as file:
    ext_storage_files = file.read()
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


# contacts_list.txt
with open("contacts_list.txt", "r") as file:
    contacts_list = file.read()
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



# sms_log.txt
with open("sms_log.txt", "r") as file:
    sms_log = eval(file.read())
d = {"Address" : [],
     "Body" : [],
     "Type" : [],
     "sendDate" : [],
     "senderName" : []}
for e, a in enumerate(sms_log):
    b = eval(
        a.replace("}", '"}').\
        replace("{", '{"').\
        replace(" ", "").\
        replace("=", '":"').\
        replace("\r", "").\
        replace("\n", "").\
        replace(',Type":', '","Type":').\
        replace(',senderName":', '","senderName":').\
        replace(',Body"', '","Body"').\
        replace(',sendDate"', '","sendDate"')
        )
    for i in d:
        d[i].append(b[i]) 
df_sms_log = pd.DataFrame(d)


# phone_battery_level.txt
with open("phone_battery_level.txt", "r") as file:
    phone_battery_level = file.read()    
a = eval(phone_battery_level)[0]
d = {"battery_level" :  a.split("=")[-1].replace("}", "")}
S_phone_battery_level = pd.Series(d)


# outgoing_call_log.txt
with open("outgoing_call_log.txt", "r") as file:
    outgoing_call_log = file.read()
d = {}
for i in outgoing_call_log.split(","):
    b = i.replace("[", "").\
    replace("{", "").\
    replace("}", "").\
    replace("]", "").strip()
    c = b.split("=")
    d[c[0]]= int(c[1])
df_outgoing_call_log = pd.DataFrame(pd.Series(d), columns=["outgoing_call_log"])
df_outgoing_call_log = df_outgoing_call_log.reset_index()
df_outgoing_call_log.columns = ["Date", "Count"]

# ip_address.txt
with open("ip_address.txt", "r") as file:
    ip_address = file.read()
d = dict([eval(ip_address)[0].\
      replace("{", "").\
      replace("}", "").split("=")])
S_ip_address = pd.Series(d)


# location.txt
with open("location.txt", "r") as file:
    location = file.read().strip().splitlines()
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


# storage_size.txt
with open("storage_size.txt", "r") as file:
    storage_size = file.read()
S_storage_size = pd.Series({"storage_size" : storage_size})

#******************************* from Here FARAZ work *******************************

# accounts_list.txt
with open("accounts_list.txt","r") as f:
    x = f.readline()
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

# app_install_log.txt
with open("app_install_log.txt","r") as f:
    x = f.readline()
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
        else:
            print('new field')
value = [package]
app_list = dict(zip(key , value))
df_app_install_log = pd.DataFrame(app_list)


# gallery_data.txt
with open("gallery_data.txt","r") as f:
    x = f.readline()
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
        else:
            print('new field')            
value = [title,timestamp,path]
galary_data = dict(zip(key , value))
df_galary_data = pd.DataFrame(galary_data)


# call_log.txt
with open("call_log.txt","r") as f:
    x = f.readline()
call_log = json.loads(x)
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
        else:
            print('new field')
value = [number,Type,DateTime,Duration]
call_log = dict(zip(key , value))
df_call_log = pd.DataFrame(call_log)


# device_info.txt
with open("device_info.txt","r") as f:
    x = f.readline()
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

# filter_app_log.txt
with open("filter_app_log.txt","r") as f:
    x = f.readline()
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


# calendar_events.txt
with open("calendar_events.txt","r") as f:
    x = f.readline()
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

import os
try:
    os.remove("ETL.db")
except:
    pass

try:
    os.remove("Master.db")
except:
    pass

conn = sqlite3.connect('ETL.db')  
c = conn.cursor()
# if_exists='append'

df_sms_log.to_sql('sms_log', conn, index=False)
df_outgoing_call_log.to_sql('outgoing_call_log', conn, index=False)
df_accounts_list.to_sql('accounts_list', conn, index=False)
df_filter_app_log.to_sql('filter_app_log', conn, index=False)
df_calendar_events.to_sql('calendar_events', conn, index=False)
df_location.to_sql('location', conn, index=False)
df_galary_data.to_sql('galary_data', conn, index=False)
df_call_log.to_sql('call_log', conn, index=False)
df_contacts_list.to_sql('contacts_list', conn, index=False)
df_app_install_log.to_sql('app_install_log', conn, index=False)
df_ext_storage_files.to_sql('ext_storage_files', conn, index=False)




conn = sqlite3.connect('Master.db')  
c = conn.cursor()
# if_exists='append'

df_master = pd.DataFrame(pd.concat([S_phone_battery_level, S_ip_address, S_device_info, S_storage_size])).T
df_master.to_sql('Master', conn, index=False)

