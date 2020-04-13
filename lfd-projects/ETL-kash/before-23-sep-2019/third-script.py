import pandas as pd
import sqlite3
import json
import zipfile
import os
import numpy as np

os.chdir('/home/amir/github/LFD-projects/ETL-cash-egypt/4-sep-2019')
file_name = "Sample 2 - Sept 2.zip"
folder_name = file_name.replace(".zip", "")
if not folder_name in os.listdir():
    os.mkdir(folder_name)
    with zipfile.ZipFile(file_name, 'r') as zip_ref:
        zip_ref.extractall(folder_name)
os.chdir(folder_name)


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


# sms_sent_log.txt
with open("sms_sent_log.txt", "r") as file:
    sms_sent_log = file.read()
d = {"count" : [],
     "date" : []}
for l in eval(sms_sent_log):
    a = l.replace(" ", "").\
        replace(",", '","').\
        replace("{", '{"').\
        replace("}", '"}').\
        replace("=", '":"')
    b = eval(a)
    for i in d:
        d[i].append(b[i])
df_sms_sent_log = pd.DataFrame(d)[["date", "count"]]


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
    sms_log = eval(file.readlines()[0])
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
#     sms_log = eval(file.read())
# d = {"Address" : [],
#      "Body" : [],
#      "Type" : [],
#      "sendDate" : [],
#      "senderName" : []}
# for e, a in enumerate(sms_log):
#     b = eval(
#         a.replace("}", '"}').\
#         replace("{", '{"').\
#         replace(" ", "").\
#         replace("=", '":"').\
#         replace("\r", "").\
#         replace("\n", "").\
#         replace(',Type":', '","Type":').\
#         replace(',senderName":', '","senderName":').\
#         replace(',Body"', '","Body"').\
#         replace(',sendDate"', '","sendDate"')
#         )
#     for i in d:
#         d[i].append(b[i]) 
# df_sms_log = pd.DataFrame(d)


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