#!/usr/bin/env python
# coding: utf-8

# In[1]:


# importing libraries
import pandas as pd
import numpy as np
import glob
import rpy2
import sys
import gc
import time
from functools import reduce


# In[2]:


rpy2.__version__


# In[3]:


a= pd.DataFrame({'abc':[1,2,3]})


# In[ ]:


a.to_parquet("a.gzip",compression="gzip")


# In[4]:


# Helper function
def times(x):
    if x >= 0 and x < 6:
        return 'night'
    elif x >= 6 and x < 12:
        return "morning"
    elif x >= 12 and x < 18:
        return 'afternoon'
    elif x >= 18 and x <= 23:
        return "evening"
    else:
        return "not specified"


# In[5]:


## feature engineering

def aggregate_on_weekday(data,access_method_id_column_name,date_column_name,dataset):
    
#    data[date_column_name] = pd.to_datetime(data[date_column_name],format='%y/%m/%d')

   # It is assumed the week starts on Monday, which is denoted by 0 and ends on Sunday which is denoted by
    data['Day_week'] = pd.DatetimeIndex(data[date_column_name]).weekday

   #check variable: 1 for weekday and 0 for weekend
    data['IsWeekDay'] = data['Day_week'].replace([0,1,2,3,4,5,6],[1,1,1,1,1,0,0])
    
    if (dataset == "vog" or dataset == "vic"):
        gr = data.groupby([access_method_id_column_name,"IsWeekDay"],as_index=False).agg({
        'CALL_DURATION' :[sum],
        'NO_OF_CALLS' : [sum]
        })
        gr.columns = pd.Index(['_'.join([str(_) for _ in v]) for v in gr.columns])
        
        
        
    elif (dataset == "sog" or dataset == "sic"):
        gr = data.groupby([access_method_id_column_name,"IsWeekDay"],as_index=False).agg({
    'No_of_Messages' : [sum]
        })
        gr.columns = pd.Index(['_'.join([str(_) for _ in v]) for v in gr.columns])
                
    
    
    elif (dataset == "gprs"):
        gr = data.groupby([access_method_id_column_name,"IsWeekDay"],as_index=False).agg({
    'GPRS_MB' : [sum,'count'] 
        })
        gr.columns = pd.Index(['_'.join([str(_) for _ in v]) for v in gr.columns])
    
    
    else:
        pass
 
    return gr

    


# In[6]:


def process_weekend_after_aggregation(data_frame):
    
    # weekend has 0 and 1 as unique values
#     data_frame = pd.read_csv(data_frame_name)
#     data_frame.drop(["Unnamed: 0"],axis=1,inplace=True)
    
    weekend_0 = data_frame[data_frame.IsWeekDay_ == 0]
    weekend_1 = data_frame[data_frame.IsWeekDay_ == 1]
    
    del data_frame
    
    joined_weekend = pd.merge(weekend_0,weekend_1,on = "ACCESS_METHOD_ID_", how= 'outer')
    
    return joined_weekend


# In[7]:


def aggregate_on_period(data,access_method_id_column_name,time_column_name,dataset):

    
    data['Period'] = data[time_column_name].apply(times)        
    
    if (dataset == "vog"):
        gr = data.groupby([access_method_id_column_name,"Period"],as_index=False).agg({
        'CALL_DURATION' :[sum],
        'NO_OF_CALLS' : [sum]
        })
        gr.columns = pd.Index(['_'.join([str(_) for _ in v]) for v in gr.columns])        
        
        
        
    elif (dataset == "sog"):
        gr = data.groupby([access_method_id_column_name,"Period"],as_index=False).agg({
    'No_of_Messages' : [sum]
    })
        gr.columns = pd.Index(['_'.join([str(_) for _ in v]) for v in gr.columns])
    
    
    elif (dataset == "gprs"):        
        gr = data.groupby([access_method_id_column_name,"Period"],as_index=False).agg({
    'GPRS_MB' : [sum,max] 
        })
        gr.columns = pd.Index(['_'.join([str(_) for _ in v]) for v in gr.columns])
        
    else:
        pass

    return gr


# In[8]:


def process_period_after_aggregation(data_period):
    
#     data_period = pd.read_csv(data_period_name)
#     print (data_period.head())
#     data_period.drop(["Unnamed: 0"],axis=1,inplace=True)
    
    morning = data_period[data_period.Period_ == "morning"]
    morning.columns = map(lambda col: '{}_{}'.format(str(col), "morning"), morning.columns)
    morning.rename(columns={'ACCESS_METHOD_ID__morning': 'ACCESS_METHOD_ID_'}, inplace=True)

    afternoon = data_period[data_period.Period_ == "afternoon"]
    afternoon.columns = map(lambda col: '{}_{}'.format(str(col), "afternoon"), afternoon.columns)
    afternoon.rename(columns={'ACCESS_METHOD_ID__afternoon': 'ACCESS_METHOD_ID_'}, inplace=True)

    evening = data_period[data_period.Period_ == "evening"]
    evening.columns = map(lambda col: '{}_{}'.format(str(col), "evening"), evening.columns)
    evening.rename(columns={'ACCESS_METHOD_ID__evening': 'ACCESS_METHOD_ID_'}, inplace=True)

    night = data_period[data_period.Period_ == "night"]
    night.columns = map(lambda col: '{}_{}'.format(str(col), "night"), night.columns)
    night.rename(columns={'ACCESS_METHOD_ID__night': 'ACCESS_METHOD_ID_'}, inplace=True)
    
    
    del data_period
    
    ## appending all frames
    all_periods = [morning,afternoon,evening,night]
    
    
    df_merged_period = reduce(lambda  left,right: pd.merge(left,right,on=['ACCESS_METHOD_ID_'],
                                            how='outer'), all_periods)
    
    return df_merged_period


# In[9]:


def aggregate_on_service_type(data,access_method_id_column_name,service_type_column_name,dataset):
    
    
    data['service_type'] = data[service_type_column_name].replace([1,9,2,87,88,3,4,6,7],["Jazz","International","PTCL","PTCL","PTCL","Others","Others","Others","Others"])
    
    
    if (dataset == "vog" or dataset == "vic"):
        gr = data.groupby([access_method_id_column_name,"service_type"],as_index=False).agg({
         'CALL_DURATION' :[sum],
        'NO_OF_CALLS' : [sum]
        })
        gr.columns = pd.Index(['_'.join([str(_) for _ in v]) for v in gr.columns])
        
        
    elif (dataset == "sog" or dataset == "sic"):
        gr = data.groupby([access_method_id_column_name,"service_type"],as_index=False).agg({
    'No_of_Messages' : [sum]
    })
        gr.columns = pd.Index(['_'.join([str(_) for _ in v]) for v in gr.columns])
        
        
    else:
        pass

    
    return gr


# In[10]:


def process_service_after_aggregation(data_service):
    
#     data_service = pd.read_csv(data_service_name)
#     data_service.drop(["Unnamed: 0"],axis=1,inplace=True)
    
    
    Jazz = data_service[data_service.service_type_ == "Jazz"]
    Jazz.columns = map(lambda col: '{}_{}'.format(str(col), "Jazz"), Jazz.columns)
    Jazz.rename(columns={'ACCESS_METHOD_ID__Jazz': 'ACCESS_METHOD_ID_'}, inplace=True)

    International = data_service[data_service.service_type_ == "International"]
    International.columns = map(lambda col: '{}_{}'.format(str(col), "International"), International.columns)
    International.rename(columns={'ACCESS_METHOD_ID__International': 'ACCESS_METHOD_ID_'}, inplace=True)

    PTCL = data_service[data_service.service_type_ == "PTCL"]
    PTCL.columns = map(lambda col: '{}_{}'.format(str(col), "PTCL"), PTCL.columns)
    PTCL.rename(columns={'ACCESS_METHOD_ID__PTCL': 'ACCESS_METHOD_ID_'}, inplace=True)

    Others = data_service[data_service.service_type_ == "Others"]
    Others.columns = map(lambda col: '{}_{}'.format(str(col), "Others"), Others.columns)
    Others.rename(columns={'ACCESS_METHOD_ID__Others': 'ACCESS_METHOD_ID_'}, inplace=True)

    _22 = data_service[data_service.service_type_ == 22]
    _22.columns = map(lambda col: '{}_{}'.format(str(col), "22"), _22.columns)
    _22.rename(columns={'ACCESS_METHOD_ID__22': 'ACCESS_METHOD_ID_'}, inplace=True)
    
#     print (Jazz.shape,International.shape,PTCL.shape,Others.shape,_22.shape)
    
    del data_service
    
    all_services = [Jazz,International,PTCL,Others,_22]
    
    df_merged_services = reduce(lambda  left,right: pd.merge(left,right,on=['ACCESS_METHOD_ID_'],
                                            how='outer'), all_services)

    return df_merged_services


# In[11]:


def aggregate_on_weekend_period_type(data,access_method_id_column_name,time_column_name,date_column_name,dataset):


#    data[date_column_name] = pd.to_datetime(data[date_column_name],format='%y/%m/%d')



    #    It is assumed the week starts on Monday, which is denoted by 0 and ends on Sunday which is denoted by
    data['Day_week'] = pd.DatetimeIndex(data[date_column_name]).weekday

    #     check variable: 1 for weekday and 0 for weekend
    data['IsWeekDay'] = data['Day_week'].replace([0,1,2,3,4,5,6],[1,1,1,1,1,0,0])

    data['Period'] = data[time_column_name].apply(times)

    if (dataset == "vog"):
        gr = data.groupby([access_method_id_column_name,"IsWeekDay","Period"],as_index=False).agg({
        'CALL_DURATION' :[sum],
        'NO_OF_CALLS' : [sum]
        })
        gr.columns = pd.Index(['_'.join([str(_) for _ in v]) for v in gr.columns])        
        
    elif (dataset == "sog"):
        gr = data.groupby([access_method_id_column_name,"IsWeekDay","Period"],as_index=False).agg({
        'No_of_Messages' : [sum]
        })
        gr.columns = pd.Index(['_'.join([str(_) for _ in v]) for v in gr.columns])
    
    elif (dataset == "gprs"):
        gr = data.groupby([access_method_id_column_name,"IsWeekDay","Period"],as_index=False).agg({
    'GPRS_MB' : [sum] 
        })
        gr.columns = pd.Index(['_'.join([str(_) for _ in v]) for v in gr.columns])

        
        
    else:
        pass
    
    return gr


# In[12]:


def process_weekday_period_after_aggregation(data_week_period):
    
#     data_week_period = pd.read_csv(data_week_period_name)
#     data_week_period.drop(["Unnamed: 0"],axis=1,inplace=True)
    
    
    morning_weekend = data_week_period[(data_week_period.Period_ == "morning") & (data_week_period.IsWeekDay_ == 0) ]
    morning_weekend.columns = map(lambda col: '{}_{}'.format(str(col), "morning_weekend"), morning_weekend.columns)
    morning_weekend.rename(columns={'ACCESS_METHOD_ID__morning_weekend': 'ACCESS_METHOD_ID_'}, inplace=True)



    morning_weekday = data_week_period[(data_week_period.Period_ == "morning") & (data_week_period.IsWeekDay_ == 1) ]
    morning_weekday.columns = map(lambda col: '{}_{}'.format(str(col), "morning_weekday"), morning_weekday.columns)
    morning_weekday.rename(columns={'ACCESS_METHOD_ID__morning_weekday': 'ACCESS_METHOD_ID_'}, inplace=True)



    afternoon_weekend = data_week_period[(data_week_period.Period_ == "afternoon") & (data_week_period.IsWeekDay_ == 0) ]
    afternoon_weekend.columns = map(lambda col: '{}_{}'.format(str(col), "afternoon_weekend"), afternoon_weekend.columns)
    afternoon_weekend.rename(columns={'ACCESS_METHOD_ID__afternoon_weekend': 'ACCESS_METHOD_ID_'}, inplace=True)



    afternoon_weekday = data_week_period[(data_week_period.Period_ == "afternoon") & (data_week_period.IsWeekDay_ == 1) ]
    afternoon_weekday.columns = map(lambda col: '{}_{}'.format(str(col), "afternoon_weekday"), afternoon_weekday.columns)
    afternoon_weekday.rename(columns={'ACCESS_METHOD_ID__afternoon_weekday': 'ACCESS_METHOD_ID_'}, inplace=True)



    evening_weekend = data_week_period[(data_week_period.Period_ == "evening") & (data_week_period.IsWeekDay_ == 0) ]
    evening_weekend.columns = map(lambda col: '{}_{}'.format(str(col), "evening_weekend"), evening_weekend.columns)
    evening_weekend.rename(columns={'ACCESS_METHOD_ID__evening_weekend': 'ACCESS_METHOD_ID_'}, inplace=True)



    evening_weekday = data_week_period[(data_week_period.Period_ == "evening") & (data_week_period.IsWeekDay_ == 1) ]
    evening_weekday.columns = map(lambda col: '{}_{}'.format(str(col), "evening_weekday"), evening_weekday.columns)
    evening_weekday.rename(columns={'ACCESS_METHOD_ID__evening_weekday': 'ACCESS_METHOD_ID_'}, inplace=True)


    night_weekend = data_week_period[(data_week_period.Period_ == "night") & (data_week_period.IsWeekDay_ == 0) ]
    night_weekend.columns = map(lambda col: '{}_{}'.format(str(col), "night_weekend"), night_weekend.columns)
    night_weekend.rename(columns={'ACCESS_METHOD_ID__night_weekend': 'ACCESS_METHOD_ID_'}, inplace=True)



    night_weekday = data_week_period[(data_week_period.Period_ == "night") & (data_week_period.IsWeekDay_ == 1) ]
    night_weekday.columns = map(lambda col: '{}_{}'.format(str(col), "night_weekday"), night_weekday.columns)
    night_weekday.rename(columns={'ACCESS_METHOD_ID__night_weekday': 'ACCESS_METHOD_ID_'}, inplace=True)

    
    del data_week_period
    
    all_combination_week_period = [morning_weekend,morning_weekday,afternoon_weekend,afternoon_weekday,evening_weekend,evening_weekday,night_weekend,night_weekday]
    
    
    df_merged_combination = reduce(lambda  left,right: pd.merge(left,right,on=['ACCESS_METHOD_ID_'],
                                            how='outer'), all_combination_week_period)
    
    return df_merged_combination


# In[25]:


## reading voice outgoing

def process_vog():
    
    print ("Start processing Voice outgoing data")

    colnames_vog = ['CALL_START_HOUR', 'CALL_START_DT','CALL_SERVICE_TYPE', 'NO_OF_CALLS' ,'ACCESS_METHOD_ID', 'CALL_DURATION']

    weekend_aggregations = []
    period_aggregations = []
    service_type_aggregations = []
    weekend_period_aggregations = []

    iter_ = 1
#     total_start = time.time()
    for data_chunk in pd.read_csv("voice_og1/voice_og1.csv",chunksize=28990000,names=['CALL_START_HOUR','CALL_START_DT','CALL_SERVICE_TYPE', 'NO_OF_CALLS' ,'ACCESS_METHOD_ID', 'CALL_DURATION'],
                         dtype= {'CALL_START_HOUR': np.uint8,'CALL_SERVICE_TYPE':np.uint8,'NO_OF_CALLS':np.uint16,'CALL_DURATION':np.uint16}):
        #data_chunk.drop("Unnamed: 0",inplace = True,axis =1)
        #print (data_chunk.head())

        data_chunk['CALL_START_DT'] = pd.to_datetime(data_chunk['CALL_START_DT'],format='%y/%m/%d')


        st = time.time()
        period_aggregations.append(aggregate_on_period(data_chunk,access_method_id_column_name = "ACCESS_METHOD_ID",time_column_name = "CALL_START_HOUR",dataset = "vog"))
        end_p = time.time()
        weekend_aggregations.append(aggregate_on_weekday(data_chunk,access_method_id_column_name = "ACCESS_METHOD_ID",date_column_name = "CALL_START_DT",dataset = "vog"))
        end_w = time.time()
        service_type_aggregations.append(aggregate_on_service_type(data_chunk,access_method_id_column_name = "ACCESS_METHOD_ID",service_type_column_name = "CALL_SERVICE_TYPE",dataset = "vog"))
        end_s = time.time()
        weekend_period_aggregations.append(aggregate_on_weekend_period_type(data_chunk,access_method_id_column_name = "ACCESS_METHOD_ID", time_column_name = "CALL_START_HOUR", date_column_name = "CALL_START_DT",dataset = "vog"))
        end_wp = time.time()
        #print ("period","weekend","service","weekend/period")
        print (end_p - st, end_w - end_p, end_s - end_w, end_wp- end_s)




    #print (time.time() - total_start)

    vog_weekend = pd.concat(weekend_aggregations)
    vog_period = pd.concat(period_aggregations)
    vog_service = pd.concat(service_type_aggregations)
    vog_week_period = pd.concat(weekend_period_aggregations)

    ## second level grouping
    vog_period = vog_period.groupby(["ACCESS_METHOD_ID_","Period_"],as_index=False).agg({
    'CALL_DURATION_sum' : sum,
    'NO_OF_CALLS_sum' : sum,
    })
    vog_weekend = vog_weekend.groupby(["ACCESS_METHOD_ID_","IsWeekDay_"],as_index=False).agg({
    'CALL_DURATION_sum' : sum,
    'NO_OF_CALLS_sum' : sum,
    })
    vog_service = vog_service.groupby(["ACCESS_METHOD_ID_","service_type_"],as_index=False).agg({
    'CALL_DURATION_sum' : sum,
    'NO_OF_CALLS_sum' : sum,
    })
    vog_week_period = vog_week_period.groupby(["ACCESS_METHOD_ID_","IsWeekDay_","Period_"],as_index=False).agg({
    'CALL_DURATION_sum' : sum,
    'NO_OF_CALLS_sum' : sum,
    })



    del (weekend_aggregations,period_aggregations,service_type_aggregations,weekend_period_aggregations)

    casted_weekend = process_weekend_after_aggregation(vog_weekend)
    casted_service = process_service_after_aggregation(vog_service)
    casted_period = process_period_after_aggregation(vog_period)
    casted_weekend_period = process_weekday_period_after_aggregation(vog_week_period)

    casted_weekend.columns = map(lambda col: '{}_{}'.format("VOG",str(col)), casted_weekend.columns)
    casted_weekend.rename(columns={'VOG_ACCESS_METHOD_ID_': 'ACCESS_METHOD_ID_'}, inplace=True)

    casted_period.columns = map(lambda col: '{}_{}'.format("VOG",str(col)), casted_period.columns)
    casted_period.rename(columns={'VOG_ACCESS_METHOD_ID_': 'ACCESS_METHOD_ID_'}, inplace=True)

    casted_service.columns = map(lambda col: '{}_{}'.format("VOG",str(col)), casted_service.columns)
    casted_service.rename(columns={'VOG_ACCESS_METHOD_ID_': 'ACCESS_METHOD_ID_'}, inplace=True)

    casted_weekend_period.columns = map(lambda col: '{}_{}'.format("VOG",str(col)), casted_weekend_period.columns)
    casted_weekend_period.rename(columns={'VOG_ACCESS_METHOD_ID_': 'ACCESS_METHOD_ID_'}, inplace=True)


    all_vog_data = [casted_weekend,casted_period,casted_service,casted_weekend_period]
    all_vog_joined = reduce(lambda left,right : pd.merge(left,right,on=['ACCESS_METHOD_ID_'],how = 'outer'),all_vog_data)

    #print (all_vog_joined.shape)


    del (vog_weekend,vog_period,vog_service,vog_week_period)

    #print (time.time() - total_start)

    all_vog_joined.to_parquet("intermediate_files/vog_all.gzip",compression = "gzip")
    
    print ("End processing Voice outgoing data")


# In[26]:


## reading SMS outgoing

def process_sog():

    print ("Start processing SMS outgoing data")

    
    colnames_sog = ['Message_START_HOUR','Message_START_DT','Message_SERVICE_TYPE', 'No_of_Messages' ,'ACCESS_METHOD_ID']

    weekend_aggregations = []
    period_aggregations = []
    service_type_aggregations = []
    weekend_period_aggregations = []

    for data_chunk in pd.read_csv("sms_og/sms_og.csv",chunksize=36600000, names = colnames_sog, dtype = {"Message_START_HOUR": np.uint8, 
                                                            "No_of_Messages":np.uint16,"Message_SERVICE_TYPE":np.uint8}):


        data_chunk['Message_START_DT'] = pd.to_datetime(data_chunk['Message_START_DT'],format='%y/%m/%d')

        #print (data_chunk.head())
        st = time.time()
        period_aggregations.append(aggregate_on_period(data_chunk,access_method_id_column_name = "ACCESS_METHOD_ID",time_column_name = "Message_START_HOUR",dataset = "sog"))
        end_p = time.time()
        weekend_aggregations.append(aggregate_on_weekday(data_chunk,access_method_id_column_name = "ACCESS_METHOD_ID",date_column_name = "Message_START_DT",dataset = "sog"))
        end_w = time.time()
        service_type_aggregations.append(aggregate_on_service_type(data_chunk,access_method_id_column_name = "ACCESS_METHOD_ID",service_type_column_name = "Message_SERVICE_TYPE",dataset = "sog"))
        end_s = time.time()
        weekend_period_aggregations.append(aggregate_on_weekend_period_type(data_chunk,access_method_id_column_name = "ACCESS_METHOD_ID", time_column_name = "Message_START_HOUR", date_column_name = "Message_START_DT",dataset = "sog"))
        end_wp = time.time()
        #print ("period","weekend","service","weekend/period")
        print (end_p - st, end_w - end_p, end_s - end_w, end_wp- end_s)




    #print (time.time() - total_start)

    sog_weekend = pd.concat(weekend_aggregations)
    sog_period = pd.concat(period_aggregations)
    sog_service = pd.concat(service_type_aggregations)
    sog_week_period = pd.concat(weekend_period_aggregations)

    ## second level grouping
    sog_period = sog_period.groupby(["ACCESS_METHOD_ID_","Period_"],as_index=False).agg({
    'No_of_Messages_sum' : sum
    })
    sog_weekend = sog_weekend.groupby(["ACCESS_METHOD_ID_","IsWeekDay_"],as_index=False).agg({
    'No_of_Messages_sum' : sum
    })
    sog_service = sog_service.groupby(["ACCESS_METHOD_ID_","service_type_"],as_index=False).agg({
    'No_of_Messages_sum' : sum
    })
    sog_week_period = sog_week_period.groupby(["ACCESS_METHOD_ID_","IsWeekDay_","Period_"],as_index=False).agg({
    'No_of_Messages_sum' : sum
    })



    del (weekend_aggregations,period_aggregations,service_type_aggregations,weekend_period_aggregations)

    casted_weekend = process_weekend_after_aggregation(sog_weekend)
    casted_service = process_service_after_aggregation(sog_service)
    casted_period = process_period_after_aggregation(sog_period)
    casted_weekend_period = process_weekday_period_after_aggregation(sog_week_period)

    casted_weekend.columns = map(lambda col: '{}_{}'.format("SOG",str(col)), casted_weekend.columns)
    casted_weekend.rename(columns={'SOG_ACCESS_METHOD_ID_': 'ACCESS_METHOD_ID_'}, inplace=True)

    casted_period.columns = map(lambda col: '{}_{}'.format("SOG",str(col)), casted_period.columns)
    casted_period.rename(columns={'SOG_ACCESS_METHOD_ID_': 'ACCESS_METHOD_ID_'}, inplace=True)

    casted_service.columns = map(lambda col: '{}_{}'.format("SOG",str(col)), casted_service.columns)
    casted_service.rename(columns={'SOG_ACCESS_METHOD_ID_': 'ACCESS_METHOD_ID_'}, inplace=True)

    casted_weekend_period.columns = map(lambda col: '{}_{}'.format("SOG",str(col)), casted_weekend_period.columns)
    casted_weekend_period.rename(columns={'SOG_ACCESS_METHOD_ID_': 'ACCESS_METHOD_ID_'}, inplace=True)


    all_sog_data = [casted_weekend,casted_period,casted_service,casted_weekend_period]
    all_sog_joined = reduce(lambda left,right : pd.merge(left,right,on=['ACCESS_METHOD_ID_'],how = 'outer'),all_sog_data)

    #print (all_sog_joined.shape)


    del (sog_weekend,sog_period,sog_service,sog_week_period)

    #print (time.time() - total_start)

    all_sog_joined.to_parquet("intermediate_files/sog_all.gzip",compression = "gzip")
    print ("End processing SMS outgoing data")


# In[27]:


## reading Voice incoming

def process_vic():

    
    print ("Start processing Voice incoming data")

    colnames_vic = ['CALL_START_DT','CALL_SERVICE_TYPE', 'NO_OF_CALLS' ,'ACCESS_METHOD_ID', 'CALL_DURATION']

    weekend_aggregations = []
    service_type_aggregations = []


    for data_chunk in pd.read_csv("voc_ic/voice_incoming.csv",chunksize = 38800000,names = colnames_vic, dtype = {"CALL_SERVICE_TYPE":np.uint8,
                                                                                            "NO_OF_CALLS":np.uint16,"CALL_DURATION":np.uint16}):

        data_chunk['CALL_START_DT'] = pd.to_datetime(data_chunk['CALL_START_DT'],format='%y/%m/%d')


        weekend_aggregations.append(aggregate_on_weekday(data_chunk,access_method_id_column_name = "ACCESS_METHOD_ID",date_column_name = "CALL_START_DT",dataset = "vic"))

        service_type_aggregations.append(aggregate_on_service_type(data_chunk,access_method_id_column_name = "ACCESS_METHOD_ID",service_type_column_name = "CALL_SERVICE_TYPE",dataset = "vic"))



    #print (time.time() - total_start)

    vic_weekend = pd.concat(weekend_aggregations)
    vic_service = pd.concat(service_type_aggregations)

    ## second level grouping
    vic_weekend = vic_weekend.groupby(["ACCESS_METHOD_ID_","IsWeekDay_"],as_index=False).agg({
    'CALL_DURATION_sum' : sum,
    'NO_OF_CALLS_sum' : sum,
    })
    vic_service = vic_service.groupby(["ACCESS_METHOD_ID_","service_type_"],as_index=False).agg({
    'CALL_DURATION_sum' : sum,
    'NO_OF_CALLS_sum' : sum,
    })



    del (weekend_aggregations,service_type_aggregations)

    casted_weekend = process_weekend_after_aggregation(vic_weekend)
    casted_service = process_service_after_aggregation(vic_service)

    casted_weekend.columns = map(lambda col: '{}_{}'.format("VIC",str(col)), casted_weekend.columns)
    casted_weekend.rename(columns={'VIC_ACCESS_METHOD_ID_': 'ACCESS_METHOD_ID_'}, inplace=True)

    casted_service.columns = map(lambda col: '{}_{}'.format("VIC",str(col)), casted_service.columns)
    casted_service.rename(columns={'VIC_ACCESS_METHOD_ID_': 'ACCESS_METHOD_ID_'}, inplace=True)



    all_vic_data = [casted_weekend,casted_service]
    all_vic_joined = reduce(lambda left,right : pd.merge(left,right,on=['ACCESS_METHOD_ID_'],how = 'outer'),all_vic_data)

    #print (all_vic_joined.shape)


    del (vic_weekend,vic_service)

#    print (time.time() - total_start)

    all_vic_joined.to_parquet("intermediate_files/vic_all.gzip",compression = "gzip")
    print ("End processing Voice incoming data")


# In[28]:


## reading SMS incoming

def process_sic():
    
    print ("Start processing SMS incoming data")

    colnames_sic = ['Message_START_DT','Message_SERVICE_TYPE', 'No_of_Messages' ,'ACCESS_METHOD_ID', 'Unknown']


    weekend_aggregations = []
    service_type_aggregations = []

    colnames_vic = ['CALL_START_DT','CALL_SERVICE_TYPE', 'NO_OF_CALLS' ,'ACCESS_METHOD_ID', 'CALL_DURATION']
    for data_chunk in pd.read_csv("sms_ic/sms_incoming.csv",chunksize = 38800000,names = colnames_vic, dtype = {"Message_SERVICE_TYPE":np.uint8,
                                                                                            "No_of_Messages":np.uint16}):

        #data_chunk.drop("Unknown",inplace = True, axis = 1)
        data_chunk['Message_START_DT'] = pd.to_datetime(data_chunk['Message_START_DT'],format='%y/%m/%d')


        weekend_aggregations.append(aggregate_on_weekday(data_chunk,access_method_id_column_name = "ACCESS_METHOD_ID",date_column_name = "Message_START_DT",dataset = "sic"))

        service_type_aggregations.append(aggregate_on_service_type(data_chunk,access_method_id_column_name = "ACCESS_METHOD_ID",service_type_column_name = "Message_SERVICE_TYPE",dataset = "sic"))


    #print (time.time() - total_start)

    sic_weekend = pd.concat(weekend_aggregations)
    sic_service = pd.concat(service_type_aggregations)

    ## second level grouping
    sic_weekend = sic_weekend.groupby(["ACCESS_METHOD_ID_","IsWeekDay_"],as_index=False).agg({
    'CALL_DURATION_sum' : sum,
    'NO_OF_CALLS_sum' : sum,
    })
    sic_service = sic_service.groupby(["ACCESS_METHOD_ID_","service_type_"],as_index=False).agg({
    'CALL_DURATION_sum' : sum,
    'NO_OF_CALLS_sum' : sum,
    })



    del (weekend_aggregations,service_type_aggregations)

    casted_weekend = process_weekend_after_aggregation(sic_weekend)
    casted_service = process_service_after_aggregation(sic_service)

    casted_weekend.columns = map(lambda col: '{}_{}'.format("SIC",str(col)), casted_weekend.columns)
    casted_weekend.rename(columns={'SIC_ACCESS_METHOD_ID_': 'ACCESS_METHOD_ID_'}, inplace=True)

    casted_service.columns = map(lambda col: '{}_{}'.format("SIC",str(col)), casted_service.columns)
    casted_service.rename(columns={'SIC_ACCESS_METHOD_ID_': 'ACCESS_METHOD_ID_'}, inplace=True)


    all_sic_data = [casted_weekend,casted_service]
    all_sic_joined = reduce(lambda left,right : pd.merge(left,right,on=['ACCESS_METHOD_ID_'],how = 'outer'),all_sic_data)

    #print (all_sic_joined.shape)


    del (sic_weekend,sic_service)

#    print (time.time() - total_start)

    all_sic_joined.to_parquet("intermediate_files/sic_all.gzip",compression = "gzip")
    print ("End processing SMS incoming data")


# In[29]:


## reading GPRS


def process_gprs():
    
    print ("Start processing GPRS data")

    colnames_gprs = ['GPRS_START_HOUR','GPRS_START_DT','ACCESS_METHOD_ID','SESSION_COUNT','GPRS_MB']


    weekend_aggregations = []
    period_aggregations = []
    weekend_period_aggregations = []

    for data_chunk in pd.read_csv("gprs/gprs.csv",chunksize = 38800000, names = colnames_gprs, dtype = {'GPRS_START_HOUR': np.uint16,
                                                                                        'SESSION_COUNT':np.uint16, 'GPRS_MB': np.uint32}):

        data_chunk['GPRS_START_DT'] = pd.to_datetime(data_chunk['GPRS_START_DT'],format='%y/%m/%d')

        #print (data_chunk.head())
        st = time.time()
        period_aggregations.append(aggregate_on_period(data_chunk,access_method_id_column_name = "ACCESS_METHOD_ID",time_column_name = "GPRS_START_HOUR",dataset = "gprs"))
        end_p = time.time()
        weekend_aggregations.append(aggregate_on_weekday(data_chunk,access_method_id_column_name = "ACCESS_METHOD_ID",date_column_name = "GPRS_START_DT",dataset = "gprs"))
        end_w = time.time()
        weekend_period_aggregations.append(aggregate_on_weekend_period_type(data_chunk,access_method_id_column_name = "ACCESS_METHOD_ID", time_column_name = "GPRS_START_HOUR", date_column_name = "GPRS_START_DT",dataset = "gprs"))
        end_wp = time.time()
        #print ("period","weekend","weekend/period")
        print (end_p - st, end_w - end_p, end_wp- end_w)





    #print (time.time() - total_start)

    gprs_weekend = pd.concat(weekend_aggregations)
    gprs_period = pd.concat(period_aggregations)
    gprs_week_period = pd.concat(weekend_period_aggregations)

    ## second level grouping
    gprs_period = gprs_period.groupby(["ACCESS_METHOD_ID_","Period_"],as_index=False).agg({
    'CALL_DURATION_sum' : sum,
    'NO_OF_CALLS_sum' : sum,
    })
    gprs_weekend = gprs_weekend.groupby(["ACCESS_METHOD_ID_","IsWeekDay_"],as_index=False).agg({
    'CALL_DURATION_sum' : sum,
    'NO_OF_CALLS_sum' : sum,
    })
    gprs_week_period = gprs_week_period.groupby(["ACCESS_METHOD_ID_","IsWeekDay_","Period_"],as_index=False).agg({
    'CALL_DURATION_sum' : sum,
    'NO_OF_CALLS_sum' : sum,
    })



    del (weekend_aggregations,period_aggregations,weekend_period_aggregations)
    gc.collect()

    casted_weekend = process_weekend_after_aggregation(gprs_weekend)
    casted_period = process_period_after_aggregation(gprs_period)
    casted_weekend_period = process_weekday_period_after_aggregation(gprs_week_period)


    del (gprs_weekend,gprs_period,gprs_service,gprs_week_period)

    casted_weekend.columns = map(lambda col: '{}_{}'.format("GPRS",str(col)), casted_weekend.columns)
    casted_weekend.rename(columns={'GPRS_ACCESS_METHOD_ID_': 'ACCESS_METHOD_ID_'}, inplace=True)

    casted_period.columns = map(lambda col: '{}_{}'.format("GPRS",str(col)), casted_period.columns)
    casted_period.rename(columns={'GPRS_ACCESS_METHOD_ID_': 'ACCESS_METHOD_ID_'}, inplace=True)

    casted_weekend_period.columns = map(lambda col: '{}_{}'.format("GPRS",str(col)), casted_weekend_period.columns)
    casted_weekend_period.rename(columns={'GPRS_ACCESS_METHOD_ID_': 'ACCESS_METHOD_ID_'}, inplace=True)


    all_gprs_data = [casted_weekend,casted_period,casted_weekend_period]
    all_gprs_joined = reduce(lambda left,right : pd.merge(left,right,on=['ACCESS_METHOD_ID_'],how = 'outer'),all_gprs_data)

    #print (all_gprs_joined.shape)

    del (casted_period,casted_weekend,casted_weekend_period)

#    print (time.time() - total_start)

    all_gprs_joined.to_parquet("intermediate_files/gprs_all.gzip",compression = "gzip")
    print ("End processing GPRS data")


# In[30]:


## read and process VAS

def process_vas():

    
    print ("Start processing VAS data")

    data = pd.read_csv("lfd_vas_sub.csv")
    data.columns = ["ACCESS_METHOD_ID","offer_desc","offer_id","timestamp"]

    data['Day_week'] = pd.DatetimeIndex(data.timestamp).weekday

    data['IsWeekDay'] = data['Day_week'].replace([0,1,2,3,4,5,6],[1,1,1,1,1,0,0])


    gr = data.groupby(["ACCESS_METHOD_ID","IsWeekDay","offer_desc"],as_index=False).agg({
        'offer_id' : 'count'
        })


    del data
    gc.collect()

    hide_balance_weekend = gr[(gr.offer_desc == "Hide Balance") & (gr.IsWeekDay == 0) ]
    hide_balance_weekend.columns = map(lambda col: '{}_{}'.format(str(col), "hide_balance_weekend"), hide_balance_weekend.columns)
    hide_balance_weekend.rename(columns={'ACCESS_METHOD_ID_hide_balance_weekend': 'ACCESS_METHOD_ID_'}, inplace=True)



    hide_balance_weekday = gr[(gr.offer_desc == "Hide Balance") & (gr.IsWeekDay == 1) ]
    hide_balance_weekday.columns = map(lambda col: '{}_{}'.format(str(col), "hide_balance_weekday"), hide_balance_weekday.columns)
    hide_balance_weekday.rename(columns={'ACCESS_METHOD_ID_hide_balance_weekday': 'ACCESS_METHOD_ID_'}, inplace=True)




    super_mobilink_advance_weekend = gr[(gr.offer_desc == "Super Mobilink Advance") & (gr.IsWeekDay == 0) ]
    super_mobilink_advance_weekend.columns = map(lambda col: '{}_{}'.format(str(col), "super_mobilink_advance_weekend"), super_mobilink_advance_weekend.columns)
    super_mobilink_advance_weekend.rename(columns={'ACCESS_METHOD_ID_super_mobilink_advance_weekend': 'ACCESS_METHOD_ID_'}, inplace=True)



    super_mobilink_advance_weekday = gr[(gr.offer_desc == "Super Mobilink Advance") & (gr.IsWeekDay == 1) ]
    super_mobilink_advance_weekday.columns = map(lambda col: '{}_{}'.format(str(col), "super_mobilink_advance_weekday"), super_mobilink_advance_weekday.columns)
    super_mobilink_advance_weekday.rename(columns={'ACCESS_METHOD_ID_super_mobilink_advance_weekday': 'ACCESS_METHOD_ID_'}, inplace=True)



    JJB_bundle_weekend = gr[(gr.offer_desc == "JJB Zero Rated Bundle") & (gr.IsWeekDay == 0) ]
    JJB_bundle_weekend.columns = map(lambda col: '{}_{}'.format(str(col), "JJB_bundle_weekend"), JJB_bundle_weekend.columns)
    JJB_bundle_weekend.rename(columns={'ACCESS_METHOD_ID_JJB_bundle_weekend': 'ACCESS_METHOD_ID_'}, inplace=True)



    JJB_bundle_weekday = gr[(gr.offer_desc == "JJB Zero Rated Bundle") & (gr.IsWeekDay == 1) ]
    JJB_bundle_weekday.columns = map(lambda col: '{}_{}'.format(str(col), "JJB_bundle_weekday"), JJB_bundle_weekday.columns)
    JJB_bundle_weekday.rename(columns={'ACCESS_METHOD_ID_JJB_bundle_weekday': 'ACCESS_METHOD_ID_'}, inplace=True)


    del gr

    all_vas = [hide_balance_weekday,hide_balance_weekend,super_mobilink_advance_weekday,super_mobilink_advance_weekend, JJB_bundle_weekday,JJB_bundle_weekend]


    df_merged_vas = reduce(lambda  left,right: pd.merge(left,right,on=['ACCESS_METHOD_ID_'],
                                                how='outer'), all_vas)


    del all_vas

    df_merged_vas.columns = map(lambda col: '{}_{}'.format("VAS",str(col)), df_merged_vas.columns)
    df_merged_vas.rename(columns={'VAS_ACCESS_METHOD_ID_': 'ACCESS_METHOD_ID_'}, inplace=True)

    df_merged_vas.to_parquet("intermediate_files/vas_all.gzip",compression = "gzip")
    print ("End processing VAS data")


# In[31]:


## reading revenue and doing initial processing

def process_rev_start():

    
    print ("Start processing Revenue data")
    
    total_revenue_data = []
    i_ = 1
    start = time.time()
    for revenue_chunk in pd.read_csv("rev/revenue.csv",chunksize=26900000,names=['REV_START_DT','GPRS_REV','SMS_REV','NON_USAGE_REV',
                                                                            'VOICE_REV','TOTAL_MOBILINK_REV','ACCESS_METHOD_ID'],
                                    dtype={'REV_START_DT': 'category','GPRS_REV':np.float16,'SMS_REV':np.float16,'SMS_REV': np.float16, 'NON_USAGE_REV': np.float16, 'VOICE_REV': np.float16, 'TOTAL_MOBILINK_REV': np.float16 }):

    #     print (revenue_chunk.head())
        revenue_chunk['codes'] = revenue_chunk['REV_START_DT'].cat.codes
        revenue_chunk['sum_revenue'] = revenue_chunk.iloc[:,1:6].sum(1)
        revenue_chunk['sum_revenue'] = revenue_chunk['sum_revenue'].astype(np.uint16)
    #     print (revenue_chunk.dtypes)
        total_revenue_data.append(revenue_chunk[['REV_START_DT','sum_revenue','ACCESS_METHOD_ID','codes']])
        #print (i_)

        if (i_%3 == 0):
            revenue_segment = pd.concat(total_revenue_data)
            total_revenue_data = []
            del revenue_chunk
            revenue_matrix_dates = revenue_segment.pivot_table(index='ACCESS_METHOD_ID', columns= 'codes', values= 'sum_revenue').reset_index()
            revenue_matrix_dates.columns = revenue_matrix_dates.columns.map(str)

            del revenue_segment
            gc.collect()
            revenue_matrix_dates.fillna(0,inplace = True)
            revenue_matrix_dates.iloc[:,1:] = revenue_matrix_dates.iloc[:,1:].astype(np.uint16)
            name_to_save = "intermediate_files/revenue/revenue_matrix_dates_"+ str(i_)+"_.gzip"
            revenue_matrix_dates.to_parquet(name_to_save,compression = "gzip")
            del revenue_matrix_dates
        i_ += 1

   # print (time.time() - start)
    print ("End processing Revenue data")


# In[32]:


## creating new features from revenue


def create_features_from_revenue():

    
    print ("Start creating new revenue features data")
    
    all_revenue_chunks = []
    for chunk_revenue in range(3,16,3):
        file_name = "intermediate_files/revenue/revenue_matrix_dates_"+ str(chunk_revenue)+"_.gzip"
        print (file_name)
        file_revenue = pd.read_parquet(file_name)
        file_revenue.iloc[:,1:] = file_revenue.iloc[:,1:].astype(np.uint16)
        all_revenue_chunks.append(file_revenue)
        del file_revenue


    revenue_all = pd.concat(all_revenue_chunks)
    del all_revenue_chunks
    gc.collect()

    grouped_revenue_all = revenue_all.groupby(['ACCESS_METHOD_ID']).agg('sum')
    grouped_revenue_all.reset_index(inplace=True)
    del revenue_all



    grouped_revenue_all['sum_last_seven'] = grouped_revenue_all.iloc[:,86:93].sum(1)
    grouped_revenue_all['sum_second_last_week'] = grouped_revenue_all.iloc[:,78:86].sum(1)
    grouped_revenue_all['sum_last_fifteen_days'] = grouped_revenue_all.iloc[:,78:93].sum(1)
    grouped_revenue_all['total_active_days'] = (grouped_revenue_all.iloc[:,2:]!=0).sum(1)
    grouped_revenue_all['last_week_active_days'] = (grouped_revenue_all.iloc[:,86:93]!=0).sum(1)
    grouped_revenue_all['second_last_week_active_days'] = (grouped_revenue_all.iloc[:,78:86]!=0).sum(1)
    grouped_revenue_all['last_fifteen_active_days'] = (grouped_revenue_all.iloc[:,78:93]!=0).sum(1)
    grouped_revenue_all['first_month_rev'] = grouped_revenue_all.iloc[:,2:32].sum(1)
    grouped_revenue_all['second_month_rev'] = grouped_revenue_all.iloc[:,32:63].sum(1)
    grouped_revenue_all['third_month_rev'] = grouped_revenue_all.iloc[:,63:93].sum(1)

    grouped_revenue_all['ratio_second_first_rev'] = grouped_revenue_all['second_month_rev']/(grouped_revenue_all['first_month_rev']+1)
    grouped_revenue_all['ratio_third_second_rev'] = grouped_revenue_all['third_month_rev']/(grouped_revenue_all['second_month_rev']+1)


    grouped_revenue_all['first_month_active_days'] = (grouped_revenue_all.iloc[:,2:32]!=0).sum(1)
    grouped_revenue_all['second_month_active_days'] = (grouped_revenue_all.iloc[:,32:63]!=0).sum(1)
    grouped_revenue_all['third_month_active_days'] = (grouped_revenue_all.iloc[:,63:93]!=0).sum(1)

    grouped_revenue_all['ratio_second_first_days_rev'] = grouped_revenue_all['second_month_active_days']/(grouped_revenue_all['first_month_active_days']+1)
    grouped_revenue_all['ratio_third_second_days_rev'] = grouped_revenue_all['third_month_active_days']/(grouped_revenue_all['second_month_active_days']+1)


    grouped_revenue_all['sum_last_seven_thresh'] = grouped_revenue_all['sum_last_seven'].apply(lambda x: 'L' if x < 20 else 'H')
    grouped_revenue_all['sum_second_last_week_thresh'] = grouped_revenue_all['sum_second_last_week'].apply(lambda x: 'L' if x < 7 else 'H')

    grouped_revenue_all['total_days_thresh'] = grouped_revenue_all['total_active_days'].apply(lambda x: 'L' if x < 15 else 'H')
    grouped_revenue_all['last_fifteen_days_thresh'] = grouped_revenue_all['last_fifteen_active_days'].apply(lambda x: 'L' if x < 5 else 'H')



    variables_important_for_revenue = grouped_revenue_all.iloc[:,92:]
    variables_important_for_revenue['ACCESS_METHOD_ID_'] = grouped_revenue_all.ACCESS_METHOD_ID
    del grouped_revenue_all
    gc.collect()

    variables_important_for_revenue.to_parquet("intermediate_files/revenue_important_features.gzip",compression = "gzip")
    
    print ("End creating new revenue features data")


# In[33]:


## joining all data together with revenue

def join_all_aggregations():
    
    # reading all casted files and do a final join of all frames
    
    print ("Start aggregating all data for model predictions")
    
    vog_all = pd.read_parquet("intermediate_files/vog_all.gzip")
    vic_all = pd.read_parquet("intermediate_files/vic_all.gzip")
    sog_all = pd.read_parquet("intermediate_files/sog_all.gzip")
    sic_all = pd.read_parquet("intermediate_files/sic_all.gzip")
    gprs_all = pd.read_parquet("intermediate_files/gprs_all.gzip")
    vas_all = pd.read_parquet("intermediate_files/vas_all.gzip")
    
    all_features_transactions = [vog_all,vic_all,sog_all,sic_all,gprs_all,vas_all]
    
    
    all_joined_jazz = reduce(lambda  left,right: pd.merge(left,right,on=['ACCESS_METHOD_ID_'],
                                            how='outer'), all_features_transactions)
    
    del vog_all,vic_all,sog_all,sic_all,gprs_all,vas_all
    del all_features_transactions
    gc.collect()
    
    
    rev_all = pd.read_parquet("intermediate_files/revenue_important_features.gzip")
    
    
    ## inner join or outer join between revenue and aggregated features

    all_jazz_features = pd.merge(all_joined_jazz,rev_all, how = "inner",left_on=['ACCESS_METHOD_ID_'], right_on=['ACCESS_METHOD_ID'])
    
    all_jazz_features.to_parquet("intermediate_files/all_features_jazz.gzip",compression = "gzip")
    
    print ("End aggregating all data for model predictions")
    


# In[34]:


def _main_():
    
    ## calling all data processing functions one by one!
    
    st_vog = time.time()
    print ("VOG start!")
    #process_vog()
    print ("VOG done!")
    end_vog = time.time()
    print (end_vog - st_vog)
    
    print ("VIC start!")
    process_vic()
    print ("VIC done!")
    end_vic = time.time()
    print (end_vic - end_vog)
    
    print ("SOG start")
    process_sog()
    print ("SOG done!")
    end_sog = time.time()
    print (end_sog - end_vic)

    print ("SIC start")
    process_sic()
    print ("SIC done!")
    end_sic = time.time()
    print (end_sic - end_sog)

    print ("GPRS start")
    process_gprs()
    print ("GPRS done!")
    end_gprs = time.time()
    print (end_gprs - end_sic)

    print ("VAS start")
    process_vas()
    print ("VAS done!")
    end_vas = time.time()
    print (end_vas - end_gprs)

    
    print ("Revenue Start")
    process_rev_start()
    create_features_from_revenue()
    print ("Revenue done!")
    end_rev = time.time()
    print (end_rev - end_vas)

   
    
#     join_all_aggregations()


# In[35]:


_main_()


