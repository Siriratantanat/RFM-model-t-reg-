
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import LabelEncoder
import seaborn as sns
import datetime as at
import re
from datetime import timezone, datetime, timedelta
import pandas as pd
from pandas.io import gbq
import numpy as np
from google.oauth2 import service_account
import pandas_gbq
from boto3.session import Session
import boto3

sql = """

SELECT userId, originalTimestamp, event, properties_value
FROM datateam-316802.transform.treg_2021
"""

credentials = service_account.Credentials.from_service_account_file('datateam-316802-ac23afcb892e.json')
pandas_gbq.context.credentials = credentials

dt = pandas_gbq.read_gbq(sql, project_id='datateam-316802')


#  Cleansing Data

dt['timestamp']=dt['originalTimestamp']
dt.rename(columns={'userId':'username','event':'events'},inplace=True)
dt1 = dt[dt['username'].notna()]
dt1.timestamp = pd.to_datetime(dt1.timestamp).dt.tz_localize(None)
dt2 = dt1[['username','timestamp','originalTimestamp','events','properties_value']]
datetimes = pd.to_datetime(dt2['originalTimestamp'])
dt2['originalTimestamp'] = dt2['originalTimestamp'].apply(lambda x: x.strftime('%Y-%m-%d'))

#deleted testing's user
dt4 =dt2[dt2["username"].str.contains("@ragnar.co.th|@hotmail.com|@gmail.com|5e17e2d7afa5556f0a76cd50|kamonchanok_ragnar|aaa")==False]
dt5 =dt4.drop(dt4[dt4.username.isin(["a"])].index)

#filter 
dt5['events'] = dt5['events'].fillna('Loading_page')

# label
dt5['label']=0
mask = dt5['username'].str.contains(r'sorkon.co.th|mcgroupnet.com|princhealth.com|somboon.co.th')
dt5['label'][mask] = 1

#regex company
dt5['group']=dt5['username'].str.extract('(?P<group>@.?[\w]+)')
dt5['group']=dt5['group'].replace(r'@','',regex=True)
dt5['group']=dt5['group'].str.lower()
dt6= dt5.sort_values(['originalTimestamp'])
dt6.reset_index(drop=True,inplace=True)

lasted_day=dt6[dt6.originalTimestamp == dt6.originalTimestamp.max()]
lasted_day.reset_index(drop=True,inplace=True)

#  1. Top_event

def top_event(data):
    keep_dataframe = []
    for companyName in data['group'].unique():
        df = data[data['group'] == companyName].reset_index(drop=True)
        df1 =df.events.value_counts()
        df2 =df1.reset_index()
        df2.rename(columns={'index':'event_name','events':'total'},inplace=True)
        df2['company']=str(companyName)
        keep_dataframe.append(df2)
    data_week = pd.concat(keep_dataframe).reset_index(drop=True)
    return data_week 

top_events = top_event(lasted_day)

# Not click module 
# ต้องแก้ non click module ด้วย ในกรณีที่มีการทำแบบรายวัน มันก็จะแค่เปรียบเทียบว่าวันนี้นั้นมีตัวไหนไม่มีการ click บางครั้งเมื่อมีการ คลิก module ใหม่ขึ้นมาเราจำเป็นที่จะต้องเข้าไปลบข้อมูลที่อยู่ใน bq และ replace เป็นข้อมูลใหม่ ณ ทุกๆวัน**
def get_previous_data_from_bq(dataset, table):

    sql = f""" 
    SELECT * FROM datateam-316802.{dataset}.{table}
    """
    df = pandas_gbq.read_gbq(sql, project_id='datateam-316802')
    return df

previous_module=get_previous_data_from_bq('t_reg','modules')
previous_module.rename(columns={'module':'event_name'},inplace =True)

#เอาข้อมูลทั้งหมดเข้ามาเช็คว่าามี module ใหม่อันไหนเพิ่มมาหรือไม่
event_all =dt6.events.value_counts()
event_all_1 = event_all.reset_index()
event_all_1.rename(columns={'index':'event_name','events':'total'},inplace=True)
event_all_2 = event_all_1[['event_name']]
new_module = pd.concat([event_all_2, previous_module]).drop_duplicates(keep='first').reset_index(drop=True)

#ต่อมาเราก็จะมาดูว่ามี event อะไรบ้างในแต่ละบริษัท
def event(data):
    keep_dataframe = []
    for companyName in data['group'].unique():
        df = data[data['group'] == companyName].reset_index(drop=True)
        df1 =df.events.value_counts()
        df2 =df1.reset_index()
        df2.rename(columns={'index':'event_name','events':'total'},inplace=True)
        df2['company']=str(companyName)
        keep_dataframe.append(df2)
    data_week = pd.concat(keep_dataframe).reset_index(drop=True)
    return data_week 
event_all_3 = event(dt6)

def non_click(data,module):
    keep_dataframe = []
    for companyName in data['company'].unique():
        df = data[data['company'] == companyName].reset_index(drop=True)
        df1 = pd.merge(module,df, how='left', on= 'event_name', indicator=True)
        df1.rename(columns={'event_name':'non_click_event'},inplace=True)
        df2=df1.loc[df1["_merge"]=='left_only']
        df2['companies']=str(companyName)
        keep_dataframe.append(df2)
    data_week = pd.concat(keep_dataframe).reset_index(drop=True)
    return data_week  

non_click1=non_click(event_all_3,new_module)
non_click_event=non_click1[['non_click_event','companies']]
non_click_event.rename(columns={'companies':'company'},inplace = True)

#re-check
company_event =event_all_3[event_all_3['company']=='somboon']
print(company_event)
nonclick =non_click_event[non_click_event['company']=='somboon']
print(nonclick)


# Top p_value

lasted_day['properties_value'] = lasted_day['properties_value'].fillna(value='None')

pp=lasted_day.loc[lasted_day['properties_value'].notnull()] #[^\u0E00-\u0E7Fa-zA-Z' ]|^'|'$|''
pp['properties_value']=pp['properties_value'].str.replace('[^\u0E00-\u0E7Fa-zA-Z]','')

def p_value(data):
    keep_dataframe = []
    for companyName in data['group'].unique():
        df = data[data['group'] == companyName].reset_index(drop=True)
        df1 =df.properties_value.value_counts()
        df2 =df1.reset_index()
        df2.rename(columns={'index':'p_value','properties_value':'total'},inplace=True)
        df2['company']=str(companyName)
        keep_dataframe.append(df2)
    data_week = pd.concat(keep_dataframe).reset_index(drop=True)
    return data_week 
p_values = p_value(pp)


# Final

lasted_day = lasted_day.reset_index(drop=True)

#replace 
pandas_gbq.to_gbq(non_click_event,'t_reg.non_click_event', project_id='datateam-316802', if_exists='replace')
pandas_gbq.to_gbq(new_module,'t_reg.modules', project_id='datateam-316802', if_exists='replace')

#append 
def save_to_bq(data, table,credentials):
    try:
        pandas_gbq.context.credentials = credentials
        pandas_gbq.to_gbq(data, f't_reg.{table}', project_id='datateam-316802', if_exists='append')
    except:
        pandas_gbq.context.credentials = credentials
        pandas_gbq.to_gbq(data, f't_reg.{table}', project_id='datateam-316802', if_exists='append')

save_to_bq(lasted_day,"daily",credentials)
save_to_bq(top_events,"top_event",credentials)
save_to_bq(p_values,"p_values",credentials)