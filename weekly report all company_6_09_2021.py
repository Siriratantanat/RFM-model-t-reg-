
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

WITH a as
    (SELECT MAX(originalTimestamp) AS Maxdate
    from datateam-316802.transform.treg_2021)
    select b.*
    from datateam-316802.transform.treg_2021 b , a
    WHERE b.originalTimestamp BETWEEN DATE_SUB(a.Maxdate, INTERVAL 6 DAY) AND  b.originalTimestamp
    order by b.originalTimestamp desc

"""

credentials = service_account.Credentials.from_service_account_file('datateam-316802-ac23afcb892e.json')
pandas_gbq.context.credentials = credentials

dt = pandas_gbq.read_gbq(sql, project_id='datateam-316802')

# # Cleansing Data

dt.rename(columns={'userId':'username','originalTimestamp':'timestamp','event':'events'},inplace=True)
dt1 = dt[dt['username'].notna()]
dt1.timestamp = pd.to_datetime(dt1.timestamp).dt.tz_localize(None)
dt2 = dt1[['username','timestamp','events','properties_value']]
datetimes = pd.to_datetime(dt2['timestamp'])


# assign your new columns
dt2['day'] = datetimes.dt.day
dt2['month'] = datetimes.dt.month
dt2['year'] = datetimes.dt.year

#deleted tesuserIDtaing's user
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
dt6= dt5.sort_values(['timestamp'])


def regex(string_input, pattern): #สร้าง columns ใหม่ โดยการ regex เอาจาก week
    string = string_input
    pattern_name = pattern
    result_name = re.findall(pattern_name, string_input)
    return result_name

#-----------------------------------------------------------------------------------------------------------------#
def sort_week(data): #สร้าง function เพื่อเอาไปใช้ใน weekFor_DataStudio function ต่อไป 
    data['week_number'] = int(0)
    for index in data.index:
        data['week_number'][index] = int(regex(data['week'][index],'\d+')[0])
    data = data.sort_values(by=['week_number']).reset_index(drop=True)
    return data

#-----------------------------------------------------------------------------------------------------------------#

def insert_MissingWeek(data): #สร้าง row ใหม่ในกรณีที่บางบริษัทนั้นไม่ได้เข้ามาใช้งาน ให้ week นั้นเป็น 0 
    list_week_check = list(range(data['week_number'].min(), data['week_number'].max()+1))# created range number begining with the first week to the last week
    list_week_data = list(data['week_number'].unique())#สร้าง object โดยการ uique week ที่บริษัทนั้นเข้ามาใช้บริการ 
    for index in list_week_check: #เป็นการเช็คว่า ถ้า week ไหน ไม่มีคนเข้ามาใช้งานเลย ให้สร้าง row of that week by showing zero value
        if index not in list_week_data:
            new_row = {'company':data['company'].unique()[0], 'week':f'week_{index}', 'username':'0',
                       'amount_of_signin':0, 'amount_of_submitted':0,'all_events':0,
                       'week_number':int(index)}
            data = data.append(new_row, ignore_index=True)
    return data

#-----------------------------------------------------------------------------------------------------------------#

def weekFor_DataStudio(data):
    # Build columns week in dataframe
    keep_dataframe = []
    for companyName in data['group'].unique():
        df = data[data['group'] == companyName].reset_index(drop=True)
        date_list = [pd.to_datetime(i.strftime(f'%Y-%m-%d')) for i in pd.date_range(df['timestamp'][0], df['timestamp'][-1:].values[0], freq='7D')]
        df['Week'] = str('0')
        for i in range(len(date_list)):
            for j in df.index:
                try:
                    if (df['timestamp'][j] >= date_list[i]) & (df['timestamp'][j] < date_list[i+1]):
                        df['Week'][j] = f'week_{i+1}'
                except IndexError:
                    if df['timestamp'][j] >= date_list[i]:
                        df['Week'][j] = f'week_{i+1}'
        keep_dataframe.append(df)

    data_week = pd.concat(keep_dataframe).reset_index(drop=True)

    # Group by data
    word_for_find = ['Signin', 'ยืนยัน']
    name_columns = ['amount_of_signin', 'amount_of_submitted']
    keep_dict = {}
    for i in range(len(word_for_find)):
        dff = data_week.groupby(['group','Week','username']).agg({'events': lambda x: x.str.contains(f'{word_for_find[i]}').sum()})
        dff.rename(columns={'events':f'{name_columns[i]}'},inplace=True)
        keep_dict.update(dff)

    all_event = data_week.groupby(['group','Week','username']).agg({'username': lambda x: x.value_counts()})
    all_event.rename(columns={'username':'all_events'},inplace=True)
    keep_dict.update(all_event)

    # Convert dict to dataframe & Rename
    final = pd.DataFrame(keep_dict)
    final.reset_index(inplace=True)
    final.rename(columns={'group':'company','Week':'week',},inplace=True)

    # Insert Missing Week 
    data_sorted = []
    for i in final['company'].unique():
        dataSort = final[final['company'] == i].reset_index(drop=True)
        # Sort & build column ['week_number']
        dataSort = sort_week(dataSort)
        # Insert Missing Week
        dataSort = insert_MissingWeek(dataSort)
        # Sort data
        dataSort = sort_week(dataSort)
        data_sorted.append(dataSort)

    dataSort = pd.concat(data_sorted)
    return dataSort

final = weekFor_DataStudio(dt6)
final.drop(['week'],axis=1,inplace=True)
final.rename(columns={'week_number':'week'},inplace=True)

def get_previous_data_from_bq(dataset, table):

    sql = f""" 
    SELECT * FROM datateam-316802.{dataset}.{table}
    """
    df = pandas_gbq.read_gbq(sql, project_id='datateam-316802')
    return df
#----------------------------------------------------------------------------------------------------#
data_studio_previous = get_previous_data_from_bq('t_reg','data_studio3')
keep = final[['company','username']]
previous =data_studio_previous[['company','username']]
data_for_save = pd.concat([keep, previous]).drop_duplicates(keep='first').reset_index(drop=True)
keep_1=data_for_save[~data_for_save.username.str.contains("0")]
keep_2=keep_1.company.value_counts()
keep_3=keep_2.reset_index()
keep_3.rename(columns={'index':'company','company':'number_of_user'},inplace=True)

#--------------------------------------------------------------------------------------------------#

keep_3['group_size'] = 0
for i in range(len(keep_3["number_of_user"])):
    if keep_3["number_of_user"][i] <= 2 :
        keep_3["group_size"][i] = "1"

    elif keep_3["number_of_user"][i] >= 3  and keep_3["number_of_user"][i] <= 6:
        keep_3["group_size"][i] = "2"

    elif keep_3["number_of_user"][i] >= 7 and keep_3["number_of_user"][i] <= 12:
        keep_3["group_size"][i] = "3"

    elif keep_3["number_of_user"][i] >= 13:
        keep_3["group_size"][i] = "4"

#----------------------------------------------------------------------------------------------#

#merge 
final_1 = pd.merge(final,keep_3, how='left', on= 'company', indicator=True)
final_2= final_1.drop('_merge', 1) #ข้อมูลใหม่ที่ทำการเติม group_size และ number of user แล้ว

def compare_data_studio3(data, data_in_bq):
    keep_diff = []
    for company_name in data['company'].unique():
        sort_data = data_in_bq.sort_values(['company','week']).reset_index(drop=True)
        data_in_bq_1 = sort_data[['company','week']]
        sort_forSave = data_in_bq_1[data_in_bq_1['company'] == company_name][-1:].reset_index(drop=True)
        keep_diff.append(sort_forSave)
    last_week = pd.concat(keep_diff).reset_index(drop=True)
    last_week['weeks']=last_week['week']
    last_week['week'] = last_week['weeks'] + 1
    last_week_1=last_week[['company','week']]

    data_1 = pd.merge(data,last_week_1, how='left', on= 'company', indicator=True)
    data_1.rename(columns={'week_y':'week'},inplace= True)
    data_1.drop(['week_x','_merge'],axis=1,inplace=True)

    #--------------------------------------------------------------------#   

    data_2 = pd.concat([data_in_bq,data_1]).drop_duplicates(keep="first").reset_index(drop=True)
    data_3 = data_2.sort_values(['company','week']).reset_index(drop=True)
    data_3['week']=data_3['week'].fillna(0)
    data_3['week'] = data_3['week'].astype(int)

    return data_3
data_studio3 = compare_data_studio3(final_2,data_studio_previous)

# Change number of user and gruop_size bc they have new users

number =final_2[["company",'number_of_user','group_size']]
number_1= number.drop_duplicates(keep="first").reset_index(drop=True)
number_2= pd.merge(data_studio3,number_1, how='left', on= 'company', indicator=True)
number_2['number_of_user_y'].fillna(number_2['number_of_user_x'], inplace=True)
number_2['number_of_user_y'] = number_2['number_of_user_y'].astype(int)
number_2['group_size_y'].fillna(number_2['group_size_x'], inplace=True)
number_2['group_size_y'] = number_2['group_size_y'].astype(int)
number_2.drop(['number_of_user_x','group_size_x','_merge'],axis=1,inplace=True)
number_2.rename(columns={'number_of_user_y':'number_of_user','group_size_y':'group_size'},inplace= True)

# 2. sum each week by company

amount_of_signin = final_2.groupby(['company', 'week']).agg({'amount_of_signin': lambda x: x.sum()})
amount_of_signin.reset_index(inplace=True)

amount_of_submitted = final_2.groupby(['company', 'week']).agg({'amount_of_submitted': lambda x: x.sum()})
amount_of_submitted.reset_index(inplace=True)

all_events = final_2.groupby(['company', 'week']).agg({'all_events': lambda x: x.sum()})
all_events.reset_index(inplace=True)

res = amount_of_signin.merge(amount_of_submitted, how='inner', left_on=['company', 'week'], right_on=['company', 'week'])
rest= res.merge(all_events, how='inner', left_on=['company', 'week'], right_on=['company', 'week'])

#merge 
rest1 = pd.merge(rest,number_1, how='left', on= 'company', indicator=True)
rest2= rest1.drop('_merge', 1)


#compare
def compare_week(data, data_in_bq):
    keep_diff = []
    for company_name in data['company'].unique():
        sort_data = data_in_bq.sort_values(['company','week']).reset_index(drop=True)
        sort_forSave = sort_data[sort_data['company'] == company_name][-1:].reset_index(drop=True)
        keep_diff.append(sort_forSave)
    last_week = pd.concat(keep_diff).reset_index(drop=True)

    company_same = last_week[last_week['company'].isin(data['company'])].reset_index(drop=True)
    company_diff = data[~data['company'].isin(last_week['company'])]
    company_same['week'] = company_same['week'] + 1
    company_same = company_same.dropna().reset_index(drop=True)
    company_same['week'] = company_same['week'].astype(int)
    data_cal = pd.concat([company_diff, company_same]).reset_index(drop=True)
    data_cal = data_cal.sort_values('company').reset_index(drop=True)
    data = data.sort_values('company').reset_index(drop=True)
    data['week'] = data_cal['week']
    return data

rest3 =compare_week(rest2,data_studio_previous)

#group save
g1 = rest3.loc[rest3['group_size']==1].reset_index(drop=True)
g2 = rest3.loc[rest3['group_size']==2].reset_index(drop=True)
g3 = rest3.loc[rest3['group_size']==3].reset_index(drop=True)
g4 = rest3.loc[rest3['group_size']==4].reset_index(drop=True)

g1_previous = get_previous_data_from_bq('t_reg','g1')
g2_previous = get_previous_data_from_bq('t_reg','g2')
g3_previous = get_previous_data_from_bq('t_reg','g3')
g4_previous = get_previous_data_from_bq('t_reg','g4')

final_g1 = pd.concat([g1_previous,g1]).drop_duplicates(keep="first").reset_index(drop=True)
final_g2 = pd.concat([g2_previous,g2]).drop_duplicates(keep="first").reset_index(drop=True)
final_g3 = pd.concat([g3_previous,g3]).drop_duplicates(keep="first").reset_index(drop=True)
final_g4 = pd.concat([g4_previous,g4]).drop_duplicates(keep="first").reset_index(drop=True)

pandas_gbq.to_gbq(data_studio3,"t_reg.data_studio3", project_id='datateam-316802', if_exists='replace')
pandas_gbq.to_gbq(final_g1,"t_reg.g1", project_id='datateam-316802', if_exists='replace')
pandas_gbq.to_gbq(final_g2,"t_reg.g2", project_id='datateam-316802', if_exists='replace')
pandas_gbq.to_gbq(final_g3,"t_reg.g3", project_id='datateam-316802', if_exists='replace')
pandas_gbq.to_gbq(final_g4,"t_reg.g4", project_id='datateam-316802', if_exists='replace')