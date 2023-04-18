
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

dt.rename(columns={'userID':'username','originalTimestamp':'timestamp','event':'events'},inplace=True)
dt1 = dt[dt['username'].notna()]
dt1.timestamp = pd.to_datetime(dt1.timestamp).dt.tz_localize(None)
dt2 = dt1[['username','timestamp','events','properties_value']]
datetimes = pd.to_datetime(dt2['timestamp'])

# assign your new columns
dt2['day'] = datetimes.dt.day
dt2['month'] = datetimes.dt.month
dt2['year'] = datetimes.dt.year

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
dt6= dt5.sort_values(['timestamp'])

#เลือกเฉพาะกลุ่ม champion
champion =dt6.loc[dt6['label']==1]


# # Groupby weekly and company

# เมื่อเรา clean ข้อมูลเสร็จแล้วเรียบร้อย เราจะทำการ นำข้อมูลของกลุ่ม champion มาทำการ group by week ก่อน โดยถ้า week ไหน ไม่มีการใช้งาน event = 0 เราก็จะให้ week นั้นเป็น 0 
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
#     dataSort = dataSort.drop('week_number', axis=1)
    return dataSort

final = weekFor_DataStudio(champion)

# # for loop to select company and week

# # final_NotSum
def RScoring(x,p,d):
    if x <= d[p][0.25]:
        return 1
    elif x <= d[p][0.50]:
        return 2
    elif x <= d[p][0.75]: 
        return 3
    else:
        return 4


def final_NotSum(data):
    keep = []
    for companyName in data['company'].unique():
        data2 = data[data['company'] == companyName].reset_index(drop=True)
        for j in data2['week_number'].unique():
            week = data2[data2['week_number']== j]
            # RFM model 
            quantiles = week.quantile(q=[0.25,0.5,0.75])
            quantiles = quantiles.to_dict()
            week['Signin'] = week['amount_of_signin'].apply(RScoring, args=('amount_of_signin',quantiles,))
            week['Submitted'] = week['amount_of_submitted'].apply(RScoring, args=('amount_of_submitted',quantiles,))
            week['Event'] = week['all_events'].apply(RScoring, args=('all_events',quantiles,))

            #Calculate and Add RFMGroup value column showing combined concatenated score of RFM
            week['RFMGroup'] = week.Signin.map(str) + week.Submitted.map(str) + week.Event.map(str)

            #Calculate and Add RFMScore value column showing total sum of RFMGroup values
            week['RFMScore'] = week[['Signin', 'Submitted', 'Event']].sum(axis = 1)

            # sunm only RFMScore >=9
            inter = week[week['RFMScore']>=9] #สนใจ
            inter["interest"]= 1 #'interest'
            non = week[week['RFMScore']<9]
            non["interest"] = 0 #'non interest'

            data_check = pd.concat([inter,non],axis=0)
            
            #เงื่อนไขคือ ถ้าใน week นั้นๆ ทุกคนคะแนนน้อยกว่า 9 ให้เลือกเอาคนที่มีคะแนนสูงสุดใน week นั้นมาเป็นเกณณ์
            if len(data_check) == 1:
                df = data_check.reset_index(drop=True)
                keep.append(df)
            elif data_check['interest'].sum() == 0:
                df = data_check[data_check['RFMScore'] == data_check['RFMScore'].max()]
                keep.append(df)
            elif len(data_check) > 1:
                df = data_check[data_check["interest"]==1]
                df = df.reset_index(drop=True)
                keep.append(df)
              
    find_total = pd.concat(keep)
    find_total = find_total.reset_index(drop=True)
    find_total
    return find_total

final_notsum_1 = final_NotSum(final)

# # gruop size and number of user
dt15=champion.username.value_counts()
dt16 = pd.DataFrame(dt15)
dt16.reset_index(inplace=True)
dt16.rename(columns={'index':'username','username':'number_of_event'},inplace=True)
dt16['group']=dt16['username'].str.extract('(?P<group>@.?[\w]+)')
dt16['group']=dt16['group'].replace(r'@','',regex=True)
dt16['group']=dt16['group'].str.lower()

#count number of user 
df1=dt16.group.value_counts()
df3 = pd.DataFrame(df1)
df3.reset_index(inplace=True)
df3.rename(columns={'index':'company','group':'group_size'},inplace=True)

mask = df3['company'].str.contains(r'sorkon')
df3['group_size'][mask] =2 
mask = df3['company'].str.contains(r'somboon')
df3['group_size'][mask] =3 
mask = df3['company'].str.contains(r'princhealth')
df3['group_size'][mask] =1
mask = df3['company'].str.contains(r'mk')
df3['group_size'][mask] =4 

df3['number_of_user']= str()
mask = df3['company'].str.contains(r'sorkon')
df3['number_of_user'][mask] =3 
mask = df3['company'].str.contains(r'somboon')
df3['number_of_user'][mask] = 10
mask = df3['company'].str.contains(r'princhealth')
df3['number_of_user'][mask] = 2
mask = df3['company'].str.contains(r'mk')
df3['number_of_user'][mask] = 22 

amount_of_signin = final_notsum_1.groupby(['company', 'week_number']).agg({'amount_of_signin': lambda x: x.sum()})
amount_of_signin.reset_index(inplace=True)

amount_of_submitted = final_notsum_1.groupby(['company', 'week_number']).agg({'amount_of_submitted': lambda x: x.sum()})
amount_of_submitted.reset_index(inplace=True)

all_events = final_notsum_1.groupby(['company', 'week_number']).agg({'all_events': lambda x: x.sum()})
all_events.reset_index(inplace=True)

res = amount_of_signin.merge(amount_of_submitted, how='inner', left_on=['company', 'week_number'], right_on=['company', 'week_number'])
rest= res.merge(all_events, how='inner', left_on=['company', 'week_number'], right_on=['company', 'week_number'])

#merge 
rest1 = pd.merge(rest,df3, how='left', on= 'company', indicator=True)
rest2= rest1.drop('_merge', 1)
rest2.rename(columns={'week_number':'week'},inplace= True)

g1 =rest2.loc[rest2['company']=='princhealth']
g2 =rest2.loc[rest2['company']=='sorkon']
g3 =rest2.loc[rest2['company']=='somboon']
g4 =rest2.loc[rest2['company']=='mcgroupnet']

def get_previous_data_from_bq(dataset, table):

    sql = f""" 
    SELECT * FROM datateam-316802.{dataset}.{table}
    """

    credentials = service_account.Credentials.from_service_account_file('datateam-316802-ac23afcb892e.json')
    pandas_gbq.context.credentials = credentials

    df = pandas_gbq.read_gbq(sql, project_id='datateam-316802')
    return df
#----------------------------------------------------------------------------------------------------#

metric_g1_previous = get_previous_data_from_bq('t_reg','metric_g1')
metric_g2_previous = get_previous_data_from_bq('t_reg','metric_g2')
metric_g3_previous = get_previous_data_from_bq('t_reg','metric_g3')
metric_g4_previous = get_previous_data_from_bq('t_reg','metric_g4')


# ต้องทำการ fill บางบริษัท เพราะบางอาทิตย์เขาก็ไม่เข้ามาใช้งานเลย ทำให้ข้อมูลไม่มีและไม่สามารถ concatได้ เราจำเป็นที่จะต้องแก้ให้มีข้อมูลก่อนโดยเติม 0 เข้าไปหมายถึงไม่มีการใช้งาน แล้วค่อน concat เข้ากับตารางเดิม และ replace เข้า bq อีกรอบ**

def compare_week(data, data_in_bq):
    keep_diff = []
    try:
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

    except:
        for company_name in data_in_bq['company'].unique():
            sort_data = data_in_bq.sort_values(['company','week']).reset_index(drop=True)
            sort_forSave = sort_data[sort_data['company'] == company_name][-1:].reset_index(drop=True)
            data['week'] = sort_forSave['week'] + 1
            data['company'] = sort_forSave['company']
            data['group_size'] = sort_forSave['group_size']
            data['number_of_user'] = sort_forSave['number_of_user']

            data.fillna(0,inplace=True)
            data['amount_of_signin'] = data['amount_of_signin'].astype(int)
            data['amount_of_submitted'] = data['amount_of_submitted'].astype(int)
            data['all_events'] = data['all_events'].astype(int)
        return data

metric_g1=compare_week(g1,metric_g1_previous )
metric_g2=compare_week(g2,metric_g2_previous )
metric_g3=compare_week(g3,metric_g3_previous )
metric_g4=compare_week(g4,metric_g4_previous )

final_mertic_g1 = pd.concat([metric_g1_previous,metric_g1]).drop_duplicates(keep="first").reset_index(drop=True)
final_mertic_g2 = pd.concat([metric_g2_previous,metric_g2]).drop_duplicates(keep="first").reset_index(drop=True)
final_mertic_g3 = pd.concat([metric_g3_previous,metric_g3]).drop_duplicates(keep="first").reset_index(drop=True)
final_mertic_g4 = pd.concat([metric_g4_previous,metric_g4]).drop_duplicates(keep="first").reset_index(drop=True)

pandas_gbq.to_gbq(final_mertic_g1,"t_reg.metric_g1", project_id='datateam-316802', if_exists='replace')
pandas_gbq.to_gbq(final_mertic_g2,"t_reg.metric_g2", project_id='datateam-316802', if_exists='replace')
pandas_gbq.to_gbq(final_mertic_g3,"t_reg.metric_g3", project_id='datateam-316802', if_exists='replace')
pandas_gbq.to_gbq(final_mertic_g4,"t_reg.metric_g4", project_id='datateam-316802', if_exists='replace')

