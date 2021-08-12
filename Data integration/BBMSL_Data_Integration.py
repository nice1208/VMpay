import os
import pandas as pd
import xml.etree.ElementTree as et
import matplotlib.pyplot as plt
import codecs
from google.colab import auth
auth.authenticate_user()
import gspread                #!pip install --upgrade gspread
from google.colab import drive 
from oauth2client.client import GoogleCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe  #!pip install gspread-dataframe
from datetime import date
import holidays

gc = gspread.authorize(GoogleCredentials.get_application_default()) #copy
#select excel
sh = gc.open("combine_BBMSL")      

#select sheet
Byday = sh.worksheet("Byday")   
Bymonth = sh.worksheet("Bymonth")
rawdata = sh.worksheet("rawdata") 
merchantid = sh.worksheet("merchantid")
cardtype =sh.worksheet('cardtype')
Weekday =sh.worksheet('Weekday')
food_licence = sh.worksheet('food_licence')
restaurant_licence = sh.worksheet('restaurant_licence')
nonfood_license = sh.worksheet('nonfood_license')
danny_workbench = sh.worksheet('danny_workbench')
holiday = sh.worksheet('holiday')
Temporary = sh.worksheet('Temporary')

drive.mount('/content/drive/')
os.system('clear')
plt.close()

#import data from rawdata 
raw_df = pd.read_csv("/content/drive/MyDrive/Vmission/Raw data/BBMSL/agent-payment.csv")    
df = pd.read_csv("/content/drive/MyDrive/Vmission/Raw data/BBMSL/agent-payment.csv")

#integrate data by different dimension 
danny_workbench_df= df                                                                      
danny_workbench_df=  pd.DataFrame(danny_workbench_df)
danny_workbench_df['created'] = pd.to_datetime(danny_workbench_df['created'])
danny_workbench_df['created'] = danny_workbench_df['created'].dt.year.astype('str') + '-' + danny_workbench_df['created'].dt.month.astype('str') + '-' +danny_workbench_df['created'].dt.day.astype('str')
danny_workbench_df=  pd.DataFrame(danny_workbench_df)
danny_workbench_df = danny_workbench_df.groupby(['created']).netAmount.sum().reset_index()

Day_df  = df
Day_df['created'] = pd.to_datetime(Day_df['created'])
Day_df['created'] = Day_df['created'].dt.year.astype('str') + '-' + Day_df['created'].dt.month.astype('str') + '-' +Day_df['created'].dt.day.astype('str')
Day_df['created'] = pd.to_datetime(Day_df['created'])
Day_df=Day_df.groupby(['agentId','created'])['netAmount'].agg(['sum','count']).reset_index()

holiday_df_raw = df 
holiday_df_raw['created'] = pd.to_datetime(holiday_df_raw['created'])
holiday_df_raw['created'] = holiday_df_raw['created'].dt.year.astype('str') + '-' + holiday_df_raw['created'].dt.month.astype('str') + '-' +holiday_df_raw['created'].dt.day.astype('str')
holiday_df_raw['created'] = pd.to_datetime(holiday_df_raw['created'])
holiday_df_raw['created'] = holiday_df_raw['created'].dt.date
holiday_df_raw=holiday_df_raw.groupby(['agentId','merchantId','created',])['netAmount'].agg(['sum','count']).reset_index()

holiday_df_list= pd.DataFrame()
holiday_date_list=[]
holiday_name_list=[]
for date, name in sorted(holidays.HK(state='CA', years=2021).items()):
  holiday_date_list.append(date)
  holiday_name_list.append(name)

holiday_df_list = pd.DataFrame(list(zip(holiday_date_list, holiday_name_list)), columns=['created','holiday_name'])
holiday_df_list['holiday']= 'holiday'
holiday_df = pd.merge(holiday_df_raw, holiday_df_list, on="created")

Month_df  = df
Month_df['created'] = pd.to_datetime(Month_df['created'])
Month_df['created'] = Month_df['created'].dt.year.astype('str') + '-' + Month_df['created'].dt.month.astype('str') 
Month_df['created'] = pd.to_datetime(Month_df['created'])
Month_df=Month_df.groupby(['agentId','created'])['netAmount'].agg(['sum','count']).reset_index()

merch_df=df
merch_df['created'] = pd.to_datetime(merch_df['created'])
merch_df['created'] = merch_df['created'].dt.year.astype('str') + '-' + merch_df['created'].dt.month.astype('str') + '-' +merch_df['created'].dt.day.astype('str')
merch_df['created'] = pd.to_datetime(merch_df['created'])
merch_df=merch_df.groupby(['created','agentId','merchantId'])['netAmount'].agg(['sum','count']).reset_index()
df = pd.read_csv("/content/drive/MyDrive/Vmission/Raw data/BBMSL/agent-payment.csv")

card_df=df
card_df['created'] = pd.to_datetime(card_df['created'])
card_df['created'] = card_df['created'].dt.year.astype('str') + '-' + card_df['created'].dt.month.astype('str') + '-' +card_df['created'].dt.day.astype('str')
card_df['created'] = pd.to_datetime(card_df['created'])
card_df=card_df.groupby(['agentId','merchantId','cardType'])['netAmount'].agg(['sum','count']).reset_index()
df = pd.read_csv("/content/drive/MyDrive/Vmission/Raw data/BBMSL/agent-payment.csv")

Weekday_df = df
Weekday_df['created'] = pd.to_datetime(Weekday_df['created'])
Weekday_df['created'] =  Weekday_df['created'] .dt.dayofweek
Weekday_df=Weekday_df.rename(columns={'created': 'Weekday'})
Weekday_df=Weekday_df.groupby(['agentId','merchantId','Weekday'])['netAmount'].agg(['sum','count']).reset_index()

mapping_Weekday = {0:'Monday',1:'Tuesday',2:'Wednesday',3:'Thursday',4:'Friday',5:'Saturday',6:'Sunday'}
Weekday_df=Weekday_df.replace({'Weekday': mapping_Weekday})

Temporary_df= df
Temporary_df['created'] = pd.to_datetime(Temporary_df['created'])
Temporary_df['created'] = Temporary_df['created'].dt.year.astype('str') + '-' + Temporary_df['created'].dt.month.astype('str') + '-' +Temporary_df['created'].dt.day.astype('str')
Temporary_df['created'] = pd.to_datetime(Temporary_df['created'])
Temporary_df=Temporary_df.groupby(['created','agentId','cardType'])['netAmount'].agg(['sum','count']).reset_index()

#write integrated data in different tab in excel 

#set_with_dataframe(rawdata ,raw_df ,include_column_header=True,include_index=False, resize=False)
set_with_dataframe(Byday ,Day_df ,include_column_header=True,include_index=False, resize=False)
set_with_dataframe(Bymonth ,Month_df ,include_column_header=True,include_index=False, resize=False)
set_with_dataframe(merchantid ,merch_df ,include_column_header=True,include_index=False, resize=False)
set_with_dataframe(cardtype ,card_df ,include_column_header=True,include_index=False, resize=False)
set_with_dataframe(Weekday ,Weekday_df ,include_column_header=True,include_index=False, resize=False)
set_with_dataframe(holiday ,holiday_df ,include_column_header=True,include_index=False, resize=False)
set_with_dataframe(Temporary ,Temporary_df ,include_column_header=True,include_index=False, resize=False)
