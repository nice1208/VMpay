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
import glob

gc = gspread.authorize(GoogleCredentials.get_application_default()) #copy
#select excel
sh = gc.open("combine_PINME")  

#select sheet
rawdata = sh.worksheet("rawdata")
Byday = sh.worksheet("Byday")
Weekday = sh.worksheet("Weekday")

drive.mount('/content/drive/')
os.system('clear')
plt.close()

#integrate data by different dimension
all_data = pd.DataFrame()
for f in glob.glob("/content/drive/MyDrive/Vmission/Raw data/PINME/*.xlsx"):
    df = pd.read_excel(f)
    df["agentId"]=str(os.path.basename(f))
    df['agentId'] = df['agentId'].str.extract(r'(\d{4})')
    all_data = all_data.append(df,ignore_index=True)

Byday_df = pd.DataFrame()
Byday_df['created'] = all_data['日期']
Byday_df['created']= pd.to_datetime(Byday_df['created'])
Byday_df['netAmount'] = all_data['sum']
Byday_df['count'] = all_data['count']
Byday_df['agentId'] = all_data['agentId']

Weekday_df = Byday_df
Weekday_df['created'] =  Weekday_df['created'] .dt.dayofweek
Weekday_df=Weekday_df.rename(columns={'created': 'Weekday'})
Weekday_df=Weekday_df.groupby(['agentId','Weekday'])['netAmount'].agg(['sum','count']).reset_index()

#write integrated data in excel tabs
set_with_dataframe(rawdata ,all_data ,include_column_header=True,include_index=False, resize=False)
set_with_dataframe(Byday ,Byday_df ,include_column_header=True,include_index=False, resize=False)
set_with_dataframe(Weekday ,Weekday_df ,include_column_header=True,include_index=False, resize=False)
