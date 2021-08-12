#predict BBMSL data by day and update (allagentId) 100Times
#import libraries
import pandas as pd
import numpy as np
import keras
import tensorflow as tf
from keras.preprocessing.sequence import TimeseriesGenerator
import plotly as py
import plotly.offline as pyoff
import plotly.graph_objs as go
from google.colab import drive 
from keras.models import Sequential 
from keras.layers import LSTM, Dense

from google.colab import auth
auth.authenticate_user()
import gspread  
from oauth2client.client import GoogleCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe

drive.mount('/content/drive/')

#import rawdata
filename = "/content/drive/MyDrive/Vmission/Raw data/BBMSL/agent-payment.csv"
df = pd.read_csv(filename)
df = pd.DataFrame(df)
final_result_df = pd.DataFrame()
result_df_new = pd.DataFrame()
agent_id= df['agentId'].unique()

for i in range(len(agent_id)): #loop for each agent
      df = pd.read_csv(filename)
      df = pd.DataFrame(df)
      df = df.loc[df['status'] == 'SUCCESS']
      df = df.loc[df['agentId'] == agent_id[i]]
      index = df.index
      number_of_rows = len(index)
      df['created'] = pd.to_datetime(df['created'])

      df = df.groupby(pd.Grouper(key='created',freq='D')).netAmount.sum().reset_index() #date time group by day
      close_data = df['netAmount'].values
      close_data = close_data.reshape((-1,1))

      split_percent = 0.75                                                             #training/testing set ratio
      split = int(split_percent*len(close_data))

      close_train = close_data[:split]
      close_test = close_data[split:]

      date_train = df['created'][:split]
      date_test = df['created'][split:]

      print(len(close_train))
      print(len(close_test))
      look_back = 31                                                                    #the lookback constant
      if look_back>len(close_train):
        look_back = 7
      if look_back>len(close_test):
        look_back = 7
      result_df= pd.DataFrame()
      train_generator = TimeseriesGenerator(close_train, close_train, length=look_back, batch_size=15)     
      test_generator = TimeseriesGenerator(close_test, close_test, length=look_back, batch_size=1)
      print(i)
      for j in range (25):                                                              #the iteration of prediction value
            model = Sequential()
            model.add(
                LSTM(50,
                    activation='relu',
                    input_shape=(look_back,1))
            )
            model.add(Dense(1))
            model.compile(optimizer='adam', loss='mse')

            num_epochs = 100
            model.fit_generator(train_generator, epochs=num_epochs, verbose=1)

            prediction = model.predict_generator(test_generator)

            close_train = close_train.reshape((-1))
            close_test = close_test.reshape((-1))
            prediction = prediction.reshape((-1))

            close_data = close_data.reshape((-1))

            def predict(num_prediction, model):
                prediction_list = close_data[-look_back:]
                
                for _ in range(num_prediction):
                    x = prediction_list[-look_back:]
                    x = x.reshape((1, look_back, 1))
                    out = model.predict(x)[0][0]
                    prediction_list = np.append(prediction_list, out)
                prediction_list = prediction_list[look_back-1:]
                    
                return prediction_list
                
            def predict_dates(num_prediction):
                last_date = df['created'].values[-1]
                prediction_dates = pd.date_range(last_date, periods=num_prediction+1).tolist()
                return prediction_dates

            num_prediction = 30                                                                 #prediction period (one month)                
            forecast = predict(num_prediction, model)
            forecast_dates = predict_dates(num_prediction)

            trace1 = go.Scatter(
                x = date_train,
                y = close_train,
                mode = 'lines',
                name = 'Data'
            )
            trace2 = go.Scatter(
                x = forecast_dates,
                y = forecast,
                mode = 'lines',
                name = 'Prediction'
            )
            trace3 = go.Scatter(
                x = date_test,
                y = close_test,
                mode='lines',
                name = 'Ground Truth'
            )
            layout = go.Layout(
                title = "BBMSL prediction"+ str(agent_id[i]),
                xaxis = {'title' : "Date"},
                yaxis = {'title' : "Netamount"}
            )
            fig = go.Figure(data=[trace1, trace2, trace3], layout=layout)
            
            fig.show()
            print(j)
            if (j==0):
              result_df['created'] = forecast_dates
              result_df['agentId']= str(agent_id[i])
              result_df["netAmount"] = ""
              result_df['Prediction_Value'] = forecast
            if (j>0):
              result_df_new['created']=forecast_dates
              result_df_new['agentId']=str(agent_id[i])
              result_df["netAmount"] = ""
              result_df_new['Prediction_Value']=forecast
              result_df = result_df.append(result_df_new)
              
      if (i==0):
          final_result_df = result_df
      if (i>0):
          final_result_df= final_result_df.append(result_df) 
filename = "/content/drive/MyDrive/Vmission/Raw data/BBMSL/agent-payment.csv"
df = pd.read_csv(filename)  
          
Temporary_df= df
Temporary_df['created'] = pd.to_datetime(Temporary_df['created'])
Temporary_df['created'] = Temporary_df['created'].dt.year.astype('str') + '-' + Temporary_df['created'].dt.month.astype('str') + '-' +Temporary_df['created'].dt.day.astype('str')
Temporary_df['created'] = pd.to_datetime(Temporary_df['created'])
Temporary_df=Temporary_df.groupby(['created','agentId'])['netAmount'].agg(['sum']).reset_index()

final_result_df= final_result_df.append(Temporary_df) 

            

with open('/content/drive/MyDrive/Vmission/Data Lake/Prediction_Test.csv', 'w') as f:
  result_df.to_csv(f)
f.close()

gc = gspread.authorize(GoogleCredentials.get_application_default()) #copy
excel = gc.open("combine_data")      #select excel
sh = excel.worksheet("Prediction_Test_60")    #select sheet

set_with_dataframe(sh ,final_result_df ,include_column_header=True,include_index=False, resize=False)
