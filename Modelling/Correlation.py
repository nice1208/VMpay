import os
import pandas as pd
import statsmodels.api as sm
import statsmodels.formula.api as smf
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.formula.api import ols
import seaborn as sns
from google.colab import drive 
from google.colab import auth
auth.authenticate_user()
import gspread  
from oauth2client.client import GoogleCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe


drive.mount('/content/drive/')                                                                   #select drive to get the raw data
pd.set_option("display.max_rows", 20, "display.max_columns", 5)

#clear console
os.system('clear')
plt.close()

df = pd.read_csv("/content/drive/MyDrive/Vmission/Raw data/BBMSL/agent-payment.csv")            #select the location of raw data
df = df.loc[(df['agentId'] == 3175) & (df['merchantId'] == 4569)]                               #select the specific merchantId

mapping = {344:'HKD'}                                                                           #mapping the currency
new_df=df.replace({'currency': mapping})

#the linear model 

model = smf.ols(formula="netAmount ~ category + cardType + currency + status + tips + employeeId", data=new_df).fit()   #the linear regression to find correlation of the specific variable 
'''
you cna chose the correlatoin index of the formula.
in this situation, we used "category + cardType + currency + status + tips + employeeId" to find the correlation with netamount 
those data is get from new_df
'''
print(new_df)
#print output

print(model.summary())                          # print the summary of the result in detial tale.

#find the correlation of factor
corr = new_df.corr()
corr.style.background_gradient(cmap='coolwarm')

result_df= model.params                         # get the result valur
result_df=result_df.to_frame()                  #change the result into dataframe
result_df=result_df.rename({0:'abc'},axis='columns')

print(result_df)

result_df.to_csv('correlation.csv')             #output as CSV

'''        
with open('/content/drive/MyDrive/Vmission/Data Lake/correlation.csv', 'w') as f:
  result_df.to_csv(f)
f.close()
'''


gc = gspread.authorize(GoogleCredentials.get_application_default()) #copy
excel = gc.open("combine_data")      #select excel
sh = excel.worksheet("coefficient")    #select sheet

set_with_dataframe(sh,result_df ,include_column_header=False,include_index=True, resize=False)   #write it into target location


'''
new_df=df.rename({0:'id',
                  1:"created", 
                  2:"category",
                  3:"cardType",
                  4:"currency",
                  5:"total",
                  6:"netAmount",                       
                  7:"status",
                  8:"tips",
                  9:"agentId"
                  10:"merchantId",
                  11:"employeeId",
                  12:"description",
                  13:"updated"}, axis='columns')
'''
