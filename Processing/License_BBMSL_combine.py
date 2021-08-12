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

'''
food license
'''

gc = gspread.authorize(GoogleCredentials.get_application_default()) #copy
sh = gc.open("combine_BBMSL")      #select excel

Byday = sh.worksheet("Byday")   #select sheet
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

food_licence_df_xtree = et.parse("/content/drive/MyDrive/Vmission/Raw data/External_data/LP_OtherFood_EN.XML")
food_licence_df_xroot = food_licence_df_xtree.getroot()
food_licence_df_cols = ["TYPE", "DIST", "LICNO", "SS", "ADR", "INFO", "EXPDATE"]
food_licence_df_data = {  # TODO: typeddict
        "TYPE": [],
        "DIST": [],
        "LICNO": [],
        "SS": [],
        "ADR": [],
        "INFO": [],
        "EXPDATE": [],
        }
for node in food_licence_df_xroot:
    print(node.tag)
    if node.tag == "LPS":
        children = list(node)
for child in children:
    for col in food_licence_df_cols:
        food_licence_df_data[col].append(child.find(col).text)

food_licence_df = pd.DataFrame.from_dict(food_licence_df_data)
food_licence_df_mapping_TYPE= {"CL":"Composite Food Shop Licence",
          "FB":"Bakery Licence",
          "FC":"Cold Store Licence",
          "FE":"Factory Canteen Licence",
          "FF":"Food Factory Licence",
          "FP":"Fresh Provision Shop Licence",
          "FG":"Frozen Confection Factory Licence",
          "FM":"Milk Factory Licence",
          "FS":"Siu Mei and Lo Mei Shop Licence"
          }
food_licence_df_mapping_DIST= {
          "11":"Eastern",
          "12":"Wan Chai",
          "15":"Southern",
          "17":"Islands",
          "18":"Central/Western",
          "51":"Kwun Tong",
          "52":"Kowloon City",
          "53":"Wong Tai Sin",
          "61":"Yau Tsim",
          "62":"Mong Kok",
          "63":"Sham Shui Po",
          "91":"Kwai Tsing",
          "92":"Tsuen Wan",
          "93":"Tuen Mun",
          "94":"Yuen Long",
          "95":"Tai Po",
          "96":"North",
          "97":"Sha Tin",
          "98":"Sai Kung"
     }
food_licence_df_mapping_INFO= {
    "#D":"Licensed food premises that are recognized by FEHD for the revised inspection regime after fully implemented the food safety system under ISO 22000 and obtained the ISO 22000 Certification",
    "#E":"Licensed restaurant approved to sell meat to be eaten in raw state for consumption on the premises",
    "#F":"Licensed restaurant approved to sell oyster to be eaten in raw state for consumption on the premises",
    "#G":"Licensed restaurant approved to sell sashimi for consumption on the premises",
    "#H":"Licensed restaurant approved to sell sushi for consumption on the premises",
    "#I":"Licensed fresh provision shop approved to sell live poultry (excluding live water birds and live quails)",
    "#J":"Licensed food factories approved to supply lunch boxes",
    "#Q":"Licensed fresh provision shop approved to sell fresh poultry (excluding water birds)",
    "#R":"Licensed fresh provision shop approved to sell shell fish (other than hairy crab)",
    "#S":"Licensed fresh provision shop approved to sell shell fish (hairy crab)"
}
food_licence_df=food_licence_df.replace({'TYPE':food_licence_df_mapping_TYPE,'DIST':food_licence_df_mapping_DIST,'INFO':food_licence_df_mapping_INFO}, regex=True)


'''
restaurant license
'''

restaurant_licence_xtree = et.parse("/content/drive/MyDrive/Vmission/Raw data/External_data/LP_Restaurants_EN.XML")
restaurant_licence_xroot = restaurant_licence_xtree.getroot()
restaurant_licence_df_LP = ["TYPE", "DIST", "LICNO", "SS", "ADR", "INFO", "EXPDATE"]
#TYPE_ID=["ID", "TYPE"]
restaurant_licence_df_data = {  # TODO: typeddict
        "TYPE": [],
        "DIST": [],
        "LICNO": [],
        "SS": [],
        "ADR": [],
        "INFO": [],
        "EXPDATE": [],
        }
for node in restaurant_licence_xroot:
    if node.tag == "LPS":
        children = list(node)
for child in children:
    for col in restaurant_licence_df_LP:
        restaurant_licence_df_data[col].append(child.find(col).text)
restaurant_licence_df = pd.DataFrame.from_dict(restaurant_licence_df_data)
restaurant_licence_df_mapping_TYPE= {"RL":"General Restaurant Licence",
          "RR":"Light Refreshment Restaurant Licence",
          "MR":"Marine Restaurant Licence"
          }
restaurant_licence_df_mapping_DIST= {
          "11":"Eastern",
          "12":"Wan Chai",
          "15":"Southern",
          "17":"Islands",
          "18":"Central/Western",
          "31":"Food Truck",
          "51":"Kwun Tong",
          "52":"Kowloon City",
          "53":"Wong Tai Sin",
          "61":"Yau Tsim",
          "62":"Mong Kok",
          "63":"Sham Shui Po",
          "91":"Kwai Tsing",
          "92":"Tsuen Wan",
          "93":"Tuen Mun",
          "94":"Yuen Long",
          "95":"Tai Po",
          "96":"North",
          "97":"Sha Tin",
          "98":"Sai Kung"
     }
restaurant_licence_df_mapping_INFO= {
    "#A":"Licensed restaurant with outside seating accommodation",
    "#B":"Licensed restaurant issued with karaoke establishment permit",
    "#C":"Licensed restaurant issued with karaoke establishment exemption order",
    "#D":"Licensed food premises that are recognized by FEHD for the revised inspection regime after fully implemented the food safety system under ISO 22000 and obtained the ISO 22000 Certification",
    "#E":"Licensed restaurant approved to sell meat to be eaten in raw state for consumption on the premises",
    "#F":"Licensed restaurant approved to sell oyster to be eaten in raw state for consumption on the premises",
    "#G":"Licensed restaurant approved to sell sashimi for consumption on the premises",
    "#H":"Licensed restaurant approved to sell sushi for consumption on the premises"
    }
restaurant_licence_df=restaurant_licence_df.replace({'TYPE':restaurant_licence_df_mapping_TYPE,'DIST':restaurant_licence_df_mapping_DIST,'INFO':restaurant_licence_df_mapping_INFO}, regex=True)

'''
nonfood license
'''

nonfood_license_xtree = et.parse("/content/drive/MyDrive/Vmission/Raw data/External_data/LP_NonFood_EN.XML")
nonfood_license_xroot = nonfood_license_xtree.getroot()
nonfood_license_df_cols = ["TYPE", "DIST", "LICNO", "SS", "ADR", "INFO", "EXPDATE"]
nonfood_license_df_data = {  # TODO: typeddict
        "TYPE": [],
        "DIST": [],
        "LICNO": [],
        "SS": [],
        "ADR": [],
        "INFO": [],
        "EXPDATE": [],
        }
for node in nonfood_license_xroot:
    print(node.tag)
    if node.tag == "LPS":
        children = list(node)
for child in children:
    for col in nonfood_license_df_cols:
        nonfood_license_df_data[col].append(child.find(col).text)
assert len(children) == len(nonfood_license_df_data["TYPE"]), f"Size not equal"
nonfood_license_df = pd.DataFrame.from_dict(nonfood_license_df_data)
nonfood_license_df_mapping_TYPE= {
    "TC":"Commercial Bathhouse Licence ",
          "TF":"Funeral Parlour Licence",
          "TO":"Offensive Trade Licence ",
          "TS":"Slaughterhouse Licence ",
          "TP":"Swimming Pool Licence ",
          "TU":"Undertaker's Licence ",
          "PC":"Places of Public Entertainment Licence (Cinema/Theatre)",
          "PE":"Places of Public Entertainment Licence (for Places Other Than Cinemas and Theatres)",
          "KE":"Karaoke Establishment Permit"
          }
nonfood_license_df_mapping_DIST= {
          "11":"Eastern",
          "12":"Wan Chai",
          "15":"Southern",
          "17":"Islands",
          "18":"Central/Western",
          "51":"Kwun Tong",
          "52":"Kowloon City",
          "53":"Wong Tai Sin",
          "61":"Yau Tsim",
          "62":"Mong Kok",
          "63":"Sham Shui Po",
          "91":"Kwai Tsing",
          "92":"Tsuen Wan",
          "93":"Tuen Mun",
          "94":"Yuen Long",
          "95":"Tai Po",
          "96":"North",
          "97":"Sha Tin",
          "98":"Sai Kung"
     }
nonfood_license_df_mapping_INFO= {
    "#K":"Licensed offensive trade endorsed for boiling of lard on the premises ",
    "#L":"Licensed offensive trade endorsed for processing of sharks' fins on the premises ",
    "#C":"Licensed offensive trade endorsed for processing of fish meal on the premises ",
    "#M":"Licensed restaurant approved to sell meat to be eaten in raw state for consumption on the premises",
    "#N":"Licensed offensive trade endorsed for dressing of leather on the premises ",
    "#O":"Licensed offensive trade endorsed for tanning of leather on the premises "
    }
nonfood_license_df=nonfood_license_df.replace({'TYPE':nonfood_license_df_mapping_TYPE,'DIST':nonfood_license_df_mapping_DIST,'INFO':nonfood_license_df_mapping_INFO}, regex=True)

set_with_dataframe(food_licence ,food_licence_df ,include_column_header=True,include_index=False, resize=False)
set_with_dataframe(restaurant_licence ,restaurant_licence_df ,include_column_header=True,include_index=False, resize=False)
set_with_dataframe(nonfood_license ,nonfood_license_df ,include_column_header=True,include_index=False, resize=False)


