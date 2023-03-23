from asyncio.windows_events import NULL
from dataclasses import replace
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import datetime
import gscDataListFallingSERP as GDL
import AverageSERPGSC as ASG

# DECLARATION OF ENGINES FOR AGS AND GSC DATA-TABLES

engine_AGStable = create_engine('mysql+pymysql://root:!ITCadmin20@10.25.38.215/intercot_AGS')
engine_GSCtable = create_engine('mysql+pymysql://root:!ITCadmin20@10.25.38.215/interco_gsc_db')

# PERIOD FOR WHICH DATA BETWEEN TABLES HAVE TO BE JOINED
start_date = datetime.date.today() - datetime.timedelta(days=7)
start_date = start_date.strftime('%Y-%m-%d')
end_date = datetime.date.today() 
end_date = end_date.strftime('%Y-%m-%d')

# QUERY TO RECOVER DATA FROM AGS AND GSC DATABASES AND CONVERT TO DATAFRAME
sql1 = 'SELECT * FROM interco_gsc_db.intercotradingco_backup_gsc WHERE `date` BETWEEN "'+ start_date +'" AND "'+end_date+'"'
sql2 = 'SELECT `id`,CONVERT(`date_time`,DATE) as `date_time`,`ags_version`,`server_id`,`instance_id`,`instance`,`xlsx_file_name`,`keyword`,`result_no`,`page_no`,`website`,`website_link`,`actual_url`FROM intercot_AGS.all_time_successful WHERE CONVERT(date_time, DATE) BETWEEN "'+ start_date +'" AND "'+end_date+'"'

dataFrame1 = pd.read_sql(sql1,con=engine_AGStable)
dataFrame2 = pd.read_sql(sql2,con=engine_GSCtable)

# MERGE DATAFRAME1 AND DATAFRAME2
dataFrame3 = pd.merge(dataFrame1,dataFrame2,how='left',left_on=['query','page','date'],right_on=['keyword','website_link','date_time'])

# COPY THE REQUIRED COLUMNS INTO A NEW DATAFRAME. REMOVE NULL VALUES AND REMOVE DUPLICATE COLUMS
dataFrame4 = dataFrame3[['date','page','query','clicks','impressions','ctr','position','result_no','page_no']].copy()
dataFrame4['result_no'] = dataFrame4['result_no'].replace('',np.nan)
dataFrame4 = dataFrame4.dropna(axis=0, subset=['result_no'])
dataFrame4['id'] = np.arange(1,len(dataFrame4)+1)
dataFrame4 = dataFrame4[['id','date','page','query','clicks','impressions','ctr','position','result_no','page_no']].copy()
dataFrame4 = dataFrame4.drop_duplicates(subset=['id','date','page','query','clicks','impressions','ctr','position','result_no','page_no'],keep=False)

dataFrame4.to_csv('c:/Users/guru/OneDrive - Interco/Desktop/test.csv',index=False)

# CONVERT DATAFRAME TO DATABASE TABLE AND ADD PRIMARY KEY
tableName = "testJoin"
dbConnection = engine_GSCtable.connect()
try:
    frame = dataFrame4.to_sql(tableName,dbConnection,if_exists= 'replace')
except ValueError as vx:
    print(vx)
except Exception as ex:
    print(ex)
else:
    print("Table created successfully!")
finally:
    dbConnection.close()

with engine_GSCtable.connect() as con:
    dbConnection = engine_GSCtable.connect()
dbConnection.execute('ALTER TABLE `testJoin` ADD PRIMARY KEY (`id`);')

dbConnection.close()

# CALL MODULE TO GENERATE A LIST FOR SITE KEYWORD COMBINATION WITH FALLING SERP POSITION
GDL.SERPFallListGenerate()

dataFrame5 = pd.read_csv('c:/Users/guru/OneDrive - Interco/Desktop/gsc_data.csv')

# APPEND TRUE OR FALSE IF KEYWORD AND SITE IS PRESENT IN FALLING SERP LIST TO THE DATAFRAME
SERPfallData = []
for i in range(len(dataFrame4)):
    # print("i: ",i)
    isPresent = "False"
    tempAvgVal = 0
    for j in range(len(dataFrame5)):
        # print("j: ",j)
        if dataFrame4.iloc[i,2] == dataFrame5.iloc[j,1] and dataFrame4.iloc[i,3] == dataFrame5.iloc[j,0]:
            isPresent = "True"
    SERPfallData.append(isPresent)

print(len(SERPfallData))

count  = 0
for i in range(len(SERPfallData)):
    if SERPfallData[i]:
        count = count+1

print(count)
dataFrame4 = dataFrame4.assign(isSERPFalling=SERPfallData)
#----------------------------------------------------------------------------------------------------------------------------------------------

# APPEND AVERAGE SERP VALUE FOR EACH KEYWORD AND WEBSITE COMBINATION 

ASG.AverageforKeyPagePair()
dataFrame6 = pd.read_csv('c:/Users/guru/OneDrive - Interco/Desktop/gsc_data_AVG.csv')
page_newList = dataFrame6['page_new'].tolist()
query_newList = dataFrame6['query_new'].tolist()
AvgPosList = dataFrame6['AvgPosition'].tolist()

print(len(page_newList),",",len(query_newList),",",len(AvgPosList))
dataFrame4 = dataFrame4.assign(page_new=page_newList)
dataFrame4 = dataFrame4.assign(query_new = query_newList)
dataFrame4 = dataFrame4.assign(averageposition = AvgPosList)

#----------------------------------------------------------------------------------------------------------------------------------------------

dataFrame4.to_csv('c:/Users/guru/OneDrive - Interco/Desktop/test.csv',index=False)
# UPDATE JOIN DATABASE TABLE WITH DATAFRAME4

tableName = "testJoin_final"
dbConnection = engine_GSCtable.connect()
try:
    frame = dataFrame4.to_sql(tableName,dbConnection,if_exists= 'replace')
except ValueError as vx:
    print(vx)
except Exception as ex:
    print(ex)
else:
    print("Table created successfully!")
finally:
    dbConnection.close()

# with engine_GSCtable.connect() as con:
dbConnection = engine_GSCtable.connect()
dbConnection.execute('ALTER TABLE `testJoin_final` ADD PRIMARY KEY (`id`);')
dbConnection.close()

print(sql2)