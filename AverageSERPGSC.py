import pandas as pd
from sqlalchemy import create_engine
import statistics

def AverageforKeyPagePair():
    # CREATE DATABASE CONNECTION WITH GSC DATABASE
    sqlEngine = create_engine('mysql+pymysql://root:!ITCadmin20@10.25.38.215/interco_gsc_db')
    connection = sqlEngine.connect()

    # QUERY DATA FOR THE PERIOD OF 7 DAYS FROM GSC DATABASE AND STORE THE DATA IN A DATAFRAME
    dataFrame = pd.read_sql("select * from interco_gsc_db.testJoin where `date` between CURRENT_DATE-7 and CURRENT_DATE",connection)

    count = 0
    AVG_Value = []
    # RECORD THE SERP DATA IN A LIST AND APPEND IT ALONG WITHH THE AVERAGE SERP VALUE FOR THE PERIOD
    for i in range(len(dataFrame)):
        SQL = 'SELECT AVG(`position`) FROM `testJoin` WHERE `page`="'+dataFrame.iloc[i,3]+'" AND `query`="'+dataFrame.iloc[i,4]+'"'
        dataFrame2 = pd.read_sql(SQL,connection)
        print(dataFrame2.iloc[0,0])
        temp = [dataFrame.iloc[i,3],dataFrame.iloc[i,4],dataFrame2.iloc[0,0]]
        AVG_Value.append(temp)
        count = count+1
    dataFrame3 = pd.DataFrame(AVG_Value)
    dataFrame3.columns = ["page_new","query_new","AvgPosition"]
    print("count: ",count)
    # dataFrame3 = dataFrame3.assign(AveragePosition = AVG_Value)

    dataFrame3.to_csv('c:/Users/guru/OneDrive - Interco/Desktop/gsc_data_AVG.csv',index=False)

if __name__ == '__main__':
    AverageforKeyPagePair()