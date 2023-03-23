import pandas as pd
from sqlalchemy import create_engine
import statistics

def SERPFallListGenerate():
    # CREATE DATABASE CONNECTION WITH GSC DATABASE
    sqlEngine = create_engine('mysql+pymysql://root:!ITCadmin20@10.25.38.215/interco_gsc_db')
    connection = sqlEngine.connect()

    # QUERY DATA FOR THE PERIOD OF 7 DAYS FROM GSC DATABASE AND STORE THE DATA IN A DATAFRAME
    dataFrame = pd.read_sql("select * from interco_gsc_db.testJoin where `date` between CURRENT_DATE-7 and CURRENT_DATE ORDER BY `query`,`page`,`date`",connection)

    # GENERATE AN ARRAYS FOR UNIQUE VALUES OF KEYWORD AND WEBSITES 
    queryUniqueArray = dataFrame["query"].unique()
    pageUniqueArray = dataFrame["page"].unique()

    print(len(queryUniqueArray),len(pageUniqueArray), " expected loop count: ",len(queryUniqueArray)*len(pageUniqueArray))

    loop_list = []
    loop_count = 0
    dataFrame3 = pd.DataFrame(pd.np.empty((0, 10)))
    dataFrame3.set_axis(['query','page','Cuttent Date - 7','Cuttent Date - 6','Cuttent Date - 5','Cuttent Date - 4',
                            'Cuttent Date - 3','Cuttent Date - 2','Cuttent Date - 1','Cuttent Date'],axis=1,inplace=True)

    # QUERY 'JOIN' TABLE FOR EVERY COMBINATION OF KEYWORD AND WEBSITE AND CHECK IF THE COMINATION HAS SERP SCORE FALLING OVERBOARD  
    for queryUniqueValue in queryUniqueArray:
        print("loop_Count: ",loop_count)
        for pageUniqueValue in pageUniqueArray:
            loop_count = loop_count+1

            dataFrame2 = pd.read_sql('select * from interco_gsc_db.testJoin where `query` = "'+queryUniqueValue+'" and `page` = "'
                                            + pageUniqueValue +'" and `date` between CURRENT_DATE-7 and CURRENT_DATE ORDER BY `query`,`page`,`date`',connection)

            if len(dataFrame2) > 1:
                i=0
                isSERPFalling = True
                tempList = [queryUniqueValue,pageUniqueValue]
                numList = []

                # RECORD THE SERP DATA IN A LIST AND APPEND IT TO THE MASTER LIST IF SERP IS FALLING CONTINUOUSLY  
                while isSERPFalling and i<len(dataFrame2):
                    tempList.append(float(dataFrame2.iloc[i,-3]))
                    numList.append(float(dataFrame2.iloc[i,-3]))                    
                    
                    if i != len(dataFrame2)-1:
                        if dataFrame2.iloc[i,-3]>dataFrame2.iloc[i+1,-3]:
                            isSERPFalling = False
                    i=i+1
                
                if isSERPFalling:
                    tempList.append(statistics.mean(numList))
                    # print(tempList)
                    loop_list.append(tempList)

     # CONVERT MASTER SERP LIST TO A DATAFRAME
    if len(loop_list)> 0:
        dataFrame3 = pd.DataFrame(loop_list)

    dataFrame3.to_csv('c:/Users/guru/OneDrive - Interco/Desktop/gsc_data.csv',index=False)

if __name__ == '__main__':
    SERPFallListGenerate()







