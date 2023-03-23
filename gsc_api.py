import pandas as pd
import datetime
import httplib2
from apiclient.discovery import build
from collections import defaultdict
from dateutil import relativedelta
import argparse
from oauth2client import client
from oauth2client import file
from oauth2client import tools
import pymysql.cursors
import sqlalchemy
from sqlalchemy import create_engine

# Create a mySQL Database
# Establish connection #Secure20

# connection = pymysql.connect(host='10.25.38.215',
#                              user='root',
#                              port=3306,
#                              password='!ITCadmin20')

# # Simulate the CREATE DATABASE function of mySQL
# try:
#     with connection.cursor() as cursor:
#         cursor.execute('CREATE DATABASE interco_gsc_db COLLATE utf8mb4_unicode_ci')
 
# finally:
#     connection.close()

connection = pymysql.connect(host='10.25.38.215',
                             user='root',
                             port=3306,
                             password='!ITCadmin20',
                             db='interco_gsc_db',
                             cursorclass=pymysql.cursors.DictCursor)


try:
    with connection.cursor() as cursor:
        sqlQuery = '''CREATE TABLE IF NOT EXISTS intercotradingco_backup_gsc(
                                                            id INT NOT NULL AUTO_INCREMENT,
                                                            Date DATE, 
                                                            Page LONGTEXT, 
                                                            Query LONGTEXT, 
                                                            Clicks INT, 
                                                            Impressions INT, 
                                                            Ctr DECIMAL(10,2), 
                                                            Position DECIMAL(5,2),
                                                            PRIMARY KEY (id))
                                                            CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci'''

        cursor.execute(sqlQuery)
finally:
    connection.close()


# Step 3: Connect to the API

sites =  ["https://www.intercotradingco.com","https://www.coppertransformers.com","https://www.digitalitad.com","https://www.eastcoastlaptoprecycling.com",
        "https://www.electricutilitymeters.com","https://www.escrapguru.com","https://www.itadcenter.com","https://madisoncartageco.com",
        "https://www.midwestlaptoprecycling.com","https://www.mixedelectricmotors.com","https://www.pacificlaptoprecycling.com",
        "https://www.rangeleadexpert.com","https://www.rangeleadguru.com","https://www.recycleautobatteries.com",
        "https://www.recycleelectricmeters.com","https://www.recycleelectricmotors.com","https://www.recycleevbatteries.com",
        "https://www.recyclerangelead.com","https://www.recycletesla.com","https://www.recycletransformers.com","https://www.recycletransformers.com",
        "https://www.recycleutilitymeters.com","https://www.scrapammo.com","https://www.scrapautobatteries.com","https://www.scrapbrassshells.com",
        "https://www.scrapcartridges.com","https://www.scrapelectricmeters.com","https://www.scrapevbatteries.com","https://www.scrapgoldmemory.com",
        "https://www.scrapleadacidautobatteries.com","https://www.scraplithiumion.com","https://www.scraplithiumionbatteries.com",
        "https://www.scraplithiumionbattery.com","https://www.scrapmotherboards.com","https://www.scrapram.com","https://www.scraprangelead.com",
        "https://www.scrapsealedunits.com","https://www.scrapservers.com","https://www.scrapteslabatteries.com","https://www.scraputilitymeters.com",
        "https://www.sellelectricmotors.com","https://www.sellscrapelectricmotors.com","https://www.sellscrapmotors.com","https://usabrasslead.com",
        "https://www.utilitiesmeter.com","https://www.wescrapammo.com"]

#"https://www.coppertransformers.com","https://www.intercotradingco.com","https://www.digitalitad.com","https://www.eastcoastlaptoprecycling.com","https://www.electricutilitymeters.com","https://www.escrapguru.com""https://www.ibuyscrapmetals.com","https://www.intercojobs.com","https://www.intercotrabajos.com","https://www.itadcenter.com","https://www.metalliczincpowder.com","https://www.midwestlaptoprecycling.com","https://www.mixedelectricmotor.com","https://www.mixedelectricmotors.com","https://www.pacificlaptoprecycling.com","https://www.rangeleadexpert.com","https://www.rangeleadguru.com","https://www.recycleautobatteries.com","https://www.recycleautobatteries.com","https://www.recycleelectricmeters.com","https://www.recycleelectricmotors.com","https://www.recycleevbatteries.com","https://www.recyclepvcell.com","https://www.recyclerangelead.com","https://www.recyclesolarpanels.com","https://www.recyclesolarpanelsusa.com","https://www.recycletesla.com","https://www.recycletransformers.com","https://www.recycleutilitymeters.com","https://www.recyclingsolarmodule.com","https://www.scrapammo.com","https://www.scrapautobatteries.com","https://www.scrapbrassshells.com","https://www.scrapcartridges.com","https://www.scrapelectricmeters.com","https://www.scrapevbatteries.com","https://www.scrapgoldmemory.com","https://www.scrapleadacidautobatteries.com","https://www.scraplithiumion.com","https://www.scraplithiumionbatteries.com","https://www.scraplithiumionbattery.com","https://www.scrapmotherboards.com","https://www.scrapmotherboards.com","https://www.scrappvcell.com","https://scraprecyclesolarpanels.com","https://www.scrapram.com","https://www.scraprangelead.com","https://www.scrapsealedunits.com","https://www.scrapservers.com","https://www.scrapshells.com","https://www.scrapsolarpanels.com","https://www.scrapteslabatteries.com","https://www.scraputilitymeters.com","https://www.sellelectricmotors.com","https://www.sellscrapelectricmotors.com","https://www.sellscrapmotors.com","https://www.solarpanelrecyclinghub.com","https://www.solarpanelrecyclingusa.com","https://usabrasslead.com","https://www.usalaptoprecycling.com","https://www.usedsolarpanelrecycling.com","https://www.utilitiesmeter.com","https://www.wescrapammo.com","https://www.wescrapbrassshells.com"

SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']
DISCOVERY_URI = ('https://www.googleapis.com/discovery/v1/apis/customsearch/v1/rest')
 
CLIENT_SECRETS_PATH = r'C:\Users\guru\Downloads\client_secrets.json'
 
parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,parents=[tools.argparser])
flags = parser.parse_args([])

 
flow = client.flow_from_clientsecrets(CLIENT_SECRETS_PATH, scope=SCOPES,message=tools.message_if_missing(CLIENT_SECRETS_PATH))
 
storage = file.Storage('searchconsole.dat')
credentials = storage.get()
 
if credentials is None or credentials.invalid:
  credentials = tools.run_flow(flow, storage, flags)
http = credentials.authorize(http=httplib2.Http())
 
webmasters_service = build('webmasters', 'v3', http=http)

#Set Date
end_date = datetime.date.today()
start_date = datetime.datetime(2000,1,1)
c = 1

#datetime.date.today()-relativedelta.relativedelta(days=3)
#datetime.datetime(2000,1,1)
#print(end_date)
 
#Execute your API Request
def execute_request(service, property_uri, request):
    return service.searchanalytics().query(siteUrl=property_uri, body=request).execute()

df2 = pd.DataFrame()
for site in sites:
    maxRows = 25000; 
    i = 0
    numRows = maxRows
    scDict = defaultdict(list)
    print(site)
    while (numRows == 25000 and i < 400000) : # Limit to 1M rows
#        print("Inside while for: ",site)
        request = {
            'startDate': datetime.datetime.strftime(start_date,"%Y-%m-%d"),
            'endDate': datetime.datetime.strftime(end_date,'%Y-%m-%d'),
            'dimensions': ['date','page','query'],
            'rowLimit': maxRows, 
            'startRow': i*maxRows
        }
        response = execute_request(webmasters_service, site, request)

    #Process the response
        for row in response['rows']:
            scDict['id'].append(c)
            scDict['date'].append(row['keys'][0] or 0)    
            scDict['page'].append(row['keys'][1] or 0)
            scDict['query'].append(row['keys'][2] or 0)
            scDict['clicks'].append(row['clicks'] or 0)
            scDict['ctr'].append(row['ctr'] or 0)
            scDict['impressions'].append(row['impressions'] or 0)
            scDict['position'].append(row['position'] or 0)
            c +=1
    #Add response to dataframe 
        df = pd.DataFrame(data = scDict)
        df['clicks'] = df['clicks'].astype('int')
        df['ctr'] = df['ctr']*100
        df['impressions'] = df['impressions'].astype('int')
        df['position'] = df['position'].round(2)
        df.sort_values('clicks',inplace=True,ascending=False)
        numRows=len(response['rows'])
        i=i+1
        df2 = pd.concat([df2,df])

# df2.to_csv('file_name4.csv')
print("done")
df2.drop_duplicates(inplace=True)
engine = sqlalchemy.create_engine('mysql+pymysql://root:!ITCadmin20@10.25.38.215/interco_gsc_db')
df2.columns
df2.to_sql(
        name = 'intercotradingco_backup_gsc',
        con = engine,
        index = False,
        if_exists = 'replace')

with engine.connect() as con:
    con.execute('ALTER TABLE `intercotradingco_backup_gsc` ADD PRIMARY KEY (`id`);')

print("pk added")

with engine.connect() as con:
    con.execute('ALTER TABLE `intercotradingco_backup_gsc` MODIFY `date` DATE;')

print("complete")