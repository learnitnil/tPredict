'''
Author : Kunchala Anil
Email : anilkunchalaece@gmail.com
Date : 16 Sep 2019
This script will download the traffic data from hereserver and save it on local disk

To process data  - we use following process
    Step 1 : take sample (random) json file and get all road id's
    Step 2 : Then collect all the data from json files
    Step 3 : Store the collected data using road id's
'''
import json
import requests
import os
from pandas import DataFrame

abuDhabiBBox = '51.42,22.63;56.02,25.45'
hereFlowBaseUrl = 'https://traffic.api.here.com/traffic/6.1/flow.json'

#this function is used to load the here credetials from external json file
def getCredentials():
    with open('credentials.json')  as f :
        data = json.load(f)
    return [data['id'],data['code']]

def saveItToDesk(data):
    data = json.loads(data)
    fileName = os.path.join('data',data["CREATED_TIMESTAMP"]+'.json')
    with open(fileName ,'w') as f :
        json.dump(data,f)

def getDataAndSaveItToDesk(params) :
    resp = requests.get(url=hereFlowBaseUrl,params=params)
    # print(resp.text)
    saveItToDesk(resp.text)

# this will process file
# we need to assign unique name to each Flow Item which consist of traffic flow data
# we can use TMC location codes (ref - https://wiki.openstreetmap.org/wiki/TMC/Location_Code_List)
# here traffic response consist of
#      number of roadway items (RW) which consist of COUNTRY_CODE and TABLE_ID
#      each roadway item consist of multiple flow items
#      each flow item consist of PC (LOCATION_CODE)
#      To Identify each road segment the id using TMC will be
#                       COUNTRY_CODE-TABLE_ID-LOCATION_CODE  ('-' is used as seperator)
def processFile(fileName) :
    with open(fileName) as f :
        data = json.load(f)
    resultDict = dict()
    for rws in data['RWS'] :
        tableId = rws['TABLE_ID']
        extendedCountryCode = rws['EXTENDED_COUNTRY_CODE']
        for fis in rws['RW'] :
            for fi in fis['FIS'] :
                # print(fi['TMC']['PC'])
                # print("number of flow elements are {}".format(len(fi['FI'])))
                for flowElement in fi['FI'] :
                    pc = flowElement['TMC']['PC'] #unique code for each road segment
                    qd = flowElement['TMC']['QD'] #Queuing direction
                    roadCode = '{0}_{1}_{2}_{3}'.format(extendedCountryCode,tableId,pc,qd)
                    # print(roadCode)
                    resultDict[roadCode] = {
                        'FF' : flowElement['CF'][0]['FF'], # free flow
                        'JF' : flowElement['CF'][0]['JF'], # jam factor
                        'SP' : flowElement['CF'][0]['SP'] # speed capped by speed limit
                    }
    print("processing for {0} is completed. file contains {1} road segements ".format(fileName,len(resultDict)))

def processFileForLookUpData(fileName) :
    with open(fileName) as f :
        data = json.load(f)
    resultDict = []
    for rws in data['RWS'] :
        tableId = rws['TABLE_ID']
        extendedCountryCode = rws['EXTENDED_COUNTRY_CODE']
        for fis in rws['RW'] :
            for fi in fis['FIS'] :
                # print(fi['TMC']['PC'])
                # print("number of flow elements are {}".format(len(fi['FI'])))
                for flowElement in fi['FI'] :
                    pc = flowElement['TMC']['PC'] #unique code for each road segment
                    qd = flowElement['TMC']['QD'] #Queuing direction
                    roadCode = '{0}_{1}_{2}_{3}'.format(extendedCountryCode,tableId,pc,qd)
                    # print(roadCode)
                    shpData = ''
                    # if we have more shp elements - make it an single list of latlongs
                    if len(flowElement['SHP']) > 1 :
                        for shp in flowElement['SHP'] :
                            shpData = shpData + str(shp['value'][0])
                    else :
                        shpData = str(flowElement['SHP'][0]['value'][0])
                    # print(shpData)
                    lookUpData = { 'rc' : roadCode, 'shp' : shpData}
                    resultDict.append(lookUpData)
    print("processing for {0} is completed. file contains {1} road segements ".format(fileName,len(resultDict)))
    table = {
        'roadCode' : [item['rc'] for item in resultDict],
        'shpValue' : [item['shp'] for item in resultDict]
    }
    df = DataFrame(table,columns= ['roadCode', 'shpValue'])
    fileToWrite = os.path.join('results','lookUpData.csv')
    df.to_csv(fileToWrite)
    print('lookupData is written to {0}'.format(fileToWrite))

# process the files and save data
# create a lookUpData.csv file if it is not present
def processAllData():
    for fileName in os.listdir('data') :
        #check for lookUpData.csv
        if os.path.exists(os.path.join('results','lookUpData.csv')) == False:
            processFileForLookUpData(os.path.join('data',fileName))
        processFile(os.path.join('data',fileName))

if __name__ == "__main__":
    appId,appCode = getCredentials()
    params = {
        'bbox' : abuDhabiBBox,
        'app_id' : appId,
        'app_code' : appCode,
        'responseattributes' : 'shape' #include the shape information - to get lat longs
    }
    # getDataAndSaveItToDesk(params)
    processAllData()
    # processFile('data/2019-09-16T11:09:22.000+0000.json')




