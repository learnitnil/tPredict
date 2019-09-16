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
                    roadCode = '{0}-{1}-{2}'.format(extendedCountryCode,tableId,pc)
                    # print(roadCode)
                    resultDict[roadCode] = {
                        'FF' : flowElement['CF'][0]['FF'], # free flow
                        'JF' : flowElement['CF'][0]['JF'], # jam factor
                        'SP' : flowElement['CF'][0]['SP'] # speed capped by speed limit
                    }
    print("processing for {0} is completed. file contains {1} road segements ".format(fileName,len(resultDict)))

def processAllData():
    for fileName in os.listdir('data') :
        processFile(os.path.join('data',fileName))

if __name__ == "__main__":
    appId,appCode = getCredentials()
    params = {
        'bbox' : abuDhabiBBox,
        'app_id' : appId,
        'app_code' : appCode,
        'responseattributes' : 'shape' #include the shape information - to get lat longs
    }
    getDataAndSaveItToDesk(params)
    processAllData()
    # processFile('data/2019-09-16T11:09:22.000+0000.json')




