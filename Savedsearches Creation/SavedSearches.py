from splunklib.client import SavedSearches
from splunklib.client import SavedSearch
import splunklib.client as client
import os
import json
import csv
import datetime


def INIT():
    try:
        global Script_File_Path
        global Conf_File_Path
        global metaData_File_Path
        global BACKFILLING_metaData_File_Path
        global now_Date
        
        now_Date=datetime.datetime.now().strftime("%c").replace(" ","_")
        Script_File_Path=os.path.join(os.getcwd(),"SavedSearches.py")
        Conf_File_Path=os.path.join(os.getcwd(),"properties.json")
        metaData_File_Path=os.path.join(os.getcwd(),"savedSearches_metaData.csv")
        BACKFILLING_metaData_File_Path=os.path.join(os.getcwd(),"BACKFILLING_metaData.csv")
    except:
        print("INIT_ERROR")
        
def READ_CONF(Conf_File_Path):
    try:
        CONF_FILE_OPENED = open(Conf_File_Path,"r+") 
        DATA = json.load(CONF_FILE_OPENED)   
        CONF_FILE_OPENED.close()
        return DATA
    except:
        print("READ_CONF_ERROR")
        
def READ_metaData(metaData_File_Path):
    try:
        Header=[]
        metaData_LIST=[]
        metaData_DICT={}
        line_count=0
        metaData_FILE_OPENED = open(metaData_File_Path)
        csv_reader  = csv.reader(metaData_FILE_OPENED, delimiter=',')
        for row in csv_reader:
            if line_count == 0:
                Header.extend(row)
                line_count += 1
            else:
                #print(str(row))
                metaData_DICT={}
                for i in range(len(row)):
                    metaData_DICT.update({str(Header[i]): str(row[i])})
                metaData_LIST.append(metaData_DICT)
                line_count += 1
        metaData_FILE_OPENED.close()
        return metaData_LIST
    except:
        print("READ_metaData_ERROR")
        
             
def SPLUNK_CONNECT(CONNECT_DICT):
    try:
        service = client.connect(
            host=CONNECT_DICT["HOST"],
            port=CONNECT_DICT["PORT"],
            username=CONNECT_DICT["USERNAME"],
            password=CONNECT_DICT["PASSWORD"],
            owner=CONNECT_DICT["OWNER"],
            app=CONNECT_DICT["APP"])
        return service
    except:
        print("SPLUNK_CONNECT_ERROR")

def CREATE_SAVED_SEARCH(service,SS_DICT):
    try:
        mysavedsearch = service.saved_searches.create(SS_DICT["NAME"], SS_DICT["QUERY"])
        
        mysavedsearch = service.saved_searches[SS_DICT["NAME"]]
        
        kwargs = {"description": SS_DICT["DESC"],
                  "is_visible":SS_DICT["IS_VISIBLE"],
                "is_scheduled": SS_DICT["IS_SCHEDULED"],
               "cron_schedule": SS_DICT["CRON_SCHEDULE"],
                "dispatch.earliest_time": SS_DICT["EARLIEST_TIME"],
                "dispatch.latest_time": SS_DICT["LATEST_TIME"]}
        
        mysavedsearch.update(**kwargs).refresh()
        
        
        print ("Created: " + mysavedsearch.name)
        print ("Description:         ", mysavedsearch["description"])
        print ("Is scheduled:        ", mysavedsearch["is_scheduled"])
        print ("Is scheduled:        ", mysavedsearch["is_visible"])
        print ("Cron schedule:       ", mysavedsearch["cron_schedule"])
        print ("Next scheduled time: ", mysavedsearch["next_scheduled_time"])
    except:
        print("CREATE_SAVED_SEARCH_ERROR")


def MAIN():
    try:
        INIT()
        CONNECT_DICT = READ_CONF(Conf_File_Path)
        service = SPLUNK_CONNECT(CONNECT_DICT)
        metaData_LIST = READ_metaData(metaData_File_Path)
        for SS_DICT in metaData_LIST:   
            CREATE_SAVED_SEARCH(service,SS_DICT)
    except:
        print("MAIN_ERROR")

MAIN()