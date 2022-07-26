from splunklib.client import SavedSearches
from splunklib.client import SavedSearch
import splunklib.results as results
import splunklib.client as client
import os
import json
import csv
import datetime
from datetime import date
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta, MO
import math
import pandas as pd
from os.path import exists


def INIT():
    try:
        global Script_File_Path
        global Conf_File_Path
        global BACKFILLING_metaData_File_Path
        
        Script_File_Path=os.path.join(os.getcwd(),"SplunkSearch_to_AppendCSV.py")
        Conf_File_Path=os.path.join(os.getcwd(),"properties.json")
        BACKFILLING_metaData_File_Path=os.path.join(os.getcwd(),"SearchMeta.csv")
    except Exception as e:
        print("INIT_ERROR"+str(e) )
        
def READ_CONF(Conf_File_Path):
    try:
        CONF_FILE_OPENED = open(Conf_File_Path,"r+") 
        DATA = json.load(CONF_FILE_OPENED)   
        CONF_FILE_OPENED.close()
        return DATA
    except Exception as e:
        print("READ_CONF_ERROR"+str(e) )
        
def READ_SearchMeta (BACKFILLING_metaData_File_Path):
    try:
        Header=[]
        metaData_LIST=[]
        metaData_DICT={}
        line_count=0
        Search_metaData_FILE_OPENED = open(BACKFILLING_metaData_File_Path)
        csv_reader  = csv.reader(Search_metaData_FILE_OPENED, delimiter=',')
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
        Search_metaData_FILE_OPENED.close()
        return metaData_LIST
    except Exception as e:
        print("READ_metaData_ERROR"+str(e) )  
        
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
    except Exception as e:
        print("SPLUNK_CONNECT_ERROR"+str(e) )
        
def SPLUNK_SEARCH(service,query,earliest,latest):
    try:
        kwargs_oneshot = {
                          "earliest_time": earliest,
                          "latest_time": latest
                          }
        searchquery_oneshot = "search "+str(query)
        oneshotsearch_results = service.jobs.oneshot(searchquery_oneshot, **kwargs_oneshot,output_mode='json')
        reader = results.JSONResultsReader(oneshotsearch_results)
        List=[]
        for item in reader:
            List.append(item)
        return List
    except Exception as e:
        print("SPLUNK_SEARCH_ERROR"+str(e) )

def Append_To_CSV(Filepath,Results):
    try:
        data = pd.DataFrame.from_records(Results)
        if exists(Filepath):    
            data.to_csv(Filepath,mode="a",index=False,header=False)
        else:
            data.to_csv(Filepath,mode="a",index=False)
    except Exception as e:
        print("Append_To_CSV_Error"+str(e) )        

def MAIN():
    try:
        INIT()
        CONNECT_DICT = READ_CONF(Conf_File_Path)
        service = SPLUNK_CONNECT(CONNECT_DICT)
        metaData_LIST = READ_SearchMeta(BACKFILLING_metaData_File_Path)
        for DICT in metaData_LIST:
            SearchResults = SPLUNK_SEARCH(service,DICT["Search"],DICT["Earliest"],DICT["Latest"])
            Append_To_CSV(DICT["Output_Result_Absolute_Path"],SearchResults)
    except Exception as e:
        print("MAIN_ERROR"+str(e) )

MAIN()