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

def INIT():
    try:
        global Script_File_Path
        global Conf_File_Path
        global BACKFILLING_metaData_File_Path
        
        Script_File_Path=os.path.join(os.getcwd(),"SavedSearches.py")
        Conf_File_Path=os.path.join(os.getcwd(),"properties.json")
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
        
def READ_BACKFILLING_metaData(BACKFILLING_metaData_File_Path):
    try:
        Header=[]
        BF_metaData_LIST=[]
        BF_metaData_DICT={}
        BF_line_count=0
        BACKFILLING_metaData_FILE_OPENED = open(BACKFILLING_metaData_File_Path)
        csv_reader  = csv.reader(BACKFILLING_metaData_FILE_OPENED, delimiter=',')
        for row in csv_reader:
            if BF_line_count == 0:
                Header.extend(row)
                BF_line_count += 1
            else:
                #print(str(row))
                BF_metaData_DICT={}
                for i in range(len(row)):
                    BF_metaData_DICT.update({str(Header[i]): str(row[i])})
                BF_metaData_LIST.append(BF_metaData_DICT)
                BF_line_count += 1
        BACKFILLING_metaData_FILE_OPENED.close()
        return BF_metaData_LIST
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
        
def SPLUNK_SEARCH(service,query,earliest,latest):
    try:
        kwargs_oneshot = {
                          "earliest_time": earliest,
                          "latest_time": latest
                          }
        searchquery_oneshot = "search "+str(query)+""" | stats earliest(_time) as E | eval E=strftime(E,"%Y %m %d %H:%M:%S")"""
        oneshotsearch_results = service.jobs.oneshot(searchquery_oneshot, **kwargs_oneshot)
        reader = results.ResultsReader(oneshotsearch_results)
        E=""
        for item in reader:
            E=item["E"]
        return E
    except:
        print("SPLUNK_SEARCH_ERROR")
        
def BACKFILLING(ER,ES):
    try:     
        ER=datetime.strptime(ER,"%Y %m %d %H:%M:%S") #15 5 2020 10:00
        ES=datetime.strptime(ES,"%Y %m %d %H:%M:%S") #30 7 2020 8:00
        print(str(ER))
        delta=ES-ER
        DICT={}
        LIST=[]
        days=delta.days
        mod=days/30
        for i in range(math.ceil(mod)):
            DICT={}
            if days >= 30:
                Earliest=ER
                Latest=ER + timedelta(days=30)
                days=days-30
                ER=Latest + timedelta(days=1)
            elif days < 30:
                Earliest=ER
                Latest=ES
                days=days-days
                ER=Latest + timedelta(days=1)
            DICT.update({"EARLIEST": str(datetime.timestamp(Earliest))})
            DICT.update({"LATEST": str(datetime.timestamp(Latest))})
            LIST.append(DICT)
        return LIST
    except:
        print("BACKFILLING_ERROR")

def MAIN():
    try:
        INIT()
        CONNECT_DICT = READ_CONF(Conf_File_Path)
        service = SPLUNK_CONNECT(CONNECT_DICT)
        metaData_LIST = READ_BACKFILLING_metaData(BACKFILLING_metaData_File_Path)
        for BF_DICT in metaData_LIST:
            ER = SPLUNK_SEARCH(service,BF_DICT["RAW"],"1","now")
            ES = SPLUNK_SEARCH(service,BF_DICT["SI"],"1","now")
            #ER=datetime.strptime("2020-7-10 8:00:00","%Y %m %d %H:%M:%S") #15 5 2020 10:00
            #ES=datetime.strptime("2020-7-30 7:00:00","%Y %m %d %H:%M:%S") #30 7 2020 8:00
            TIME_PERIODS_LIST = BACKFILLING(ER,ES)
            for TIMEPERIOD in TIME_PERIODS_LIST:
                print(str(BF_DICT["BACKFILLING"])+" - "+str(TIMEPERIOD["EARLIEST"])+" - "+str(TIMEPERIOD["LATEST"]))
                SPLUNK_SEARCH(service,BF_DICT["BACKFILLING"],TIMEPERIOD["EARLIEST"],TIMEPERIOD["LATEST"])    
    except:
        print("MAIN_ERROR")

MAIN()