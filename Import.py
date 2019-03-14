#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
##############################################################################
# NetatmoInflux
#
# Python script for importing Netatmo data into an InfluxDB.
#
# (C) 2015-2019, Ulrich Thiel
# ulrich.thiel@sydney.edu.au
##############################################################################
#This file is part of NetatmoInflux.
#
#NetatmoInflux is free software: you can redistribute it and/or modify
#it under the terms of the GNU General Public License as published by
#the Free Software Foundation, either version 3 of the License, or
#(at your option) any later version.
#
#NetatmoInflux is distributed in the hope that it will be useful,
#but WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#GNU General Public License for more details.
#
#You should have received a copy of the GNU General Public License
#along with WeatherStats. If not, see <http://www.gnu.org/licenses/>.
##############################################################################

##############################################################################
#This script automatically updates all data from Netatmo
##############################################################################

##############################################################################
#imports
import sqlite3
import os.path
from lib import ColorPrint
from lib import Netatmo
from lib import DateHelper
from lib import Tools
import getpass
import sys
import signal
from influxdb import InfluxDBClient
from optparse import OptionParser
import time

###############################################################################
#parse options
parser = OptionParser()
parser.add_option("--service", action="store_true", dest="service",help="Run as service", default=False)
(options, args) = parser.parse_args()
service = options.service

##############################################################################
#database connection
dbconn = sqlite3.connect('Netatmo.db')
dbcursor = dbconn.cursor()

##############################################################################
#InfluxDB connection
host = dbcursor.execute("SELECT Host FROM InfluxDB WHERE Id=1").fetchone()[0]
port = dbcursor.execute("SELECT Port FROM InfluxDB WHERE Id=1").fetchone()[0]
user = dbcursor.execute("SELECT User FROM InfluxDB WHERE Id=1").fetchone()[0]
password = dbcursor.execute("SELECT Password FROM InfluxDB WHERE Id=1").fetchone()[0]
if password == None:
  password = getpass.getpass("InfluxDB password: ")
db = dbcursor.execute("SELECT Database FROM InfluxDB WHERE Id=1").fetchone()[0]
ssl = dbcursor.execute("SELECT SSL FROM InfluxDB WHERE Id=1").fetchone()[0]
if ssl == None or ssl == 0:
  ssl = False
elif ssl == 1:
  ssl = True
  sslVerify = dbcursor.execute("SELECT SSLVerify FROM InfluxDB WHERE Id=1").fetchone()[0]
  if sslVerify == None or sslVerify == 0:
    sslVerify = False
    #the following suppresses warnings
    import requests
    requests.packages.urllib3.disable_warnings()
  else:
    sslVerify = True

influxClient = InfluxDBClient(host, port, user, password, db, ssl=ssl, verify_ssl=sslVerify)

##############################################################################
#get locations
res = dbcursor.execute("SELECT \"Module Id\", \"Begin Timestamp\", \"End Timestamp\", \"Location Name\" FROM ModulesView").fetchall()
locations = dict()
for r in res:
  if not r[0] in locations.keys():
    locations[r[0]] = []
  entry = dict()

  if r[1] != None:
    entry["Begin"] = int(r[1])
  else:
    entry["Begin"] = None

  if r[2] != None:
    entry["End"] = int(r[2])
  else:
    entry["End"] = None

  entry["Location"] = r[3]
  locations[r[0]].append(entry)

def GetModuleLocation(moduleid, timestamp):
  moduleLocations = locations[moduleid]
  for loc in moduleLocations:
    if loc["Begin"] == None and loc["End"] == None:
      return loc["Location"]
    elif loc["Begin"] == None and timestamp <= loc["End"]:
      return loc["Location"]
    elif loc["Begin"] <= timestamp and loc["End"] == None:
      return loc["Location"]
    elif loc["Begin"] <= timestamp and timestamp <= loc["End"]:
      return loc["Location"]


##############################################################################
#function to import all data for specific account (this is the main function)
def ImportDataForAccount(account):

  username = account[0]
  password = account[1]
  clientId = account[2]
  clientSecret = account[3]

  print "Importing data for account " + username

  if password == None:
    password = getpass.getpass("Account password: ")

  netatm = Netatmo.NetatmoClient(username, password, clientId, clientSecret)
  netatm.getStationData()

  # go through all modules for this device
  for id in netatm.devicemoduleids:

    if id[1] == None:
      moduleid = id[0]
    else:
      moduleid = id[1]

    # check if module is in database
    moduleInDB = dbcursor.execute("SELECT Id FROM Modules WHERE Id IS \""+moduleid+"\"").fetchone()

    #if not exists, raise warning and skip this module
    if moduleInDB == None:
      ColorPrint.ColorPrint("Module "+moduleid+" not in database yet. Skipping this module.\nRun GetModules and update settings in database first.", "warning")
      continue

    #now, import data
    modulename = dbcursor.execute("SELECT Name FROM Modules WHERE Id IS \""+moduleid+"\"").fetchone()[0]
    modulename = modulename + " " + moduleid

    print "  Importing data for " + modulename

    currenttime = DateHelper.CurrentTimestamp()
    res = dbcursor.execute("SELECT Id,Measurand,Calibration FROM Sensors WHERE Module IS \""+moduleid+"\"").fetchall()
    sensorids = [ r[0] for r in res ]
    measurands = [ r[1] for r in res ]
    measurandsstring = ""
    for i in range(0,len(measurands)):
      measurandsstring = measurandsstring + measurands[i]
      if i < len(measurands)-1:
        measurandsstring = measurandsstring + ","
    calibrations = [ r[2] for r in res ]

    #find last timestamp among all sensors (this is the minimal point up to which we have to update data)
    maxdbtimestampForSensors = []
    maxdbtimestamp = None
    for i in range(0,len(sensorids)):
      sensorid = sensorids[i]
      res =  influxClient.query("SELECT LAST(value) FROM " + db + ".autogen." + measurands[i] + " WHERE sensorid=\'" + modulename + "\';", epoch="s")
      mt = [ r["time"] for r in res.get_points() ]
      if len(mt) != 0:
        maxdbtimestampForSensors.append(mt[0])
        if maxdbtimestamp == None or mt[0] < maxdbtimestamp:
          maxdbtimestamp = mt[0]
      else:
        maxdbtimestampForSensors.append(None)

    if maxdbtimestamp == None:
      #get minimal timestamp for device/module from server
      data = netatm.getMeasure(id[0],id[1],"max",measurandsstring,None,None,None,"false")
      maxdbtimestamp = min(map(int,data.keys()))

    Tools.PrintWithoutNewline("    Retrieving data from "+DateHelper.DatetimeFromTimestamp(maxdbtimestamp,None)+" to now: 0%  ")

    date_begin = maxdbtimestamp
    date_end = maxdbtimestamp + 1 #will be modified soon
    timestampcounter = 0 #will count number of retrieved timestamps
    datapointcounter = 0 #will count number of datapoints
    maxval=currenttime-maxdbtimestamp #for progress bar
    while date_end < currenttime:
      date_end = min(currenttime,date_begin + 1000*5*60) #this is a bit ugly. netatmo resolution is one data point every 5 minutes. we can retrieve at most 1024 data points per request. so this is a block of 85.3 hours. now, there might be some on additional on demand measurements in this time window, this is why I used 1000 instead of 1024 above. by design, there will be no duplicates in the database, so this should be fine. this only causes a bit more traffic (and of course we might miss some on demand measurements if there are more than 24 within the 85.3 hour time window (but who's doing this anyways? moreover, for weather statistics it doesn't really matter if we miss a point between the two five minute ones!)).

      data = netatm.getMeasure(id[0],id[1],"max",measurandsstring,date_begin,date_end,None,"false") #retrieves 1024 entries

      timestampcounter = timestampcounter + len(data)
      date_begin = date_end+1

      Tools.PrintWithoutNewline("    Retrieving data from "+DateHelper.DatetimeFromTimestamp(maxdbtimestamp,None)+" to now: "+str(int((float(date_end)-float(maxdbtimestamp))/float(maxval)*100.0))+'%  ')

      if len(data) != 0: #might be empty in case there is no data in this time window. we shouldn't break here though since there might still be earlier data
        #create influxdata
        influxdata = []
        for timestamp in data.keys():
          for i in range(0,len(sensorids)):
            sensorid = sensorids[i]
            if data[timestamp][i] != None:

              if maxdbtimestampForSensors[i] != None and timestamp <= maxdbtimestampForSensors[i]:
                continue

              influxpoint = {}
              influxpoint["time"] = int(timestamp)
              influxpoint["measurement"] = measurands[i]
              influxpoint["fields"] = {}

              #use correct data types
              if measurands[i] in set(['CO2','Noise','Humidity','Pressure']):
                value = int(data[timestamp][i])+int(calibrations[i])
              else:
                value = float(data[timestamp][i])+float(calibrations[i])

              influxpoint["fields"]["value"] = value

              influxpoint["tags"] = {}
              influxpoint["tags"]["location"] = GetModuleLocation(moduleid,int(timestamp))
              influxpoint["tags"]["sensorid"] = modulename

              influxdata.append(influxpoint)
              datapointcounter = datapointcounter + 1

        influxClient.write_points(influxdata, time_precision="s")

    Tools.PrintWithoutNewline("    Retrieving data from "+DateHelper.DatetimeFromTimestamp(maxdbtimestamp,None)+" to now: 100%  ")
    print ""
    ColorPrint.ColorPrint("    "+str(datapointcounter)+" data points for "+str(timestampcounter)+" timestamps received", "okgreen")


##############################################################################
#This iterates through all accounts
def ImportData():

  dbcursor.execute("SELECT * From Accounts")
  res = dbcursor.fetchall()
  for account in res:
    ImportDataForAccount(account)

##############################################################################
#Ctrl+C handler
def signal_handler(signal, frame):
  ColorPrint.ColorPrint("\nYou pressed Ctrl+C", "error")
  dbconn.close()
  influxClient.close()
  signal.signal(signal.SIGINT, signal_handler)
  sys.exit(0)

##############################################################################
#Main
if service == False:
  ImportData()
else:
  while True:
    ImportData()
    time.sleep(605) #add an extra 5 seconds in case there's a lag

dbconn.close()
influxClient.close()
