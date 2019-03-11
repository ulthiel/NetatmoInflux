#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
##############################################################################
# WeatherStats
#
# A collection of Python scripts for general sensor data management and analysis,
# with Netatmo support.
#
# (C) 2015-2018, Ulrich Thiel
# ulrich.thiel@sydney.edu.au
##############################################################################
#This file is part of WeatherStats.
#
#WeatherStats is free software: you can redistribute it and/or modify
#it under the terms of the GNU General Public License as published by
#the Free Software Foundation, either version 3 of the License, or
#(at your option) any later version.
#
#WeatherStats is distributed in the hope that it will be useful,
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
import getpass
from lib import DateHelper
from lib import Tools
from SetDatesInDB import SetDates
from AddSensor import AddNewDataTable
import sys
import signal


##############################################################################
#database connection
dbconn = sqlite3.connect('Netatmo.db')
dbcursor = dbconn.cursor()

##############################################################################
#function to import all data for specific account (this is the main function)
def ImportDataForAccount(account):

  username = account[0]
  password = account[1]
  clientId = account[2]
  clientSecret = account[3]

  print "Importing data for account " + username

  if password == "":
    password = getpass.getpass()

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
    print "  Importing data for module "+moduleid

    currenttime = DateHelper.CurrentTimestamp()
    res = dbcursor.execute("SELECT Id FROM Sensors WHERE Module IS \""+moduleid+"\"").fetchall()
    sensorids = [ r[0] for r in res ]
    measurands = []
    for sensorid in sensorids:
      measurands.append(dbcursor.execute("SELECT Measurand FROM Sensors WHERE Id IS "+str(sensorid)).fetchone()[0])
    measurandsstring = ""
    for i in range(0,len(measurands)):
      measurandsstring = measurandsstring + measurands[i]
      if i < len(measurands)-1:
        measurandsstring = measurandsstring + ","

    #find last timestamp among all sensors (this is the minimal point up to which we have to update data)
    maxdbtimestamp = None
#    for sensorid in sensorids:
#      mt = dbcursor.execute("SELECT MAX(Timestamp) FROM Data"+str(sensorid)).fetchone()[0]
#      if mt != None and (maxdbtimestamp == None or mt < maxdbtimestamp): #< is correct here
#        maxdbtimestamp = mt

    if maxdbtimestamp == None:
      #get minimal timestamp for device/module from server
      data = netatm.getMeasure(id[0],id[1],"max",measurandsstring,None,None,None,"false")
      maxdbtimestamp = min(map(int,data.keys()))

    Tools.PrintWithoutNewline("    Retrieving data from "+DateHelper.DateFromDatetime(DateHelper.DatetimeFromTimestamp(maxdbtimestamp,None))+" to now: 0%  ")

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

      Tools.PrintWithoutNewline("    Retrieving data from "+DateHelper.DateFromDatetime(DateHelper.DatetimeFromTimestamp(maxdbtimestamp,None))+" to now: "+str(int((float(date_end)-float(maxdbtimestamp))/float(maxval)*100.0))+'%  ')

      if len(data) != 0: #might be empty in case there is no data in this time window. we shouldn't break here though since there might still be earlier data
        for timestamp in data.keys():
          for i in range(0,len(sensorids)):
            sensorid = sensorids[i]
            if data[timestamp][i] != None:
              #dbcursor.execute("INSERT INTO Data"+str(sensorid)+" (Timestamp,Value) VALUES ("+str(timestamp)+","+str(data[timestamp][i])+")")
              datapointcounter = datapointcounter + 1

          #dbconn.commit()

    Tools.PrintWithoutNewline("    Retrieving data from "+DateHelper.DateFromDatetime(DateHelper.DatetimeFromTimestamp(maxdbtimestamp,None))+" to now: 100%  ")
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
  SetDates(dbconn, dbcursor)
  dbconn.commit()
  dbconn.close()
  sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

##############################################################################
#Main
ImportData()
dbconn.commit()
dbconn.close()
