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
#function to update all devices/modules for specific account (this is the main function)
def UpdateNetatmoForAccount(account):

  username = account[0]
  password = account[1]
  clientId = account[2]
  clientSecret = account[3]

  print "Getting modules for account " + username

  if password == "":
    password = getpass.getpass()

  netatm = Netatmo.NetatmoClient(username, password, clientId, clientSecret)
  netatm.getStationData()

  #first, update modules and sensors
  for id in netatm.devicemoduleids:

    if id[1] == None:
      moduleid = dbcursor.execute("SELECT Id FROM Modules WHERE Id IS \""+str(id[0])+"\"").fetchone()

    else:
      moduleid = dbcursor.execute("SELECT Id FROM Modules WHERE Id IS \""+str(id[1])+"\"").fetchone()

    #if not exists, add new device/module and its sensors
    if moduleid == None:

      #check if location exists
      location = netatm.locations[id]
      locationid = dbcursor.execute("SELECT Id FROM Locations WHERE PositionNorth IS "+str(location[0][1])+" AND PositionEast IS "+str(location[0][0])+" AND Elevation IS "+str(location[1])+" AND Name IS \""+location[3]+"\" AND Timezone IS \""+location[2]+"\"").fetchone()

      if locationid == None:
        dbcursor.execute("INSERT INTO Locations (PositionNorth,PositionEast,Elevation,Name,Timezone) VALUES ("+str(location[0][1]) + "," + str(location[0][0]) + "," + str(location[1]) + ",\"" + location[3] + "\",\"" + location[2] + "\")")

        locationid = (dbcursor.execute("SELECT last_insert_rowid();").fetchone())[0]
      else:
        locationid = locationid[0]

      if id[1] == None:
        moduleid = id[0]
      else:
        moduleid = id[1]

      dbcursor.execute("INSERT INTO Modules (Id,Name) VALUES (\""+moduleid+"\",\""+netatm.types[id]+"\")")

      sensorids = []
      #set correct units
      for measurand in netatm.measurands[id]:

        if measurand == "CO2":
          unit = "ppm"

        if measurand == "Humidity":
          unit = "%"

        if measurand == "Noise":
          unit = "dB"

        if measurand == "Temperature":
          if netatm.units[id] == 0:
            unit = u"\u00b0"+"C"
          else:
            unit = u"\u00b0"+"F"

        if measurand == "Wind":
          if netatm.windunits[id] == 0:
            unit = "kph"
          elif netatm.windunits[id] == 1:
            unit = "mph"
          elif netatm.windunits[id] == 2:
            unit = "ms"
          elif netatm.windunits[id] == 3:
            unit = "Bft"
          elif netatm.windunits[id] == 4:
            unit = "kn"

        if measurand == "Pressure":
          if netatm.pressureunits[id] == 0:
            unit = "mbar"
          elif netatm.pressureunits[id] == 1:
            unit = "inHg"
          elif netatm.pressureunits[id] == 2:
            unit = "mmHg"

        if measurand == "Rain":
          measurand = "Precipitation"
          unit = "mm"

        dbcursor.execute("INSERT INTO Sensors (Module,Measurand,Unit,Name,Calibration,Interval) VALUES (\""+str(moduleid)+"\",\""+measurand+"\",\""+unit+"\",\""+measurand+" sensor\",0,300)") #one point every 5 minutes for Netatmo
        sensorid = (dbcursor.execute("SELECT last_insert_rowid();").fetchone())[0]
        sensorids.append(sensorid)

      #get first time stamp for module and set this as Begin for location of module
      measurandsstring = ""
      measurands = netatm.measurands[id]
      for i in range(0,len(measurands)):
        measurandsstring = measurandsstring + measurands[i]
        if i < len(measurands)-1:
          measurandsstring = measurandsstring + ","
      data = netatm.getMeasure(id[0],id[1],"max",measurandsstring,None,None,None,"false")
      minservertimestamp = min(map(int,data.keys()))

      dbcursor.execute("INSERT INTO ModuleLocations (Module,Begin,Location) VALUES (\""+moduleid+"\","+str(minservertimestamp)+","+str(locationid)+")")

      if id[1] == None:
        ColorPrint.ColorPrint("Added device "+id[0]+" ("+netatm.types[id]+") at location "+str(locationid)+" ("+netatm.locations[id][3]+")", "okgreen")

      else:
        ColorPrint.ColorPrint("Added module "+id[1]+" ("+netatm.types[id]+") at location "+str(locationid)+" ("+netatm.locations[id][3]+")", "okgreen")

    else:
      moduleid = moduleid[0]

    dbconn.commit()


##############################################################################
#This iterates through all accounts
def UpdateNetatmoAccounts():

  dbcursor.execute("SELECT * From Accounts")
  res = dbcursor.fetchall()
  for account in res:
    UpdateNetatmoForAccount(account)

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
UpdateNetatmoAccounts()
dbconn.commit()
dbconn.close()
