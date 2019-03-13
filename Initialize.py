#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
##############################################################################
# NetatmoInflux
#
# Python scripts for importing Netatmo data into an InfluxDB.
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
import getpass
import sys
import signal

##############################################################################
# database connection
dbexists = os.path.isfile("Netatmo.db")
dbconn = sqlite3.connect('Netatmo.db')
dbcursor = dbconn.cursor()


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
#Creates empty database
def CreateEmptyDB():

  dbcursor.execute(\
    "CREATE TABLE \"Accounts\" (\n" \
    "`User` TEXT,\n" \
    "`Password` TEXT,\n" \
    "`ClientID` TEXT,\n" \
    "`ClientSecret` TEXT,\n" \
    "PRIMARY KEY(User) ON CONFLICT REPLACE)\n"\
  )

  dbcursor.execute(\
    "CREATE TABLE \"Modules\" (\n" \
    "`Id` TEXT,\n" \
    "`Name` TEXT,\n" \
    "PRIMARY KEY(Id))\n"\
  )

  dbcursor.execute(\
    "CREATE TABLE \"Sensors\" (\n" \
    "`Id` INTEGER,\n" \
    "`Module` TEXT,\n" \
    "`Measurand` INTEGER,\n" \
    "`Unit` INTEGER,\n" \
    "`Name` TEXT,\n" \
    "`Calibration` REAL,\n" \
    "`Interval` INTEGER,\n" \
    "PRIMARY KEY(Id))\n" \
  )

  dbcursor.execute(\
    "CREATE TABLE \"Locations\" (\n" \
    "`Id` INTEGER,\n" \
    "`PositionNorth` REAL,\n" \
    "`PositionEast` REAL,\n" \
    "`Elevation` INTEGER,\n" \
    "`Name` TEXT,\n" \
    "`Timezone` TEXT,\n" \
    "PRIMARY KEY(Id))\n" \
  )

  dbcursor.execute(\
    "CREATE TABLE \"ModuleLocations\" (\n" \
    "`Module` TEXT,\n" \
    "`Begin` TEXT,\n" \
    "`End` TEXT,\n" \
    "`Location` INTEGER,\n" \
    "PRIMARY KEY(Module, Begin, End))\n" \
  )

  dbcursor.execute(\
    "CREATE VIEW \"ModulesView\" AS \n" \
    "SELECT Modules.Id AS \"Module Id\", \n" \
    "Modules.Name AS \"Module Name\", \n" \
    "STRFTIME(\"%Y-%m-%d %H:%M:%SZ\", DATETIME(ModuleLocations.Begin, \'unixepoch\')) AS \"Begin\" , \n" \
    "STRFTIME(\"%Y-%m-%d %H:%M:%SZ\", DATETIME(ModuleLocations.End, \'unixepoch\')) AS \"End\", \n" \
    "Locations.Name AS \"Location Name\", \n"\
    "ModuleLocations.Begin AS \"Begin Timestamp\", \n" \
    "ModuleLocations.End AS \"End Timestamp\", \n" \
    "Locations.Timezone AS \"Timezone\" \n" \
    "FROM Modules \n" \
    "INNER JOIN ModuleLocations ON ModuleLocations.Module = Modules.Id \n" \
    "INNER JOIN Locations ON ModuleLocations.Location = Locations.Id \n" \
  )

  dbcursor.execute(\
  "CREATE TABLE \"InfluxDB\" ( \n" \
  "`Id` INTEGER, \n" \
  "`Host` TEXT, \n" \
  "`Port` INTEGER, \n" \
  "`User` INTEGER, \n" \
  "`Password` INTEGER, \n" \
  "`Database` TEXT, \n" \
  "`SSL` INTEGER, \n" \
  "PRIMARY KEY(Id)) \n" \
  )

  dbconn.commit()

##############################################################################
#add all devices/modules for specific account
def InitializeAccount(account):

  username = account[0]
  password = account[1]
  clientId = account[2]
  clientSecret = account[3]

  print "Getting modules for account " + username

  if password == None:
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

      dbcursor.execute("INSERT INTO Modules (Id,Name) VALUES (\""+moduleid+"\",\"Netatmo "+netatm.types[id]+"\")")

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
def InitializeAccounts():

  dbcursor.execute("SELECT * From Accounts")
  res = dbcursor.fetchall()
  for account in res:
    InitializeAccount(account)

##############################################################################
#function to add a Netatmo account to the data base
def AddAccount():
  print "-----------------------"
  print "| Add Netatmo account |"
  print "-----------------------"
  username = raw_input("User: ")
  password = getpass.getpass()
  ColorPrint.ColorPrint("Do you want to save the password as clear text to the database?\nIf not, you have to enter it on any update.", "warning")
  savepassw = raw_input("Save (y/n)?: ")
  if not (savepassw == "Y" or savepassw == "y"):
    savepassw = False
  else:
    savepassw = True
  ColorPrint.ColorPrint("You have to grant client access for WeatherStats. If not done yet,\nlog into\n\thttps://dev.netatmo.com/dev/myaccount\nand add an app called \"WeatherStats\". You will be given a client \nid and a client secret.", "warning")
  clientId = raw_input("Client id: ")
  clientSecret = raw_input("Client secret: ")

  #check if it works
  netatm = Netatmo.NetatmoClient(username, password, clientId, clientSecret)
  netatm.getStationData()

  if savepassw:
    dbcursor.execute(\
      "INSERT INTO Accounts (User, Password, ClientID, ClientSecret)\n"\
      "VALUES (\"" + username + "\",\"" + password + "\",\"" + clientId + "\",\"" + clientSecret + "\")"
    )
  else:
    dbcursor.execute(\
      "INSERT INTO Accounts (User, Password, ClientID, ClientSecret)\n"\
      "VALUES (\"" + username + "\",NULL,\"" + clientId + "\",\"" + clientSecret + "\")"
    )

  ColorPrint.ColorPrint("Account added", "okgreen")

  dbconn.commit()

##############################################################################
#function to add an InfluxDB
def AddInflux():
  print "-----------------------"
  print "| Add InfluxDB         |"
  print "-----------------------"
  host = raw_input("Host: ")
  port = raw_input("Port: ")
  user = raw_input("User: ")
  password = getpass.getpass()
  ColorPrint.ColorPrint("Do you want to save the password as clear text to the database?\nIf not, you have to enter it on any update.", "warning")
  savepassw = raw_input("Save (y/n)?: ")
  if not (savepassw == "Y" or savepassw == "y"):
    savepassw = False
  else:
    savepassw = True
  db = raw_input("Database: ")
  ssl = raw_input("Use SSL (y/n)?: ")
  if not (ssl == "Y" or ssl == "y"):
    ssl = 0
  else:
    ssl = 1

  if savepassw == True:
    dbcursor.execute(\
      "INSERT INTO InfluxDB (Host, Port, User, Password, Database,SSL)\n"\
      "VALUES (\"" + host + "\",\"" + port + "\",\"" + user + "\",\"" + password + "\", \""+db+"\", "+str(ssl)+" )"
    )
  else:
    dbcursor.execute(\
      "INSERT INTO InfluxDB (Host, Port, User, Password, Database,SSL)\n"\
      "VALUES (\"" + host + "\",\"" + port + "\",\"" + user + "\", NULL, \""+db+"\", "+str(ssl)+" )"
    )

  ColorPrint.ColorPrint("InfluxDB added", "okgreen")

  dbconn.commit()

##############################################################################
#Main

#First, check if database exists and create empty one if not
if dbexists == False:
  CreateEmptyDB()
  ColorPrint.ColorPrint("New database created", "okgreen")

#Now, add accounts
res = dbcursor.execute("SELECT * FROM Accounts").fetchall()
if len(res) == 0:
  AddAccount()

# Initialize accounts
InitializeAccounts()

#Add InfluxDB
res = dbcursor.execute("SELECT * FROM InfluxDB").fetchall()
if len(res) == 0:
  AddInflux()

dbconn.commit()
dbconn.close()
