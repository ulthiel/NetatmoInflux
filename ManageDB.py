#!/usr/bin/python
# -*- coding: utf-8 -*-
##############################################################################
# WeatherStats
# Tiny Python scripts for general weather data management and analysis (with Netatmo support)
# (C) 2015-2016, Ulrich Thiel
# thiel@mathematik.uni-stuttgart.de
##############################################################################

##############################################################################
#This script is for managing the database

import sqlite3
import os.path
from lib import ColorPrint
import getpass

#Creates empty database
def CreateEmptyDB():
	dbconn = sqlite3.connect('Weather.db')
	dbcursor = dbconn.cursor()
			
	dbcursor.execute(\
		"CREATE TABLE \"Data\" (\n" \
		"`Timestamp` INTEGER,\n" \
		"`Sensor` INTEGER,\n" \
		"`Value` REAL,\n" \
		"`Year` INTEGER,\n" \
		"`Month` INTEGER,\n" \
		"`Day` INTEGER,\n" \
		"`Hour` INTEGER,\n" \
		"`Minute` INTEGER,\n" \
		"`Second` INTEGER,\n" \
		"PRIMARY KEY(Timestamp,Sensor) ON CONFLICT REPLACE)"\
	)
	
	dbcursor.execute(\
		"CREATE TABLE \"Measurands\" (\n" \
		"`ID` INTEGER,\n" \
		"`Measurand` TEXT,\n" \
		"`Unit` TEXT,\n" \
		"PRIMARY KEY(ID))\n" \
	)
	
	dbcursor.execute(\
		"CREATE TABLE \"Sensors\" (\n" \
		"`ID` INTEGER,\n" \
		"`Location` INTEGER,\n" \
		"`Measurand` INTEGER,\n" \
		"`Sensor` TEXT,\n" \
		"`Calibration` NUMERIC,\n" \
		"PRIMARY KEY(ID))\n" \
	)
	
	dbcursor.execute(\
		"CREATE TABLE \"Locations\" (\n" \
		"`ID` INTEGER,\n" \
		"`PositionNorth` NUMERIC,\n" \
		"`PositionEast` NUMERIC,\n" \
		"`Elevation` NUMERIC,\n" \
		"`Description` TEXT,\n" \
		"`Timezone` TEXT,\n" \
		"PRIMARY KEY(ID))\n" \
	)
	
	dbcursor.execute(\
		"CREATE TABLE \"Netatmo\" (\n" \
		"`User` TEXT,\n" \
		"`Password` TEXT,\n" \
		"`ClientID` TEXT,\n" \
		"`ClientSecret` TEXT,\n" \
		"PRIMARY KEY(User, Password, ClientID, ClientSecret) ON CONFLICT REPLACE)"\
	)
	
	dbconn.commit()
	dbconn.close()
	
#Add Netatmo account
def AddNetatmo():
	username = raw_input("User: ")
	password = getpass.getpass()
	savepassw = raw_input(ColorPrint.ColorPrintString("Do you want to save the password as clear text to the database?\nIf not, you have to enter it on any update.\nSave? (y/n) ", "warning"))
	if not (savepassw == "Y" or savepassw == "y"):
		password = ""
	ColorPrint.ColorPrint("You have to grant client access for WeatherStats. If not done yet,\ngo to http://dev.netatmo.com/dev/listapps and add a client. You will\nbe presented a client id and a client secret.", "warning")
	clientid = raw_input("Client id: ")
	clientsecret = raw_input("Client secret: ")
	
	dbconn = sqlite3.connect('Weather.db')
	dbcursor = dbconn.cursor()
	
	dbcursor.execute(\
		"INSERT INTO Netatmo (User, Password, ClientID, ClientSecret)\n"\
		"VALUES (\"" + username + "\",\"" + password + "\",\"" + clientid + "\",\"" + clientsecret + "\")"
	)
	
	dbconn.commit()
	dbconn.close()
	
#First, check if database exists and create empty one if not
if not os.path.isfile("Weather.db"):
	CreateEmptyDB()
	
#Print options
print "(1) Add Netatmo account"
print "(2) Update Netatmo data"
print "(3) Sync Netatmo data"
action = raw_input("Action: ")

if action == "1":
	AddNetatmo()
elif action == "2" or action == "3":
	#do action for all netatmo accounts
	
