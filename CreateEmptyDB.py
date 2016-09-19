#!/usr/bin/python
# -*- coding: utf-8 -*-
##############################################################################
# WeatherStats
# Tiny Python scripts for general weather data management and analysis (with Netatmo support)
# (C) 2015-2016, Ulrich Thiel
# thiel@mathematik.uni-stuttgart.de
##############################################################################

##############################################################################
#This script creates an empty database

import sqlite3
import os.path
from lib import ColorPrint
from lib import Netatmo
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
	
#	dbcursor.execute(\
#		"CREATE TABLE \"Measurands\" (\n" \
#		"`Id` INTEGER,\n" \
#		"`Measurand` TEXT,\n" \
#		"`Unit` TEXT,\n" \
#		"PRIMARY KEY(Id))\n" \
#	)
	
	dbcursor.execute(\
		"CREATE TABLE \"Sensors\" (\n" \
		"`Id` INTEGER,\n" \
		"`Measurand` TEXT,\n" \
		"`Unit` TEXT,\n" \
		"`Description` TEXT,\n" \
		"`Calibration` NUMERIC,\n" \
		"PRIMARY KEY(Id))\n" \
	)
	
	dbcursor.execute(\
		"CREATE TABLE \"Modules\" (\n" \
		"`Id` INTEGER,\n" \
		"`SensorIds` TEXT,\n" \
		"PRIMARY KEY(Id))\n" \
	)
	
	dbcursor.execute(\
		"CREATE TABLE \"ModuleLocations\" (\n" \
		"`ModuleId` INTEGER,\n" \
		"`BeginTimestamp` INTEGER,\n" \
		"`EndTimestamp` TEXT,\n" \
		"`LocationId` INTEGER)\n" \
	)
	
	dbcursor.execute(\
		"CREATE TABLE \"Locations\" (\n" \
		"`Id` INTEGER,\n" \
		"`PositionNorth` NUMERIC,\n" \
		"`PositionEast` NUMERIC,\n" \
		"`Elevation` NUMERIC,\n" \
		"`Description` TEXT,\n" \
		"`Timezone` TEXT,\n" \
		"PRIMARY KEY(Id))\n" \
	)
	
	dbcursor.execute(\
		"CREATE TABLE \"NetatmoAccounts\" (\n" \
		"`User` TEXT,\n" \
		"`Password` TEXT,\n" \
		"`ClientID` TEXT,\n" \
		"`ClientSecret` TEXT,\n" \
		"PRIMARY KEY(User) ON CONFLICT REPLACE)"\
	)
	
	dbcursor.execute(\
		"CREATE TABLE \"NetatmoModules\" (\n" \
		"`NetatmoDeviceId` TEXT,\n" \
		"`NetatmoModuleId` TEXT,\n" \
		"`ModuleId` INTEGER,\n" \
		"PRIMARY KEY(NetatmoDeviceId,NetatmoModuleId) ON CONFLICT REPLACE)"\
	)
	
	dbconn.commit()
	dbconn.close()
	
#First, check if database exists and create empty one if not
if not os.path.isfile("Weather.db"):
	CreateEmptyDB()
	ColorPrint.ColorPrint("New database created", "okgreen")
else:
	ColorPrint.ColorPrint("Database exists already", "error")