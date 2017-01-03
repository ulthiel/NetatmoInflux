#!/usr/bin/python
# -*- coding: utf-8 -*-
##############################################################################
# WeatherStats
# A collection of Python scripts for general weather data management and analysis with Netatmo support
# (C) 2015-2017, Ulrich Thiel
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
		"CREATE TABLE \"Sensors\" (\n" \
		"`Id` INTEGER,\n" \
		"`Measurand` TEXT,\n" \
		"`Unit` TEXT,\n" \
		"`Description` TEXT,\n" \
		"`Calibration` DECIMAL,\n" \
		"`Module` INTEGER,\n" \
		"`pph` INTEGER,\n" \
		"PRIMARY KEY(Id))\n" \
	)
	
	dbcursor.execute(\
		"CREATE TABLE \"Modules\" (\n" \
		"`Id` INTEGER,\n" \
		"`Description` TEXT,\n" \
		"PRIMARY KEY(Id))\n" \
	)
	
	dbcursor.execute(\
		"CREATE TABLE \"ModuleLocations\" (\n" \
		"`ModuleId` INTEGER,\n" \
		"`BeginTimestamp` BIGINT,\n" \
		"`EndTimestamp` BIGINT,\n" \
		"`LocationId` INTEGER)\n" \
	)
	
	dbcursor.execute(\
		"CREATE TABLE \"Locations\" (\n" \
		"`Id` INTEGER,\n" \
		"`PositionNorth` DECIMAL,\n" \
		"`PositionEast` DECIMAL,\n" \
		"`Elevation` SMALLINT,\n" \
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
	

		
	dbcursor.execute("CREATE VIEW ModuleLocationsFull AS\
		SELECT Sensors.Id, ModuleLocations.BeginTimestamp, \
		ModuleLocations.EndTimestamp, ModuleLocations.LocationId, \
		Locations.Description, Locations.Timezone\
		FROM Sensors\
		INNER JOIN\
			ModuleLocations\
		ON\
			ModuleLocations.ModuleId = Sensors.Module\
		INNER JOIN	\
			Locations\
		ON\
			ModuleLocations.LocationId = Locations.Id")
	
	dbconn.commit()
	dbconn.close()
	
#First, check if database exists and create empty one if not
if not os.path.isfile("Weather.db"):
	CreateEmptyDB()
	ColorPrint.ColorPrint("New database created", "okgreen")
else:
	ColorPrint.ColorPrint("Database exists already", "error")