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
		#"`Year` INTEGER,\n" \
		#"`Month` INTEGER,\n" \
		#"`Day` INTEGER,\n" \
		#"`Hour` INTEGER,\n" \
		#"`Minute` INTEGER,\n" \
		#"`Second` INTEGER,\n" \
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
		"`NetatmoAccountID` TEXT,\n" \
		"`NetatmoModuleID` TEXT,\n" \
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
		"PRIMARY KEY(User) ON CONFLICT REPLACE)"\
	)
	
	dbconn.commit()
	dbconn.close()
	
#First, check if database exists and create empty one if not
if not os.path.isfile("Weather.db"):
	CreateEmptyDB()
else:
	ColorPrint.ColorPrint("Database exists already", "error")