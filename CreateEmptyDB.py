#!/usr/bin/python
# -*- coding: utf-8 -*-
##############################################################################
# WeatherStats
# A collection of Python scripts for general weather data management and analysis with Netatmo support
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
		"`Timestamp` BIGINT,\n" \
		"`Sensor` INTEGER,\n" \
		"`Value` DECIMAL,\n" \
		"`Year` SMALLINT,\n" \
		"`Month` TINYINT,\n" \
		"`Day` TINYINT,\n" \
		"`Hour` TINYINT,\n" \
		"`Minute` TINYINT,\n" \
		"`Second` TINYINT,\n" \
		"PRIMARY KEY(Timestamp,Sensor) ON CONFLICT REPLACE)"\
	)
	
	dbcursor.execute("CREATE INDEX idx ON Data (Timestamp ASC, Sensor ASC)")
	
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
	
	dbcursor.execute(\
		"CREATE VIEW DataWithTimeZone AS\
		SELECT Data.Timestamp, Sensors.Id AS Sensor, Data.Value, \
		Locations.Timezone, Data.Year \
		FROM\
    		Data\
        INNER JOIN\
    		Sensors\
        ON Data.Sensor = Sensors.Id\
		INNER JOIN\
			ModuleLocations\
		ON Sensors.Module = ModuleLocations.ModuleId\
		INNER JOIN\
			Locations\
		ON ModuleLocations.LocationId = Locations.Id\
		WHERE Data.Timestamp BETWEEN ModuleLocations.BeginTimestamp AND ModuleLocations.EndTimestamp\
		ORDER BY Timestamp ASC")
		
		dbcursor.execute(\
		"CREATE VIEW DataWithCalibration AS\
		SELECT Data.Timestamp, Sensors.Id AS Sensor, (Data.Value+Sensors.Calibration) AS ValueCalibrated, Data.Year, Data.Month, Data.Day, Data.Hour, Data.Minute, Data.Second\
		FROM\
    		Data\
        INNER JOIN\
    		Sensors\
        ON Data.Sensor = Sensors.Id\
		INNER JOIN\
			ModuleLocations\
		ON Sensors.Module = ModuleLocations.ModuleId\
		INNER JOIN\
			Locations\
		ON ModuleLocations.LocationId = Locations.Id\
		WHERE Data.Timestamp BETWEEN ModuleLocations.BeginTimestamp AND ModuleLocations.EndTimestamp\
		ORDER BY Year ASC, Month ASC, Day ASC, Hour ASC, Minute ASC, Second ASC")
	
#	dbcursor.execute(\
#		"CREATE VIEW DataWithUTC AS\
#		SELECT Data.Timestamp, Sensors.Id AS Sensor, Data.Value,\
#		Locations.Timezone,\
#		strftime('%Y', datetime(Data.Timestamp, 'unixepoch', 'utc')) As UTCYear,\
#  		strftime('%m', datetime(Data.Timestamp, 'unixepoch', 'utc')) As UTCMonth,\
#   		strftime('%d', datetime(Data.Timestamp, 'unixepoch', 'utc')) As UTCDay,\
#   		strftime('%H', datetime(Data.Timestamp, 'unixepoch', 'utc')) As UTCHour,\
#   		strftime('%M', datetime(Data.Timestamp, 'unixepoch', 'utc')) As UTCMinute,\
#   		strftime('%S', datetime(Data.Timestamp, 'unixepoch', 'utc')) As UTCSecond\
#		FROM\
#    		Data\
#        INNER JOIN\
#    		Sensors\
#        ON Data.Sensor = Sensors.Id\
#		INNER JOIN\
#			ModuleLocations\
#		ON Sensors.Module = ModuleLocations.ModuleId\
#		INNER JOIN\
#			Locations\
#		ON ModuleLocations.LocationId = Locations.Id\
#		WHERE Data.Timestamp BETWEEN ModuleLocations.BeginTimestamp AND ModuleLocations.EndTimestamp\
#		ORDER BY Timestamp ASC")

	
	dbconn.commit()
	dbconn.close()
	
#First, check if database exists and create empty one if not
if not os.path.isfile("Weather.db"):
	CreateEmptyDB()
	ColorPrint.ColorPrint("New database created", "okgreen")
else:
	ColorPrint.ColorPrint("Database exists already", "error")