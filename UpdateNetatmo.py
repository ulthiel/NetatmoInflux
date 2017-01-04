#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
##############################################################################
# WeatherStats
# A collection of Python scripts for general weather data management and analysis with Netatmo support
# (C) 2015-2017, Ulrich Thiel
# thiel@mathematik.uni-stuttgart.de
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
import sys


##############################################################################
#database connection
dbconn = sqlite3.connect('Weather.db')
dbcursor = dbconn.cursor()

##############################################################################
#Adds new table DataX where X is the sensor id
def AddNewDataTable(sensor):
				
	dbcursor.execute(\
		"CREATE TABLE Data"+str(sensor)+" (\n" \
		"`Timestamp` BIGINT,\n" \
		"`Value` DECIMAL,\n" \
		"`Year` SMALLINT,\n" \
		"`Month` TINYINT,\n" \
		"`Day` TINYINT,\n" \
		"`Hour` TINYINT,\n" \
		"`Minute` TINYINT,\n" \
		"`Second` TINYINT,\n" \
		"PRIMARY KEY(Timestamp, Value) ON CONFLICT REPLACE)"\
	)
	
	#dbcursor.execute("CREATE INDEX idx ON Data"+str(sensor)+" (Timestamp ASC, Sensor ASC)")

	dbcursor.execute(\
		"CREATE VIEW Data"+str(sensor)+"Full AS\n"\
		"SELECT Data"+str(sensor)+".Timestamp, Data"+str(sensor)+".Value, (Data"+str(sensor)+".Value+Sensors.Calibration) AS ValueCalibrated,Data"+str(sensor)+".Year, Data"+str(sensor)+".Month, Data"+str(sensor)+".Day, Data"+str(sensor)+".Hour, Data"+str(sensor)+".Minute, Data"+str(sensor)+".Second, Locations.Id AS Location,  Locations.Description AS LocationDescription, Locations.Timezone\n"\
		"FROM\n"\
   		"  Data"+str(sensor)+"\n"\
        "INNER JOIN\n"\
        "  Sensors\n"\
        "ON Sensors.Id = "+str(sensor)+"\n"\
		"INNER JOIN\n"\
		  "ModuleLocations\n"\
		"ON Sensors.Module = ModuleLocations.ModuleId\n"\
		"INNER JOIN\n"\
		  "Locations\n"\
		"ON ModuleLocations.LocationId = Locations.Id\n"\
		"WHERE Data"+str(sensor)+".Timestamp BETWEEN ModuleLocations.BeginTimestamp AND ModuleLocations.EndTimestamp\n"\
		"ORDER BY Data"+str(sensor)+".Year ASC, Data"+str(sensor)+".Month ASC, Data"+str(sensor)+".Day ASC, Data"+str(sensor)+".Hour ASC, Data"+str(sensor)+".Minute ASC, Data"+str(sensor)+".Second ASC"\
	)
		
#if this would work not only for UTC but for arbitrary timezone, I would not need year, month, etc. in the table...but sqlite seems not to support this.
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
		
	
##############################################################################
#function to update all devices/modules for specific account (this is the main function)
def UpdateNetatmoForAccount(account):

	username = account[0]
	password = account[1]
	clientId = account[2]
	clientSecret = account[3]

	print "Updating account " + username
			
	if password == "":
		password = getpass.getpass()
	
	netatm = Netatmo.NetatmoClient(username, password, clientId, clientSecret)
	netatm.getStationData()
	
	#first, update modules and sensors
	for id in netatm.devicemoduleids:
			
		if id[1] == None:
			moduleid = dbcursor.execute("SELECT ModuleId FROM NetatmoModules WHERE NetatmoDeviceId IS \""+str(id[0])+"\" AND NetatmoModuleId IS NULL").fetchone()

		else:
			moduleid = dbcursor.execute("SELECT ModuleId FROM NetatmoModules WHERE NetatmoDeviceId IS \""+str(id[0])+"\" AND NetatmoModuleId IS \""+id[1]+"\"").fetchone()
		
		#if not exists, add new device/module and its sensors
		if moduleid == None:
		
			#check if location exists
			location = netatm.locations[id]
			locationid = dbcursor.execute("SELECT ID FROM Locations WHERE PositionNorth IS "+str(location[0][1])+" AND PositionEast IS "+str(location[0][0])+" AND Elevation IS "+str(location[1])+" AND Description IS \""+location[3]+"\" AND Timezone IS \""+location[2]+"\"").fetchone()
		
			if locationid == None:
				dbcursor.execute("INSERT INTO Locations (PositionNorth,PositionEast,Elevation,Description,Timezone) VALUES ("+str(location[0][1]) + "," + str(location[0][0]) + "," + str(location[1]) + ",\"" + location[3] + "\",\"" + location[2] + "\")")
			
				locationid = (dbcursor.execute("SELECT last_insert_rowid();").fetchone())[0]
			else:
				locationid = locationid[0]
				
			dbcursor.execute("INSERT INTO Modules (Description) VALUES (\"Netatmo Module\")")

			moduleid = (dbcursor.execute("SELECT last_insert_rowid();").fetchone())[0]
				
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
					
				dbcursor.execute("INSERT INTO Sensors (Measurand,Unit,Description,Calibration,Module,pph) VALUES (\""+measurand + "\",\""+unit+"\",\"Netatmo sensor\",0,"+str(moduleid)+",12)") #12 points per hour is Netatmo resolution
				sensorid = (dbcursor.execute("SELECT last_insert_rowid();").fetchone())[0]
				sensorids.append(sensorid)
				
							
			#get first time stamp for module and set this as BeginTimestamp for location of module
			measurandsstring = ""
			measurands = netatm.measurands[id]
			for i in range(0,len(measurands)):
				measurandsstring = measurandsstring + measurands[i]
				if i < len(measurands)-1:
					measurandsstring = measurandsstring + ","
			data = netatm.getMeasure(id[0],id[1],"max",measurandsstring,None,None,None,"false")
			minservertimestamp = min(map(int,data.keys()))
			
			dbcursor.execute("INSERT INTO ModuleLocations (ModuleId,BeginTimestamp,EndTimestamp,LocationId) VALUES ("+str(moduleid)+","+str(minservertimestamp)+","+str(2**63-1)+","+str(locationid)+")")
					
			if id[1] == None:
				dbcursor.execute("INSERT INTO NetatmoModules (NetatmoDeviceId,ModuleId) VALUES (\""+id[0]+"\","+str(moduleid)+")")
				
				ColorPrint.ColorPrint("Added device "+id[0]+" as module "+str(moduleid)+" at location "+str(locationid), "okgreen")
				
			else:
				
				dbcursor.execute("INSERT INTO NetatmoModules (NetatmoDeviceId,NetatmoModuleId,ModuleId) VALUES (\""+id[0]+"\",\""+id[1]+"\","+str(moduleid)+")")
				
				ColorPrint.ColorPrint("Added module "+id[1]+" of device "+id[0]+" as module "+str(moduleid)+" at location "+str(locationid), "okgreen")
			
			#add data tables for all sensors	
			for sensor in sensorids:
				AddNewDataTable(sensor)
						
		else:
			moduleid = moduleid[0]
	
		dbconn.commit()
				
		#now, update data
		currenttime = DateHelper.CurrentTimestamp()
		res = dbcursor.execute("SELECT Id FROM Sensors WHERE Module IS "+str(moduleid)).fetchall()
		sensorids = [ r[0] for r in res ]
		measurands = []
		for sensorid in sensorids:
			measurands.append(dbcursor.execute("SELECT Measurand FROM Sensors WHERE Id IS "+str(sensorid)).fetchone()[0])
		measurandsstring = ""
		for i in range(0,len(measurands)):
			measurandsstring = measurandsstring + measurands[i]
			if i < len(measurands)-1:
				measurandsstring = measurandsstring + ","
				
		if id[1] == None:
			print "  Updating data for device "+id[0]
		else:
			print "  Updating data for module "+id[1]+" of device "+id[0]
			
		#find last timestamp among all sensors (this is the minimal point up to which we have to update data)
		maxdbtimestamp = None
		for sensorid in sensorids:
			mt = dbcursor.execute("SELECT MAX(Timestamp) FROM Data"+str(sensorid)).fetchone()[0]
			if mt != None and (maxdbtimestamp == None or mt < maxdbtimestamp): #< is correct here
				maxdbtimestamp = mt
				
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
							dbcursor.execute("INSERT INTO Data"+str(sensorid)+" (Timestamp,Value) VALUES ("+str(timestamp)+","+str(data[timestamp][i])+")")
							datapointcounter = datapointcounter + 1
			
					dbconn.commit()

		Tools.PrintWithoutNewline("    Retrieving data from "+DateHelper.DateFromDatetime(DateHelper.DatetimeFromTimestamp(maxdbtimestamp,None))+" to now: 100%  ")
		print ""
		ColorPrint.ColorPrint("    "+str(datapointcounter)+" data points for "+str(timestampcounter)+" timestamps received", "okgreen")

##############################################################################
#This iterates through all accounts
def UpdateNetatmo():
	
	dbcursor.execute("SELECT * From NetatmoAccounts")
	res = dbcursor.fetchall()
	for account in res:
		UpdateNetatmoForAccount(account)

	
UpdateNetatmo()
SetDates(dbconn, dbcursor)
dbconn.commit()
dbconn.close()