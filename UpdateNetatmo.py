#!/usr/bin/python
# -*- coding: utf-8 -*-
##############################################################################
# WeatherStats
# Tiny Python scripts for general weather data management and analysis (with Netatmo support)
# (C) 2015-2016, Ulrich Thiel
# thiel@mathematik.uni-stuttgart.de
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

##############################################################################
#database connection
dbconn = sqlite3.connect('Weather.db')
dbcursor = dbconn.cursor()
	
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
					
				dbcursor.execute("INSERT INTO Sensors (Measurand,Unit,Description,Calibration) VALUES (\""+measurand + "\",\""+unit+"\",\"Netatmo sensor\",0)")
				sensorid = (dbcursor.execute("SELECT last_insert_rowid();").fetchone())[0]
				sensorids.append(sensorid)
				
			sensoridsstr = ""
			for i in range(0,len(sensorids)):
				sensoridsstr = sensoridsstr + str(sensorids[i])
				if i < len(sensorids)-1:
					sensoridsstr = sensoridsstr + ","
			
			dbcursor.execute("INSERT INTO Modules (SensorIds) VALUES (\""+sensoridsstr+"\")")

			moduleid = (dbcursor.execute("SELECT last_insert_rowid();").fetchone())[0]
			
			#get first time stamp for module and set this as BeginTimestamp for location of module
			measurandsstring = ""
			measurands = netatm.measurands[id]
			for i in range(0,len(measurands)):
				measurandsstring = measurandsstring + measurands[i]
				if i < len(measurands)-1:
					measurandsstring = measurandsstring + ","
			data = netatm.getMeasure(id[0],id[1],"max",measurandsstring,None,None,None,"false")
			minservertimestamp = min(map(int,data.keys()))
			
			dbcursor.execute("INSERT INTO ModuleLocations (ModuleId,BeginTimestamp,LocationId) VALUES ("+str(moduleid)+","+str(minservertimestamp)+","+str(locationid)+")")
					
			if id[1] == None:
				dbcursor.execute("INSERT INTO NetatmoModules (NetatmoDeviceId,ModuleId) VALUES (\""+id[0]+"\","+str(moduleid)+")")
				
				ColorPrint.ColorPrint("Added device "+id[0]+" as module "+str(moduleid)+" at location "+str(locationid), "okgreen")
				
			else:
				
				dbcursor.execute("INSERT INTO NetatmoModules (NetatmoDeviceId,NetatmoModuleId,ModuleId) VALUES (\""+id[0]+"\",\""+id[1]+"\","+str(moduleid)+")")
				
				ColorPrint.ColorPrint("Added module "+id[1]+" of device "+id[0]+" as module "+str(moduleid)+" at location "+str(locationid), "okgreen")
						
		else:
			moduleid = moduleid[0]
	
		dbconn.commit()
				
		#now, update data
		currenttime = DateHelper.CurrentTimestamp()
		res = dbcursor.execute("SELECT SensorIds FROM Modules WHERE Id IS "+str(moduleid)).fetchone()[0]
		sensorids = map(int, res.split(','))
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
			mt = dbcursor.execute("SELECT MAX(Timestamp) FROM Data WHERE Sensor IS "+str(sensorid)).fetchone()[0]
			if mt != None and (maxdbtimestamp == None or mt < maxdbtimestamp): #< is correct here
				maxdbtimestamp = mt
				
		if maxdbtimestamp == None:
			#get minimal timestamp for device/module from server
			data = netatm.getMeasure(id[0],id[1],"max",measurandsstring,None,None,None,"false")
			maxdbtimestamp = min(map(int,data.keys()))
			
		Tools.PrintWithoutNewline("    Retrieving data from "+DateHelper.DateFromTimestamp(maxdbtimestamp,None)+" to now: 0%  ")
			
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
			
			Tools.PrintWithoutNewline("    Retrieving data from "+DateHelper.DateFromTimestamp(maxdbtimestamp,None)+" to now: "+str(int((float(date_end)-float(maxdbtimestamp))/float(maxval)*100.0))+'%  ')
			
			if len(data) != 0: #might be empty in case there is no data in this time window. we shouldn't break here though since there might still be earlier data
				for timestamp in data.keys():
					for i in range(0,len(sensorids)):
						sensorid = sensorids[i]
						if data[timestamp][i] != None:
							dbcursor.execute("INSERT INTO Data (Timestamp,Sensor,Value) VALUES ("+str(timestamp)+","+str(sensorid)+","+str(data[timestamp][i])+")")
							datapointcounter = datapointcounter + 1
			
					dbconn.commit()

		Tools.PrintWithoutNewline("    Retrieving data from "+DateHelper.DateFromTimestamp(maxdbtimestamp,None)+" to now: 100%  ")
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
dbconn.commit()
dbconn.close()