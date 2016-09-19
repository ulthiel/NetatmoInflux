#!/usr/bin/python
# -*- coding: utf-8 -*-
##############################################################################
# WeatherStats
# Tiny Python scripts for general weather data management and analysis (with Netatmo support)
# (C) 2015-2016, Ulrich Thiel
# thiel@mathematik.uni-stuttgart.de
##############################################################################

##############################################################################
#This script updates all data from Netatmo

import sqlite3
import os.path
import sys
from lib import ColorPrint
from lib import Netatmo
import getpass
from lib import DateHelper


dbconn = sqlite3.connect('Weather.db')
dbcursor = dbconn.cursor()
	
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
			
			dbcursor.execute("INSERT INTO ModuleLocations (ModuleId,LocationId) VALUES ("+str(moduleid)+","+str(locationid)+")")
					
			if id[1] == None:
				dbcursor.execute("INSERT INTO NetatmoModules (NetatmoDeviceId,ModuleId) VALUES (\""+id[0]+"\","+str(moduleid)+")")
				
				ColorPrint.ColorPrint("Added device "+id[0]+" as module "+str(moduleid)+" at location "+str(locationid), "okblue")
				
			else:
				
				dbcursor.execute("INSERT INTO NetatmoModules (NetatmoDeviceId,NetatmoModuleId,ModuleId) VALUES (\""+id[0]+"\",\""+id[1]+"\","+str(moduleid)+")")
				
				ColorPrint.ColorPrint("Added module "+id[1]+" of device "+id[0]+" as module "+str(moduleid)+" at location "+str(locationid), "okblue")
					
		else:
			moduleid = moduleid[0]
			
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
			print "Updating data for device "+id[0]
		else:
			print "Updating data for module "+id[1]+" of device "+id[0]
			
		#find last timestamp among all sensors (this is the minimal point up to which we have to update data)
		maxtimestamp = 0
		for sensorid in sensorids:
			mt = dbcursor.execute("SELECT MAX(Timestamp) FROM Data WHERE Sensor IS "+str(sensorid)).fetchone()[0]
			if mt != None and mt < maxtimestamp:
				maxtimestamp = mt
				
		if maxtimestamp == 0:
			#get minimal timestamp for device/module from server
			data = netatm.getMeasure(id[0],id[1],"max",measurandsstring,None,None,None,"false")
			maxtimestamp = min(map(int,data.keys()))
			
		print "    Will retrieve data from "+DateHelper.DateFromTimestamp(maxtimestamp,None)+" to now"
			
		date_end = currenttime
		timestampcounter = 0
			
		while date_end > maxtimestamp:
			date_begin = date_end - 1024*5*60 #this is a bit ugly. netatmo resolution is one data point every 5 minutes. we can retrieve at most 1024 data points per request. this is where this number comes from. on demand measurements may be missing but this should be fine
			sys.stdout.write('\r' + '    Retrieving data in range '+DateHelper.DateFromTimestamp(date_begin,None)+' to '+DateHelper.DateFromTimestamp(date_end,None)) #timezone doesn't matter here for status message
			sys.stdout.flush()
			data = netatm.getMeasure(id[0],id[1],"max",measurandsstring,date_begin,date_end,None,"false")
			timestampcounter = timestampcounter + len(data)
			date_end = date_begin-1
				
			for timestamp in data.keys():
				for i in range(0,len(sensorids)):
					sensorid = sensorids[i]
					if data[timestamp][i] != None:
						dbcursor.execute("INSERT INTO Data (Timestamp,Sensor,Value) VALUES ("+str(timestamp)+","+str(sensorid)+","+str(data[timestamp][i])+")")
			
				dbconn.commit()

		sys.stdout.write('\n')
		sys.stdout.flush()
		print "    Done"
		print "    Data for "+str(timestampcounter)+" time stamps received"

	
#Update Netatmo 
def UpdateNetatmo():
	
	dbcursor.execute("SELECT * From NetatmoAccounts")
	res = dbcursor.fetchall()
	for account in res:
		UpdateNetatmoForAccount(account)

	
UpdateNetatmo()
dbconn.commit()
dbconn.close()