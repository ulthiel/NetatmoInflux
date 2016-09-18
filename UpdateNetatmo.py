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
from lib import ColorPrint
from lib import Netatmo
import getpass


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
		currenttime = 
		res = dbcursor.execute("SELECT SensorIds FROM Modules WHERE Id IS "+str(moduleid)).fetchone()[0]
		sensorids = map(int, res.split(','))
		for i in sensorids:
			measurand = dbcursor.execute("SELECT Measurand FROM Sensors WHERE Id IS "+str(i)).fetchone()[0]
			maxtimestamp = dbcursor.execute("SELECT MAX(Timestamp) FROM Data WHERE Sensor IS "+str(i)).fetchone()[0]
			
			
		
		

#	for sensor in netatm.sensors:
#		dbcursor.execute("SELECT ID From Locations WHERE PositionNorth IS " + str(sensor['Location'][1]) + " AND PositionEast IS " + str(sensor['Location'][0]) + " AND Elevation IS " + str(sensor['Elevation']) + " AND NetatmoName IS \"" + sensor['LocationName'] + "\"")
#		res = dbcursor.fetchall()
#		if len(res) == 0:
#			dbcursor.execute("INSERT INTO Locations (PositionNorth, PositionEast, Elevation, Description, NetatmoName, Timezone) VALUES (" + str(sensor['Location'][1]) + "," + str(sensor['Location'][0]) + "," + str(sensor['Elevation']) + ",\"" + sensor['LocationName'] + "\", \"" + sensor['LocationName'] + "\",\"" + sensor['Timezone'] + "\")")
			
			#get location id
#			dbcursor.execute("SELECT ID From Locations WHERE PositionNorth IS " + str(sensor['Location'][1]) + " AND PositionEast IS " + str(sensor['Location'][0]) + " AND Elevation IS " + str(sensor['Elevation']) + " AND NetatmoName IS \"" + sensor['LocationName'] + "\"")
#			locationid = dbcursor.fetchall()[0]
			
#			print "Adding new location \"" + sensor['LocationName'] + "\" with ID " + str(locationid)
			
#		elif len(res) == 1:
#			locationid = res[0][0]
			
#		else:
#			ColorPrint.ColorPrint("Multiple matching locations for " + sensor['Measurand'] + " sensor in module " + senor['NetatmoModule'] + " found: "+str(res), "warning")
#			locationid = raw_input("Correct location ID: ")
	
#Update Netatmo 
def UpdateNetatmo():
	
	dbcursor.execute("SELECT * From NetatmoAccounts")
	res = dbcursor.fetchall()
	for account in res:
		UpdateNetatmoForAccount(account)

	
UpdateNetatmo()
dbconn.commit()
dbconn.close()