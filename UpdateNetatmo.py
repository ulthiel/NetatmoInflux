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
			res = (dbcursor.execute("SELECT COUNT(*) FROM NetatmoModules WHERE NetatmoDeviceId IS \""+str(id[0])+"\" AND NetatmoModuleId IS NULL").fetchone())[0]
		else:
			res = (dbcursor.execute("SELECT COUNT(*) FROM NetatmoModules WHERE NetatmoDeviceId IS \""+str(id[0])+"\" AND NetatmoModuleId IS \""+id[1]+"\"").fetchone())[0]
		if res is None or res == 0:
			print netatm.locations[id]

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