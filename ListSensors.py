#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
##############################################################################
# WeatherStats
# A collection of Python scripts for general sensor data management and analysis with Netatmo support.	
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

import sqlite3

##############################################################################
#connect to db
dbconn = sqlite3.connect('Weather.db')
dbcursor = dbconn.cursor()

##############################################################################

dbcursor.execute("SELECT Id From Sensors")
res = dbcursor.fetchall()
sensors = []
for sensor in res:
	sensors.append(int(sensor[0]))
	
dbcursor.execute("SELECT Id, Description From Locations")
res = dbcursor.fetchall()
locations = dict()
for location in res:
	locations[int(location[0])] = location[1]

dbcursor.execute("SELECT Id, Description From Locations")
res = dbcursor.fetchall()
modules = []
for module in res:
	modules.append(int(module[0]))
	
modulelocations = dict()
for module in modules:
	dbcursor.execute("SELECT LocationId From ModuleLocations WHERE ModuleID IS "+str(module))
	res = dbcursor.fetchall()
	modulelocations[module] = []
	for loc in res:
		modulelocations[module].append(locations[loc[0]])	
		
for sensor in sensors:
	measurand = ((dbcursor.execute("SELECT Measurand From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]
	unit = ((dbcursor.execute("SELECT Unit From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]
	calibration = ((dbcursor.execute("SELECT Calibration From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]	
	description = ((dbcursor.execute("SELECT Description From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]	
	module = ((dbcursor.execute("SELECT Module From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]	
	pph = ((dbcursor.execute("SELECT pph FROM Sensors WHERE Id IS "+str(sensor))).fetchone())[0]
	locstr = "  Location:\t"
	for i in range(0,len(modulelocations[module])):
		locstr = locstr + modulelocations[module][i]
		if i < len(modulelocations[module])-1:
			locstr = locstr + ", "
	print "Sensor: \t" + str(sensor)
	print "  Module: \t" + str(module)
	print ("  Measurand: \t" + measurand + " ("+unit+")").encode('utf-8')
	print locstr.encode('utf-8')
	print "  Calibration: \t" + str(calibration)
	print "  Resolution: \t" + str(pph) + " pph"
	print ""