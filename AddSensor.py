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
#Adds new table DataX where X is the sensor id
def AddNewDataTable(dbcursor, sensor):
				
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
		


##############################################################################
#Standalone program
if __name__ == "__main__":
	
	print "Please provide information about the sensor to be added to the database."
	measurand = raw_input("Measurand: ")
	unit = raw_input("Unit: ")
	desc = raw_input("Description: ")
	cal = raw_input("Calibration: ")
	moduleid = raw_input("Module ID (if empty, a new module will be added): ")
	if moduleid == "":
		moduledesc = raw_input("Module description: ")
	pph = raw_input("Resolution (in pph): ")

	dbconn = sqlite3.connect('Weather.db')
	dbcursor = dbconn.cursor()

	if moduleid == "":
		dbcursor.execute("INSERT INTO Modules (Description) VALUES (\""+moduledesc+"\")")
		moduleid = (dbcursor.execute("SELECT last_insert_rowid();").fetchone())[0]
	"Print added module "+str(moduleid)
					
	dbcursor.execute("INSERT INTO Sensors (Measurand,Unit,Description,Calibration,Module,pph) VALUES (\""+measurand + "\",\""+unit+"\",\""+desc+"\","+cal+","+str(moduleid)+","+str(pph)+")")
	sensorid = (dbcursor.execute("SELECT last_insert_rowid();").fetchone())[0]

	AddNewDataTable(dbcursor, sensorid)
	print "Added sensor with id " + str(sensorid) + " to module with id "+str(moduleid)
	
	dbconn.commit()
	dbconn.close()