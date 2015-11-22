#!/usr/bin/python

import sys
import sqlite3
import numpy
import datetime
from scipy.signal import argrelextrema

#connect to db
dbconn = sqlite3.connect('Weather.db')
dbcursor = dbconn.cursor()

#sensor ids
sensors = []
dbcursor.execute("SELECT ID From Sensors")
res = dbcursor.fetchall()
for sensor in res:
	sensors.append(int(sensor[0]))
	
def DayFromTimestamp(t):
	return datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d')
	
def TimestampFromDate(d):
	return time.mktime(datetime.datetime.strptime(s, "%Y-%m-%d").timetuple())
	
#compute data for sensor
def DataForSensor(sensor):

	#data from db
	timezone = ((dbcursor.execute("SELECT Timezone From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]
	unit = timezone = ((dbcursor.execute("SELECT Unit From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]
	calibration = ((dbcursor.execute("SELECT Calibration From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]	
	description = timezone = ((dbcursor.execute("SELECT Description From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]
	dbcursor.execute("SELECT Timestamp, Value From Data WHERE Sensor IS "+str(sensor))
	res = dbcursor.fetchall()
	timestamps = numpy.array([elt[0] for elt in res])
	values = numpy.array([elt[1] + calibration for elt in res])
	if len(values) == 0:
		return
	
	#covered days
	earliesttimestamp = min(timestamps)
	earliestday = DayFromTimestamp(earliesttimestamp)
	latesttimestamp = max(timestamps)
	latesday = DayFromTimestamp(latesttimestamp)
	
	days = []
	for timestamp in timestamps:
		day = DayFromTimestamp(timestamp)
		if not day in days:
			days.append(day)
			
	print "Sensor " + str(sensor)
	print "\tUnit: \t\t" + unit
	print "\tLocation: \t" + description
	print "\tCalibration: \t" + str(calibration)
	print "\tData points: \t" + str(len(values))
	print "\tCoverage: \t" + earliestday + " to " + latesday + " (" + str(len(days)) + " days)"
		
	
	print ""
	
#Overall stats
for sensor in sensors:
	DataForSensor(sensor)


dbconn.close()
