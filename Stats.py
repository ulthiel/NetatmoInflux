#!/usr/bin/python

import sys
import sqlite3
import numpy
import datetime
import time
from scipy.signal import argrelextrema
from sets import Set
from optparse import OptionParser

#parse options
parser = OptionParser()
parser.add_option("--sensors", dest="sensors",help="Sensors (can be a comma separated list)")
parser.add_option("--months", dest="months",help="Months (can be a comma separated list)")
parser.add_option("--years", dest="years",help="Years (can be a comma separated list)")
(options, args) = parser.parse_args()
sensors = options.sensors
months = options.months
years = options.years

#connect to db
dbconn = sqlite3.connect('Weather.db')
dbcursor = dbconn.cursor()

#sensor ids
if sensors is None:
	sensors = []
	dbcursor.execute("SELECT ID From Sensors")
	res = dbcursor.fetchall()
	for sensor in res:
		sensors.append(int(sensor[0]))
else:
	sensors = sensors.split(',')
	
timefilter = False

#months
if months is not None:
	if months.find(',') > -1:
		months = [ int(m) for m in months.split(',')]
	elif months.find('-') > -1:
		months = months.split('-')
		months = range(int(months[0]), int(months[1])+1)
	else:
		months = [int(months)]
	
	timefilter = True

#months
if years is not None:
	if years.find(',') > -1:
		years = [ int(m) for m in years.split(',')]
	elif years.find('-') > -1:
		years = years.split('-')
		years = range(int(years[0]), int(years[1])+1)
	else:
		years = [int(years)]
	
	timefilter = True
	
#some timestamp helper functions	
def DayFromTimestamp(t):
	#Returns the day in format Y-m-d from a timestamp
	return datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d')
	
def MonthFromTimestamp(t):
	#Returns the month from a timestamp
	return int(datetime.datetime.fromtimestamp(t).strftime('%m'))
	
def YearFromTimestamp(t):
	#Returns the month from a timestamp
	return int(datetime.datetime.fromtimestamp(t).strftime('%Y'))

def FirstTimestampOfDay(d):
	#Returns first timestamp of day d (d in format Y-m-d)
	return time.mktime(datetime.datetime.strptime(d, "%Y-%m-%d").timetuple())

def LastTimestampOfDay(d):
	#Returns last timestamp of day d (d in format Y-m-d)
	return time.mktime((datetime.datetime.strptime(d, "%Y-%m-%d") + datetime.timedelta(1)).timetuple() )
	
#compute data for sensor
def DataForSensor(sensor):

	#data from db
	timezone = ((dbcursor.execute("SELECT Timezone From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]
	unit = ((dbcursor.execute("SELECT Unit From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]
	calibration = ((dbcursor.execute("SELECT Calibration From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]	
	description = timezone = ((dbcursor.execute("SELECT Description From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]	
	
	dbcursor.execute("SELECT Timestamp, Value From Data WHERE Sensor IS "+str(sensor)+' ORDER BY Timestamp ASC')
	res = dbcursor.fetchall()
	
	timestamps = numpy.array([elt[0] for elt in res])
	values = numpy.array([elt[1] + calibration for elt in res])
	
	#filter out by time restriction
	if timefilter:
		if months is not None:
			timestampindices = filter(lambda i:MonthFromTimestamp(timestamps[i]) in months, range(0,len(timestamps)))
			
			timestamps = timestamps[timestampindices]
			values = values[timestampindices]
		
		if years is not None:
			timestampindices = filter(lambda i:YearFromTimestamp(timestamps[i]) in years, range(0,len(timestamps)))
			
			timestamps = timestamps[timestampindices]
			values = values[timestampindices]
		
	if len(values) == 0:
		return
		
	#covered days
	earliesttimestamp = min(timestamps)
	earliestday = DayFromTimestamp(earliesttimestamp)
	latesttimestamp = max(timestamps)
	latesday = DayFromTimestamp(latesttimestamp)
	days = Set([DayFromTimestamp(timestamp) for timestamp in timestamps])
			
	#print general info
	print "Sensor " + str(sensor)
	print "\tUnit: \t\t" + unit
	print "\tLocation: \t" + description
	print "\tCalibration: \t" + str(calibration)
	print "\tData points: \t" + str(len(values))
	print "\tCoverage: \t" + earliestday + " to " + latesday + " (" + str(len(days)) + " days)"
		
	#maximal values
	maxvalue = max(values)
	maxindices = (numpy.argwhere(values == numpy.amax(values))).flatten().tolist()
	maxdays = Set([ DayFromTimestamp(timestamps[i]) for i in maxindices])
	output = "\tMaximum: \t" + str(maxvalue)
	output = output + " ("
	count = 0
	for day in maxdays:
		count = count + 1
		output = output + day
		if count < len(maxdays):
			output = output + ", "
	output = output + ")"
	print output
	
	#minimal values
	minvalue = min(values)
	minindices = (numpy.argwhere(values == numpy.amin(values))).flatten().tolist()
	mindays = Set([ DayFromTimestamp(timestamps[i]) for i in minindices])
	output = "\tMinimum: \t" + str(minvalue)
	output = output + " ("
	count = 0
	for day in mindays:
		count = count + 1
		output = output + day
		if count < len(mindays):
			output = output + ", "
	output = output + ")"
	print output
	
	#average
	avg = numpy.average(values)
	print "\tAverage:\t" + str(round(avg, 1)) + " (sigma=" + str(round(numpy.std(values),1)) + ")"
	
	#average of daily maxima
	dailymax = []
	for day in days:
		firsttimestamp = FirstTimestampOfDay(day)
		lasttimestamp = LastTimestampOfDay(day)
		dayvalues = values[(timestamps >= firsttimestamp) & (timestamps < lasttimestamp)]
		dailymax.append(max(dayvalues))
	dailymax = numpy.array(dailymax)
	dailymaxavg = numpy.average(dailymax)
	print "\tDaily maximum:\t" + str(round(dailymaxavg, 1)) + " (sigma=" + str(round(numpy.std(dailymax),1)) + ")"
	
	#average of daily minima
	dailymin = []
	for day in days:
		firsttimestamp = FirstTimestampOfDay(day)
		lasttimestamp = LastTimestampOfDay(day)
		dayvalues = values[(timestamps >= firsttimestamp) & (timestamps < lasttimestamp)]
		dailymin.append(min(dayvalues))
	dailymin = numpy.array(dailymin)
	dailyminavg = numpy.average(dailymin)
	print "\tDaily minimum:\t" + str(round(dailyminavg, 1)) + " (sigma=" + str(round(numpy.std(dailymin),1)) + ")"
	
	
#Overall stats
for sensor in sensors:
	DataForSensor(sensor)

dbconn.close()
