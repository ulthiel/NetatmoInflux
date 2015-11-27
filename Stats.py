#!/usr/bin/python

import sys
import sqlite3
import numpy
import datetime
import time
from scipy.signal import argrelextrema
from sets import Set
from optparse import OptionParser
from itertools import chain

#parse options
parser = OptionParser()
parser.add_option("--sensors", dest="sensors",help="Sensors (e.g., --sensors=1-2,6-7)")
parser.add_option("--hours", dest="hours",help="Hours (e.g., --hours=7-9,21)")
parser.add_option("--months", dest="months",help="Months (e.g., --months=5-8,12)")
parser.add_option("--years", dest="years",help="Years (e.g., --years=2012,2014-2015)")
parser.add_option("--missing", action="store_false", dest="printmissing",help="Print missing data", default=False)
(options, args) = parser.parse_args()
sensors = options.sensors
hours = options.hours
months = options.months
years = options.years
printmissing = options.printmissing

#connect to db
dbconn = sqlite3.connect('Weather.db')
dbcursor = dbconn.cursor()
	
#to parse range for the time filter (source: https://gist.github.com/kgaughan/2491663)
def parse_range(rng):
    parts = rng.split('-')
    if 1 > len(parts) > 2:
        raise ValueError("Bad range: '%s'" % (rng,))
    parts = [int(i) for i in parts]
    start = parts[0]
    end = start if len(parts) == 1 else parts[1]
    if start > end:
        end, start = start, end
    return range(start, end + 1)

def parse_range_list(rngs):
    return sorted(set(chain(*[parse_range(rng) for rng in rngs.split(',')])))
    
#sensor ids
if sensors is None:
	sensors = []
	dbcursor.execute("SELECT ID From Sensors")
	res = dbcursor.fetchall()
	for sensor in res:
		sensors.append(int(sensor[0]))
else:
	sensors = parse_range_list(sensors)
	
#timefilter
timefilter = False

#hours
if hours is not None:
	hours = parse_range_list(hours)
	timefilter = True
	
#months
if months is not None:
	months = parse_range_list(months)
	timefilter = True

#years
if years is not None:
	years = parse_range_list(years)
	timefilter = True
	
#some timestamp helper functions	
def DayFromTimestamp(t):
	#Returns the day in format Y-m-d from a timestamp
	return datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d')
	
#some timestamp helper functions	
def DateFromTimestamp(t):
	#Returns the day in format Y-m-d from a timestamp
	return datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M')
	
def HourFromTimestamp(t):
	#Returns the month from a timestamp
	return int(datetime.datetime.fromtimestamp(t).strftime('%H'))

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
		if hours is not None:
			timestampindices = filter(lambda i:HourFromTimestamp(timestamps[i]) in hours, range(0,len(timestamps)))
			
			timestamps = timestamps[timestampindices]
			values = values[timestampindices]
			
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
	latestday = DayFromTimestamp(latesttimestamp)
	days = Set([DayFromTimestamp(timestamp) for timestamp in timestamps]) #number of days between earliest and latest
	totaldays = (datetime.datetime.strptime(latestday, "%Y-%m-%d") - datetime.datetime.strptime(earliestday, "%Y-%m-%d")).days
				
	#print general info
	print "Sensor ID:\t" + str(sensor)
	print "Unit: \t\t" + unit
	print "Location: \t" + description
	print "Calibration: \t" + str(calibration)
	print "Data points: \t" + str(len(values))
	print "Coverage: \t" + earliestday + " to " + latestday + " (" + str(len(days)) + " days)"
	
	#data quality (require one record per hour)
	hoursperdaycovered = dict()
	for day in days:
		hoursperdaycovered[day] = set()
	for t in timestamps:
		day = DayFromTimestamp(t)
		H = HourFromTimestamp(t)
		hoursperdaycovered[day] = hoursperdaycovered[day] | {H}
	totalhourscovered = 0
	missing = []
	for day in days:
		totalhourscovered = totalhourscovered + len(hoursperdaycovered[day])
		if len(hoursperdaycovered[day]) != 24:
			for i in range(0,24):
				if i not in hoursperdaycovered[day]:
					missing.append(day +  ' ' + str(i) + 'h')
	quality = float(totalhourscovered)/(24.0*float(len(days)))*100.0
	if printmissing == True:
		missingstr = ""
		for i in range(0,len(missing)):
			missingstr = missingstr + missing[i]
			if i < len(missing)-1:
				missingstr = missingstr + ", "
		print "Quality: \t" + str(round(quality,3)) + "%" + " (missing: " + missingstr + ")"
	else:
		print "Quality: \t" + str(round(quality,3)) + "%"
		
	#maximal values
	maxvalue = max(values)
	maxindices = (numpy.argwhere(values == numpy.amax(values))).flatten().tolist()
	maxdays = Set([ DayFromTimestamp(timestamps[i]) for i in maxindices])
	output = "Maximum: \t" + str(maxvalue)
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
	output = "Minimum: \t" + str(minvalue)
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
	print "Average:\t" + str(round(avg, 1)) + " (sigma=" + str(round(numpy.std(values),1)) + ")"
	
	#average of daily maxima
	dailymax = []
	for day in days:
		firsttimestamp = FirstTimestampOfDay(day)
		lasttimestamp = LastTimestampOfDay(day)
		dayvalues = values[(timestamps >= firsttimestamp) & (timestamps < lasttimestamp)]
		dailymax.append(max(dayvalues))
	dailymax = numpy.array(dailymax)
	dailymaxavg = numpy.average(dailymax)
	print "Daily maximum:\t" + str(round(dailymaxavg, 1)) + " (sigma=" + str(round(numpy.std(dailymax),1)) + ")"
	
	#average of daily minima
	dailymin = []
	for day in days:
		firsttimestamp = FirstTimestampOfDay(day)
		lasttimestamp = LastTimestampOfDay(day)
		dayvalues = values[(timestamps >= firsttimestamp) & (timestamps < lasttimestamp)]
		dailymin.append(min(dayvalues))
	dailymin = numpy.array(dailymin)
	dailyminavg = numpy.average(dailymin)
	print "Daily minimum:\t" + str(round(dailyminavg, 1)) + " (sigma=" + str(round(numpy.std(dailymin),1)) + ")"
	
	print ""
	
#Overall stats
for sensor in sensors:
	DataForSensor(sensor)

dbconn.close()
