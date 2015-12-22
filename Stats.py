#!/usr/bin/python
# -*- coding: utf-8 -*-
##############################################################################
# WeatherStats
# A tiny Python script for weather data management and analysis
# (C) 2015, Ulrich Thiel
# thiel@mathematik.uni-stuttgart.de
##############################################################################



##############################################################################
# imports
import sys
import sqlite3
import numpy
import datetime
import calendar
import time
from scipy.signal import argrelextrema
from sets import Set
from optparse import OptionParser
from itertools import chain
from progressbar import AnimatedMarker, Bar, BouncingBar, Counter, ETA, \
    FileTransferSpeed, FormatLabel, Percentage, \
    ProgressBar, ReverseBar, RotatingMarker, \
    SimpleProgress, Timer, AdaptiveETA, AdaptiveTransferSpeed

    
##############################################################################
#print colors
class bcolors:
    HEADER = '\033[35m'
    OKBLUE = '\033[34m'
    OKGREEN = '\033[32m'
    WARNING = '\033[33m'
    FAIL = '\033[31m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

##############################################################################
#parse options
parser = OptionParser()
parser.add_option("--sensors", dest="sensors",help="Sensors (e.g., --sensors=1-2,6-7)")
parser.add_option("--hours", dest="hours",help="Hours (e.g., --hours=7-9,21)")
parser.add_option("--days", dest="days",help="Days (e.g., --days=5-8,12)")
parser.add_option("--months", dest="months",help="Months (e.g., --months=5-8,12)")
parser.add_option("--years", dest="years",help="Years (e.g., --years=2012,2014-2015)")
#parser.add_option("--start", dest="start",help="Start date (e.g., --start=2014-05-24)")
#parser.add_option("--end", dest="end",help="End date (e.g., --end=2015-05-24)")
parser.add_option("--missing", action="store_true", dest="printmissing",help="Print missing data", default=False)
(options, args) = parser.parse_args()
sensors = options.sensors
hours = options.hours
days = options.days
months = options.months
years = options.years
printmissing = options.printmissing

	
##############################################################################
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
  
##############################################################################
#connect to db
dbconn = sqlite3.connect('Weather.db')
dbcursor = dbconn.cursor()
  
##############################################################################
#sensor ids
if sensors is None:
	sensors = []
	dbcursor.execute("SELECT ID From Sensors")
	res = dbcursor.fetchall()
	for sensor in res:
		sensors.append(int(sensor[0]))
else:
	sensors = parse_range_list(sensors)

##############################################################################
#parse time filter options
if hours is not None:
	hours = parse_range_list(hours)

if days is not None:
	days = parse_range_list(days)
		
if months is not None:
	months = parse_range_list(months)

if years is not None:
	years = parse_range_list(years)
	
##############################################################################
#some timestamp helper functions	
def DateFromTimestamp(t):
	#Returns the date in format Y-m-d from a timestamp
	return datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d')
	
#some timestamp helper functions	
def FullDateFromTimestamp(t):
	#Returns the day in format Y-m-d from a timestamp
	return datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M')
	
def DateAndHourFromTimestamp(t):
	#Returns the day in format Y-m-d from a timestamp
	return datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d %Hh')
	
def HourFromTimestamp(t):
	#Returns the month from a timestamp
	return int(datetime.datetime.fromtimestamp(t).strftime('%H'))
	
def DayFromTimestamp(t):
	#Returns the month from a timestamp
	return int(datetime.datetime.fromtimestamp(t).strftime('%d'))

def MonthFromTimestamp(t):
	#Returns the month from a timestamp
	return int(datetime.datetime.fromtimestamp(t).strftime('%m'))
	
def YearFromTimestamp(t):
	#Returns the month from a timestamp
	return int(datetime.datetime.fromtimestamp(t).strftime('%Y'))

def FirstTimestampOfDate(s):
	#Returns first timestamp of date s (s in format Y-m-d)
	return time.mktime(datetime.datetime.strptime(s, "%Y-%m-%d").timetuple())
	
def LastTimestampOfDate(s):
	#Returns last timestamp of date s (s in format Y-m-d)
	return time.mktime((datetime.datetime.strptime(s, "%Y-%m-%d") + datetime.timedelta(1)).timetuple() )
	
def FirstTimestampOfYear(s):
	#Returns first timestamp of year s
	return FirstTimestampOfDate(s+"-01-01")
	
def LastTimestampOfYear(s):
	#Returns last timestamp of year s
	return LastTimestampOfDate(s+"-12-31")
	
def FirstTimestampOfMonth(y,m):
	#Returns first timestamp of month m of year y
	return FirstTimestampOfDate(str(y)+"-"+str(m)+"-01")

def LastDayOfMonth(y,m):
	#Returns last date of month m of year y
	return calendar.monthrange(y,m)[1]
	
def LastTimestampOfMonth(y,m):
	#Returns first timestamp of month m of year y
	return LastTimestampOfDate(str(y)+"-"+str(m)+"-"+str(LastDayOfMonth(y,m)))
		
def TimestampOfFullDate(s):
	#Returns first timestamp of date s (s in format Y-m-d H:m)
	return time.mktime(datetime.datetime.strptime(s, "%Y-%m-%d %H:%M").timetuple())

##############################################################################
#compute data for sensor
def DataForSensor(sensor):

	#sensor info
	timezone = ((dbcursor.execute("SELECT Timezone From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]
	unit = ((dbcursor.execute("SELECT Unit From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]
	calibration = ((dbcursor.execute("SELECT Calibration From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]	
	description = timezone = ((dbcursor.execute("SELECT Description From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]	
	
	#print general info
	print "Sensor ID:\t" + str(sensor)
	print "Unit: \t\t" + unit
	print "Location: \t" + description
	print "Calibration: \t" + str(calibration)
	
	#get start and end year if not defined
	if years is None:
		mincoveredtimestamp = ((dbcursor.execute("SELECT MIN(Timestamp) FROM Data WHERE Sensor IS "+str(sensor))).fetchone())[0]
		minyear = YearFromTimestamp(mincoveredtimestamp)
		maxcoveredtimestamp = ((dbcursor.execute("SELECT MAX(Timestamp) FROM Data WHERE Sensor IS "+str(sensor))).fetchone())[0]
		maxyear = YearFromTimestamp(maxcoveredtimestamp)
		yearslocal = range(minyear,maxyear+1)
	else:
		minyear = min(years)
		maxyear = max(years)
		yearslocal = years
		
	if months is None:
		monthslocal = range(1,13)
	else:
		monthslocal = months
	
	if days is None:
		dayslocal = range(1,32)
	else:
		dayslocal = days
			
	if hours is None:
		hourslocal = range(0,24)
	else:
		hourslocal = hours
				
	missingslots = []
	dates = []	#will be the list of dates Y-m-d which lie in selection
	numdates = 0	#will be length of dates
	data = dict()	#will be a dictionary indexed by dates containing data for each day in selection
	numdatapoints = 0	#will be total number of data points in selection
	numslots = 0	#a slot is of the form Y-m-d H in selection
	missingslots = []	#slots where we do not have any data
	for y in yearslocal:
		data[y] = dict()
		for m in monthslocal:
			data[y][m] = dict()
			for d in [day for day in range(1, calendar.monthrange(y,m)[1]+1) if day in dayslocal]:
				numdates = numdates + 1
				data[y][m][d] = dict()	
				daydata = []	
				dates.append([y,m,d])
				for h in hourslocal:
					numslots = numslots + 1
					starttimestamp = TimestampOfFullDate(str(y)+"-"+str(m)+"-"+str(d)+" "+str(h)+":00")
					stoptimestamp = TimestampOfFullDate(str(y)+"-"+str(m)+"-"+str(d)+" "+str(h)+":59")+60.0
					res = (dbcursor.execute("SELECT Timestamp, Value FROM Data WHERE Sensor IS "+str(sensor)+' AND Timestamp>='+str(starttimestamp)+' AND Timestamp<'+str(stoptimestamp)+' ORDER BY Timestamp ASC').fetchall())
					daydata = daydata + res
					numdatapoints = numdatapoints + len(res)
					if len(res) == 0:
						missingslots.append([y,m,d,h])
				
				data[y][m][d] = numpy.array(daydata, dtype=[('timestamp', numpy.uint32),('value',numpy.float64)])	
					
	minmonth = min(monthslocal)
	if minmonth < 10:
		minmonthstr = "0"+str(minmonth)
	else:
		minmonthstr = str(minmonth)
	minday = min([day for day in range(1, calendar.monthrange(minyear,minmonth)[1]+1) if day in dayslocal])
	if minday < 10:
		mindaystr = "0"+str(minday)
	else:
		mindaystr = str(minday)
	maxmonth = max(monthslocal)
	if maxmonth < 10:
		maxmonthstr = "0"+str(maxmonth)
	else:
		maxmonthstr = str(maxmonth)
	maxday = max([day for day in range(1, calendar.monthrange(maxyear,maxmonth)[1]+1) if day in dayslocal])
	if maxday < 10:
		maxdaystr = "0"+str(maxday)
	else:
		maxdaystr = str(maxday)
	
	print "Range bounds: \t" + str(minyear)+"-"+minmonthstr+"-"+mindaystr+" to "+str(maxyear)+"-"+maxmonthstr+"-"+maxdaystr + " ("+ str(numdates)+" days in selection)"
	
	print "Data points: \t" + str(numdatapoints)
	
	#quality
	quality = 100.0*(float(numslots-len(missingslots))/float(numslots))

	if quality < 90:
		print "Data quality: \t" + bcolors.FAIL + str(round(quality,3)) + "%" + bcolors.ENDC + " (" + str(numslots-len(missingslots)) + "/" + str(numslots) + ")"
	else:
		print "Data quality: \t" + bcolors.OKGREEN + str(round(quality,3)) + "%" + bcolors.ENDC + " (" + str(numslots-len(missingslots)) + "/" + str(numslots) + ")"
		
	#analysis
	dailymax = numpy.array([ data[date[0]][date[1]][date[2]]['value'].max() for date in dates if len(data[date[0]][date[1]][date[2]]) > 0 ])
	dailymin = numpy.array([ data[date[0]][date[1]][date[2]]['value'].min() for date in dates if len(data[date[0]][date[1]][date[2]]) > 0 ])
	dailymean = numpy.array([ data[date[0]][date[1]][date[2]]['value'].mean() for date in dates if len(data[date[0]][date[1]][date[2]]) > 0 ])
	dailymaxmean = dailymax.mean()
	dailymaxstd = dailymax.std()
	dailyminmean = dailymin.mean()
	dailyminstd = dailymin.std()
	flatlist = numpy.concatenate([ data[date[0]][date[1]][date[2]] for date in dates if len(data[date[0]][date[1]][date[2]]) > 0 ])
	maxvalue = flatlist['value'].max()
	maxdatestmp = [DateAndHourFromTimestamp(flatlist['timestamp'][i]) for i in (numpy.argwhere(flatlist['value'] == maxvalue)).flatten().tolist()]
	maxdates = [d for n, d in enumerate(maxdatestmp) if d not in maxdatestmp[:n]]
	minvalue = flatlist['value'].min()
	mindatestmp = [DateAndHourFromTimestamp(flatlist['timestamp'][i]) for i in (numpy.argwhere(flatlist['value'] == minvalue)).flatten().tolist()]
	mindates = [d for n, d in enumerate(mindatestmp) if d not in mindatestmp[:n]]
	mean = flatlist['value'].mean()
	std = flatlist['value'].std()
		
	maxstr = "Maximum: \t" + str(maxvalue) + " ("
	for i in range(0,len(maxdates)):
		maxstr = maxstr + maxdates[i]
		if i < len(maxdates)-1:
			maxstr = maxstr + ", "
	maxstr = maxstr + ")"
	print maxstr
	
	minstr = "Minimum: \t" + str(minvalue) + " ("
	for i in range(0,len(mindates)):
		minstr = minstr + mindates[i]
		if i < len(mindates)-1:
			minstr = minstr + ", "
	minstr = minstr + ")"
	print minstr
	
	print "Average: \t" + str(round(mean,3))  + " (σ=" + str(round(std,3))+")"
	print "Daily maximum: \t" + str(round(dailymaxmean,3)) + " (σ=" + str(round(dailymaxstd,3))+")"
	print "Daily minimum: \t" + str(round(dailyminmean,3)) + " (σ=" + str(round(dailyminstd,3))+")"
		
	
#Overall stats
for sensor in sensors:
	DataForSensor(sensor)

dbconn.close()
