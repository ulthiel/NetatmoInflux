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
from scipy.signal import argrelextrema
from sets import Set
from optparse import OptionParser
from itertools import chain
from progressbar import AnimatedMarker, Bar, BouncingBar, Counter, ETA, \
    FileTransferSpeed, FormatLabel, Percentage, \
    ProgressBar, ReverseBar, RotatingMarker, \
    SimpleProgress, Timer, AdaptiveETA, AdaptiveTransferSpeed
from DateHelper import *

    
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
parser.add_option("--start", dest="start",help="Start date (e.g., --start=2014-05-24)")
parser.add_option("--end", dest="end",help="End date (e.g., --end=2015-05-24)")
parser.add_option("--missing", action="store_true", dest="printmissing",help="Print missing data", default=False)
(options, args) = parser.parse_args()
sensors = options.sensors
hours = options.hours
days = options.days
months = options.months
years = options.years
start = options.start
end = options.end
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
def GetSensorQuality(sensor, datehours, verbose=None):
	
	if verbose is None:	
		verbose = False
	
	if verbose:
		widgets = [
			'Data quality:  ', Percentage(),
			' ', Bar(),
			' ', ETA()
		]
		pbar = ProgressBar(widgets=widgets, maxval=len(datehours), CR=True)
		pbar.start()

	count = 0
	missingdatehours = []
	numdatapoints = 0
	for e in datehours:
		y = e[0]
		m = e[1]
		d = e[2]
		h = e[3]
		starttimestamp = TimestampOfDatetime(str(y)+"-"+str(m)+"-"+str(d)+" "+str(h)+":00")
		stoptimestamp = TimestampOfDatetime(str(y)+"-"+str(m)+"-"+str(d)+" "+str(h)+":59")+60.0
		res = (dbcursor.execute("SELECT COUNT(Timestamp) FROM Data WHERE Sensor IS "+str(sensor)+' AND Timestamp>='+str(starttimestamp)+' AND Timestamp<'+str(stoptimestamp)+' ORDER BY Timestamp ASC').fetchone())[0]
		if res is None or res == 0:
			missingdatehours.append(e)
		else:
			numdatapoints = numdatapoints + int(res)
		count = count + 1
		if verbose:
			pbar.update(count)
	
	if verbose:		
		pbar.finish()
		sys.stdout.write("\033[K") # 
		
	#quality
	quality = 100.0*(float(len(datehours)-len(missingdatehours))/float(len(datehours)))
	
	#verbose = kwargs.get('verbose', )
	
	if verbose is not None and verbose:	
		print "Data points: \t" + str(numdatapoints)
	
		if quality < 90:
			print "Data quality: \t" + bcolors.FAIL + str(round(quality,3)) + "%" + bcolors.ENDC + " (" + str(len(datehours)-len(missingdatehours)) + "/" + str(len(datehours)) + ")"
		else:
			print "Data quality: \t" + bcolors.OKGREEN + str(round(quality,3)) + "%" + bcolors.ENDC + " (" + str(len(datehours)-len(missingdatehours)) + "/" + str(len(datehours)) + ")"
	
	return [quality, missingdatehours, numdatapoints]
	
##############################################################################
def Analyze(sensor, datehours, verbose=None):
	
	calibration = ((dbcursor.execute("SELECT Calibration From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]	

	#read data	
	if verbose is None:	
		verbose = False
	
	if verbose:
		widgets = [
			'Reading data:  ', Percentage(),
			' ', Bar(),
			' ', ETA()
		]
		pbar = ProgressBar(widgets=widgets, maxval=2*len(datehours), CR=True)
		pbar.start()

	#create data dictionary
	data = dict()
	dates = []
	count = 0
	for e in datehours:
		y = e[0]
		if y not in data.keys():
			data[y] = dict()
		m = e[1]
		if m not in data[y].keys():
			data[y][m] = dict()
		d = e[2]
		if d not in data[y][m].keys():
			data[y][m][d] = dict()
		if [y,m,d] not in dates:
			dates.append([y,m,d])
		count = count + 1
		if verbose:
			pbar.update(count)
			
	#read data
	numdatapoints = 0
	missingdatehours = []		
	for e in datehours:
		y = e[0]
		m = e[1]
		d = e[2]
		h = e[3]
		starttimestamp = TimestampOfDatetime(str(y)+"-"+str(m)+"-"+str(d)+" "+str(h)+":00")
		stoptimestamp = TimestampOfDatetime(str(y)+"-"+str(m)+"-"+str(d)+" "+str(h)+":59")+60.0
		res = (dbcursor.execute("SELECT Timestamp, Value FROM Data WHERE Sensor IS "+str(sensor)+' AND Timestamp>='+str(starttimestamp)+' AND Timestamp<'+str(stoptimestamp)+' ORDER BY Timestamp ASC').fetchall())
		calibres = [ (r[0],r[1]+calibration) for r in res ]
		data[y][m][d][h] = numpy.array(calibres, dtype=[('timestamp', numpy.uint32),('value',numpy.float64)])	
		count = count + 1		
		if verbose:
			pbar.update(count)
		
	if verbose:	
		pbar.finish()
		sys.stdout.write("\033[K") # 
	
	#total data
	totaldata = numpy.concatenate([data[e[0]][e[1]][e[2]][e[3]] for e in datehours if len(data[e[0]][e[1]][e[2]][e[3]]) > 0])
	
	#total maximum
	totalmax = totaldata['value'].max()	
	totalmaxdatehours = [DateAndHourFromTimestamp(totaldata['timestamp'][i]) for i in (numpy.argwhere(totaldata['value'] == totalmax)).flatten().tolist()]
	totalmaxdatehours = [d for n, d in enumerate(totalmaxdatehours) if d not in totalmaxdatehours[:n]]
	
	if verbose:
		out = "Maximum: \t" + str(totalmax) + " ("
		for i in range(0,len(totalmaxdatehours)):
			out = out + totalmaxdatehours[i]
			if i < len(totalmaxdatehours)-1:
				out = out + ", "
		out = out + ")"
		print out
	
	#total minimum
	totalmin = totaldata['value'].min()
	totalmindatehours = [DateAndHourFromTimestamp(totaldata['timestamp'][i]) for i in (numpy.argwhere(totaldata['value'] == totalmin)).flatten().tolist()]
	totalmindatehours = [d for n, d in enumerate(totalmindatehours) if d not in totalmindatehours[:n]]
	
	if verbose:
		out = "Minimum: \t" + str(totalmin) + " ("
		for i in range(0,len(totalmindatehours)):
			out = out + totalmindatehours[i]
			if i < len(totalmindatehours)-1:
				out = out + ", "
		out = out + ")"
		print out
	
	#total average
	totalavg = totaldata['value'].mean()
	totalsigma = totaldata['value'].std()
	if verbose:
		print "Average:\t" + str(round(totalavg,3)) + " (σ=" + str(round(totalsigma,3))+")"
		
	#daily data
#	dailydata = dict()
#	for day in dates:
#		y = day[0]
#		m = day[1]
#		d = day[2]
#		if y not in dailydata.keys():
#			dailydata[y] = dict()
#		if m not in dailydata[y].keys():
#			dailydata[y][m] = dict()
#		dailydata[y][m][d] = numpy.concatenate([ data[y][m][d][h] for h in data[y][m][d].keys() if len(data[y][m][d][h]) > 0])
		
	
##############################################################################
def PrintGeneralSensorInfo(sensor):
	
	timezone = ((dbcursor.execute("SELECT Timezone From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]
	measurand = ((dbcursor.execute("SELECT Measurand From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]
	unit = ((dbcursor.execute("SELECT Unit From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]
	calibration = ((dbcursor.execute("SELECT Calibration From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]	
	description = timezone = ((dbcursor.execute("SELECT Description From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]	
	
	print "Sensor ID:\t" + str(sensor)
	print "Measurand: \t" + measurand + " ("+unit+")"
	print "Location: \t" + description
	print "Calibration: \t" + str(calibration)
	
##############################################################################		
#Overall stats
for sensor in sensors:

	PrintGeneralSensorInfo(sensor)
	
	#we first determine the datehour range from selected time restriction
	#if no year range is given, we pick all available years for sensor
	if years is None:
		mincoveredtimestamp = ((dbcursor.execute("SELECT MIN(Timestamp) FROM Data WHERE Sensor IS "+str(sensor))).fetchone())[0]
		minyear = YearFromTimestamp(mincoveredtimestamp)
		maxcoveredtimestamp = ((dbcursor.execute("SELECT MAX(Timestamp) FROM Data WHERE Sensor IS "+str(sensor))).fetchone())[0]
		maxyear = YearFromTimestamp(maxcoveredtimestamp)
		years = range(minyear,maxyear+1)
		
	tmp = GetDateHours(years, months, days, hours, start, end)
	dates = tmp[0]
	datehours = tmp[1]
	startdate = time.strftime("%Y-%m-%d", dates[0] + [0,0,0,0,0,0])
	enddate = time.strftime("%Y-%m-%d", dates[len(dates)-1] + [0,0,0,0,0,0])
	print "Coverage: \t" + startdate + " to " + enddate  + " ("+str(len(dates))+" days)"
	tmp = GetSensorQuality(sensor, datehours,verbose=True)
	quality = round(tmp[0],3)
	missingdatehours = tmp[1]
	numdatapoints = tmp[2]
		
	Analyze(sensor, datehours,verbose=True)
	
dbconn.close()
