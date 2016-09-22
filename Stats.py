#!/usr/bin/python
# -*- coding: utf-8 -*-
##############################################################################
# WeatherStats
# Tiny Python scripts for general weather data management and analysis (with Netatmo support)
# (C) 2015-2016, Ulrich Thiel
# thiel@mathematik.uni-stuttgart.de
##############################################################################



##############################################################################
# imports
import sys
import sqlite3
import numpy
import time
import datetime
from scipy.signal import argrelextrema
from sets import Set
from optparse import OptionParser
from itertools import chain
import matplotlib.pyplot as plt
from lib import ColorPrint
from lib import DateHelper
from lib import Tools

##############################################################################
#parse options
parser = OptionParser()
parser.add_option("--sensors", dest="sensors",help="Sensors (e.g., --sensors=1-2,6-7)")
parser.add_option("--modules", dest="modules",help="Modules (e.g., --modules=1-2,6-7)")
parser.add_option("--hours", dest="hours",help="Hours (e.g., --hours=7-9,21)")
parser.add_option("--days", dest="days",help="Days (e.g., --days=5-8,12)")
parser.add_option("--months", dest="months",help="Months (e.g., --months=5-8,12)")
parser.add_option("--years", dest="years",help="Years (e.g., --years=2012,2014-2015)")
parser.add_option("--start", dest="start",help="Start date (e.g., --start=2014-05-24)")
parser.add_option("--end", dest="end",help="End date (e.g., --end=2015-05-24)")
parser.add_option("--missing", action="store_true", dest="printmissing",help="Print missing data", default=False)
(options, args) = parser.parse_args()
sensors = options.sensors
modules = options.modules
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
#get sensor ids from input (via sensors and modules option)
if modules == None and sensors == None:
	sensors = []
	modules = []
	dbcursor.execute("SELECT Id From Sensors")
	res = dbcursor.fetchall()
	for sensor in res:
		sensors.append(int(sensor[0]))
	
elif modules != None and sensors == None:
	modules = parse_range_list(modules)
	sensors = []
	
elif modules == None and sensors != None:
	modules = []
	sensors = parse_range_list(sensors)
	
for module in modules:
	dbcursor.execute("SELECT SensorIds From Modules WHERE Id IS "+str(module))
	res = dbcursor.fetchone()[0] #this is a comma separated list of sensors
	res = map(int, res.split(','))
	for sensor in res:
		sensors.append(sensor)	
	
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
def Analyze(sensor, datehours, verbose=None):
	
	measurand = ((dbcursor.execute("SELECT Measurand From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]
	unit = ((dbcursor.execute("SELECT Unit From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]
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
		pbar = ProgressBar(widgets=widgets, maxval=len(datehours), CR=True)
		pbar.start()
			
	#read data
	data = dict()
	availabledatehours = []	#tuples [y,m,d,h] so that at least one data point exists for this datehour
	availabledates = []
	count = 0	
	for e in datehours:
		y = e[0]
		m = e[1]
		d = e[2]
		h = e[3]
		starttimestamp = TimestampOfDatetime(str(y)+"-"+str(m)+"-"+str(d)+" "+str(h)+":00")
		stoptimestamp = TimestampOfDatetime(str(y)+"-"+str(m)+"-"+str(d)+" "+str(h)+":59")+60.0
		res = (dbcursor.execute("SELECT Timestamp, Value FROM Data WHERE Sensor IS "+str(sensor)+' AND Timestamp>='+str(starttimestamp)+' AND Timestamp<'+str(stoptimestamp)+' ORDER BY Timestamp ASC').fetchall())
		if len(res) > 0:
			if y not in data.keys():
				data[y] = dict()
			if m not in data[y].keys():
				data[y][m] = dict()
			if d not in data[y][m].keys():
				data[y][m][d] = dict()
			if [y,m,d,h] not in availabledatehours:
				availabledatehours.append([y,m,d,h])
			if [y,m,d] not in availabledates:
				availabledates.append([y,m,d])
			calibres = [ (r[0],r[1]+calibration) for r in res ]	
			data[y][m][d][h] = numpy.array(calibres, dtype=[('timestamp', numpy.uint32),('value',numpy.float64)])	
			
		count = count + 1		
		if verbose:
			pbar.update(count)
		
	if verbose:	
		pbar.finish()
		sys.stdout.write("\033[K") # 
		
	#total data
	totaldata = numpy.concatenate([data[e[0]][e[1]][e[2]][e[3]] for e in availabledatehours])
	
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
		
	#daily maximum
	dailymax = numpy.array([ (time.strftime("%Y-%m-%d", day + [0,0,0,0,0,0]), numpy.concatenate([ data[day[0]][day[1]][day[2]][h] for h in data[day[0]][day[1]][day[2]].keys() ])['value'].max()) for day in availabledates ], dtype=[('date', '|S10'),('value',numpy.float64)])	
	dailymaxaverage = dailymax['value'].mean()
	dailymaxsigma = dailymax['value'].std()
	if verbose:
		print "Daily maximum:\t" + str(round(dailymaxaverage,3)) + " (σ=" + str(round(dailymaxsigma,3))+")"	
	
	#daily minimum
	dailymin = numpy.array([ (time.strftime("%Y-%m-%d", day + [0,0,0,0,0,0]), numpy.concatenate([ data[day[0]][day[1]][day[2]][h] for h in data[day[0]][day[1]][day[2]].keys() ])['value'].min()) for day in availabledates ], dtype=[('date', '|S10'),('value',numpy.float64)])	
	dailyminaverage = dailymin['value'].mean()
	dailyminsigma = dailymin['value'].std()
	if verbose:
		print "Daily minimum:\t" + str(round(dailyminaverage,3)) + " (σ=" + str(round(dailyminsigma,3))+")"	
		
	#hour data
	hourdata = dict()
	for h in range(0,24):
		hdata = [ data[e[0]][e[1]][e[2]][e[3]] for e in availabledatehours if e[3] == h ]
		if len(hdata) == 0:
			continue
		else:
			hourdata[h] = numpy.concatenate(hdata)
	
	#hour average
	availablehours = hourdata.keys()
	houravg = [ hourdata[h]['value'].mean() for h in availablehours ]
	hourstd = [ hourdata[h]['value'].std() for h in availablehours ]
			
	#hour average plot
	#plt.errorbar(availablehours, houravg, hourstd, marker='^')
	#plt.xlim([min(availablehours)-1,max(availablehours)+1])
	#plt.xlabel('Hour')
	#plt.ylabel('Average ' + measurand + " (" + unit + ")")
	#plt.xticks(availablehours, availablehours)
	#plt.figure()
	
	#month data
	monthdata = dict()
	for m in range(1,13):
		mdata = [ data[e[0]][e[1]][e[2]][e[3]] for e in availabledatehours if e[1] == m ]
		if len(mdata) == 0:
			continue
		else:
			monthdata[m] = numpy.concatenate(mdata)
			
	#month average
	availablemonths = monthdata.keys()
	monthavg = [ monthdata[h]['value'].mean() for h in availablemonths ]
	monthstd = [ monthdata[h]['value'].std() for h in availablemonths ]
			
	#hour average plot
	#if len(availablemonths) > 1:
		#plt.errorbar(availablemonths, monthavg, monthstd, marker='^')
		#plt.xlim([min(availablemonths)-1,max(availablemonths)+1])
		#plt.xlabel('Month')
		#plt.ylabel('Average ' + measurand + " (" + unit + ")")
		#plt.xticks(availablemonths, availablemonths)
	
	#plt.show()
	
	
##############################################################################
#General info about sensor
def PrintGeneralSensorInfo(sensor):
	

	measurand = ((dbcursor.execute("SELECT Measurand From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]
	unit = ((dbcursor.execute("SELECT Unit From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]
	calibration = ((dbcursor.execute("SELECT Calibration From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]	
	description = ((dbcursor.execute("SELECT Description From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]	
	module = ((dbcursor.execute("SELECT Module From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]	
	pph = ((dbcursor.execute("SELECT pph FROM Sensors WHERE Id IS "+str(sensor))).fetchone())[0]
	
	print "Sensor: \t" + str(sensor)
	print "  Module: \t" + str(module)
	print "  Measurand: \t" + measurand + " ("+unit+")"
	print "  Calibration: \t" + str(calibration)
	print "  Resolution: \t" + str(pph) + " pph"
	
##############################################################################
#Creates an array of dates and an array of datehours relative to the selections by the user
def GetDateHours(years, months, days, hours, start, end):
	
	if start is not None:
		startdate = datetime.datetime.strptime(start, "%Y-%m-%d")
		startyear = startdate.year
		startmonth = startdate.month
		startday = startdate.day
	
	if end is not None:
		enddate = datetime.datetime.strptime(end, "%Y-%m-%d")
		endyear = enddate.year
		endmonth = enddate.month
		endday = enddate.day
		
	if start is None:
		if years is not None:
			startyear = min(years)
		else:
			if endyear is not None:
				startyear = endyear
			else:
				raise MyError('Cannot determine date range')
		if months is not None:
			startmonth = min(months)
		else:
			startmonth = 1
		
		if days is not None:
			startday = min(days)
		else:
			startday = 1
			
		startdate = datetime.datetime.strptime(str(startyear)+"-"+str(startmonth)+"-"+str(startday), "%Y-%m-%d")
			
	if end is None:
		if years is not None:
			endyear = max(years)
		else:
			if startyear is not None:
				endyear = startyear
			else:
				raise MyError('Cannot determine date range')
		if months is not None:
			endmonth = max(months)
		else:
			endmonth = 12
		if days is not None:
			endday = max(days)
		else:
			endday = DateHelper.LastDayOfMonth(endyear, endmonth)
			
		enddate = datetime.datetime.strptime(str(endyear)+"-"+str(endmonth)+"-"+str(endday), "%Y-%m-%d")
	
	if years is None:
		years = range(startyear, endyear+1)
	if months is None:
		months = range(1,13)
	if days is None:
		days = range(1,32)
	if hours is None:
		hours = range(0,24)
		
	datestmp = [ startdate + datetime.timedelta(days=d) for d in range( (enddate-startdate).days + 1) ]
	
	dates = [ (d.year,d.month,d.day) for d in datestmp if d.year in years and d.month in months and d.day in days]
		
	datehours = []
	for d in dates:
		for h in hours:
			datehours.append((d[0],d[1],d[2],h))
			
	return [dates, datehours]
	
	
##############################################################################
#Reads data
def ReadData(sensor,years, months, days, hours, start, end):
	
	#if no year range is given, we pick all available years for sensor
	if years is None:
		minyear = ((dbcursor.execute("SELECT MIN(Year) FROM Data WHERE Sensor IS "+str(sensor))).fetchone())[0]
		maxyear = ((dbcursor.execute("SELECT MAX(Year) FROM Data WHERE Sensor IS "+str(sensor))).fetchone())[0]
		years = range(minyear,maxyear+1)
		
	slots = GetDateHours(years, months, days, hours, start, end)
	dates = slots[0]
	datehours = slots[1]	
	print "  Selection: \t" + str(len(dates)) + " days/" + str(len(datehours)) + " hours"
	
	#I don't know why but the filtering of data by datehours is MUCH quicker in Python than in sql. So, we retrieve the data for all datemonths and do the further filtering in Python.	
	datemonths = []
	for date in dates:
		d = [date[0],date[1]]
		if not d in datemonths:
			datemonths.append(d)
	
	Tools.PrintWithoutNewline("  Reading data  ")	
	data = dict()
	N = float(len(datehours))
	progresscounter = 0
	numberofdatapoints = 0
	for datemonth in datemonths:
		dbcursor.execute("SELECT Timestamp,ValueCalibrated,Year,Month,Day,Hour,Minute,Second FROM DataWithCalibration WHERE Sensor IS "+str(sensor)+" AND Year IS "+str(datemonth[0])+" AND Month IS "+str(datemonth[1])+" ORDER BY Timestamp ASC")
		res = dbcursor.fetchall()
		datehoursfordatemonth = [ d for d in datehours if d[0] == datemonth[0] and d[1] == datemonth[1] ]
		if len(res) == 0:
			progresscounter = progresscounter + len(datehoursfordatemonth) 
			Tools.PrintWithoutNewline("  Reading data:\t"+str(int(100.0*float(progresscounter)/float(N)))+"%")
			continue
		for datehour in datehoursfordatemonth:
			filteredres = [ (r[0],r[1]) for r in res if r[2] == datehour[0] and r[3] == datehour[1] and r[4] == datehour[2] and r[5] == datehour[3] ]
			if len(filteredres) == 0:
				continue
			numberofdatapoints = numberofdatapoints + len(filteredres)
			data[datehour] = numpy.array(filteredres, dtype=[('timestamp', numpy.uint32),('value',numpy.float64)])	
			
			progresscounter = progresscounter + 1
			Tools.PrintWithoutNewline("  Reading data:\t"+str(int(100.0*float(progresscounter)/float(N)))+"%")
	
	Tools.PrintWithoutNewline("  Reading data:\t100%")
	Tools.PrintWithoutNewline("")
	
	#sensor quality
	pph = ((dbcursor.execute("SELECT pph FROM Sensors WHERE Id IS "+str(sensor))).fetchone())[0]
	nonsufficientdatehours = []
	for datehour in datehours:
		if not datehour in data.keys() or len(data[datehour]) < pph-1:
			nonsufficientdatehours.append(datehour)
			
	quality = 100.0-100.0*float(len(nonsufficientdatehours))/float(len(datehours))
	
	if quality > 90:
		color = "okgreen"
	else:
		color = "warning"
	
	ColorPrint.ColorPrint("  Data quality:\t"+str(int(quality))+"% ("+str(len(datehours)-len(nonsufficientdatehours))+"/"+str(len(datehours))+")", color)
		
	return [datehours,data,quality]


##############################################################################		
#Overall stats
for sensor in sensors:

	PrintGeneralSensorInfo(sensor)
		
	#Check if data available
	numpoints = ((dbcursor.execute("SELECT COUNT(Timestamp) FROM Data WHERE Sensor IS "+str(sensor))).fetchone())[0]

	if numpoints == 0:	
		ColorPrint.ColorPrint("  No data available", "error")
		print ""
		continue
	
	res = ReadData(sensor,years, months, days, hours, start, end)
	datehours = res[0]
	data = res[1]
	quality = res[2]
	
	sys.exit(0)
	
	tmp = GetSensorQuality(sensor, datehours,verbose=True)
	quality = round(tmp[0],3)
	missingdatehours = tmp[1]
	numdatapoints = tmp[2]
		
	Analyze(sensor, datehours,verbose=True)
	
	if len(sensors) > 1:
		print ""
	
dbconn.close()
