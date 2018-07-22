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


##############################################################################
# imports
import sys
reload(sys)  
sys.setdefaultencoding('utf8')
import sqlite3
import numpy
import time
import datetime
import scipy.signal
from sets import Set
from optparse import OptionParser
from itertools import chain
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
from lib import ColorPrint
from lib import DateHelper
from lib import Tools
import FileDialog #to fix pyInstaller problem with matplotlib
import math
import signal
from lib import peakdetect
import csv

##############################################################################
#parse options
parser = OptionParser()
parser.add_option("--sensors", dest="sensors",help="Sensors (e.g., --sensors=1-2,6-7)")
parser.add_option("--modules", dest="modules",help="Modules (e.g., --modules=1-2,6-7)")
parser.add_option("--locations", dest="locations",help="Locations (e.g., --modules=1-2,6-7)")
parser.add_option("--hours", dest="hours",help="Hours (e.g., --hours=7-9,21)")
parser.add_option("--days", dest="days",help="Days (e.g., --days=5-8,12)")
parser.add_option("--months", dest="months",help="Months (e.g., --months=5-8,12)")
parser.add_option("--years", dest="years",help="Years (e.g., --years=2012,2014-2015)")
parser.add_option("--start", dest="start",help="Start date (e.g., --start=2014-05-24)")
parser.add_option("--end", dest="end",help="End date (e.g., --end=2015-05-24)")
parser.add_option("--lastweek", action="store_true", dest="lastweek",help="Data of the last seven days", default=False)
parser.add_option("--lastmonth", action="store_true", dest="lastmonth",help="Data of the last 31 days", default=False)
parser.add_option("--missing", action="store_true", dest="printmissing",help="Print missing data", default=False)
parser.add_option("--yearly", action="store_true", dest="yearlystats",help="Compute yearly data", default=False)
parser.add_option("--monthly", action="store_true", dest="monthlystats",help="Compute monthly data", default=False)
parser.add_option("--daily", action="store_true", dest="dailystats",help="Compute daily data", default=False)
parser.add_option("--plotavg", action="store_true", dest="plottingavg",help="Create average plot", default=False)
parser.add_option("--plot", action="store_true", dest="plotting",help="Create continuous plot", default=False)
parser.add_option("--report", action="store_true", dest="report",help="Create report", default=False)
(options, args) = parser.parse_args()
sensors = options.sensors
modules = options.modules
locations = options.locations
hours = options.hours
days = options.days
months = options.months
years = options.years
start = options.start
end = options.end
printmissing = options.printmissing
yearlystats = options.yearlystats
monthlystats = options.monthlystats
dailystats = options.dailystats
plottingavg = options.plottingavg
plotting = options.plotting
lastweek = options.lastweek
lastmonth = options.lastmonth
report = options.report
	
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
#locations
if locations == None:
	locations = []
else:
	locations = parse_range_list(locations)
	
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
#Lastweek option
if lastweek:
	res = DateHelper.SubtractDaysFromCurrentDate(6)
	years = None
	months = None
	days = None
	hours = None
	start = res[0]
	end = res[1]
	
##############################################################################
#Lastmonth option
if lastmonth:
	res = DateHelper.SubtractDaysFromCurrentDate(30)
	years = None
	months = None
	days = None
	hours = None
	start = res[0]
	end = res[1]
	
##############################################################################
def Analyze(sensor, datehours, data):
		
	Tools.PrintWithoutNewline("    Computing...           ")
	
	#sensor quality
	measurand = ((dbcursor.execute("SELECT Measurand From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]
	unit = ((dbcursor.execute("SELECT Unit From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]
	calibration = ((dbcursor.execute("SELECT Calibration From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]	
	description = ((dbcursor.execute("SELECT Description From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]	
	module = ((dbcursor.execute("SELECT Module From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]	
	pph = ((dbcursor.execute("SELECT pph FROM Sensors WHERE Id IS "+str(sensor))).fetchone())[0]
	nonsufficientdatehours = []
	availablepoints = 0
	theorypoints = 0
	for datehour in datehours:
		theorypoints = theorypoints + pph
		if not datehour in data.keys() or len(data[datehour]) < pph-1:
			nonsufficientdatehours.append(datehour)
		else:
			availablepoints = availablepoints + len(data[datehour])
			
	quality = 100.0-100.0*float(len(nonsufficientdatehours))/float(len(datehours))
	
	if quality > 90:
		color = "okgreen"
	else:
		color = "warning"
	
	Tools.PrintWithoutNewline("                          ")
	Tools.PrintWithoutNewline("")
	
	ColorPrint.ColorPrint("    Quality:  \t"+str(int(quality))+"% ("+str(len(datehours)-len(nonsufficientdatehours))+"/"+str(len(datehours))+", "+str(int(availablepoints))+"/"+str(int(theorypoints))+")", color)
	
	#total data
	totaldata = numpy.concatenate([data[d] for d in datehours if d in data.keys()])
	
	if len(totaldata) == 0:
		res = dict()
		return res
	
	
		
	#total average
	totalavg = totaldata['value'].mean()
	totalsigma = totaldata['value'].std()
	print "    Average:\t" + str(round(totalavg,3)) + " (sigma=" + str(round(totalsigma,3))+")"
	
	#daily data
	dates = list(set([ (d[0],d[1],d[2]) for d in data.keys() ]))
	hoursfordate = dict()
	for date in dates:
		hoursfordate = sorted([ d[3] for d in data.keys() if d[0] == date[0] and d[1] == date[1] and d[2] == date[2] ])
		
	dailydata = dict()
	for date in dates:
		dailydata[date] = numpy.concatenate([data[d] for d in data.keys() if d[0] == date[0] and d[1] == date[1] and d[2] == date[2]])
		
	dailymax = numpy.array([ (date, dailydata[date]['value'].max()) for date in dates], dtype=[('date', (numpy.int32,(1,3))), ('value',numpy.float64)])
	dailymaxavg = dailymax['value'].mean()
	dailymaxsigma = dailymax['value'].std()
	print "    Daily max:\t" + str(round(dailymaxavg,3)) + " (sigma=" + str(round(dailymaxsigma,3))+")"	
	
	dailymin = numpy.array([ (date, dailydata[date]['value'].min()) for date in dates], dtype=[('date', (numpy.int32,(1,3))), ('value',numpy.float64)])
	dailyminavg = dailymin['value'].mean()
	dailyminsigma = dailymin['value'].std()
	print "    Daily min:\t" + str(round(dailyminavg,3)) + " (sigma=" + str(round(dailyminsigma,3))+")"	
	
	#total maximum
	totalmax = totaldata['value'].max()	
	totalmaxtimestamps = sorted([totaldata['timestamp'][i] for i in (numpy.argwhere(totaldata['value'] == totalmax)).flatten().tolist()])
	
	totalmaxdatetimes = []
	totalmaxdatehours = []
	for t in totalmaxtimestamps:
		dbcursor.execute("Select Year,Month,Day,Hour,Minute,Second FROM Data"+str(sensor)+" WHERE Timestamp IS "+str(t))
		res = dbcursor.fetchone()
		s = str(res[0])+"-"+str(res[1]).zfill(2)+"-"+str(res[2]).zfill(2)+" "+str(res[3]).zfill(2)+":"+str(res[4]).zfill(2)+":"+str(res[5]).zfill(2)
		totalmaxdatetimes.append(s)
		sh = DateHelper.DateHourFromDatetime(s)
		if not sh in totalmaxdatehours:
			totalmaxdatehours.append(sh)
	
	out = "    Maximum: \t" + str(totalmax) + " ("
	for i in range(0,len(totalmaxdatehours)):
		out = out + totalmaxdatehours[i]
		if i < len(totalmaxdatehours)-1:
			out = out + ", "
	out = out + ")"
	print out
	
	#total minimum
	totalmin = totaldata['value'].min()	
	totalmintimestamps = sorted([totaldata['timestamp'][i] for i in (numpy.argwhere(totaldata['value'] == totalmin)).flatten().tolist()])
	
	totalmindatetimes = []
	totalmindatehours = []
	for t in totalmintimestamps:
		dbcursor.execute("Select Year,Month,Day,Hour,Minute,Second FROM Data"+str(sensor)+" WHERE Timestamp IS "+str(t))
		res = dbcursor.fetchone()
		s = str(res[0])+"-"+str(res[1]).zfill(2)+"-"+str(res[2]).zfill(2)+" "+str(res[3]).zfill(2)+":"+str(res[4]).zfill(2)+":"+str(res[5]).zfill(2)
		totalmindatetimes.append(s)
		sh = DateHelper.DateHourFromDatetime(s)
		if not sh in totalmindatehours:
			totalmindatehours.append(sh)
	
	out = "    Minimum: \t" + str(totalmin) + " ("
	for i in range(0,len(totalmindatehours)):
		out = out + totalmindatehours[i]
		if i < len(totalmindatehours)-1:
			out = out + ", "
	out = out + ")"
	print out
	
	#Extrema
	peaks = peakdetect.peakdetect(totaldata['value'], lookahead=pph)
	localmaxindices = [ p[0] for p in peaks[0] ]
	localminindices = [ p[0] for p in peaks[1] ]
	peakindices = sorted(localmaxindices + localminindices)
	largestclimb = 0
	largestclimbindex = 0
	largestdrop = 0
	largestdropdindex = 0
	for i in range(0,len(peakindices)-1):
		delta = abs(totaldata['value'][peakindices[i]] - totaldata['value'][peakindices[i+1]])
		if peakindices[i] in localmaxindices:
			if delta > largestdrop:
				largestdrop = delta
				largestdropdindex = peakindices[i]
		else:
			if delta > largestclimb:
				largestclimb = delta
				largestclimbindex = peakindices[i]
				
	largestdropdate = str(totaldata[largestdropdindex]['year'])+"-"+str(totaldata[largestdropdindex]['month']).zfill(2)+"-"+str(totaldata[largestdropdindex]['day']).zfill(2)+" "+str(totaldata[largestdropdindex]['hour']).zfill(2)
	
	largestclimbdate = str(totaldata[largestclimbindex]['year'])+"-"+str(totaldata[largestclimbindex]['month']).zfill(2)+"-"+str(totaldata[largestclimbindex]['day']).zfill(2)+" "+str(totaldata[largestclimbindex]['hour']).zfill(2)
	
	print "    L. drop:\t" + str(largestdrop) + " ("+largestdropdate+"h)"
	print "    L. climb:\t" + str(largestclimb) + " ("+largestclimbdate+"h)"
	
	res = dict()
	res["totalavg"] = totalavg
	res["totalsigma"] = totalsigma
	res["totalmax"] = totalmax
	res["totalmin"] = totalmin
	res["dailyminavg"] = dailyminavg
	res["dailymaxavg"] = dailymaxavg

	if plotting or report:
		totaldatetuples = []
		for d in datehours:
			if not d in data.keys():
				continue 
			totaldatetuples = totaldatetuples + [(x[2], x[3], x[4], x[5], x[6], x[7]) for x in data[d]]
		
		if len(set([(x[0]) for x in totaldatetuples])) == 1:
			if len(set([(x[1]) for x in totaldatetuples])) == 1:
				if len(set([(x[2]) for x in totaldatetuples])) == 1:
					datelabels = [str(x[3]).zfill(2)+":"+str(x[4]).zfill(2)+":"+str(x[5]).zfill(2) for x in totaldatetuples]
				else:
					datelabels = [str(x[2]).zfill(2) for x in totaldatetuples]
			else:
				datelabels = [str(x[1]).zfill(2)+"-"+str(x[2]).zfill(2) for x in totaldatetuples]
		else:
			datelabels = [str(x[0])+"-"+str(x[1]).zfill(2)+"-"+str(x[2]).zfill(2) for x in totaldatetuples]
	
	if plotting:
 		totaldataplot, = plt.plot(totaldata['value'])
 		#localmaxplot, = plt.plot([ p[0] for p in peaks[0] ], [ p[1] for p in peaks[0] ], 'rx')
 		#localminplot, = plt.plot([ p[0] for p in peaks[1] ], [ p[1] for p in peaks[1] ], 'rx')
 		
 		#x axis labels
 		step = int(len(totaldata)/20)
 		xticks = range(0,len(totaldata['value']))[0::step]
 		plt.xticks(xticks, [datelabels[i] for i in xticks], rotation=70)
 		plt.xlabel("Date")
 		
 		plt.ylabel(measurand + " ("+unit+")")
  			
 		print ""
 		title = raw_input("    Plot title:\t")
 		if title == "":
 			title = "Statistics for sensor " + str(sensor)
  		plt.title(title)
 			
 		plt.show()
	
	if report:
		fieldnames = ['Number', 'Date', measurand]
		reportcsvfile = open('Reports/'+str(sensor)+'.csv', 'w')
		writer = csv.DictWriter(reportcsvfile, fieldnames=fieldnames, delimiter=',')
		writer.writeheader()
		for i in range(0,len(datelabels)):
			writer.writerow({'Number':str(i), 'Date':datelabels[i], measurand:totaldata['value'][i] })
			
		reportfile = open('Reports/'+str(sensor)+'.tex', 'w')
		reportfile.write(\
"""\\documentclass{thielstyle}

\\usepackage{tikz}
\\usepackage{pgfplots}
\\pgfplotsset{compat=newest} % Allows to place the legend below plot
\\usepgfplotslibrary{units,smithchart,polar} % Allows to enter the units nicely

\\usepackage{siunitx}
\\sisetup{
  round-mode          = places,
  round-precision     = 2,
}

\\title{Statistics for sensor """ + str(sensor) + """}
\\author{Ulrich Thiel}

\\begin{document}

""")

		reportfile.write(\
"""	
\\section{Sensor information}
Sensor ID: """+str(sensor)+"""\\\\
Module ID: """+str(module)+"""\\\\
Measurand: """+str(measurand)+""" ("""+str(unit)+""")\\\
Calibration: """+str(calibration)+"""\\\\
Resolution: """+str(pph)+"""\\\\

\\section{Statistics summary}
Selection: """ + str(len(dates)) + """ days/""" + str(len(datehours)) + """ hours between """ + str(start)[0:10] + """ and """ + str(end)[0:10] + """\\\\
Quality: """ + str(int(quality))+"""\\% ("""+str(len(datehours)-len(nonsufficientdatehours))+"""/"""+str(len(datehours))+""", """+str(int(availablepoints))+"""/"""+str(int(theorypoints))+""")\\\\
Maximum: """+str(totalmax) + """\\\\
Minimum: """+str(totalmin) + """\\\\
Average: """ + str(round(totalavg,3)) + """ ($\\sigma=""" + str(round(totalsigma,3))+"""$)\\\\
Daily max: """ + str(round(dailymaxavg,3)) + """ ($\\sigma=""" + str(round(dailymaxsigma,3))+"""$)\\\\
Daily min: """ + str(round(dailyminavg,3)) + """ ($\\sigma=""" + str(round(dailyminsigma,3))+"""$)\\\\	
Largest drop: """ + str(largestdrop) + """\\\\
Largest climb: """ + str(largestclimb).encode('utf-8', 'ignore').decode('utf-8') + """\\\\
""")	

		reportfile.write(\
"""
\\section{Plot}
\\begin{figure}[h!]
  \\begin{center}
    \\begin{tikzpicture}[every mark/.append style={mark size=0.5pt}]
      \\begin{axis}[
          width=\\linewidth, % Scale the plot to \linewidth
          grid=major, % Display a grid
          grid style={dashed,gray!30}, % Set the style
          xlabel=Date, % Set the labels
          ylabel="""+measurand+""",
          %x unit=\\si{\\mega\\hertz}, % Set the respective units
          %y unit=\\si{\\ohm},
          %legend style={at={(0.5,-0.2)},anchor=north}, % Put the legend below the plot
          x tick label style={rotate=90,anchor=east} % Display labels sideways
        ]
        \\addplot 
        % add a plot from table; you select the columns by using the actual name in
        % the .csv file (on top)
        table[x=Number,y="""+measurand+""",col sep=comma] {"""+str(sensor)+""".csv}; 
        %\\legend{Plot}
      \\end{axis}
    \\end{tikzpicture}
  \\end{center}
\\end{figure}
""")


		reportfile.write("\\end{document}")
	
	return res
	
##############################################################################
#Reads data
def ReadData(sensor,years,months, days, hours, userstart,userend):
	
	#we first create an array of datehours reflecting the user selection. this is actually a bit tricky somehow.
	
	#minimum date available for sensor
	minyear = ((dbcursor.execute("SELECT MIN(Year) FROM Data"+str(sensor))).fetchone())[0]
	minmonth = ((dbcursor.execute("SELECT MIN(Month) FROM Data"+str(sensor)+" WHERE YEAR IS "+str(minyear))).fetchone())[0]
	minday = ((dbcursor.execute("SELECT MIN(Day) FROM Data"+str(sensor)+" WHERE Year IS "+str(minyear)+" AND Month IS "+str(minmonth))).fetchone())[0]
	mindate = str(minyear)+"-"+str(minmonth).zfill(2)+"-"+str(minday).zfill(2)
	mindate = datetime.datetime.strptime(mindate, "%Y-%m-%d")
	
	#user selected start date
	if userstart != None:
		userstartyear = int(userstart[0:4])
		userstartmonth = int(userstart[5:7])
		userstartday = int(userstart[8:10])
		userstart = datetime.datetime.strptime(userstart, "%Y-%m-%d")
	
	#now, get actual start date relative to selection
	#there will certainly still be some errors here
	#1
	if userstart == None and years == None and months == None and days == None:
		start = mindate
	#2	
	elif userstart != None and years == None and months == None and days == None:
		start = userstart
	#3
	elif userstart == None and years != None and months == None and days == None:
		start = str(min(years))+"-01-01"
		start = datetime.datetime.strptime(start, "%Y-%m-%d")
	#4
	elif userstart == None and years == None and months != None and days == None:
		startyear = str(minyear)
		startmonth = str(min(months)).zfill(2)
		startday = "01"
		start = datetime.datetime.strptime(startyear+"-"+startmonth+"-"+startday, "%Y-%m-%d")
	#5
	elif userstart == None and years == None and months == None and days != None:
		startyear = str(minyear)
		startmonth = str(minmonth)
		startday = str(min(days)).zfill(2)
		start = datetime.datetime.strptime(startyear+"-"+startmonth+"-"+startday, "%Y-%m-%d")	
	#6
	elif userstart != None and years != None and months == None and days == None:
		t = [ y for y in years if y >= userstartyear ]
		if len(t) == 0:
			return []	#empty selection
		else:
			start = str(min(t))+"-01-01"
			start = datetime.datetime.strptime(start, "%Y-%m-%d")
	#7
	elif userstart == None and years != None and months != None and days == None:	 	
		startyear = str(min(years))
		startmonth = str(min(months)).zfill(2)
		startday = "01"
		start = datetime.datetime.strptime(startyear+"-"+startmonth+"-"+startday, "%Y-%m-%d")
	#8
	elif userstart == None and years == None and months != None and days != None:
		startyear = str(minyear)
		startmonth = str(min(months)).zfill(2)
		startday = str(min(days)).zfill(2)
		start = datetime.datetime.strptime(startyear+"-"+startmonth+"-"+startday, "%Y-%m-%d")
	#9	
	elif userstart != None and years == None and months != None and days == None:	 	
		startyear = str(userstartyear)
		s = [ m for m in months if m >= userstartmonth ]
		if len(s) > 0:
			startmonth = str(min(s)).zfill(2)
			if min(s) == userstartmonth:
				startday = str(userstartday).zfill(2)
			else:
				startday = "01"
		else:
			startyear = str(userstartyear+1)
			startmonth = str(min(months)).zfill(2)
			startday = "01"
			
		start = startyear+"-"+startmonth+"-"+startday
		start = datetime.datetime.strptime(start, "%Y-%m-%d")
	#10
	elif userstart != None and years == None and months == None and days != None:		
		startyear = str(userstartyear)
		startmonth = str(userstartmonth)
		startday = str(userstartday)
		start = datetime.datetime.strptime(startyear+"-"+startmonth+"-"+startday, "%Y-%m-%d")
	#11
	elif userstart == None and years != None and months == None and days != None:	
		startyear = min(years)
		startmonth = "01"
		startday = min(days)
		start = str(startyear)+"-"+str(startmonth).zfill(2)+"-"+str(startday).zfill(2)
		start = datetime.datetime.strptime(start, "%Y-%m-%d")
	#12
	elif userstart != None and years != None and months != None and days == None:
		t = [ y for y in years if y >= userstartyear ]
		if len(t) == 0:
			return []	#empty selection
		else:
			if min(t) == userstartyear:
				s = [ m for m in months if m >= userstartmonth ]
				if len(s) > 0:
					startyear = str(min(t))
					startmonth = str(min(s)).zfill(2)
				else:
					t.remove(min(t))	#next year
					startyear = str(min(t))
					startmonth = str(min(months)).zfill(2) 
			else:
				startyear = str(min(t))
				startmonth = str(min(s)).zfill(2)
			
			startday = "01"
			start = datetime.datetime.strptime(startyear+"-"+startmonth+"-"+startday, "%Y-%m-%d")
	#13
	elif userstart != None and years == None and months != None and days != None:
		s = [ m for m in months if m >= userstartmonth ]
		if len(s) > 0:
			startyear = str(userstartyear)
			startmonth = str(min(s)).zfill(2)
		else:
			startyear = str(userstartyear+1)
			startmonth = str(min(months)).zfill(2)
		startday = str(min(days)).zfill(2)
		if min(days) > DateHelper.LastDayOfMonth(int(startyear),int(startmonth)):
			return []
		start = datetime.datetime.strptime(startyear+"-"+startmonth+"-"+startday, "%Y-%m-%d")
	#14
	elif userstart != None and years != None and months == None and days != None:
		t = [ y for y in years if y >= userstartyear ]
		if len(t) == 0:
			return []	#empty selection
		else:
			startyear = str(min(t))
			startmonth = "01"
			startday = str(min(days)).zfill(2)
			start = datetime.datetime.strptime(startyear+"-"+startmonth+"-"+startday, "%Y-%m-%d")
	#15
	elif userstart == None and years != None and months != None and days != None:
		startyear = str(min(years))
		startmonth = str(min(months)).zfill(2)
		startday = str(min(days)).zfill(2)
		start = datetime.datetime.strptime(startyear+"-"+startmonth+"-"+startday, "%Y-%m-%d")
	#16
	elif userstart != None and years != None and months != None and days != None:
		t = [ y for y in years if y >= userstartyear ]
		if len(t) == 0:
			return []	#empty selection
		else:
			if min(t) == userstartyear:
				s = [ m for m in months if m >= userstartmonth ]
				if len(s) > 0:
					startyear = str(min(t))
					startmonth = str(min(s)).zfill(2)
				else:
					t.remove(min(t))	#next year
					startyear = str(min(t))
					startmonth = str(min(months)).zfill(2) 
			else:
				startyear = str(min(t))
				startmonth = str(min(s)).zfill(2)
			
			startday = str(min(days)).zfill(2)
			start = datetime.datetime.strptime(startyear+"-"+startmonth+"-"+startday, "%Y-%m-%d")
			
	#maximum date available for sensor
	maxyear = ((dbcursor.execute("SELECT MAX(Year) FROM Data"+str(sensor))).fetchone())[0]
	maxmonth = ((dbcursor.execute("SELECT MAX(Month) FROM Data"+str(sensor)+" WHERE YEAR IS "+str(maxyear))).fetchone())[0]
	maxday = ((dbcursor.execute("SELECT MAX(Day) FROM Data"+str(sensor)+" WHERE Year IS "+str(maxyear)+" AND Month IS "+str(maxmonth))).fetchone())[0]
	maxdate = str(maxyear)+"-"+str(maxmonth).zfill(2)+"-"+str(maxday).zfill(2)
	maxdate = datetime.datetime.strptime(maxdate, "%Y-%m-%d")
	
	#user selected end date
	if userend != None:
		userendyear = int(userend[0:4])
		userendmonth = int(userend[5:7])
		userendday = int(userend[8:10])
		userend = datetime.datetime.strptime(userend, "%Y-%m-%d")
	
	#now, get actual start date relative to selection
	#there will certainly still be some errors here
	#1
	if userend == None and years == None and months == None and days == None:
		end = maxdate
	#2	
	elif userend != None and years == None and months == None and days == None:
		end = userend
	#3
	elif userend == None and years != None and months == None and days == None:
		end = str(max(years))+"-12-31"
		end = datetime.datetime.strptime(end, "%Y-%m-%d")
	#4
	elif userend == None and years == None and months != None and days == None:
		endyear = maxyear
		endmonth = max(months)
		endday = DateHelper.LastDayOfMonth(endyear, endmonth)
		end = str(endyear)+"-"+str(endmonth).zfill(2)+"-"+str(endday).zfill(2)
		end = datetime.datetime.strptime(end, "%Y-%m-%d")
	#5
	elif userend == None and years == None and months == None and days != None:
		endyear = str(maxyear)
		endmonth = str(maxmonth).zfill(2)
		endday = str(max(days)).zfill(2)
		if max(days) > DateHelper.LastDayOfMonth(maxyear, maxmonth):
			endmonth = str(maxmonth+1).zfill(2) #is <= 12 since December has 31 days
			endday = "01"
		end = datetime.datetime.strptime(endyear+"-"+endmonth+"-"+startday, "%Y-%m-%d")	
	#6
	elif userend != None and years != None and months == None and days == None:
		t = [ y for y in years if y <= userendyear ]
		if len(t) == 0:
			return []	#empty selection
		else:
			end = str(max(t))+"-12-31"
			end = datetime.datetime.strptime(end, "%Y-%m-%d")
	#7
	elif userend == None and years != None and months != None and days == None:	 	
		endyear = max(years)
		endmonth = max(months)
		endday = DateHelper.LastDayOfMonth(endyear, endmonth)
		end = str(endyear)+"-"+str(endmonth).zfill(2)+"-"+str(endday).zfill(2)
		end = datetime.datetime.strptime(end, "%Y-%m-%d")
	#8
	elif userend == None and years == None and months != None and days != None:
		endyear = str(maxyear)
		endmonth = str(max(months)).zfill(2)
		endday = str(max(days)).zfill(2)
		if max(days) > DateHelper.LastDayOfMonth(maxyear, max(months)):
			endmonth = str(max(months)+1).zfill(2)
			endday = "01"
		end = datetime.datetime.strptime(endyear+"-"+endmonth+"-"+endday, "%Y-%m-%d")
	#9	
	elif userend != None and years == None and months != None and days == None:	 	
		endyear = userendyear
		endmonth = max([userendmonth, max(months)])
		endday = DateHelper.LastDayOfMonth(endyear, endmonth)
				
		end = str(endyear)+"-"+str(endmonth).zfill(2)+"-"+str(endday).zfill(2)
		end = datetime.datetime.strptime(end, "%Y-%m-%d")
	#10
	elif userend != None and years == None and months == None and days != None:		
		end = userend
		
	#11
	elif userend == None and years != None and months == None and days != None:	
		endyear = max(years)
		endmonth = "12"
		endday = max(days)
		end = str(endyear)+"-"+str(endmonth).zfill(2)+"-"+str(endday).zfill(2)
		end = datetime.datetime.strptime(end, "%Y-%m-%d")
	#12
	elif userend != None and years != None and months != None and days == None:
		endyear = max([userendyear, max(years)])
		endmonth = max([userendmonth, max(months)])
		endday = DateHelper.LastDayOfMonth(endyear, endmonth)
		end = str(endyear)+"-"+str(endmonth).zfill(2)+"-"+str(endday).zfill(2)
		end = datetime.datetime.strptime(end, "%Y-%m-%d")
		
	#13
	elif userend != None and years == None and months != None and days != None:
		endyear = userendyear
		endmonth = max([userendmonth, max(months)])
		endday = max(days)
		end = str(endyear)+"-"+str(endmonth).zfill(2)+"-"+str(endday).zfill(2)
		end = datetime.datetime.strptime(end, "%Y-%m-%d")
	#14
	elif userend != None and years != None and months == None and days != None:
		endyear = max(years)
		endmonth = userendmonth
		endday = max(days)
		end = str(endyear)+"-"+str(endmonth).zfill(2)+"-"+str(endday).zfill(2)
		end = datetime.datetime.strptime(end, "%Y-%m-%d")
		
	#15
	elif userend == None and years != None and months != None and days != None:
		endyear = max(years)
		endmonth = max(months)
		endday = max(days)
		if endday > DateHelper.LastDayOfMonth(endyear, endmonth):
			if endmonth < 12:
				endmonth = endmonth + 1
				endday = "01"
			else:
				endyear = endyear + 1
				endmonth = "01"
				endday = "01"
		end = str(endyear)+"-"+str(endmonth).zfill(2)+"-"+str(endday).zfill(2)
		end = datetime.datetime.strptime(end, "%Y-%m-%d")
	#16
	elif userend != None and years != None and months != None and days != None:
		endyear = max([userendyear, max(years)])
		endmonth = max([userendmonth, max(months)])
		endday = max([userendday, max(days)])
		if endday > DateHelper.LastDayOfMonth(endyear, endmonth):
			if endmonth < 12:
				endmonth = endmonth + 1
				endday = "01"
			else:
				endyear = endyear + 1
				endmonth = "01"
				endday = "01"
		end = str(endyear)+"-"+str(endmonth).zfill(2)+"-"+str(endday).zfill(2)
		end = datetime.datetime.strptime(end, "%Y-%m-%d")	

	if years is None:
		years = range(minyear, maxyear+1)
	if months is None:
		months = range(1,13)
	if days is None:
		days = range(1,32)
	if hours is None:
		hours = range(0,24)
				
	datestmp = [ start + datetime.timedelta(days=d) for d in range( (end-start).days + 1) ]
	
	dates = [ (d.year,d.month,d.day) for d in datestmp if d.year in years and d.month in months and d.day in days]
	
	
	datehours = []
	for d in dates:
		for h in hours:
			datehours.append((d[0],d[1],d[2],h))
	
	print " \n  Overall statistics:"
	print "    Selection: \t" + str(len(dates)) + " days/" + str(len(datehours)) + " hours between " + str(start)[0:10] + " and " + str(end)[0:10]
	
	
	
	#I don't know why but the filtering of data by datehours is MUCH quicker in Python than in sql. So, we retrieve the data for all datemonths and do the further filtering in Python.		
	datemonths = []
	for date in dates:
		d = [date[0],date[1]]
		if not d in datemonths:
			datemonths.append(d)
	
	Tools.PrintWithoutNewline("    Reading:")	
	data = dict()
	N = float(len(datehours))
	progresscounter = 0
	numberofdatapoints = 0
	for datemonth in datemonths:
		sql = "SELECT Timestamp,ValueCalibrated,Year,Month,Day,Hour,Minute,Second FROM Data"+str(sensor)+"Full WHERE Year IS "+str(datemonth[0])+" AND Month IS "+str(datemonth[1])
		if locations != []:
			sql = sql + " AND Location IN " + str(locations).replace('[', '(').replace(']', ')')
		sql = sql +" ORDER BY Timestamp ASC"
		dbcursor.execute(sql)
		res = dbcursor.fetchall()
		datehoursfordatemonth = [ d for d in datehours if d[0] == datemonth[0] and d[1] == datemonth[1] ]
		if len(res) == 0:
			progresscounter = progresscounter + len(datehoursfordatemonth) 
			Tools.PrintWithoutNewline("    Reading:\t"+str(int(100.0*float(progresscounter)/float(N)))+"%")
			continue
		for datehour in datehoursfordatemonth:
			filteredres = [ (r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7]) for r in res if r[2] == datehour[0] and r[3] == datehour[1] and r[4] == datehour[2] and r[5] == datehour[3] ]
			if len(filteredres) == 0:
				continue
			numberofdatapoints = numberofdatapoints + len(filteredres)
			data[datehour] = numpy.array(filteredres, dtype=[('timestamp', numpy.uint32),('value',numpy.float64),('year', numpy.uint16),('month', numpy.uint16),('day', numpy.uint16),('hour', numpy.uint16),('minute', numpy.uint16),('second', numpy.uint16)])	
			
			progresscounter = progresscounter + 1
			Tools.PrintWithoutNewline("    Reading:\t"+str(int(100.0*float(progresscounter)/float(N)))+"%")
	
	Tools.PrintWithoutNewline("    Reading:\t100%")
	Tools.PrintWithoutNewline("")
		
	return [datehours,data]


##############################################################################
#Ctrl+C handler
def signal_handler(signal, frame):
    ColorPrint.ColorPrint("\nYou pressed Ctrl+C", "error")
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)  
        
##############################################################################		
#Overall stats
for sensor in sensors:

	try:
		measurand = ((dbcursor.execute("SELECT Measurand From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]
	except:
		ColorPrint.ColorPrint("No such sensor", "error")
		sys.exit(0)
		
	unit = ((dbcursor.execute("SELECT Unit From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]
	calibration = ((dbcursor.execute("SELECT Calibration From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]	
	description = ((dbcursor.execute("SELECT Description From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]	
	module = ((dbcursor.execute("SELECT Module From Sensors WHERE ID IS " + str(sensor))).fetchone())[0]	
	pph = ((dbcursor.execute("SELECT pph FROM Sensors WHERE Id IS "+str(sensor))).fetchone())[0]
	
	print "Sensor: \t" + str(sensor)
	print "  Module: \t" + str(module)
	print ("  Measurand: \t" + measurand + " ("+unit+")").encode('utf-8')
	print "  Calibration: \t" + str(calibration)
	print "  Resolution: \t" + str(pph) + " pph"
		
	#Check if data available
	numpoints = ((dbcursor.execute("SELECT COUNT(Timestamp) FROM Data"+str(sensor))).fetchone())[0]

	if numpoints == 0:	
		ColorPrint.ColorPrint("  No data available", "error")
		print ""
		continue
	
	res = ReadData(sensor, years, months, days, hours, start, end)
	if len(res) == 0:
		ColorPrint.ColorPrint("  No data in selection.", "error")
		continue
	datehours = res[0]
	data = res[1]
	res = Analyze(sensor, datehours, data)
	if len(res.keys()) == 0:
		continue
	plotting = False
		
	if yearlystats:
		yearstmp = sorted(list(set([d[0] for d in datehours])), key=int)
		totalavg = []
		totalsigma = []
		totalmax = []
		totalmin = []
		dailymaxavg = []
		dailyminavg = []
		for y in yearstmp:
			ydatehours = [ d for d in datehours if d[0] == y ]
			ydata = dict()	
			for d in ydatehours:
				if d in data.keys():
					ydata[d] = data[d]
			print ""
			print "  Statistics for " + str(y) + ":"
 			res = Analyze(sensor, ydatehours, ydata)
 			totalavg.append(res["totalavg"])
 			totalsigma.append(res["totalsigma"])
 			totalmax.append(res["totalmax"])
 			totalmin.append(res["totalmin"])
 			dailymaxavg.append(res["dailymaxavg"])
 			dailyminavg.append(res["dailyminavg"])
 			
 		if plottingavg:
 			totalavgplot, = plt.plot(yearstmp, totalavg, 'go')
 			totalmaxplot, = plt.plot(yearstmp, totalmax, 'ro')
 			totalminplot, = plt.plot(yearstmp, totalmin, 'bo')
 			dailymaxavgplot, = plt.plot(yearstmp, dailymaxavg, 'r^')
 			dailyminavgplot, = plt.plot(yearstmp, dailyminavg, 'bv')
  			plt.xlim([min(yearstmp)-1,max(yearstmp)+1])
 			plt.xticks(yearstmp, yearstmp)
 			plt.xlabel("Year")
 			plt.ylabel(measurand + " ("+unit+")")
 			#plt.errorbar(yearstmp, totalavg, totalsigma)
 			fontP = FontProperties()
  			fontP.set_size('small')
 			plt.legend([totalmaxplot, dailymaxavgplot, totalavgplot, dailyminavgplot, totalminplot], ["Maximum", "Daily maximum", "Average", "Daily minimum", "Minimum"], prop = fontP)
 			
 			print ""
 			title = raw_input("    Plot title:\t")
 			if title == "":
 				title = "Yearly statistics for sensor " + str(sensor)
  			plt.title(title)
  			
 			plt.show()
 			
 	if monthlystats:
 		monthstmp = sorted(list(set([d[1] for d in datehours])), key=int)
 		totalavg = []
		totalsigma = []
		totalmax = []
		totalmin = []
		dailymaxavg = []
		dailyminavg = []
 		monthnames = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
 		for m in monthstmp:
			mdatehours = [ d for d in datehours if d[1] == m ]
			mdata = dict()	
			for d in mdatehours:
				if d in data.keys():
					mdata[d] = data[d]
			print ""
			print "  Statistics for " + monthnames[m-1]+":"
 			res = Analyze(sensor, mdatehours, mdata)	
 			totalavg.append(res["totalavg"])
 			totalsigma.append(res["totalsigma"])
 			totalmax.append(res["totalmax"])
 			totalmin.append(res["totalmin"])
 			dailymaxavg.append(res["dailymaxavg"])
 			dailyminavg.append(res["dailyminavg"])

		if plottingavg:
 			totalavgplot, = plt.plot(monthstmp, totalavg, 'go')
 			totalmaxplot, = plt.plot(monthstmp, totalmax, 'ro')
 			totalminplot, = plt.plot(monthstmp, totalmin, 'bo')
 			dailymaxavgplot, = plt.plot(monthstmp, dailymaxavg, 'r^')
 			dailyminavgplot, = plt.plot(monthstmp, dailyminavg, 'bv')
  			plt.xlim([min(monthstmp)-1,max(monthstmp)+1])
 			plt.xticks(monthstmp, monthstmp)
 			plt.xlabel("Month")
 			plt.ylabel(measurand + " ("+unit+")")
 			#plt.errorbar(yearstmp, totalavg, totalsigma)
 			fontP = FontProperties()
  			fontP.set_size('small')
 			plt.legend([totalmaxplot, dailymaxavgplot, totalavgplot, dailyminavgplot, totalminplot], ["Maximum", "Daily maximum", "Average", "Daily minimum", "Minimum"], prop = fontP)
 			
 			print ""
 			title = raw_input("    Plot title:\t")
 			if title == "":
 				title = "Monthly statistics for sensor " + str(sensor)
  			plt.title(title)
 			
 			plt.show()
 	
 	if dailystats:
 		daysstmp = sorted(list(set([d[2] for d in datehours])), key=int)
 		totalavg = []
		totalsigma = []
		totalmax = []
		totalmin = []
		dailymaxavg = []
		dailyminavg = []
 		for t in daysstmp:
			tdatehours = [ d for d in datehours if d[2] == t ]
			tdata = dict()	
			for d in tdatehours:
				if d in data.keys():
					tdata[d] = data[d]
			print ""
			print "  Statistics for day " + str(t)+":"
 			res = Analyze(sensor, tdatehours, tdata)	
 			totalavg.append(res["totalavg"])
 			totalsigma.append(res["totalsigma"])
 			totalmax.append(res["totalmax"])
 			totalmin.append(res["totalmin"])
 			dailymaxavg.append(res["dailymaxavg"])
 			dailyminavg.append(res["dailyminavg"])
 			
 		if plottingavg:
 			totalavgplot, = plt.plot(daysstmp, totalavg, 'go')
 			totalmaxplot, = plt.plot(daysstmp, totalmax, 'ro')
 			totalminplot, = plt.plot(daysstmp, totalmin, 'bo')
  			plt.xlim([min(daysstmp)-1,max(daysstmp)+1])
 			plt.xticks(daysstmp, daysstmp)
 			plt.xlabel("Day")
 			plt.ylabel(measurand + " ("+unit+")")
 			#plt.errorbar(yearstmp, totalavg, totalsigma)
 			fontP = FontProperties()
  			fontP.set_size('small')
 			plt.legend([totalmaxplot, totalavgplot, totalminplot], ["Maximum",  "Average", "Minimum"], prop = fontP)
 			
 			print ""
 			title = raw_input("    Plot title:\t")
 			if title == "":
 				title = "Daily statistics for sensor " + str(sensor)
  			plt.title(title)
 			
 			plt.show()
	
	if len(sensors) > 1:
		print ""
	
dbconn.close()
