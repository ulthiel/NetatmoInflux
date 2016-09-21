#!/usr/bin/python
# -*- coding: utf-8 -*-
##############################################################################
# WeatherStats
# A collection of Python scripts for general weather data management and analysis with Netatmo support
# (C) 2015-2016, Ulrich Thiel
# thiel@mathematik.uni-stuttgart.de
##############################################################################

##############################################################################
#This script add correct dates (taking timezone at location into account) in the database

##############################################################################
#imports
import sqlite3
from optparse import OptionParser
from lib import Tools
from lib import DateHelper

##############################################################################
#parse options
parser = OptionParser()
parser.add_option("--all", dest="setall",help="Set all dates", default=False,action="store_true")
(options, args) = parser.parse_args()
setall = options.setall


##############################################################################
#Set dates
def SetDates(dbconn, dbcursor):
	Tools.PrintWithoutNewline("Setting dates... ")
	
	if setall:
		dbcursor.execute("SELECT * From DataWithTimezone")
	else:
		dbcursor.execute("SELECT * From DataWithTimezone WHERE Year IS NULL")
	res = dbcursor.fetchall()
	if res is None or len(res) == 0:
		Tools.PrintWithoutNewline("")
		return
	reswithdatetimes = [ [r[0],r[1],r[2],DateHelper.DatetimeFromTimestamp(r[0],r[3])] for r in res ] #r[3] is timezone is DataWithTimezone view
	del res
		
	counter = 0
	N = float(len(reswithdatetimes))
	for r in reswithdatetimes:
		s = r[3]		
		dbcursor.execute("INSERT INTO Data (Timestamp,Sensor,Value,Year,Month,Day,Hour,Minute,Second,DateSet) VALUES ("+str(r[0])+","+str(r[1])+","+str(r[2])+","+str(DateHelper.YearFromDatetime(s))+", \
			"+str(DateHelper.MonthFromDatetime(s))+", \
			"+str(DateHelper.DayFromDatetime(s))+", \
			"+str(DateHelper.HourFromDatetime(s))+", \
			"+str(DateHelper.MinuteFromDatetime(s))+", \
			"+str(DateHelper.SecondFromDatetime(s))+", \
			1)")
			
		counter = counter + 1
		Tools.PrintWithoutNewline("Setting dates: "+str(int(100.0*float(counter)/N))+"%   ")
		
	Tools.PrintWithoutNewline("")
	
	dbconn.commit()
	
##############################################################################
#Standalone program
dbconn = sqlite3.connect('Weather.db')
dbcursor = dbconn.cursor()
SetDates(dbconn, dbcursor)