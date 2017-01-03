#!/usr/bin/python
# -*- coding: utf-8 -*-
##############################################################################
# WeatherStats
# A collection of Python scripts for general weather data management and analysis with Netatmo support
# (C) 2015-2017, Ulrich Thiel
# thiel@mathematik.uni-stuttgart.de
##############################################################################

##############################################################################
#This script adds correct dates (taking timezone at location into account) in the database. With option "--all", all dates are set again.

##############################################################################
#imports
import sqlite3
from optparse import OptionParser
from lib import Tools
from lib import DateHelper

setall = False

##############################################################################
#Set dates
def SetDates(dbconn, dbcursor):
	Tools.PrintWithoutNewline("Setting dates... ")
	
	res = (dbcursor.execute("SELECT Id FROM Sensors")).fetchall()
	sensors = []
	for r in res:
		sensors.append(r[0])

	for sensor in sensors:
		if setall:
			dbcursor.execute("SELECT * From Data"+str(sensor)+"Full")
		else:
			dbcursor.execute("SELECT Timestamp,Value,Timezone From Data"+str(sensor)+"Full WHERE Year IS NULL")
		res = dbcursor.fetchall()
		if res is None or len(res) == 0:
			continue
		reswithdatetimes = [ [r[0],r[1],DateHelper.DatetimeFromTimestamp(r[0],r[2])] for r in res ] #r[2] is timezone is DataFull view
		del res
		
		for r in reswithdatetimes:
			s = r[2]		
			dbcursor.execute("INSERT INTO Data"+str(sensor)+" (Timestamp,Value,Year,Month,Day,Hour,Minute,Second) VALUES ("+str(r[0])+","+str(r[1])+","+str(DateHelper.YearFromDatetime(s))+", \
			"+str(DateHelper.MonthFromDatetime(s))+", \
			"+str(DateHelper.DayFromDatetime(s))+", \
			"+str(DateHelper.HourFromDatetime(s))+", \
			"+str(DateHelper.MinuteFromDatetime(s))+", \
			"+str(DateHelper.SecondFromDatetime(s))+")")
						
		dbconn.commit()
		
	Tools.PrintWithoutNewline("")
		
##############################################################################
#Standalone program
if __name__ == "__main__":

	#parse options
	parser = OptionParser()
	parser.add_option("--all", dest="setall",help="Set all dates", default=False,action="store_true")
	(options, args) = parser.parse_args()
	setall = options.setall
	
	#run setdates
	dbconn = sqlite3.connect('Weather.db')
	dbcursor = dbconn.cursor()
	SetDates(dbconn, dbcursor)