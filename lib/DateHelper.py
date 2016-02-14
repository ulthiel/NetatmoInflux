#!/usr/bin/python
# -*- coding: utf-8 -*-
##############################################################################
# WeatherStats
# Tiny Python scripts for general weather data management and analysis (with Netatmo support)
# (C) 2015-2016, Ulrich Thiel
# thiel@mathematik.uni-stuttgart.de
##############################################################################

##############################################################################
#This file contains some functions to convert timestamps to usual date formats and conversely

import datetime
import calendar
import time

#Returns the date in format Y-m-d from a timestamp
def DateFromTimestamp(t):
	return datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d')
	
#Returns the datetime in format Y-m-d H:M
def DatetimeFromTimestamp(t):
	return datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M')
	
#Returns the day in format Y-m-d from a timestamp
def DateAndHourFromTimestamp(t):
	return datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d %Hh')
	
#Returns the minute from a timestamp
def MinuteFromTimestamp(t):
	return int(datetime.datetime.fromtimestamp(t).strftime('%M'))

#Returns the second from a timestamp
def SecondFromTimestamp(t):
	return int(datetime.datetime.fromtimestamp(t).strftime('%S'))
		
#Returns the month from a timestamp
def HourFromTimestamp(t):
	return int(datetime.datetime.fromtimestamp(t).strftime('%H'))
	
#Returns the month from a timestamp
def DayFromTimestamp(t):
	return int(datetime.datetime.fromtimestamp(t).strftime('%d'))

#Returns the month from a timestamp
def MonthFromTimestamp(t):
	return int(datetime.datetime.fromtimestamp(t).strftime('%m'))
	
#Returns the month from a timestamp
def YearFromTimestamp(t):
	return int(datetime.datetime.fromtimestamp(t).strftime('%Y'))

#Returns first timestamp of date s (s in format Y-m-d)
def FirstTimestampOfDate(s):
	return time.mktime(datetime.datetime.strptime(s, "%Y-%m-%d").timetuple())
	
#Returns last timestamp of date s (s in format Y-m-d)
def LastTimestampOfDate(s):
	return time.mktime((datetime.datetime.strptime(s, "%Y-%m-%d") + datetime.timedelta(1)).timetuple() )
	
#Returns first timestamp of year s
def FirstTimestampOfYear(s):
	return FirstTimestampOfDate(s+"-01-01")
	
#Returns last timestamp of year s
def LastTimestampOfYear(s):
	return LastTimestampOfDate(s+"-12-31")
	
#Returns first timestamp of month m of year y
def FirstTimestampOfMonth(y,m):
	return FirstTimestampOfDate(str(y)+"-"+str(m)+"-01")

#Returns last date of month m of year y
def LastDayOfMonth(y,m):
	return calendar.monthrange(y,m)[1]
	
#Returns first timestamp of month m of year y
def LastTimestampOfMonth(y,m):
	return LastTimestampOfDate(str(y)+"-"+str(m)+"-"+str(LastDayOfMonth(y,m)))
		
#Returns first timestamp of date s (s in format Y-m-d H:m)
def TimestampOfDatetime(s):
	return time.mktime(datetime.datetime.strptime(s, "%Y-%m-%d %H:%M").timetuple())


##############################################################################
# the number of days between end and start (given in format Y-m-d)
def NumberOfDaysBetween(start, end):
	date_format = "%Y-%m-%d"
	a = datetime.datetime.strptime(start, date_format)
	b = datetime.datetime.strptime(end, date_format)
	delta = b - a
	return delta.days 
	
##############################################################################
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
			endday = LastDayOfMonth(endyear, endmonth)
			
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
	
	dates = [ [d.year,d.month,d.day] for d in datestmp if d.year in years and d.month in months and d.day in days]
		
	datehours = []
	for d in dates:
		for h in hours:
			datehours.append(d + [h])
			
	return [dates, datehours]