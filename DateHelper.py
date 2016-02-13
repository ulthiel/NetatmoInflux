#!/usr/bin/python
# -*- coding: utf-8 -*-
##############################################################################
# WeatherStats
# A tiny Python script for weather data management and analysis
# (C) 2015, Ulrich Thiel
# thiel@mathematik.uni-stuttgart.de
##############################################################################


import datetime
import calendar
import time

##############################################################################
#some timestamp helper functions	
def DateFromTimestamp(t):
	#Returns the date in format Y-m-d from a timestamp
	return datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d')
	
#some timestamp helper functions	
def DatetimeFromTimestamp(t):
	#Returns the datetime in format Y-m-d H:M
	return datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M')
	
def DateAndHourFromTimestamp(t):
	#Returns the day in format Y-m-d from a timestamp
	return datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d %Hh')
	
def MinuteFromTimestamp(t):
	#Returns the minute from a timestamp
	return int(datetime.datetime.fromtimestamp(t).strftime('%M'))

def SecondFromTimestamp(t):
	#Returns the second from a timestamp
	return int(datetime.datetime.fromtimestamp(t).strftime('%S'))
		
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
		
def TimestampOfDatetime(s):
	#Returns first timestamp of date s (s in format Y-m-d H:m)
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