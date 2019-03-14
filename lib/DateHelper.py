#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
##############################################################################
# NetatmoInflux
#
# Python script for importing Netatmo data into an InfluxDB.
#
# (C) 2015-2019, Ulrich Thiel
# ulrich.thiel@sydney.edu.au
##############################################################################
#This file is part of NetatmoInflux.
#
#NetatmoInflux is free software: you can redistribute it and/or modify
#it under the terms of the GNU General Public License as published by
#the Free Software Foundation, either version 3 of the License, or
#(at your option) any later version.
#
#NetatmoInflux is distributed in the hope that it will be useful,
#but WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#GNU General Public License for more details.
#
#You should have received a copy of the GNU General Public License
#along with WeatherStats. If not, see <http://www.gnu.org/licenses/>.
##############################################################################

##############################################################################
#This file contains some functions to convert timestamps to usual date formats and conversely

import datetime
import calendar
import time
from pytz import timezone

#Current timestamp
def CurrentTimestamp():
	return int(time.time())

#Returns the datetime in format YYYY-mm-dd HH:MM:SS
def DatetimeFromTimestamp(t,tz):
	if tz != None:
		tz = timezone(tz)
	return datetime.datetime.fromtimestamp(t,tz).strftime('%Y-%m-%d %H:%M:%S')

#Returns the date in format Y-m-d from a datetime
def DateFromDatetime(s):
	return s[0:10]

#Returns the day in format Y-m-d Hh from a datetime
def DateHourFromDatetime(s):
	return s[0:13]+"h"

#Returns the year from a datetime
def YearFromDatetime(s):
	return int(s[0:4])

#Returns the month from a datetime
def MonthFromDatetime(s):
	return int(s[5:7])

#Returns the day from a datetime
def DayFromDatetime(s):
	return int(s[8:10])

#Returns the month from a datetime
def HourFromDatetime(s):
	return int(s[11:13])

#Returns the minute from a datetime
def MinuteFromDatetime(s):
	return int(s[14:16])

#Returns the second from a datetime
def SecondFromDatetime(s):
	return int(s[17:19])

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

#Returns first timestamp of datehour s (s in format Y-m-d H)
def TimestampOfDatehour(s):
	return int(time.mktime(datetime.datetime.strptime(s, "%Y-%m-%d %H").timetuple()))


##############################################################################
# the number of days between end and start (given in format Y-m-d)
def NumberOfDaysBetween(start, end):
	date_format = "%Y-%m-%d"
	a = datetime.datetime.strptime(start, date_format)
	b = datetime.datetime.strptime(end, date_format)
	delta = b - a
	return delta.days

##############################################################################
#Returns current date and date obtained by subtracting a given amount of days
def SubtractDaysFromCurrentDate(days):
	b = datetime.datetime.today()
	a = b - datetime.timedelta(days=days)
	date_format = "%Y-%m-%d"
	a = datetime.datetime.strftime(a, date_format)
	b = datetime.datetime.strftime(b, date_format)
	return [a,b]
