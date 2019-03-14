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
import platform
if platform.system() == "Windows":
	from colorama import init
	init()

##############################################################################
#colors
class bcolors:
    HEADER = '\033[35m'
    OKBLUE = '\033[34m'
    OKGREEN = '\033[32m'
    WARNING = '\033[33m'
    FAIL = '\033[31m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def ColorPrintString(msg, type):
	if type == "error":
		colstr = bcolors.FAIL
	elif type == "warning":
		colstr = bcolors.WARNING
	elif type == "okgreen":
		colstr = bcolors.OKGREEN
	elif type == "okblue":
		colstr = bcolors.OKBLUE
	else:
		raise Exception('Message type not found')

	return colstr + msg + bcolors.ENDC

def ColorPrint(msg, type):
	print ColorPrintString(msg, type)
