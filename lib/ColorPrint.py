#!/usr/bin/python
# -*- coding: utf-8 -*-
##############################################################################
# WeatherStats
# A collection of Python scripts for general weather data management and analysis with Netatmo support
# (C) 2015-2016, Ulrich Thiel
# thiel@mathematik.uni-stuttgart.de
##############################################################################


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