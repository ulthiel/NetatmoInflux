#!/usr/bin/python
# -*- coding: utf-8 -*-
##############################################################################
# WeatherStats
# A collection of Python scripts for general weather data management and analysis with Netatmo support
# (C) 2015-2016, Ulrich Thiel
# thiel@mathematik.uni-stuttgart.de
##############################################################################

import sys

#prints str without newline so that str can be overwritten
def PrintWithoutNewline(str):
	sys.stdout.write('\r' + str) 
	sys.stdout.flush()