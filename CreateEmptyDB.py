#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
##############################################################################
# WeatherStats
#
# A collection of Python scripts for general sensor data management and analysis,
# with Netatmo support.
#
# (C) 2015-2018, Ulrich Thiel
# ulrich.thiel@sydney.edu.au
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
#This script creates an empty database

import sqlite3
import os.path
from lib import ColorPrint
from lib import Netatmo
import getpass

#Creates empty database
def CreateEmptyDB():
  dbconn = sqlite3.connect('Netatmo.db')
  dbcursor = dbconn.cursor()

  dbcursor.execute(\
    "CREATE TABLE \"Accounts\" (\n" \
    "`User` TEXT,\n" \
    "`Password` TEXT,\n" \
    "`ClientID` TEXT,\n" \
    "`ClientSecret` TEXT,\n" \
    "PRIMARY KEY(User) ON CONFLICT REPLACE)\n"\
  )

  dbcursor.execute(\
    "CREATE TABLE \"Modules\" (\n" \
    "`Id` TEXT,\n" \
    "`Name` TEXT,\n" \
    "PRIMARY KEY(Id))\n"\
  )

  dbcursor.execute(\
    "CREATE TABLE \"Sensors\" (\n" \
    "`Id` INTEGER,\n" \
    "`Module` TEXT,\n" \
    "`Measurand` INTEGER,\n" \
    "`Unit` INTEGER,\n" \
    "`Name` TEXT,\n" \
    "`Calibration` REAL,\n" \
    "`Interval` INTEGER,\n" \
    "PRIMARY KEY(Id))\n" \
  )

  dbcursor.execute(\
    "CREATE TABLE \"Locations\" (\n" \
    "`Id` INTEGER,\n" \
    "`PositionNorth` REAL,\n" \
    "`PositionEast` REAL,\n" \
    "`Elevation` INTEGER,\n" \
    "`Name` TEXT,\n" \
    "`Timezone` TEXT,\n" \
    "PRIMARY KEY(Id))\n" \
  )

  dbcursor.execute(\
    "CREATE TABLE \"ModuleLocations\" (\n" \
    "`Module` TEXT,\n" \
    "`Begin` TEXT,\n" \
    "`End` TEXT,\n" \
    "`Location` INTEGER,\n" \
    "PRIMARY KEY(Module, Begin, End))\n" \
  )

  dbconn.commit()
  dbconn.close()

#First, check if database exists and create empty one if not
if not os.path.isfile("Netatmo.db"):
  CreateEmptyDB()
  ColorPrint.ColorPrint("New database created", "okgreen")
else:
  ColorPrint.ColorPrint("Database exists already", "error")
