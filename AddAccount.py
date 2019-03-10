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
#This script adds a Netatmo account to the database
##############################################################################

##############################################################################
#imports
import sqlite3
import os.path
from lib import ColorPrint
from lib import Netatmo
import getpass

##############################################################################
#program start
username = raw_input("User: ")
password = getpass.getpass()
ColorPrint.ColorPrint("Do you want to save the password as clear text to the database?\nIf not, you have to enter it on any update.", "warning")
savepassw = raw_input("Save? (y/n) ")
if not (savepassw == "Y" or savepassw == "y"):
  password = ""
ColorPrint.ColorPrint("You have to grant client access for WeatherStats. If not done yet,\nlog into\n\thttps://dev.netatmo.com/dev/myaccount\nand add an app called \"WeatherStats\". You will be given a client \nid and a client secret.", "warning")
clientId = raw_input("Client id: ")
clientSecret = raw_input("Client secret: ")

dbconn = sqlite3.connect('Netatmo.db')
dbcursor = dbconn.cursor()

dbcursor.execute(\
  "INSERT INTO Accounts (User, Password, ClientID, ClientSecret)\n"\
  "VALUES (\"" + username + "\",\"" + password + "\",\"" + clientId + "\",\"" + clientSecret + "\")"
)

#check if it works
netatm = Netatmo.NetatmoClient(username, password, clientId, clientSecret)
netatm.getStationData()

ColorPrint.ColorPrint("Account added", "okgreen")

dbconn.commit()
dbconn.close()
