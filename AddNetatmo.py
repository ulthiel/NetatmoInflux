#!/usr/bin/python
# -*- coding: utf-8 -*-
##############################################################################
# WeatherStats
# A collection of Python scripts for general weather data management and analysis with Netatmo support
# (C) 2015-2016, Ulrich Thiel
# thiel@mathematik.uni-stuttgart.de
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
	
dbconn = sqlite3.connect('Weather.db')
dbcursor = dbconn.cursor()
	
dbcursor.execute(\
	"INSERT INTO NetatmoAccounts (User, Password, ClientID, ClientSecret)\n"\
	"VALUES (\"" + username + "\",\"" + password + "\",\"" + clientId + "\",\"" + clientSecret + "\")"
)
	
#check if it works
netatm = Netatmo.NetatmoClient(username, password, clientId, clientSecret)
netatm.getStationData()
	
ColorPrint.ColorPrint("Account added", "okgreen")

dbconn.commit()
dbconn.close()