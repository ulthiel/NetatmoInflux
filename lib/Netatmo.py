#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
##############################################################################
# WeatherStats
# A collection of Python scripts for general sensor data management and analysis with Netatmo support.
# (C) 2015-2017, Ulrich Thiel
# thiel@mathematik.uni-stuttgart.de
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
#This script contains basic functions for Netatmo API connection
#To a large extend taken from Philippe Larduinat, https://github.com/philippelt/netatmo-api-python/blob/master/lnetatmo.py

import json
import sys
import time

#HTTP libraries depends upon Python 2 or 3
if sys.version_info.major == 3 :
    import urllib.parse, urllib.request
else:
    from urllib import urlencode
    import urllib2


import pprint
import ColorPrint

##############################################################################
#Retrieves the device data for an account in JSON format
def postRequest(url, params):
    if sys.version_info.major == 3:
        req = urllib.request.Request(url)
        req.add_header("Content-Type","application/x-www-form-urlencoded;charset=utf-8")
        params = urllib.parse.urlencode(params).encode('utf-8')
        resp = urllib.request.urlopen(req, params).readall().decode("utf-8")
    else:
        params = urlencode(params)
        headers = {"Content-Type" : "application/x-www-form-urlencoded;charset=utf-8"}
        req = urllib2.Request(url=url, data=params, headers=headers)
        try:
        	resp = urllib2.urlopen(req).read()
        except urllib2.HTTPError, e:
			ColorPrint.ColorPrint(str(e), "error")
			sys.exit(1)
    return json.loads(resp)


##############################################################################
#Netatmo class
class NetatmoClient:

	#Netatmo URLs
	BASE_URL       = "https://api.netatmo.net/"
	AUTH_REQ       = BASE_URL + "oauth2/token"
	GETUSER_REQ    = BASE_URL + "api/getuser"	#deprecated
	DEVICELIST_REQ = BASE_URL + "api/devicelist"	#deprecated
	GETSTATION_REQ = BASE_URL + "api/getstationsdata"
	GETMEASURE_REQ = BASE_URL + "api/getmeasure"

	def __init__(self, username, password, clientId, clientSecret):

		self.username = username
		self.password = password
		self.clientId = clientId
		self.clientSecret = clientSecret

		postParams = {
                "grant_type" : "password",
                "client_id" : clientId,
                "client_secret" : clientSecret,
                "username" : username,
                "password" : password,
                "scope" : "read_station"
                }

		resp = postRequest(self.AUTH_REQ, postParams)

		self.accessToken = resp['access_token']
		self.refreshToken = resp['refresh_token']
		self.scope = resp['scope']
		self.expiration = int(resp['expire_in'] + time.time())

	#function for refreshing access token if necessary
	def refreshAccessToken(self):

		if self.expiration < time.time(): # Token should be renewed

			postParams = {
                    "grant_type" : "refresh_token",
                    "refresh_token" : self.refreshToken,
                    "client_id" : self.clientId,
                    "client_secret" : self.clientSecret
                    }
			resp = postRequest(self.AUTH_REQ, postParams)

			self.accessToken = resp['access_token']
			self.refreshToken = resp['refresh_token']
			self.expiration = int(resp['expire_in'] + time.time())

			self.getStationData()

	def getStationData(self):
		self.refreshAccessToken()	#will only do if necessary
		postParams = {"access_token" : self.accessToken}
		resp = postRequest(self.GETSTATION_REQ, postParams)
		raw = resp['body']

		#collect information about all devices and all modules
		devicemoduleids = []
		measurands = dict()
		locations = dict()
		unit = raw['user']['administrative']['unit']
		units = dict()
		windunit = raw['user']['administrative']['windunit']
		windunits = dict()
		pressureunit = raw['user']['administrative']['pressureunit']
		pressureunits = dict()
		for device in raw['devices']:
			location = device['place']['location']
			alt = device['place']['altitude']
			timezone = device['place']['timezone']

			#sensors of base station
			deviceid = device['_id']
			id = (deviceid,None)
			devicemoduleids.append(id)
			name = device['station_name']+", "+device['module_name']
			measurands[id] = device['data_type']
			locations[id] = (location,alt,timezone,name)
			units[id] = unit
			windunits[id] = windunit
			pressureunits[id] = pressureunit

			#sensors of modules
			for module in device['modules']:
				moduleid = module['_id']
				id = (deviceid,moduleid)
				devicemoduleids.append(id)
				measurands[id] = module['data_type']
				name = device['station_name']+", "+module['module_name']
				locations[id] = (location,alt,timezone,name)
				units[id] = unit
				windunits[id] = windunit
				pressureunits[id] = pressureunits

		self.devicemoduleids = devicemoduleids
		self.measurands = measurands
		self.locations = locations
		self.units = units
		self.windunits = windunits
		self.pressureunits = pressureunits

	def getMeasure(self,device_id,module_id,scale,type,date_begin,date_end,limit,optimize):
		self.refreshAccessToken()	#will only do if necessary
		postParams = {"access_token" : self.accessToken, "device_id" : device_id, "scale":scale, "type":type}
		if module_id != None:
			postParams['module_id'] = module_id
		if date_begin != None:
			postParams['date_begin'] = str(date_begin)
		if date_end != None:
			postParams['date_end'] = str(date_end)
		if limit != None:
			postParams['limit'] = str(limit)
		if optimize != None:
			postParams['optimize'] = optimize
		resp = postRequest(self.GETMEASURE_REQ, postParams)
		return resp['body']
