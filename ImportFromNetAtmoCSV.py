#!/usr/bin/python
#Import NetAtmo CSV data to database

import sys
import csv
import sqlite3
from optparse import OptionParser
from progressbar import Bar, ETA, Percentage, ProgressBar
from DateHelper import *

#parse options
parser = OptionParser()
parser.add_option("--input", dest="inputfile",help="Input file name")
parser.add_option("--module", dest="module",help="Module ID")
(options, args) = parser.parse_args()
inputfile = options.inputfile
moduleid = options.module

#check arguments
if inputfile is None:
	sys.exit('Please specify input file with --input option')
	
if moduleid is None:
	sys.exit('Please module ID with --module option')
	
#connect to db
dbconn = sqlite3.connect('Weather.db')
dbcursor = dbconn.cursor()

#get sensor ids for module
tempid = None
humidid = None	
co2id = None
noiseid = None
pressureid = None

res = (dbcursor.execute("SELECT ID, Measurand FROM Sensors WHERE Module IS "+str(moduleid)).fetchall())

for sensor in res:
	if sensor[1] == "Temperature":
		tempid = sensor[0]
	elif sensor[1] == "Humidity":
		humidid = sensor[0]
	elif sensor[1] == "CO2":
		co2id = sensor[0]
	elif sensor[1] == "Noise":
		noiseid = sensor[0]
	elif sensor[1] == "Pressure":
		pressureid = sensor[0]

#get column names in CSV file
cols = {}
startrow = 0
csvfile = open(inputfile, 'rb')
csvreader = csv.reader(csvfile, delimiter=';', quotechar='\"')
for row in csvreader:
	startrow = startrow + 1
	if row[0] == 'Timestamp':
		for i in range(0, len(row)):
			cols[row[i]] = i
		break

if startrow == 0:
	sys.exit("Could not find header in CSV file")

#get number of lines in file
totalrows = len(open(inputfile).readlines())
totalrows = totalrows - startrow

#read data	
rowcount = 0
widgets = [
        Percentage(),
        ' ', Bar(),
        ' ', ETA()
    ]
pbar = ProgressBar(widgets=widgets, maxval=totalrows)
pbar.start()

#add data point
def AddDataPoint(timestamp, sensorid, value):

	year = YearFromTimestamp(timestamp)
	month = MonthFromTimestamp(timestamp)
	day = DayFromTimestamp(timestamp)
	hour = HourFromTimestamp(timestamp)
	minute = MinuteFromTimestamp(timestamp)
	second = SecondFromTimestamp(timestamp)
	
	dbcursor.execute("INSERT INTO Data (Timestamp, Sensor, Value, Year, Month, Day, Hour, Minute, Second) VALUES (" + str(timestamp) + ", " + str(sensorid) + ", " + str(value) + ", " + str(year) + ", " + str(month) + ", " + str(day) + ", " + str(hour) + ", " + str(minute) + ", " + str(second) + ")")
    
# read each line of CSV file
for row in csvreader:
	rowcount = rowcount + 1
	
	timestamp = int(row[cols['Timestamp']])
	
	for col in cols:
		if col == 'Temperature' and tempid is not None:
			sensorid = tempid
		elif col == 'Humidity' and humidid is not None:
			sensorid = humidid
		elif col == 'CO2' and co2id is not None:
			sensorid = co2id
		elif col == 'Noise' and noiseid is not None:
			sensorid = noiseid
		elif col == 'Pressure' and pressureid is not None:
			sensorid = pressureid
		else:
			sensorid = None
			
		if sensorid is not None:
			value = row[cols[col]]
			if value == '':		#might be empty
				continue
			value = float(value)
			AddDataPoint(timestamp,sensorid, value) 
			
	pbar.update(rowcount)
        
pbar.finish()
				
		
dbconn.commit()				
dbconn.close()
		