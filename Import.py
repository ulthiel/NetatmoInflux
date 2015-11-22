#!/usr/bin/python
#Import NetAtmo data to database

import sys
import csv
import sqlite3
from optparse import OptionParser
from progressbar import AnimatedMarker, Bar, BouncingBar, Counter, ETA, \
    FileTransferSpeed, FormatLabel, Percentage, \
    ProgressBar, ReverseBar, RotatingMarker, \
    SimpleProgress, Timer, AdaptiveETA, AdaptiveTransferSpeed

#parse options
parser = OptionParser()
parser.add_option("--input", dest="inputfile",help="Input file name")
parser.add_option("--tempid", dest="tempid",help="Temperature sensor ID")
parser.add_option("--humidid", dest="humidid",help="Humidity sensor ID")
(options, args) = parser.parse_args()
inputfile = options.inputfile
tempid = options.tempid
humidid = options.humidid	

#get column names
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
	sys.exit("Could not find header")

#check if sensor ids given
if 'Temperature' in cols:
	if tempid is None:
		sys.exit('No ID for temperature sensor given')
	tempid = int(tempid)
	
if 'Humidity' in cols:
	if humidid is None:
		sys.exit('No ID for humidity sensor given')
	humidid = int(humidid)

#connect to db
dbconn = sqlite3.connect('Weather.db')
dbcursor = dbconn.cursor()

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
    
for row in csvreader:
	rowcount = rowcount + 1
	
	timestamp = int(row[cols['Timestamp']])
		
	#temperature
	if 'Temperature' in cols:
		temp = float(row[cols['Temperature']])
		dbcursor.execute("INSERT OR IGNORE INTO Data (Timestamp, Sensor, Value) VALUES (" + str(timestamp) + ", " + str(tempid) + ", " + str(temp) + ")")
			
	#humidity
	if 'Humidity' in cols:
		humid = float(row[cols['Humidity']])
		dbcursor.execute("INSERT OR IGNORE INTO Data (Timestamp, Sensor, Value) VALUES (" + str(timestamp) + ", " + str(humidid) + ", " + str(humid) + ")")
			
	pbar.update(rowcount)
        
pbar.finish()
				
		
dbconn.commit()				
dbconn.close()
		