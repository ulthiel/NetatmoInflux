# NetatmoInflux

A [Python](https://www.python.org) script for importing [Netatmo](https://www.netatmo.com/) sensor data into an [InfluxDB](https://docs.influxdata.com/influxdb/) time series data base. Using [Chronograf](https://docs.influxdata.com/chronograf/) you can then run complex data analysis and create beautiful dashboards like this one here:

![dashboard](https://raw.githubusercontent.com/ulthiel/NetatmoInflux/master/doc/dashboard.jpg)

## Installation

You will need some minor programming skills, but I guess otherwise you wouldn't be here.

### Python
You'll need [Python 2](https://www.python.org/downloads/). Note: **version 2**, not version 3. Both versions may co-exist on your system though, but note that in this case you may need to specifically call Python version 2 with ```python2``` or something like this. If you have troubles installing Python, you can also try installing a pre-compiled bundle like [Anaconda](https://www.anaconda.com/distribution/).

### Python packages
You'll need the additional Python packages *influxdb* (for InfluxDB) and *pytz* (for time zone conversions). It's easiest to install this with the [pip](https://pip.pypa.io/en/stable/installing/) tool:

```
pip install influxdb
pip install pytz
```

Depending on your system you may need to call these commands with ```sudo```. If you don't have pip, you should install it as described [here](https://pip.pypa.io/en/stable/installing/).

### InfluxDB

You need to set up an InfluxDB somewhere for storing the sensor values. I'm not going into details about this here, see [here](https://docs.influxdata.com/influxdb/).

### Initialization

Now, you should try to run the initialization script and follow the instructions:

```
python Initialize.py
-----------------------
| Add Netatmo account |
-----------------------
User: blah@blah.com
Password:
Do you want to save the password as clear text to the database?
If not, you have to enter it on any update.
Save (y/n)?: y
You have to grant client access for WeatherStats. If not done yet,
log into
	https://dev.netatmo.com/dev/myaccount
and add an app called "WeatherStats". You will be given a client
id and a client secret.
Client id: blah
Client secret: blah
Account added
Getting modules for account globalproj@gmx.net
Added device blah (Indoor module) at location 1 (Sydney, Indoor)
Added module blah (Rain gauge) at location 2 (Sydney, Rain gauge)
Added module blah (Outdoor module) at location 3 (Sydney, Outdoor)
-----------------------
| Add InfluxDB         |
-----------------------
Host: blah
Port: 8086
User: blah
Password:
Do you want to save the password as clear text to the database?
If not, you have to enter it on any update.
Save (y/n)?: y
Database: Environment
Use SSL (y/n)?: y
InfluxDB added
```

If you didn't get any errors, you're almost ready to go. Otherwise, you have to help yourself.

### Setup

In principle, you could already run the import script ```Import.py``` to import all data from the Netatmo server into your InfluxDB. Before you do this, hold on for a minute.

When I've written NetatmoInflux, I had a Netatmo module for almost 5 years already and I've moved cities a couple of times in between. I wanted to preserve the location information correctly as tags in the InfluxDB, so I had to tell the import script in which time windows the module was located where. This is done as follows (if you never moved or if you don't care, you can skip all of this).

The initialization script will create a local [SQLite](https://www.sqlite.org) database named ```Netatmo.db``` in which all the account information is stored. You can either view this database with the SQLite command line tool or you get neat [DB Browser for SQLite](https://sqlitebrowser.org). There is a table called *Locations* to store location information. This should contain the location you've currently stored on the Netatmo server. But here you may add all your past locations as well. This is how my table looks like:

![locations](https://raw.githubusercontent.com/ulthiel/NetatmoInflux/master/doc/locations.jpg)

Now, with the table *ModuleLocations* you can manage the locations of your modules. For each module you have to provide a begin/end time stamp (this may be empty as well)  describing the time window and then you have to give a location id as in the *Locations* table.

![modulelocations](https://raw.githubusercontent.com/ulthiel/NetatmoInflux/master/doc/modulelocations.jpg)

You can check out the *ModulesView* table for a complete overview.

### Import

The import script ```Import.py``` will import the sensor data from the Netatmo server into your InfluxDB. Just run:

```
python Import.py
```

You can also start this as a service via

```
python Import.py --service
```

which will execute the import every 10 minutes so that your data in the InfluxDB is always up to date. The Netamo module id and the location will be stored as tags for each measurement.
