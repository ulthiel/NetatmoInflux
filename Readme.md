#WeatherStats

***
WeatherStats is a collection of Python scripts for general sensor data management and analysis with a local SQLite database as backbone. The design of the software is quite general but the main application is weather data like temperature, precipitation, etc. A central feature is a user-friendly interface to [Netatmo](https://www.netatmo.com/) servers: remote data of Netatmo sensors can be automatically downloaded and added to the local database for analysis. Interfaces for other data devices may be added in the future.

I've written this software just for myself, it's open source and free (GPL). If it's also useful for you, I'd be happy about a donation. 

<form action="https://www.paypal.com/cgi-bin/webscr" method="post" target="_top">
<input type="hidden" name="cmd" value="_s-xclick">
<input type="hidden" name="hosted_button_id" value="EQPUVXXEJCELW">
<input type="image" src="https://www.paypalobjects.com/en_US/i/btn/btn_donate_SM.gif" border="0" name="submit" alt="PayPal - The safer, easier way to pay online!">
<img alt="" border="0" src="https://www.paypalobjects.com/de_DE/i/scr/pixel.gif" width="1" height="1">
</form>

   
by Ulrich Thiel, thiel@mathematik.uni-stuttgart.de
***


##Quick start
This is a quick start guide for using WeatherStats to manage and analyze Netatmo data. You can find more detailed information below.  
First, create an empty database with the program ```CreateEmptyDB.py```. Add your Netatmo account using ```AddNetatmo.py``` and follow the on-screen instructions for obtaining a client secret. You can add as many accounts as you like. All modules and sensors managed by your accounts are automatically added to the database and are assigned a unique id. You can get an overview of the sensors and their ids with ```ListSensors.py```. Now, run ```UpdateNetatmo.py```. This adds all available data for all sensors of your Netatmo accounts to the SQLite database ```Weather.db```. If you interrupt this program or run it again at a later time, all new data will be added automatically. You can now compute statistics using the program ```Stats.py```. Running ```Stats.py --help``` lists the available options. Here are four examples:

####Example 1
```python Stats.py```

This computes the overall statistics for all sensors found in the database. Depending on the number of sensors and the size of the database, this may take a while.

####Example 2

```python Stats.py --sensors=6 --years=2014-2016 --months=12 --days=24-26 --yearly --plot```

My outdoor temperature sensor has the id number 6 and in this example we get an overview of the temperatures during Christmas over the years 2014 to 2016. The additional ```plot``` option also creates a plot of the results. 

![](doc/Christmas.png)

####Example 3

```python Stats.py --sensors=6 --years=2015-2016  --monthly --plot```

Again we consider the outdoor temperature but this time we compute statistics for each month between 2015 and 2016, thus obtaining an actual climate diagram. In my case, 204,679 data points were taken into account with a total data quality of 97% (see below for a discusssion of data quality), so the average over these two years is quite accurate.

![](doc/Climate.png) 

####Example 4

```python Stats.py --sensors=6 --start=2016-05-11 --end=2016-06-17 --hours=7-8```

This computes statistics for the outdoor temperature between 7 and 8 o'clock between May 11, 2016 and June 17, 2016.

##Detailed functionality

In this section, the functionality of WeatherStats is discussed in more detail. 

###The database
The backbone of WeatherStats is the local SQLite database ```Weather.db``` in the directory of WeatherStats. If you just downloaded WeatherStats, you need to create an empty database with the program ```CreateEmptyDB.py```. I suggest taking a look at this database with an SQLite browser like [DB Browser for SQLite](http://sqlitebrowser.org). With such a browser you can also edit the tables and do some fine-tuning. The basic idea behind the structure of the database is as follows. 

####Sensors
We will manage data of arbitrary and arbitrarily many sensors, so there is a table called **Sensors** listing these with additional information. Each sensor has a unique id given by the **Id** column. This is also the id you can pass to the ```Stats.py``` program. The meaning of the **Measurand**, **Unit**, and **Description** columns should be clear. In the **Calibration** column you can define a fixed calibration for each sensor meaning that this value will be added to each recorded value to get the actual value. For example, I know that the humidity of my outdoor humidity sensor is always 6% too low, so I set calibration to 6. The column **pph** gives the temporal resolution of the sensor in *points per hour* (pph). For example, Netatmo devices record one value every 5 minutes, so this is 12 pph. This information is used for the quality analysis of the data. 

###Data quality