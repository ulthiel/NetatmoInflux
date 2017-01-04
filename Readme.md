#WeatherStats

***
WeatherStats is a collection of simple Python scripts for general weather data management and analysis with a local SQLite database as backbone. A central feature is the support of  [Netatmo](https://www.netatmo.com/) devices: remote data of Netatmo accounts can be added automatically to the local SQLite database for detailed local analysis.
   
by Ulrich Thiel, thiel@mathematik.uni-stuttgart.de
***

###Download and installation 
You can download the most recent version of WeatherStats [here](). 

###Quick start using Netatmo
This is a quick start guide for using WeatherStats to analyze Netatmo data. You can find more detailed information below.  
First, create an empty database with the program ```CreateEmptyDB.py```. Add your Netatmo account with the program ```AddNetatmo.py``` and follow the on-screen instructions for obtaining a client secret. You can add as many accounts as you like. All modules and sensors managed by your accounts are automatically added to the database and are assigned a unique id number. Run ```UpdateNetatmo.py```. This adds all available data for all sensors of your Netatmo accounts to the SQLite database ```Weather.db```. If you interrupt this program or run it again at a later time, all new data will be added automatically. You can now obtain statistics using the program ```Stats.db```. Running ```Stats.db --help``` lists the available options. Here are three examples:

####Example 1
```python Stats.py```

This computes the overall statistics for all sensors found in the database.

####Example 2

```python Stats.py --sensors=6 --years=2014-2016 --months=12 --days=24-26 --yearly --plot```

My outdoor temperature sensor was assigned the id number 6 and in this example we get an overview of the temperatures during Christmas over the years 2014 to 2016. The additional ```plot``` option also creates a plot of the results. My feeling was that Christmas in 2014 was a bit colder than in 2016, and as you can see from the plot, I was right.

![](doc/Christmas.png)

####Example 3

```python Stats.py --sensors=6 --years=2014-2016  --monthly --plot```

Again we consider the outdoor temperature but this time we compute statistics for each month between 2014 and 2016, thus obtaining an actual climate diagram.

![](doc/Climate.png) 


###Functionality

The backbone of WeatherStats is the local SQLite database ```Weather.db``` in the directory of WeatherStats. If you just downloaded WeatherStats, you need to create an empty database with the program ```CreateEmptyDB.py```. You can view this database with an SQLite browser like [DB Browser for SQLite](http://sqlitebrowser.org). 