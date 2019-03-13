del /Q dist\WeatherStats-win\*.*
del /Q dist\WeatherStats-win.zip
pyinstaller --distpath dist/WeatherStats-win -F AddNetatmo.py
pyinstaller --distpath dist/WeatherStats-win -F AddSensor.py
pyinstaller --distpath dist/WeatherStats-win -F CreateEmptyDB.py
pyinstaller --distpath dist/WeatherStats-win -F ListSensors.py
pyinstaller --distpath dist/WeatherStats-win -F SetDatesInDB.py
pyinstaller --distpath dist/WeatherStats-win -F Stats.py
pyinstaller --distpath dist/WeatherStats-win -F UpdateNetatmo.py