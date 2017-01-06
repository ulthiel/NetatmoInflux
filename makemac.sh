#!/bin/bash
rm dist/WeatherStats-mac/*
rm dist/WeatherStats-mac.zip
pyinstaller --distpath dist/WeatherStats-mac -F AddNetatmo.py
pyinstaller --distpath dist/WeatherStats-mac -F AddSensor.py
pyinstaller --distpath dist/WeatherStats-mac -F CreateEmptyDB.py
pyinstaller --distpath dist/WeatherStats-mac -F ListSensors.py
pyinstaller --distpath dist/WeatherStats-mac -F SetDatesInDB.py
pyinstaller --distpath dist/WeatherStats-mac -F Stats.py
pyinstaller --distpath dist/WeatherStats-mac -F UpdateNetatmo.py