#!/bin/bash
rm dist/mac/*.*
pyinstaller --distpath dist/mac -F AddNetatmo.py
pyinstaller --distpath dist/mac -F AddSensor.py
pyinstaller --distpath dist/mac -F CreateEmptyDB.py
pyinstaller --distpath dist/mac -F ListSensors.py
pyinstaller --distpath dist/mac -F SetDatesInDB.py
pyinstaller --distpath dist/mac -F Stats.py
pyinstaller --distpath dist/mac -F UpdateNetatmo.py