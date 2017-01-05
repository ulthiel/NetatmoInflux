del dist/win/*.*
pyinstaller --distpath dist/win -F AddNetatmo.py
pyinstaller --distpath dist/win -F AddSensor.py
pyinstaller --distpath dist/win -F CreateEmptyDB.py
pyinstaller --distpath dist/win -F ListSensors.py
pyinstaller --distpath dist/win -F SetDatesInDB.py
pyinstaller --distpath dist/win -F Stats.py
pyinstaller --distpath dist/win -F UpdateNetatmo.py