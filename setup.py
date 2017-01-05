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

from distutils.core import setup

setup(name='WeatherStats',
      version='0.9',
      description='A collection of Python scripts for general sensor data management and analysis with Netatmo support',
      url='http://github.com/thielul/WeatherStats',
      author='Ulrich Thiel',
      author_email='thiel@mathematik.uni-stuttgart.de',
      license='GPL',
      packages=['WeatherStats'],
      install_requires=[
          'numpy',
          'scipy',
          'matplotlib',
          'sqlite3'
      ],
      keywords = ['weather', 'statistics', 'Netatmo', 'data']
      )