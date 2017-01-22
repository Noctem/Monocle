#!/usr/bin/env python3

from setuptools import setup
from os.path import exists
from shutil import copyfile

if not exists('pokeminer/config.py'):
    copyfile('config.example.py', 'pokeminer/config.py')

setup(
    name="pokeminer",
    version="0.8a0",
    packages=('pokeminer',),
    include_package_data=True,
    zip_safe=False,
    scripts=('scan.py', 'web.py', 'gyms.py', 'solve_captchas.py')
)
