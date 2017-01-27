#!/usr/bin/env python3

from setuptools import setup
from os.path import exists
from shutil import copyfile

setup(
    name="monocle",
    version="0.8a0",
    packages=('monocle',),
    include_package_data=True,
    zip_safe=False,
    scripts=('scan.py', 'web.py', 'gyms.py', 'solve_captchas.py')
)
