#!/usr/bin/env python3

from setuptools import setup
from os.path import exists
from shutil import copyfile

from monocle import __version__ as version, __title__ as name

if not exists('monocle/config.py'):
    copyfile('config.example.py', 'monocle/config.py')

setup(
    name=name,
    version=version,
    packages=(name,),
    include_package_data=True,
    zip_safe=False,
    scripts=('scan.py', 'web.py', 'web-sanic.py', 'gyms.py', 'solve_captchas.py')
)
