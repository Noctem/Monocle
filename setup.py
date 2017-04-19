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
    scripts=('scan.py', 'web.py', 'web_sanic.py', 'gyms.py', 'solve_captchas.py'),
    install_requires=[
        'geopy>=1.11.0',
        'protobuf>=3.0.0',
        'flask>=0.11.1',
        'gpsoauth>=0.4.0',
        'werkzeug>=0.11.15',
        'sqlalchemy>=1.1.0',
        'aiopogo>=1.8.0',
        'polyline>=1.3.1',
        'aiohttp>=2.0.7,<2.1',
        'pogeo>=0.3',
        'cyrandom>=0.1.2'
    ],
    extras_require={
        'twitter': ['peony-twitter>=0.9.3'],
        'pushbullet': ['asyncpushbullet>=0.12'],
        'landmarks': ['shapely>=1.3.0'],
        'boundaries': ['shapely>=1.3.0'],
        'manual_captcha': ['selenium>=3.0'],
        'performance': ['uvloop>=0.7.0', 'cchardet>=1.1.0', 'aiodns>=1.1.0', 'ujson>=1.35'],
        'mysql': ['mysqlclient>=1.3'],
        'postgres': ['psycopg2>=2.6'],
        'images': ['pycairo>=1.10.0'],
        'socks': ['aiosocks>=0.2.2'],
        'sanic': ['sanic>=0.4', 'asyncpg>=0.8', 'ujson>=1.35']
    }
)
