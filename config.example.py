# coding: utf-8
from datetime import datetime
from landmarks import Landmarks

DB_ENGINE = 'sqlite:///db.sqlite'
ENCRYPT_PATH = './libencrypt.so'

AREA_NAME = 'Salt Lake City'
LANGUAGE = 'EN'  # ISO 639-1 codes EN, DE, FR, and ZH currently supported.
MAP_START = (12.3456, 34.5678)
MAP_END = (13.4567, 35.6789)
GRID = (2, 2)  # row, column
CYCLES_PER_WORKER = 3
SCAN_DELAY = (10, 12, 10.5)  # varies between these values, favoring the third
PROXIES = None  # Insert dict or tuple of dicts with 'http' and 'https' keys
ALT_RANGE = (1450, 1550)  # Fall back to altitudes in this range if generation fails
LONGSPAWNS = False  # Store sightings with invalid times in another DB table

# convert spawn_id to integer for more efficient DB storage, set to False if
# using an existing database since the data types are incompatible.
SPAWN_ID_INT = True

SCAN_RADIUS = 70  # meters

_workers_count = GRID[0] * GRID[1]
COMPUTE_THREADS = int(_workers_count / 10) + 1
NETWORK_THREADS = int(_workers_count / 2) + 1

ALL_ACCOUNTS = [
    ('ash_ketchum', 'pik4chu', 'ptc', 'iPhone6,1', '9.3.4', '67c51fda79104a5a87935992e15d2246'),
    ('ziemniak_kalafior', 'ogorek', 'google', 'iPhone5,4', '9.0', 'bf8d044125424678be8e6050aac205f6'),
    ('noideawhattoputhere', 's3cr3t', 'ptc', 'iPhone8,2', '9.3.3', '433a80e3168f488288ae587c3e67441c')
]

ACCOUNTS = []
EXTRA_ACCOUNTS = []
# If you have more accounts than workers, this will add extras to
# a separate list and swap them in if another account has problems or
# gets banned.
for account in ALL_ACCOUNTS:
    if len(ACCOUNTS) < _workers_count:
        ACCOUNTS.append(account)
    else:
        EXTRA_ACCOUNTS.append(account)

TRASH_IDS = (13, 16, 19, 21, 41, 96)
STAGE2 = (141, 142, 143, 144, 145, 146, 148, 149, 150, 151)

REPORT_SINCE = datetime(2016, 7, 29)
GOOGLE_MAPS_KEY = 's3cr3t'

MAP_PROVIDER_URL = '//{s}.tile.osm.org/{z}/{x}/{y}.png'
MAP_PROVIDER_ATTRIBUTION = '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'


### OPTIONS BELOW THIS POINT ARE ONLY NECESSARY FOR NOTIFICATIONS ###

# As many hashtags as can fit will be included in your tweets, these will
# be combined with landmark-specific hashtags (if applicable).
HASHTAGS = {AREA_NAME, 'PokemonGO'}
TZ_OFFSET = 0  # hours offset from server time for reported times


# Only set one of the following two options. Only use NOTIFY_RANKING if your
# database has enough data to accurately determine rareness.
NOTIFY_IDS = STAGE2  # a list or tuple of Pokémon IDs to notify about
#NOTIFY_RANKING = 50  # notify about the (x) rarest according to your database

# the required number of seconds remaining to notify about a Pokémon
MIN_TIME = 300

# Sightings of the top (x) will always be notified about, even if below MIN_TIME
ALWAYS_NOTIFY = 11

# If this is set, the amount of seconds required for Pokémon will be a sliding
# scale from MIN_TIME to MAX_TIME, from rarest to least rare, or in the order
# you specify in your NOTIFY_IDS (depending on which variable you set above)
#MAX_TIME = 600

# The following values are fake, replace them with your own keys to enable
# PushBullet notifications and/or tweeting, otherwise leave them out of your
# config or set them to None.
PB_API_KEY = 'o.9187cb7d5b857c97bfcaa8d63eaa8494'
PB_CHANNEL = 0  # set to the integer of your channel, or to None to push privately
TWITTER_CONSUMER_KEY = '53d997264eb7f6452b7bf101d'
TWITTER_CONSUMER_SECRET = '64b9ebf618829a51f8c0535b56cebc808eb3e80d3d18bf9e00'
TWITTER_ACCESS_KEY = '1dfb143d4f29-6b007a5917df2b23d0f6db951c4227cdf768b'
TWITTER_ACCESS_SECRET = 'e743ed1353b6e9a45589f061f7d08374db32229ec4a61'



# It is recommended to store the LANDMARKS object in a pickle to reduce startup
# time if you are using queries. An example script for this is available at:
# scripts/pickle_landmarks.example.py
LANDMARKS = Landmarks(query_suffix=AREA_NAME)

# Landmarks to reference when Pokémon are nearby
# If no points are specified then it will query OpenStreetMap for the coordinates
# If 1 point is provided then it will use those coordinates but not create a shape
# If 2 points are provided it will create a rectangle with its corners at those points
# If 3 or more points are provided it will create a polygon with vertices at each point
# You can specify the string to search for on OpenStreetMap with the query parameter
# If no query or points is provided it will query with the name of the landmark (and query_suffix)
# Optionally provide a set of hashtags to be used for tweets about this landmark
# Use is_area for large neighborhoods or regions
# When selecting a landmark, non-areas will be chosen first if any are close enough
# the default phrase is 'in' for areas and 'at' for non-areas, but can be overriden for either.

# since no points or query is provided, the names provided will be queried and suffixed with AREA_NAME
LANDMARKS.add('Rice Eccles Stadium', hashtags={'Utes'})
LANDMARKS.add('the Salt Lake Temple', hashtags={'TempleSquare'})

# provide two corner points to create a square for this area
LANDMARKS.add('City Creek Center', points=((40.769210, -111.893901), (40.767231, -111.888275)), hashtags={'CityCreek'})

# provide a query that is different from the landmark name so that OpenStreetMap finds the correct one
LANDMARKS.add('the State Capitol', query='Utah State Capitol Building')

## area examples ##
# query using name, override the default area phrase so that it says 'at (name)' instead of 'in'
LANDMARKS.add('the University of Utah', hashtags={'Utes'}, phrase='at', is_area=True)
# provide corner points to create a polygon of the area since OpenStreetMap does not have a shape for it
LANDMARKS.add('Yalecrest', points=((40.750263, -111.836502), (40.750377, -111.851108), (40.751515, -111.853833), (40.741212, -111.853909), (40.741188, -111.836519)), is_area=True)
