# coding: utf-8
from datetime import datetime

DB_ENGINE = 'sqlite:///db.sqlite'

AREA_NAME = 'Salt Lake City'
LANGUAGE = 'EN'  # ISO 639-1 codes EN, DE, FR, and ZH currently supported for Pokemon names.
MAP_START = (12.3456, 34.5678)
MAP_END = (13.4567, 35.6789)
GRID = (2, 2)  # row, column. ideally provide more accounts than the product of these numbers so that they can be swapped
MAX_CAPTCHAS = 100  # stop launching new visits if this many CAPTCHAs are pending
SCAN_DELAY = 10.1  # do not visit within this many seconds of the last visit
SPEED_LIMIT = 19  # do not travel over this many MPH

# your key for the hashing server, otherwise the old hashing lib will be used
#HASH_KEY = '9d87af14461b93cb3605'  # this key is fake

### these options use more requests but will make you look more like the real client
APP_SIMULATION = True  # mimic the actual app's login requests
ENCOUNTER = None  # encounter pokemon to store IVs. (currently must be 'notifying' or 'all' to use notifications)
#SPIN_POKESTOPS = False  # spin all pokestops that are within range (until inventory is full)
#COMPLETE_TUTORIAL = False  # run through the tutorial process and configure avatar for all accounts that haven't yet

# If accounts use the same provider and password you can set defaults here
# and omit them from the accounts list.
#PASS = 'pik4chu'
#PROVIDER = 'ptc'

## Device information will be generated for you if you do not provide it.
## valid account formats (without PASS and PROVIDER set):
# (username, password, provider, iPhone, iOS, device_id)
# (username, password, provider)
## valid account formats (with PASS and PROVIDER set):
# (username, iPhone, iOS, device_id)
# (username,)
ACCOUNTS = [
    ('ash_ketchum', 'pik4chu', 'ptc'),
    ('ziemniak_kalafior', 'ogorek', 'google'),
    ('noideawhattoputhere', 's3cr3t', 'ptc'),
    ('misty', 'bulbus4ur', 'ptc')
]

# only show these when enabling the trash layer on the map
TRASH_IDS = (
    16, 19, 21, 29, 32, 41, 46, 48, 50, 52, 56, 58, 74, 77, 81, 96, 111, 133
)

# include these on the "rare" report
RARE_IDS = (
    83, 115, 122, 132, 144, 145, 146, 150, 151, 130, 89, 3, 9, 131, 134, 62, 148, 94, 91, 87, 71, 45, 85, 114, 80, 6, 117, 121, 2, 8, 88, 136, 73, 103, 110, 137, 55, 28, 119, 68, 139, 141, 149, 65, 61, 142, 101, 40, 99, 38
)

MAP_WORKERS = True  # allow displaying the live location of workers on the map

REPORT_SINCE = datetime(2016, 11, 1)
GOOGLE_MAPS_KEY = 's3cr3t'

#ALT_RANGE = (1250, 1450)  # Fall back to altitudes in this range if Google query fails

#LAST_MIGRATION = 1481932800  # unix timestamp of last spawn point migration

#MAP_PROVIDER_URL = '//{s}.tile.osm.org/{z}/{x}/{y}.png'
#MAP_PROVIDER_ATTRIBUTION = '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'

# proxy address and port or tuple of proxy addresses and ports.
#PROXIES = ('socks5://127.0.0.1:9050',
#           'socks5://127.0.0.1:9051')

# convert spawn_id to integer for more efficient DB storage, set to False if
# using an old database since the data types are incompatible.
#SPAWN_ID_INT = True

# Bytestring key to authenticate with manager for inter-process communication
#AUTHKEY = b'm3wtw0'


# the number of threads to use for simultaneous API requests
#NETWORK_THREADS = round((GRID[0] * GRID[1]) / 10) + 1

### ignore spawn points that are not within the defined polygon
#from shapely.geometry import Polygon
#BOUNDARIES = BOUNDARIES = Polygon(((40.799609, -111.948556), (40.792749, -111.887341), (40.779264, -111.838078), (40.761410, -111.817908), (40.728636, -111.805293), (40.688833, -111.785564), (40.689768, -111.919389), (40.750461, -111.949938)))

'''
### OPTIONS BELOW THIS POINT ARE ONLY NECESSARY FOR NOTIFICATIONS ###
from landmarks import Landmarks

NOTIFY = True  # enable notifications

# As many hashtags as can fit will be included in your tweets, these will
# be combined with landmark-specific hashtags (if applicable).
HASHTAGS = {AREA_NAME, 'Pokeminer+', 'PokemonGO'}
#TZ_OFFSET = 0  # UTC offset for reported times (if different from system time)

# the required number of seconds remaining to notify about a Pokémon
TIME_REQUIRED = 300

# Sightings of the top (x) will always be notified about, even if below TIME_REQUIRED
ALWAYS_NOTIFY = 14

# The (x) rarest pokemon will be eligible for notification. Whether a 
# notification is sent or not depends on its score, as explained below
NOTIFY_RANKING = 90

# The Pokemon score required to notify is on a sliding scale from INITIAL_SCORE
# to MAXIMUM_SCORE over the course of FULL_TIME seconds from last notification
# Pokemon scores are an average of the Pokemon's rarity score and IV score (from 0 to 1)
# If NOTIFY_RANKING is 90, the 90th most common Pokemon will have a rarity of score 0, the rarest will be 1.
# Perfect IVs have a score of 1, the worst IVs have a score of 0. Attack IV is weighted more heavily.
FULL_TIME = 1680  # the number of seconds from last notification before only MINIMUM_SCORE will be required 
INITIAL_SCORE = 0.7  # the score required to notify immediately after a notification
MINIMUM_SCORE = 0.55  # the score required to notify after FULL_TIME seconds have passed

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

# if you pickle it, just load the pickle and omit the rest
#with open('pickles/landmarks.pickle', 'rb') as f:
#    LANDMARKS = load(f)

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
LANDMARKS.add('Rice Eccles Stadium', shortname='Rice Eccles', hashtags={'Utes'})
LANDMARKS.add('the Salt Lake Temple', shortname='the temple', hashtags={'TempleSquare'})

# provide two corner points to create a square for this area
LANDMARKS.add('City Creek Center', points=((40.769210, -111.893901), (40.767231, -111.888275)), hashtags={'CityCreek'})

# provide a query that is different from the landmark name so that OpenStreetMap finds the correct one
LANDMARKS.add('the State Capitol', shortname='the Capitol', query='Utah State Capitol Building')

## area examples ##
# query using name, override the default area phrase so that it says 'at (name)' instead of 'in'
#LANDMARKS.add('the University of Utah', shortname='the U of U', hashtags={'Utes'}, phrase='at', is_area=True)
# provide corner points to create a polygon of the area since OpenStreetMap does not have a shape for it
#LANDMARKS.add('Yalecrest', points=((40.750263, -111.836502), (40.750377, -111.851108), (40.751515, -111.853833), (40.741212, -111.853909), (40.741188, -111.836519)), is_area=True)
'''
