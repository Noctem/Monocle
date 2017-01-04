### All lines that are commented out (and some that aren't) are optional ###

DB_ENGINE = 'sqlite:///db.sqlite'
#DB_ENGINE = 'mysql://user:pass@localhost/pokeminer'
#DB_ENGINE = 'postgresql://user:pass@localhost/pokeminer

AREA_NAME = 'SLC'   # the city or region you are scanning
LANGUAGE = 'EN'     # ISO 639-1 codes EN, DE, FR, and ZH for Pokémon names.
MAX_CAPTCHAS = 100  # stop launching new visits if this many CAPTCHAs are pending
SCAN_DELAY = 10.1   # wait at least this many seconds before scanning with the same account
SPEED_LIMIT = 19    # don't travel over this many miles per hour (sorry non-Americans)

# The number of simultaneous workers will be these two numbers multiplied.
# On the initial run, workers will arrange themselves in a grid across the
# rectangle you defined with MAP_START and MAP_END.
# The rows/columns will also be used for the dot grid in the console output.
# Provide more accounts than the product of your grid to allow swapping.
GRID = (4, 4)  # rows, columns

# the corner points of a rectangle for your workers to spread out over before
# any spawn points have been discovered
MAP_START = (40.7913, -111.9398)
MAP_END = (40.7143, -111.8046)

# do not visit spawn points outside of your MAP_START and MAP_END rectangle
# the boundaries will be the rectangle created by MAP_START and MAP_END, unless
STAY_WITHIN_MAP = True

# Limit the number of simultaneous logins (and app simulations) to this many.
# Lower numbers will increase the amount of time it takes for all workers to
# get started but are recommended to avoid suddenly flooding the servers with
# accounts and arousing suspicion.
SIMULTANEOUS_LOGINS = 4

## alternatively define a Polygon to use as boundaries (requires shapely)
## if BOUNDARIES is set, STAY_WITHIN_MAP will be ignored
## more information available in the shapely manual:
## http://toblerity.org/shapely/manual.html#polygons
#from shapely.geometry import Polygon
#BOUNDARIES = Polygon(((40.799609, -111.948556), (40.792749, -111.887341), (40.779264, -111.838078), (40.761410, -111.817908), (40.728636, -111.805293), (40.688833, -111.785564), (40.689768, -111.919389), (40.750461, -111.949938)))

# If accounts use the same provider and password you can set defaults here
# and omit them from the accounts list.
#PASS = 'pik4chu'
#PROVIDER = 'ptc'

### Device information will be generated for you if you do not provide it.
### Account details are automatically retained in pickles/accounts.pickle
## valid account formats (without PASS and PROVIDER set):
# (username, password, provider, iPhone, iOS, device_id)
# (username, password, provider)
## valid account formats (with PASS and PROVIDER set):
# (username, iPhone, iOS, device_id)
# [username]
ACCOUNTS = [
    ('ash_ketchum', 'pik4chu', 'ptc'),
    ('ziemniak_kalafior', 'ogorek', 'google'),
    ('noideawhattoputhere', 's3cr3t', 'ptc'),
    ('misty', 'bulbus4ur', 'ptc')
]

# key for Bossland's hashing server, otherwise the old hashing lib will be used
#HASH_KEY = '9d87af14461b93cb3605'  # this key is fake


### these next 5 options use more requests but look more like the real client
APP_SIMULATION = True     # mimic the actual app's login requests
COMPLETE_TUTORIAL = True  # complete the tutorial process and configure avatar for all accounts that haven't yet

## encounter Pokémon to store IVs.
## valid options:
# 'all' will encounter every Pokémon that hasn't been already been encountered
# 'notifying' will encounter Pokémon that are eligible for notifications
# None will never encounter Pokémon
ENCOUNTER = None

# spin all PokéStops that are within range
SPIN_POKESTOPS = False

# minimum number of each item to keep if the bag is cleaned
# remove or set to None to disable bag cleaning
# automatically disabled if SPIN_POKESTOPS is disabled
ITEM_LIMITS = {
    1:    20,  # Poké Ball
    2:    50,  # Great Ball
    3:   100,  # Ultra Ball
    101:   0,  # Potion
    102:   0,  # Super Potion
    103:   0,  # Hyper Potion
    104:  40,  # Max Potion
    201:   0,  # Revive
    202:  40,  # Max Revive
}


# retry a request after failure this many times before giving up
MAX_RETRIES = 3


# exclude these Pokémon from the map by default (only visible in trash layer)
TRASH_IDS = (
    16, 19, 21, 29, 32, 41, 46, 48, 50, 52, 56, 74, 77, 96, 111, 133
)

# include these Pokémon on the "rare" report
RARE_IDS = (
    3, 6, 9, 45, 62, 71, 80, 85, 87, 89, 91, 94, 114, 130, 131, 134
)

# the number of threads to use for simultaneous API requests
#NETWORK_THREADS = round((GRID[0] * GRID[1]) / 15) + 1

from datetime import datetime
REPORT_SINCE = datetime(2016, 12, 17)  # base reports on data from after this date

# used for altitude queries and maps in reports
GOOGLE_MAPS_KEY = 'OYOgW1wryrp2RKJ81u7BLvHfYUA6aArIyuQCXu4'  # this key is fake
#ALT_RANGE = (1250, 1450)  # Fall back to altitudes in this range if Google query fails

MAP_WORKERS = True  # allow displaying the live location of workers on the map

# unix timestamp of last spawn point migration, spawn times learned before this will be ignored
LAST_MIGRATION = 1481932800  # Dec. 17th, 2016

## Map data provider and appearance, previews available at:
## https://leaflet-extras.github.io/leaflet-providers/preview/
#MAP_PROVIDER_URL = '//{s}.tile.osm.org/{z}/{x}/{y}.png'
#MAP_PROVIDER_ATTRIBUTION = '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'

# set of proxy addresses and ports
#PROXIES = {'socks5://127.0.0.1:1080', 'socks5://127.0.0.1:1081'}

# convert spawn_id to integer for more efficient DB storage, set to False if
# using an old database since the data types are incompatible.
#SPAWN_ID_INT = True

# Bytestring key to authenticate with manager for inter-process communication
#AUTHKEY = b'm3wtw0'

### OPTIONS BELOW THIS POINT ARE ONLY NECESSARY FOR NOTIFICATIONS ###
'''
NOTIFY = True  # enable notifications

# As many hashtags as can fit will be included in your tweets, these will
# be combined with landmark-specific hashtags (if applicable).
HASHTAGS = {AREA_NAME, 'Pokeminer+', 'PokemonGO'}
#TZ_OFFSET = 0  # UTC offset in hours (if different from system time)

# the required number of seconds remaining to notify about a Pokémon
TIME_REQUIRED = 600  # 10 minutes

# Sightings of the top (x) will always be notified about, even if below TIME_REQUIRED
ALWAYS_NOTIFY = 14

# The (x) rarest Pokémon will be eligible for notification. Whether a
# notification is sent or not depends on its score, as explained below.
NOTIFY_RANKING = 90

# The Pokémon score required to notify is on a sliding scale from INITIAL_SCORE
# to MAXIMUM_SCORE over the course of FULL_TIME seconds following a notification
# Pokémon scores are an average of the Pokémon's rarity score and IV score (from 0 to 1)
# If NOTIFY_RANKING is 90, the 90th most common Pokémon will have a rarity of score 0, the rarest will be 1.
# Perfect IVs have a score of 1, the worst IVs have a score of 0. Attack IV is weighted more heavily.
FULL_TIME = 1680  # the number of seconds from last notification before only MINIMUM_SCORE will be required 
INITIAL_SCORE = 0.7  # the score required to notify immediately after a notification
MINIMUM_SCORE = 0.55  # the score required to notify after FULL_TIME seconds have passed

### The following values are fake, replace them with your own keys to enable
### PushBullet notifications and/or tweeting, otherwise leave them out of your
### config or set them to None.
## you must provide keys for at least one service (Twitter and/or PushBullet) to use notifications
#PB_API_KEY = 'o.9187cb7d5b857c97bfcaa8d63eaa8494'
#PB_CHANNEL = 0  # set to the integer of your channel, or to None to push privately
#TWITTER_CONSUMER_KEY = '53d997264eb7f6452b7bf101d'
#TWITTER_CONSUMER_SECRET = '64b9ebf618829a51f8c0535b56cebc808eb3e80d3d18bf9e00'
#TWITTER_ACCESS_KEY = '1dfb143d4f29-6b007a5917df2b23d0f6db951c4227cdf768b'
#TWITTER_ACCESS_SECRET = 'e743ed1353b6e9a45589f061f7d08374db32229ec4a61'

#### It is recommended to store the LANDMARKS object in a pickle to reduce startup
#### time if you are using queries. An example script for this is in:
#### scripts/pickle_landmarks.example.py

#from pickle import load
#with open('pickles/landmarks.pickle', 'rb') as f:
#    LANDMARKS = load(f)

### if you do pickle it, just load the pickle and omit everything below this point

from landmarks import Landmarks
LANDMARKS = Landmarks(query_suffix=AREA_NAME)

# Landmarks to reference when Pokémon are nearby
# If no points are specified then it will query OpenStreetMap for the coordinates
# If 1 point is provided then it will use those coordinates but not create a shape
# If 2 points are provided it will create a rectangle with its corners at those points
# If 3 or more points are provided it will create a polygon with vertices at each point
# You can specify the string to search for on OpenStreetMap with the query parameter
# If no query or points is provided it will query with the name of the landmark (and query_suffix)
# Optionally provide a set of hashtags to be used for tweets about this landmark
# Use is_area for neighborhoods, regions, etc.
# When selecting a landmark, non-areas will be chosen first if any are close enough
# the default phrase is 'in' for areas and 'at' for non-areas, but can be overriden for either.

### replace these with well-known places in your area

# since no points or query is provided, the names provided will be queried and suffixed with AREA_NAME
LANDMARKS.add('Rice Eccles Stadium', shortname='Rice Eccles', hashtags={'Utes'})
LANDMARKS.add('the Salt Lake Temple', shortname='the temple', hashtags={'TempleSquare'})

# provide two corner points to create a square for this area
LANDMARKS.add('City Creek Center', points=((40.769210, -111.893901), (40.767231, -111.888275)), hashtags={'CityCreek'})

# provide a query that is different from the landmark name so that OpenStreetMap finds the correct one
LANDMARKS.add('the State Capitol', shortname='the Capitol', query='Utah State Capitol Building')

## area examples ##
# query using name, override the default area phrase so that it says 'at (name)' instead of 'in'
LANDMARKS.add('the University of Utah', shortname='the U of U', hashtags={'Utes'}, phrase='at', is_area=True)
# provide corner points to create a polygon of the area since OpenStreetMap does not have a shape for it
LANDMARKS.add('Yalecrest', points=((40.750263, -111.836502), (40.750377, -111.851108), (40.751515, -111.853833), (40.741212, -111.853909), (40.741188, -111.836519)), is_area=True)
'''
