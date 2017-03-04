### All lines that are commented out (and some that aren't) are optional ###

DB_ENGINE = 'sqlite:///db.sqlite'
#DB_ENGINE = 'mysql://user:pass@localhost/monocle'
#DB_ENGINE = 'postgresql://user:pass@localhost/monocle

AREA_NAME = 'SLC'     # the city or region you are scanning
LANGUAGE = 'EN'       # ISO 639-1 codes EN, DE, FR, and ZH for Pokémon names.
MAX_CAPTCHAS = 100    # stop launching new visits if this many CAPTCHAs are pending
SCAN_DELAY = 10       # wait at least this many seconds before scanning with the same account
SPEED_UNIT = 'miles'  # valid options are 'miles', 'kilometers', 'meters'
SPEED_LIMIT = 19.5    # limit worker speed to this many SPEED_UNITs per hour

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

# ensure that you visit within this many meters of every part of your map during bootstrap
#BOOTSTRAP_RADIUS = 240

GIVE_UP_KNOWN = 75   # try to find a worker for a known spawn for this many seconds before giving up
GIVE_UP_UNKNOWN = 60 # try to find a worker for an unknown point for this many seconds before giving up
SKIP_SPAWN = 90      # don't even try to find a worker for a spawn if the spawn time was more than this many seconds ago

# filename of accounts CSV
ACCOUNTS_CSV = 'accounts.csv'

# the directory that the pickles folder, socket, CSV, etc. will go in
# defaults to working directory if not set
#DIRECTORY = None

# Limit the number of simultaneous logins to this many at a time.
# Lower numbers will increase the amount of time it takes for all workers to
# get started but are recommended to avoid suddenly flooding the servers with
# accounts and arousing suspicion.
SIMULTANEOUS_LOGINS = 4

# Limit the number of workers simulating the app startup process simultaneously.
SIMULTANEOUS_SIMULATION = 10

# Immediately select workers whose speed are below (SPEED_UNIT)p/h instead of
# continuing to try to find the worker with the lowest speed.
# May increase clustering if you have a high density of workers.
#GOOD_ENOUGH = 4

# Seconds to sleep after failing to find an eligible worker before trying again.
SEARCH_SLEEP = 2.5

## alternatively define a Polygon to use as boundaries (requires shapely)
## if BOUNDARIES is set, STAY_WITHIN_MAP will be ignored
## more information available in the shapely manual:
## http://toblerity.org/shapely/manual.html#polygons
#from shapely.geometry import Polygon
#BOUNDARIES = Polygon(((40.799609, -111.948556), (40.792749, -111.887341), (40.779264, -111.838078), (40.761410, -111.817908), (40.728636, -111.805293), (40.688833, -111.785564), (40.689768, -111.919389), (40.750461, -111.949938)))

# key for Bossland's hashing server, otherwise the old hashing lib will be used
#HASH_KEY = '9d87af14461b93cb3605'  # this key is fake

# Skip PokéStop spinning and egg incubation if your request rate is too high
# for your hashing subscription.
# e.g.
#   75/150 hashes available 35/60 seconds passed => fine
#   70/150 hashes available 30/60 seconds passed => throttle (only scan)
# value: how many requests to keep as spare (0.1 = 10%), False to disable
#SMART_THROTTLE = 0.1

# Swap the worker that has seen the fewest Pokémon every x seconds
# Defaults to whatever will allow every worker to be swapped within 6 hours
#SWAP_OLDEST = 300  # 5 minutes
# Only swap if it's been active for more than x minutes
#MINIMUM_RUNTIME = 10

### these next 6 options use more requests but look more like the real client
APP_SIMULATION = True     # mimic the actual app's login requests
COMPLETE_TUTORIAL = True  # complete the tutorial process and configure avatar for all accounts that haven't yet
INCUBATE_EGGS = False        # incubate eggs if available

## encounter Pokémon to store IVs.
## valid options:
# 'all' will encounter every Pokémon that hasn't been already been encountered
# 'notifying' will encounter Pokémon that are eligible for notifications
# None will never encounter Pokémon
ENCOUNTER = None

# PokéStops
SPIN_POKESTOPS = False # spin all PokéStops that are within range
SPIN_COOLDOWN = 300    # spin only one PokéStop every n seconds (default 300)

# minimum number of each item to keep if the bag is cleaned
# bag cleaning is disabled if this is not present or is commented out
''' # triple quotes are comments, remove them to use this ITEM_LIMITS example
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
    701:  20,  # Razz Berry
    702:  20,  # Bluk Berry
    703:  20,  # Nanab Berry
    704:  20,  # Wepar Berry
    705:  20,  # Pinap Berry
}
'''

# Update the console output every x seconds
REFRESH_RATE = 0.6  # 600ms
# Update the seen/speed/visit/speed stats every x seconds
STAT_REFRESH = 5

# sent with GET_PLAYER requests, should match your region
PLAYER_LOCALE = {'country': 'US', 'language': 'en', 'timezone': 'America/Denver'}

# retry a request after failure this many times before giving up
MAX_RETRIES = 3

# number of seconds before timing out on a login request
LOGIN_TIMEOUT = 2.5

# add spawn points reported in cell_ids to the unknown spawns list
# disable if your workers already have more than enough points to visit
MORE_POINTS = True

# Set to True to kill the scanner when a newer version is forced
#FORCED_KILL = False

# exclude these Pokémon from the map by default (only visible in trash layer)
TRASH_IDS = (
    16, 19, 21, 29, 32, 41, 46, 48, 50, 52, 56, 74, 77, 96, 111, 133,
    161, 163, 167, 177, 183, 191, 194
)

# include these Pokémon on the "rare" report
RARE_IDS = (
    3, 6, 9, 45, 62, 71, 80, 85, 87, 89, 91, 94, 114, 130, 131, 134
)

from datetime import datetime
REPORT_SINCE = datetime(2017, 2, 17)  # base reports on data from after this date

# used for altitude queries and maps in reports
GOOGLE_MAPS_KEY = 'OYOgW1wryrp2RKJ81u7BLvHfYUA6aArIyuQCXu4'  # this key is fake
#ALT_RANGE = (1250, 1450)  # Fall back to altitudes in this range if Google query fails
REPORT_MAPS = True  # Show maps on reports

## Automatically resolve captchas using 2Captcha key.
#CAPTCHA_KEY = '1abc234de56fab7c89012d34e56fa7b8'
## the number of CAPTCHAs an account is allowed to receive before being swapped out
#CAPTCHAS_ALLOWED = 3
## Get new accounts from the CAPTCHA queue first if it's not empty
#FAVOR_CAPTCHA = True

MAP_WORKERS = True  # allow displaying the live location of workers on the map

# unix timestamp of last spawn point migration, spawn times learned before this will be ignored
LAST_MIGRATION = 1481932800  # Dec. 17th, 2016

## Map data provider and appearance, previews available at:
## https://leaflet-extras.github.io/leaflet-providers/preview/
#MAP_PROVIDER_URL = '//{s}.tile.openstreetmap.org/{z}/{x}/{y}.png'
#MAP_PROVIDER_ATTRIBUTION = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'

# set of proxy addresses and ports
# SOCKS requires aiosocks to be installed
#PROXIES = {'http://127.0.0.1:8080', 'https://127.0.0.1:8443', 'socks5://127.0.0.1:1080'}

# convert spawn_id to integer for more efficient DB storage, set to False if
# using an old database since the data types are incompatible.
#SPAWN_ID_INT = True

# Bytestring key to authenticate with manager for inter-process communication
#AUTHKEY = b'm3wtw0'
# Address to use for manager, leave commented if you're not sure.
#MANAGER_ADDRESS = r'\\.\pipe\monocle'  # must be in this format for Windows
#MANAGER_ADDRESS = 'monocle.sock'       # the socket name for Unix systems
#MANAGER_ADDRESS = ('127.0.0.1', 5002)  # could be used for CAPTCHA solving and live worker maps on remote systems

# Store the cell IDs so that they don't have to be recalculated every visit.
# Enabling will increase memory usage.
#CACHE_CELLS = False

# Only for use with web-sanic (requires PostgreSQL)
#DB = {'host': '127.0.0.1', 'user': 'monocle_role', 'password': 'pik4chu', 'port': '5432', 'database': 'monocle'}

# Disable to use Python's event loop even if uvloop is installed
#UVLOOP = True

# The number of coroutines that are allowed to run simultaneously.
#COROUTINES_LIMIT = GRID[0] * GRID[1]

### FRONTEND CONFIGURATION
LOAD_CUSTOM_HTML_FILE = False # File path MUST be 'templates/custom.html'
LOAD_CUSTOM_CSS_FILE = False  # File path MUST be 'static/css/custom.css'
LOAD_CUSTOM_JS_FILE = False  # File path MUST be 'static/js/custom.js'

#FB_PAGE_ID = None
#TWITTER_SCREEN_NAME = None  # Username withouth '@' char
#DISCORD_INVITE_ID = None
#TELEGRAM_USERNAME = None  # Username withouth '@' char

## Variables below will be used as default values on frontend
FIXED_OPACITY = False  # Make marker opacity independent of remaining time
SHOW_TIMER = False  # Show remaining time on a label under each pokemon marker

### OPTIONS BELOW THIS POINT ARE ONLY NECESSARY FOR NOTIFICATIONS ###
NOTIFY = False  # enable notifications

# create images with Pokémon stats for Tweets
# requires cairo and ENCOUNTER = 'notifying' or 'all'
TWEET_IMAGES = True

# As many hashtags as can fit will be included in your tweets, these will
# be combined with landmark-specific hashtags (if applicable).
HASHTAGS = {AREA_NAME, 'Monocle', 'PokemonGO'}
#TZ_OFFSET = 0  # UTC offset in hours (if different from system time)

# the required number of seconds remaining to notify about a Pokémon
TIME_REQUIRED = 600  # 10 minutes

### Only set either the NOTIFY_RANKING or NOTIFY_IDS, not both!
# The (x) rarest Pokémon will be eligible for notification. Whether a
# notification is sent or not depends on its score, as explained below.
NOTIFY_RANKING = 90

# Pokémon to potentially notify about, in order of preference.
# The first in the list will have a rarity score of 1, the last will be 0.
#NOTIFY_IDS = (130, 89, 131, 3, 9, 134, 62, 94, 91, 87, 71, 45, 85, 114, 80, 6)

# Sightings of the top (x) will always be notified about, even if below TIME_REQUIRED
# (ignored if using NOTIFY_IDS instead of NOTIFY_RANKING)
ALWAYS_NOTIFY = 14

# Always notify about the following Pokémon even if their time remaining or scores are not high enough
#ALWAYS_NOTIFY_IDS = {89, 130, 144, 145, 146, 150, 151}

# Never notify about the following Pokémon, even if they would otherwise be eligible
#NEVER_NOTIFY_IDS = TRASH_IDS

# Override the rarity score for particular Pokémon
# format is: {pokemon_id: rarity_score}
#RARITY_OVERRIDE = {148: 0.6, 149: 0.9}

# Ignore IV score and only base decision on rarity score (default if IVs not known)
#IGNORE_IVS = False

# Ignore rarity score and only base decision on IV score
#IGNORE_RARITY = False

# The Pokémon score required to notify goes on a sliding scale from INITIAL_SCORE
# to MINIMUM_SCORE over the course of FULL_TIME seconds following a notification
# Pokémon scores are an average of the Pokémon's rarity score and IV score (from 0 to 1)
# If NOTIFY_RANKING is 90, the 90th most common Pokémon will have a rarity of score 0, the rarest will be 1.
# IV score is the IV sum divided by 45 (perfect IVs).
FULL_TIME = 1800  # the number of seconds after a notification when only MINIMUM_SCORE will be required
INITIAL_SCORE = 0.7  # the required score immediately after a notification
MINIMUM_SCORE = 0.4  # the required score after FULL_TIME seconds have passed

### The following values are fake, replace them with your own keys to enable
### notifications, otherwise exclude them from your config
### You must provide keys for at least one service to use notifications.

#PB_API_KEY = 'o.9187cb7d5b857c97bfcaa8d63eaa8494'
#PB_CHANNEL = 0  # set to the integer of your channel, or to None to push privately

#TWITTER_CONSUMER_KEY = '53d997264eb7f6452b7bf101d'
#TWITTER_CONSUMER_SECRET = '64b9ebf618829a51f8c0535b56cebc808eb3e80d3d18bf9e00'
#TWITTER_ACCESS_KEY = '1dfb143d4f29-6b007a5917df2b23d0f6db951c4227cdf768b'
#TWITTER_ACCESS_SECRET = 'e743ed1353b6e9a45589f061f7d08374db32229ec4a61'

## Telegram bot token is the one Botfather sends to you after completing bot creation.
## Chat ID can be two different values:
## 1) '@channel_name' for channels
## 2) Your chat_id if you will use your own account. To retrieve your ID, write to your bot and check this URL:
##     https://api.telegram.org/bot<BOT_TOKEN_HERE>/getUpdates
#TELEGRAM_BOT_TOKEN = '123456789:AA12345qT6QDd12345RekXSQeoZBXVt-AAA'
#TELEGRAM_CHAT_ID = '@your_channel'

#WEBHOOKS = {'http://127.0.0.1:4000'}


##### Referencing landmarks in your tweets/notifications

#### It is recommended to store the LANDMARKS object in a pickle to reduce startup
#### time if you are using queries. An example script for this is in:
#### scripts/pickle_landmarks.example.py
#from pickle import load
#with open('pickles/landmarks.pickle', 'rb') as f:
#    LANDMARKS = load(f)

### if you do pickle it, just load the pickle and omit everything below this point

#from monocle.landmarks import Landmarks
#LANDMARKS = Landmarks(query_suffix=AREA_NAME)

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

## since no points or query is provided, the names provided will be queried and suffixed with AREA_NAME
#LANDMARKS.add('Rice Eccles Stadium', shortname='Rice Eccles', hashtags={'Utes'})
#LANDMARKS.add('the Salt Lake Temple', shortname='the temple', hashtags={'TempleSquare'})

## provide two corner points to create a square for this area
#LANDMARKS.add('City Creek Center', points=((40.769210, -111.893901), (40.767231, -111.888275)), hashtags={'CityCreek'})

## provide a query that is different from the landmark name so that OpenStreetMap finds the correct one
#LANDMARKS.add('the State Capitol', shortname='the Capitol', query='Utah State Capitol Building')

### area examples ###
## query using name, override the default area phrase so that it says 'at (name)' instead of 'in'
#LANDMARKS.add('the University of Utah', shortname='the U of U', hashtags={'Utes'}, phrase='at', is_area=True)
## provide corner points to create a polygon of the area since OpenStreetMap does not have a shape for it
#LANDMARKS.add('Yalecrest', points=((40.750263, -111.836502), (40.750377, -111.851108), (40.751515, -111.853833), (40.741212, -111.853909), (40.741188, -111.836519)), is_area=True)
