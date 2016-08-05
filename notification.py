import time
from datetime import datetime
from collections import deque
from geopy.distance import distance, Point

from names import POKEMON_NAMES
import config


# Maintain deque of 200 most recent sightings to check for duplicates. Could
# also be used in future for making decisions about whether to notify or not
# i.e. skip species if most of the recent notifications have been about it.
recent_monsters = deque(maxlen=200)


def notify(pokemon):
    """Send a PushBullet notification and/or a Tweet, depending on if their
    respective API keys have been set in config.
    """

    # put relevant variables into dict for simple initalization checking
    conf = {}
    for variable_name in ['PB_API_KEY', 'PB_CHANNEL', 'TWITTER_CONSUMER_KEY',
                          'TWITTER_CONSUMER_SECRET', 'TWITTER_ACCESS_KEY',
                          'TWITTER_ACCESS_SECRET', 'LANDMARKS', 'AREA_NAME',
                          'HASHTAGS']:
        try:
            variable = getattr(config, variable_name)
            if variable is not None:
                conf[variable_name] = variable
        except AttributeError:
            pass

    # skip if no API keys have been set in config
    if ('PB_API_KEY' in conf) or ('TWITTER_CONSUMER_KEY' in conf):
        expire_timestamp = pokemon['expire_timestamp']
        coordinates = Point(latitude=round(pokemon['lat'], 6),
                            longitude=round(pokemon['lon'], 6))
        pokeid = pokemon['pokemon_id']
        current_monster = (pokeid, coordinates, int(expire_timestamp))
        if current_monster in recent_monsters:
            return  # skip duplicate
        else:
            recent_monsters.append(current_monster)
            pokename = POKEMON_NAMES[pokeid]
            expire_timestamp = pokemon['expire_timestamp']
            seconds_remaining = int(expire_timestamp - time.time())
            # do not notify if it expires in less than 3 minutes
            if seconds_remaining < 180:
                return
            expire_time = datetime.fromtimestamp(
                expire_timestamp).strftime('%I:%M %p')

            map_link = ('https://maps.google.com/maps?q=' +
                        str(coordinates.latitude) + ',' +
                        str(coordinates.longitude))
            place_string, landmark = find_landmark(coordinates, conf)
            if landmark.hashtags:
                conf['HASHTAGS'] = landmark.hashtags

            if 'PB_API_KEY' in conf:
                pbpush(pokename, seconds_remaining, expire_time, map_link,
                       place_string, conf)
            if all(x in conf for x in ['TWITTER_CONSUMER_KEY',
                                       'TWITTER_CONSUMER_SECRET',
                                       'TWITTER_ACCESS_KEY',
                                       'TWITTER_ACCESS_SECRET']):
                tweet(pokename, expire_time, map_link, place_string,
                      coordinates, conf)


def find_landmark(coordinates, conf):
    """ Try to create a description of the location of the coordinates based
    on nearby landmarks, or fallback to a generic description depending
    on what variables are set in the config and whether any landmarks are
    close enough.
    """

    try:
        closest_distance = None
        for landmark in conf['LANDMARKS']:
            landmark_coordinates = landmark.center
            landmark_distance = distance(coordinates,
                                         landmark_coordinates).meters
            if (closest_distance is None) or (
                    landmark_distance < closest_distance):
                closest_distance = landmark_distance
                closest_landmark = landmark
    except KeyError:
        return (generic_place_string(), None)

    within = closest_landmark.is_within(coordinates)
    if within:
        place_string = closest_landmark.phrase + ' ' + closest_landmark.name
    elif closest_distance < 3000:
        place_string = 'near ' + closest_landmark.name
    return (place_string, closest_landmark)


def generic_place_string():
    """ Create a place string with area name (if available)"""
    try:
        # no landmarks defined, just use area name
        place_string = 'in ' + config.AREA_NAME
        return place_string
    except NameError:
        # no landmarks or area name defined, just say 'around'
        return 'around'


def pbpush(pokename, seconds_remaining, expire_time, map_link, place_string,
           conf):
    """ Send a PushBullet notification either privately or to a channel,
    depending on whether or not PB_CHANNEL is set in config.
    """

    try:
        from pushbullet import Pushbullet
        pb = Pushbullet(conf['PB_API_KEY'])
    except (ImportError, Pushbullet.errors.InvalidKeyError):
        return

    minutes, seconds = divmod(seconds_remaining, 60)
    time_remaining = str(minutes) + 'm' + str(seconds) + 's'

    try:
        title = 'A wild ' + pokename + ' appeared ' + conf['AREA_NAME'] + '!'
    except KeyError:
        title = 'A wild ' + pokename + ' appeared!'
    body = ('It will be ' + place_string + ' until ' + expire_time + ' (' +
            time_remaining + ')')

    try:
        channel = pb.channels[conf['PB_CHANNEL']]
        channel.push_link(title, map_link, body)
    except (IndexError, KeyError):
        pb.push_link(title, map_link, body)


def tweet(pokename, expire_time, map_link, place_string, coordinates, conf):
    """ Create message, reduce it until it fits in a tweet, and then tweet it
    with a link to Google maps and tweet location included.
    """
    try:
        import twitter
    except ImportError:
        return

    def generate_tag_string(hashtags):
        '''convert hashtag set to string'''
        tag_string = ''
        if hashtags:
            for hashtag in hashtags:
                tag_string += '#' + hashtag + ' '
        return tag_string

    try:
        hashtags = conf['HASHTAGS']
    except KeyError:
        hashtags = None

    tag_string = generate_tag_string(hashtags)

    tweet_text = ('A wild ' + pokename + ' appeared! It will be ' +
                  place_string + ' until ' + expire_time + '. ' + tag_string)

    while len(tweet_text) > 116:
        if hashtags:
            hashtags.pop()
            tag_string = generate_tag_string(hashtags)
            tweet_text = ('A wild ' + pokename + ' appeared! It will be ' +
                          place_string + ' until ' + expire_time + '. ' +
                          tag_string)
        else:
            if (len(tweet_text) > 116) and ('AREA_NAME' in conf):
                tweet_text = ('A wild ' + pokename + ' appeared in ' +
                              conf['AREA_NAME'] + '! It expires at ' +
                              expire_time + '. ')
            if len(tweet_text) > 116:
                tweet_text = ('A wild ' + pokename + ' appeared! It will'
                              ' expire at ' + expire_time + '. ')
            break

    try:
        api = twitter.Api(consumer_key=conf['TWITTER_CONSUMER_KEY'],
                          consumer_secret=conf['TWITTER_CONSUMER_SECRET'],
                          access_token_key=conf['TWITTER_ACCESS_KEY'],
                          access_token_secret=conf['TWITTER_ACCESS_SECRET'])

        api.PostUpdate(tweet_text + map_link,
                       latitude=coordinates.latitude,
                       longitude=coordinates.longitude,
                       display_coordinates=True)
    except twitter.error.TwitterError:
        return
