from datetime import datetime, timedelta, timezone
from collections import deque
from geopy.distance import distance, Point

from names import POKEMON_NAMES
import config


# Maintain deque of 200 most recent sightings to check for duplicates. Could
# also be used in future for making decisions about whether to notify or not
# i.e. skip species if most of the recent notifications have been about it.
recent_encounters = deque(maxlen=200)

# put relevant variables into dict for simple initalization checking
cnf = {}
for variable_name in ['PB_API_KEY', 'PB_CHANNEL', 'TWITTER_CONSUMER_KEY',
                      'TWITTER_CONSUMER_SECRET', 'TWITTER_ACCESS_KEY',
                      'TWITTER_ACCESS_SECRET', 'LANDMARKS', 'AREA_NAME',
                      'HASHTAGS', 'TZ_OFFSET']:
    try:
        variable = getattr(config, variable_name)
        cnf[variable_name] = variable
    except AttributeError:
        cnf[variable_name] = None


def notify(pokemon):
    """Send a PushBullet notification and/or a Tweet, depending on if their
    respective API keys have been set in config.
    """

    # skip if no API keys have been set in config
    if cnf['PB_API_KEY'] or cnf['TWITTER_CONSUMER_KEY']:
        time_till_hidden = pokemon['time_till_hidden_ms']
        coordinates = Point(latitude=round(pokemon['latitude'], 6),
                            longitude=round(pokemon['longitude'], 6))
        pokeid = pokemon['pokemon_data']['pokemon_id']
        encounter_id = pokemon['encounter_id']
        if encounter_id in recent_encounters:
            return (False, 'Already notified.')  # skip duplicate
        else:
            pokename = POKEMON_NAMES[pokeid]
            # do not notify if it expires in less than 3 minutes
            if time_till_hidden < 180000:
                return (False, pokename + ' was expiring too soon to notify.')

            if cnf['TZ_OFFSET']:
                now = datetime.now(timezone(timedelta(hours=cnf['TZ_OFFSET'])))
            else:
                now = datetime.now()

            if time_till_hidden > 3600000:
                # actual expiration time should be a minimum of 15 minutes away
                delta = timedelta(minutes=15)
            else:
                delta = timedelta(milliseconds=time_till_hidden)

            expire_time = (now + delta).strftime('%I:%M %p').lstrip('0')

            if time_till_hidden > 3600000:
                expire_time = 'at least ' + expire_time

            map_link = ('https://maps.google.com/maps?q=' +
                        str(coordinates.latitude) + ',' +
                        str(coordinates.longitude))
            place_string, landmark = find_landmark(coordinates)
            try:
                if landmark.hashtags:
                    cnf['HASHTAGS'] = landmark.hashtags
            except AttributeError:
                pass

            tweeted = False
            pushed = False
            if cnf['PB_API_KEY']:
                pushed = pbpush(pokename, delta, expire_time, map_link,
                                place_string)

            if (cnf['TWITTER_CONSUMER_KEY'] and
                    cnf['TWITTER_CONSUMER_SECRET'] and
                    cnf['TWITTER_ACCESS_KEY'] and
                    cnf['TWITTER_ACCESS_SECRET']):
                tweeted = tweet(pokename, expire_time, map_link, place_string,
                                coordinates)
            if tweeted and pushed:
                recent_encounters.append(encounter_id)
                return (True, 'tweeted and pushed about ' + pokename)
            elif tweeted:
                recent_encounters.append(encounter_id)
                return (True, 'tweeted about ' + pokename)
            elif pushed:
                recent_encounters.append(encounter_id)
                return (True, 'pushed about ' + pokename)
            else:
                return (False, 'Failed to notify about ' + pokename)
    else:
        return (False, 'Did not notify, no Twitter/PushBullet keys set.')


def find_landmark(coordinates):
    """ Try to create a description of the location of the coordinates based
    on nearby landmarks, or fallback to a generic description depending
    on what variables are set in the config and whether any landmarks are
    close enough.
    """

    try:
        closest_distance = None
        if cnf['LANDMARKS']:
            for landmark in cnf['LANDMARKS']:
                landmark_coordinates = landmark.center
                landmark_distance = distance(coordinates,
                                             landmark_coordinates).meters
                if (closest_distance is None) or (
                        landmark_distance < closest_distance):
                    closest_distance = landmark_distance
                    closest_landmark = landmark
        else:
            return (generic_place_string(), None)
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


def pbpush(pokename, delta, expire_time, map_link, place_string):
    """ Send a PushBullet notification either privately or to a channel,
    depending on whether or not PB_CHANNEL is set in config.
    """

    try:
        from pushbullet import Pushbullet
        pb = Pushbullet(cnf['PB_API_KEY'])
    except (ImportError, Pushbullet.errors.InvalidKeyError):
        return False

    minutes, seconds = divmod(delta.total_seconds(), 60)
    time_remaining = str(int(minutes)) + 'm' + str(round(seconds)) + 's'

    try:
        title = ('A wild ' + pokename + ' will be in ' + cnf['AREA_NAME'] +
                 ' until ' + expire_time + '!')
    except KeyError:
        title = ('A wild ' + pokename + ' will be around until ' + expire_time
                 + '!')

    body = ('It will be ' + place_string + ' until ' + expire_time + ' (' +
            time_remaining + ')')

    try:
        channel = pb.channels[cnf['PB_CHANNEL']]
        channel.push_link(title, map_link, body)
    except (IndexError, KeyError):
        pb.push_link(title, map_link, body)
    else:
        return True


def tweet(pokename, expire_time, map_link, place_string, coordinates):
    """ Create message, reduce it until it fits in a tweet, and then tweet it
    with a link to Google maps and tweet location included.
    """
    try:
        import twitter
    except ImportError:
        return False

    def generate_tag_string(hashtags):
        '''convert hashtag set to string'''
        tag_string = ''
        if hashtags:
            for hashtag in hashtags:
                tag_string += '#' + hashtag + ' '
        return tag_string

    try:
        hashtags = cnf['HASHTAGS']
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
            if (len(tweet_text) > 116) and cnf['AREA_NAME']:
                tweet_text = ('A wild ' + pokename + ' will be in ' +
                              cnf['AREA_NAME'] + ' until ' +
                              expire_time + '. ')
            if len(tweet_text) > 116:
                tweet_text = ('A wild ' + pokename + ' will be around until '
                              + expire_time + '. ')
            break

    try:
        api = twitter.Api(consumer_key=cnf['TWITTER_CONSUMER_KEY'],
                          consumer_secret=cnf['TWITTER_CONSUMER_SECRET'],
                          access_token_key=cnf['TWITTER_ACCESS_KEY'],
                          access_token_secret=cnf['TWITTER_ACCESS_SECRET'])

        api.PostUpdate(tweet_text + map_link,
                       latitude=coordinates.latitude,
                       longitude=coordinates.longitude,
                       display_coordinates=True)
    except twitter.error.TwitterError:
        return False
    else:
        return True
