from datetime import datetime, timedelta, timezone
from collections import deque
from geopy.distance import distance, Point

from names import POKEMON_NAMES
import config
import db

for variable_name in ['PB_API_KEY', 'PB_CHANNEL', 'TWITTER_CONSUMER_KEY',
                      'TWITTER_CONSUMER_SECRET', 'TWITTER_ACCESS_KEY',
                      'TWITTER_ACCESS_SECRET', 'LANDMARKS', 'AREA_NAME',
                      'HASHTAGS', 'TZ_OFFSET', 'MAXIMUM_TIME',
                      'ALWAYS_NOTIFY', 'NOTIFY_RANKING']:
    if not hasattr(config, variable_name):
        setattr(config, variable_name, None)

class Notifier:
    def __init__(self):
        self.recent_notifications = deque(maxlen=200)
        self.set_pokemon_ranking()
        self.set_required_times()

    def set_pokemon_ranking(self):
        session = db.Session()
        self.pokemon_ranking = db.get_pokemon_ranking(session)
        session.close()

    def is_worthy(self, pokemon_id):
        return pokemon_id in self.pokemon_ranking[0:config.NOTIFY_RANKING]

    def set_required_times(self):
        self.time_required = dict()
        for pokemon_id in self.pokemon_ranking[0:config.ALWAYS_NOTIFY]:
            self.time_required[pokemon_id] = 0
        required_time = 0
        increment = config.MAXIMUM_TIME / (config.NOTIFY_RANKING - config.ALWAYS_NOTIFY)
        for pokemon_id in self.pokemon_ranking[config.ALWAYS_NOTIFY:config.NOTIFY_RANKING]:
            required_time += increment
            self.time_required[pokemon_id] = int(required_time)

    def already_notified(self, pokemon):
        return pokemon['encounter_id'] in self.recent_notifications

    def notify(self, pokemon):
        """Send a PushBullet notification and/or a Tweet, depending on if their
        respective API keys have been set in config.
        """

        # skip if no API keys have been set in config
        if config.PB_API_KEY or config.TWITTER_CONSUMER_KEY:
            time_till_hidden = pokemon['time_till_hidden_ms'] / 1000
            coordinates = Point(latitude=round(pokemon['latitude'], 6),
                                longitude=round(pokemon['longitude'], 6))
            pokeid = pokemon['pokemon_data']['pokemon_id']
            encounter_id = pokemon['encounter_id']
            if encounter_id in self.recent_notifications:
                return (False, 'Already notified.')  # skip duplicate
            else:
                pokename = POKEMON_NAMES[pokeid]

                if time_till_hidden < self.time_required[pokeid]:
                    return (False, pokename +' was expiring too soon to notify. '
                            + str(time_till_hidden) + 's')

                if config.TZ_OFFSET:
                    now = datetime.now(timezone(timedelta(hours=config.TZ_OFFSET)))
                else:
                    now = datetime.now()

                if time_till_hidden > 3600:
                    # actual expiration time should be a minimum of 15 minutes away
                    delta = timedelta(minutes=15)
                else:
                    delta = timedelta(seconds=time_till_hidden)

                expire_time = (now + delta).strftime('%I:%M %p').lstrip('0')

                if time_till_hidden > 3600:
                    expire_time = 'at least ' + expire_time

                map_link = ('https://maps.google.com/maps?q=' +
                            str(coordinates.latitude) + ',' +
                            str(coordinates.longitude))
                landmark = config.LANDMARKS.find_landmark((coordinates.latitude, coordinates.longitude))
                place_string = landmark.generate_string((coordinates.latitude, coordinates.longitude))
                try:
                    if landmark.hashtags:
                        config.HASHTAGS = landmark.hashtags
                except AttributeError:
                    pass

                tweeted = False
                pushed = False
                if config.PB_API_KEY:
                    pushed = self.pbpush(pokename, delta, expire_time, map_link,
                                    place_string)

                if (config.TWITTER_CONSUMER_KEY and
                        config.TWITTER_CONSUMER_SECRET and
                        config.TWITTER_ACCESS_KEY and
                        config.TWITTER_ACCESS_SECRET):
                    tweeted = self.tweet(pokename, expire_time, map_link, place_string,
                                    coordinates)
                if tweeted and pushed:
                    self.recent_notifications.append(encounter_id)
                    return (True, 'tweeted and pushed about ' + pokename)
                elif tweeted:
                    self.recent_notifications.append(encounter_id)
                    return (True, 'tweeted about ' + pokename)
                elif pushed:
                    self.recent_notifications.append(encounter_id)
                    return (True, 'pushed about ' + pokename)
                else:
                    return (False, 'Failed to notify about ' + pokename)
        else:
            return (False, 'Did not notify, no Twitter/PushBullet keys set.')

    def generic_place_string(self):
        """ Create a place string with area name (if available)"""
        try:
            # no landmarks defined, just use area name
            place_string = 'in ' + config.AREA_NAME
            return place_string
        except NameError:
            # no landmarks or area name defined, just say 'around'
            return 'around'

    def pbpush(self, pokename, delta, expire_time, map_link, place_string):
        """ Send a PushBullet notification either privately or to a channel,
        depending on whether or not PB_CHANNEL is set in config.
        """

        try:
            from pushbullet import Pushbullet
            pb = Pushbullet(config.PB_API_KEY)
        except (ImportError, Pushbullet.errors.InvalidKeyError):
            return False

        minutes, seconds = divmod(delta.total_seconds(), 60)
        time_remaining = str(int(minutes)) + 'm' + str(round(seconds)) + 's'

        try:
            title = ('A wild ' + pokename + ' will be in ' + config.AREA_NAME +
                     ' until ' + expire_time + '!')
        except KeyError:
            title = ('A wild ' + pokename + ' will be around until ' + expire_time
                     + '!')

        body = ('It will be ' + place_string + ' until ' + expire_time + ' (' +
                time_remaining + ')')

        try:
            channel = pb.channels[config.PB_CHANNEL]
            channel.push_link(title, map_link, body)
        except (IndexError, KeyError):
            pb.push_link(title, map_link, body)
        else:
            return True

    def tweet(self, pokename, expire_time, map_link, place_string, coordinates):
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


        tag_string = generate_tag_string(config.HASHTAGS)

        tweet_text = ('A wild ' + pokename + ' appeared! It will be ' +
                      place_string + ' until ' + expire_time + '. ' + tag_string)

        while len(tweet_text) > 116:
            if config.HASHTAGS:
                config.HASHTAGS.pop()
                tag_string = generate_tag_string(config.HASHTAGS)
                tweet_text = ('A wild ' + pokename + ' appeared! It will be ' +
                              place_string + ' until ' + expire_time + '. ' +
                              tag_string)
            else:
                if (len(tweet_text) > 116) and config.AREA_NAME:
                    tweet_text = ('A wild ' + pokename + ' will be in ' +
                                  config.AREA_NAME + ' until ' +
                                  expire_time + '. ')
                if len(tweet_text) > 116:
                    tweet_text = ('A wild ' + pokename + ' will be around until '
                                  + expire_time + '. ')
                break

        try:
            api = twitter.Api(consumer_key=config.TWITTER_CONSUMER_KEY,
                              consumer_secret=config.TWITTER_CONSUMER_SECRET,
                              access_token_key=config.TWITTER_ACCESS_KEY,
                              access_token_secret=config.TWITTER_ACCESS_SECRET)

            api.PostUpdate(tweet_text + map_link,
                           latitude=coordinates.latitude,
                           longitude=coordinates.longitude,
                           display_coordinates=True)
        except twitter.error.TwitterError:
            return False
        else:
            return True

notifier = Notifier()
