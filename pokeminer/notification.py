from datetime import datetime, timedelta, timezone
from collections import deque
from math import sqrt
from time import monotonic
from logging import getLogger
from pkg_resources import resource_stream
from tempfile import NamedTemporaryFile

from .utils import load_pickle
from .db import Session, get_pokemon_ranking, estimate_remaining_time
from .names import POKEMON_NAMES, MOVES
from . import config

# set unset config options to None
for variable_name in ('PB_API_KEY', 'PB_CHANNEL', 'TWITTER_CONSUMER_KEY',
                      'TWITTER_CONSUMER_SECRET', 'TWITTER_ACCESS_KEY',
                      'TWITTER_ACCESS_SECRET', 'LANDMARKS', 'AREA_NAME',
                      'HASHTAGS', 'TZ_OFFSET', 'ENCOUNTER', 'INITIAL_RANKING',
                      'NOTIFY', 'NAME_FONT', 'IV_FONT', 'MOVE_FONT',
                      'TWEET_IMAGES', 'NOTIFY_IDS', 'NEVER_NOTIFY_IDS',
                      'RARITY_OVERRIDE', 'IGNORE_IVS', 'IGNORE_RARITY',
                      'WEBHOOKS'):
    if not hasattr(config, variable_name):
        setattr(config, variable_name, None)

_optional = {
    'ALWAYS_NOTIFY': 9,
    'FULL_TIME': 1800,
    'TIME_REQUIRED': 300,
    'NOTIFY_RANKING': 90,
    'ALWAYS_NOTIFY_IDS': set(),
    'NOTIFICATION_CACHE': 100
}
# set defaults for unset config options
for setting_name, default in _optional.items():
    if not hasattr(config, setting_name):
        setattr(config, setting_name, default)
del _optional

if config.NOTIFY:
    WEBHOOK = False
    TWITTER = False
    PUSHBULLET = False

    if all((config.TWITTER_CONSUMER_KEY, config.TWITTER_CONSUMER_SECRET,
            config.TWITTER_ACCESS_KEY, config.TWITTER_ACCESS_SECRET)):
        try:
            import twitter
            from twitter.twitter_utils import calc_expected_status_length
        except ImportError as e:
            raise ImportError("You specified a TWITTER_ACCESS_KEY but you don't have python-twitter installed.") from e
        TWITTER=True

        if config.TWEET_IMAGES:
            if not config.ENCOUNTER:
                raise ValueError('You enabled TWEET_IMAGES but ENCOUNTER is not set.')
            try:
                import cairo
            except ImportError as e:
                raise ImportError('You enabled TWEET_IMAGES but Cairo could not be imported.') from e

    if config.PB_API_KEY:
        try:
            from pushbullet import Pushbullet
        except ImportError as e:
            raise ImportError("You specified a PB_API_KEY but you don't have pushbullet.py installed.") from e
        PUSHBULLET=True

    if config.WEBHOOKS:
        if not isinstance(config.WEBHOOKS, (set, list, tuple)):
            raise ValueError('WEBHOOKS must be a set of addresses.')
        try:
            import requests
        except ImportError as e:
            raise ImportError("You specified a WEBHOOKS address but you don't have requests installed.") from e
        WEBHOOK = True

    NATIVE = TWITTER or PUSHBULLET

    if not (NATIVE or WEBHOOK):
        raise ValueError('NOTIFY is enabled but no keys or webhook address were provided.')

    try:
        if config.INITIAL_SCORE < config.MINIMUM_SCORE:
            raise ValueError('INITIAL_SCORE should be greater than or equal to MINIMUM_SCORE.')
    except TypeError:
        raise AttributeError('INITIAL_SCORE or MINIMUM_SCORE are not set.')

    if config.NOTIFY_RANKING and config.NOTIFY_IDS:
        raise ValueError('Only set NOTIFY_RANKING or NOTIFY_IDS, not both.')
    elif not any((config.NOTIFY_RANKING, config.NOTIFY_IDS, config.ALWAYS_NOTIFY_IDS)):
        raise ValueError('Must set either NOTIFY_RANKING, NOTIFY_IDS, or ALWAYS_NOTIFY_IDS.')


class PokeImage:
    def __init__(self, pokemon_id, iv, moves, time_of_day):
        self.pokemon_id =  pokemon_id
        self.name = POKEMON_NAMES[pokemon_id]
        self.attack, self.defense, self.stamina = iv
        self.move1, self.move2 = moves
        self.time_of_day = time_of_day

    def create(self):
        if self.time_of_day > 1:
            bg = resource_stream('pokeminer', 'static/img/notification-bg-night.png')
        else:
            bg = resource_stream('pokeminer', 'static/img/notification-bg-day.png', 'pokeminer')
        ims = cairo.ImageSurface.create_from_png(bg)
        self.context = cairo.Context(ims)
        pokepic = resource_stream('pokeminer', 'static/original-icons/{}.png'.format(self.pokemon_id))
        self.draw_stats()
        self.draw_image(pokepic, 204, 224)
        self.draw_name()
        image = NamedTemporaryFile(suffix='.png', delete=True)
        ims.write_to_png(image)
        image.mode = 'rb'
        return image

    def draw_stats(self):
        """Draw the Pokemon's IV's and moves."""

        self.context.set_line_width(1.75)
        text_x = 240

        if self.attack is not None:
            self.context.select_font_face(config.IV_FONT or "monospace")
            self.context.set_font_size(22)

            # black stroke
            self.draw_ivs(text_x)
            self.context.set_source_rgba(0, 0, 0)
            self.context.stroke()

            # white fill
            self.context.move_to(text_x, 90)
            self.draw_ivs(text_x)
            self.context.set_source_rgba(1, 1, 1)
            self.context.fill()

        if self.move1 or self.move2:
            self.context.select_font_face(config.MOVE_FONT or "sans-serif")
            self.context.set_font_size(16)

            # black stroke
            self.draw_moves(text_x)
            self.context.set_source_rgba(0, 0, 0)
            self.context.stroke()

            # white fill
            self.draw_moves(text_x)
            self.context.set_source_rgba(1, 1, 1)
            self.context.fill()

    def draw_ivs(self, text_x):
        self.context.move_to(text_x, 90)
        self.context.text_path("Attack:  {:>2}/15".format(self.attack))
        self.context.move_to(text_x, 116)
        self.context.text_path("Defense: {:>2}/15".format(self.defense))
        self.context.move_to(text_x, 142)
        self.context.text_path("Stamina: {:>2}/15".format(self.stamina))

    def draw_moves(self, text_x):
        if self.move1:
            self.context.move_to(text_x, 170)
            self.context.text_path("Move 1: {}".format(self.move1))
        if self.move2:
            self.context.move_to(text_x, 188)
            self.context.text_path("Move 2: {}".format(self.move2))

    def draw_image(self, pokepic, height, width):
        """Draw a scaled image on a given context."""
        ims = cairo.ImageSurface.create_from_png(pokepic)
        # calculate proportional scaling
        img_height = ims.get_height()
        img_width = ims.get_width()
        width_ratio = float(width) / float(img_width)
        height_ratio = float(height) / float(img_height)
        scale_xy = min(height_ratio, width_ratio)
        # scale image and add it
        self.context.save()
        if scale_xy < 1:
            self.context.scale(scale_xy, scale_xy)
            if scale_xy == width_ratio:
                new_height = img_height * scale_xy
                top = (height - new_height) / 2
                self.context.translate(8, top + 8)
            else:
                new_width = img_width * scale_xy
                left = (width - new_width) / 2
                self.context.translate(left + 8, 8)
        else:
            left = (width - img_width) / 2
            top = (height - img_height) / 2
            self.context.translate(left + 8, top + 8)
        self.context.set_source_surface(ims)
        self.context.paint()
        self.context.restore()

    def draw_name(self):
        """Draw the Pokemon's name."""
        self.context.set_line_width(2.5)
        text_x = 240
        text_y = 50
        self.context.select_font_face(config.NAME_FONT or "sans-serif")
        self.context.set_font_size(32)
        self.context.move_to(text_x, text_y)
        self.context.set_source_rgba(0, 0, 0)
        self.context.text_path(self.name)
        self.context.stroke()
        self.context.move_to(text_x, text_y)
        self.context.set_source_rgba(1, 1, 1)
        self.context.show_text(self.name)

class Notification:

    def __init__(self, pokemon_id, coordinates, time_till_hidden, iv, moves, score, time_of_day):
        self.pokemon_id = pokemon_id
        self.name = POKEMON_NAMES[pokemon_id]
        self.coordinates = coordinates
        self.moves = moves
        self.score = score
        self.iv = iv
        self.time_of_day = time_of_day
        self.logger = getLogger('notifier')
        self.description = 'wild'

        try:
            if self.score == 1:
                self.description = 'perfect'
            elif self.score > .83:
                self.description = 'great'
            elif self.score > .6:
                self.description = 'good'
        except TypeError:
            pass

        if config.TZ_OFFSET:
            now = datetime.now(timezone(timedelta(hours=config.TZ_OFFSET)))
        else:
            now = datetime.now()

        if TWITTER and config.HASHTAGS:
            self.hashtags = config.HASHTAGS.copy()
        else:
            self.hashtags = set()

        # check if expiration time is known, or a range
        if isinstance(time_till_hidden, (tuple, list)):
            soonest, latest = time_till_hidden
            self.min_delta = timedelta(seconds=soonest)
            self.max_delta = timedelta(seconds=latest)
            # check if the two TTHs end on same minute
            if ((now + self.min_delta).strftime('%I:%M') ==
                    (now + self.max_delta).strftime('%I:%M')):
                average = (soonest + latest) / 2
                time_till_hidden = average
                self.delta = timedelta(seconds=average)
                self.expire_time = (
                    now + self.delta).strftime('%I:%M %p').lstrip('0')
            else:
                self.delta = None
                self.expire_time = None
                self.min_expire_time = (
                    now + self.min_delta).strftime('%I:%M').lstrip('0')
                self.max_expire_time = (
                    now + self.max_delta).strftime('%I:%M %p').lstrip('0')
        else:
            self.delta = timedelta(seconds=time_till_hidden)
            self.expire_time = (
                now + self.delta).strftime('%I:%M %p').lstrip('0')
            self.min_delta = None

        self.map_link = 'https://maps.google.com/maps?q={0[0]:.5f},{0[1]:.5f}'.format(
            self.coordinates)
        self.place = None

    def notify(self):
        if config.LANDMARKS:
            self.landmark = config.LANDMARKS.find_landmark(self.coordinates)
        else:
            self.landmark = None

        if self.landmark:
            self.place = self.landmark.generate_string(self.coordinates)
            if TWITTER and self.landmark.hashtags:
                self.hashtags.update(self.landmark.hashtags)
        else:
            self.place = self.generic_place_string()

        tweeted = False
        pushed = False

        if PUSHBULLET:
            pushed = self.pbpush()

        if TWITTER:
            tweeted = self.tweet()

        return tweeted or pushed

    def pbpush(self):
        """ Send a PushBullet notification either privately or to a channel,
        depending on whether or not PB_CHANNEL is set in config.
        """

        try:
            pb = Pushbullet(config.PB_API_KEY)
        except Exception:
            self.logger.exception('Failed to create a PushBullet object.')
            return False

        description = self.description
        try:
            if self.score < .45:
                description = 'weak'
            elif self.score < .35:
                description = 'bad'
        except TypeError:
            pass

        area = config.AREA_NAME
        if self.delta:
            expiry = 'until {}'.format(self.expire_time)

            minutes, seconds = divmod(self.delta.total_seconds(), 60)
            remaining = 'for {m}m{s:.0f}s'.format(m=int(minutes), s=seconds)
        else:
            expiry = 'until between {t1} and {t2}'.format(
                     t1=self.min_expire_time, t2=self.max_expire_time)

            minutes, seconds = divmod(self.min_delta.total_seconds(), 60)
            min_remaining = '{m}m{s:.0f}s'.format(m=int(minutes), s=seconds)
            minutes, seconds = divmod(self.max_delta.total_seconds(), 60)
            max_remaining = '{m}m{s:.0f}s'.format(m=int(minutes), s=seconds)

            remaining = 'for between {r1} and {r2}'.format(
                        r1=min_remaining, r2=max_remaining)

        title = ('A {d} {n} will be in {a} {e}!'
                 ).format(d=description, n=self.name, a=area, e=expiry)

        body = ('It will be {p} {r}.\n\n'
                'Attack: {iv[0]}\n'
                'Defense: {iv[1]}\n'
                'Stamina: {iv[2]}\n'
                'Move 1: {m[0]}\n'
                'Move 2: {m[1]}\n\n').format(
                p=self.place, r=remaining, iv=self.iv, m=self.moves)

        try:
            try:
                channel = pb.channels[config.PB_CHANNEL]
                channel.push_link(title, self.map_link, body)
            except (IndexError, KeyError):
                pb.push_link(title, self.map_link, body)
        except Exception:
            self.logger.exception('Failed to send a PushBullet notification about {}.'.format(self.name))
            return False
        else:
            self.logger.info('Sent a PushBullet notification about {}.'.format(self.name))
            return True

    def tweet(self):
        """ Create message, reduce it until it fits in a tweet, and then tweet
        it with a link to Google maps and tweet location included.
        """

        def generate_tag_string(hashtags):
            '''create hashtag string'''
            tag_string = ''
            if hashtags:
                for hashtag in hashtags:
                    tag_string += ' #{}'.format(hashtag)
            return tag_string

        try:
            api = twitter.Api(consumer_key=config.TWITTER_CONSUMER_KEY,
                              consumer_secret=config.TWITTER_CONSUMER_SECRET,
                              access_token_key=config.TWITTER_ACCESS_KEY,
                              access_token_secret=config.TWITTER_ACCESS_SECRET)
        except Exception:
            self.logger.exception('Failed to create a Twitter API object.')

        tag_string = generate_tag_string(self.hashtags)

        if self.expire_time:
            tweet_text = (
                'A {d} {n} appeared! It will be {p} until {e}. {t} {u}').format(
                d=self.description, n=self.name, p=self.place,
                e=self.expire_time, t=tag_string, u=self.map_link)
        else:
            tweet_text = (
                'A {d} {n} appeared {p}! It will expire sometime between '
                '{e1} and {e2}. {t} {u}').format(
                d=self.description, n=self.name, p=self.place,
                e1=self.min_expire_time, e2=self.max_expire_time,
                t=tag_string, u=self.map_link)

        if calc_expected_status_length(tweet_text) > 140:
            tweet_text = tweet_text.replace(' meters ', 'm ')

        # remove hashtags until length is short enough
        while calc_expected_status_length(tweet_text) > 140:
            if self.hashtags:
                hashtag = self.hashtags.pop()
                tweet_text = tweet_text.replace(' #' + hashtag, '')
            else:
                break

        if (calc_expected_status_length(tweet_text) > 140 and
                self.landmark.shortname):
            tweet_text = tweet_text.replace(self.landmark.name,
                                            self.landmark.shortname)

        if calc_expected_status_length(tweet_text) > 140:
            place = self.landmark.shortname or self.landmark.name
            phrase = self.landmark.phrase
            if self.place.startswith(phrase):
                place_string = '{ph} {pl}'.format(ph=phrase, pl=place)
            else:
                place_string = 'near {}'.format(place)
            tweet_text = tweet_text.replace(self.place, place_string)

        if calc_expected_status_length(tweet_text) > 140:
            if self.expire_time:
                tweet_text = 'A {d} {n} will be {p} until {e}. {u}'.format(
                             d=self.description, n=self.name,
                             p=place_string, e=self.expire_time,
                             u=self.map_link)
            else:
                tweet_text = (
                    "A {d} {n} appeared {p}! It'll expire between {e1} & {e2}."
                    ' {u}').format(d=self.description, n=self.name,
                                   p=place_string, e1=self.min_expire_time,
                                   e2=self.max_expire_time, u=self.map_link)

        if calc_expected_status_length(tweet_text) > 140:
            if self.expire_time:
                tweet_text = 'A {d} {n} will expire at {e}. {u}'.format(
                             n=self.name, e=self.expire_time, u=self.map_link)
            else:
                tweet_text = (
                    'A {d} {n} will expire between {e1} & {e2}. {u}').format(
                    d=self.description, n=self.name, e1=self.min_expire_time,
                    e2=self.max_expire_time, u=self.map_link)

        image = None
        if config.TWEET_IMAGES:
            try:
                image = PokeImage(self.pokemon_id, self.iv, self.moves, self.time_of_day).create()
            except Exception:
                self.logger.exception('Failed to create a Tweet image.')

        try:
            api.PostUpdate(tweet_text,
                           media=image,
                           latitude=self.coordinates[0],
                           longitude=self.coordinates[1],
                           display_coordinates=True)
        except Exception:
            self.logger.exception('Failed to Tweet about {}.'.format(self.name))
            return False
        else:
            self.logger.info('Sent a tweet about {}.'.format(self.name))
            return True
        finally:
            try:
                image.close()
            except AttributeError:
                pass

    @staticmethod
    def generic_place_string():
        """ Create a place string with area name (if available)"""
        if config.AREA_NAME:
            # no landmarks defined, just use area name
            place = 'in {}'.format(config.AREA_NAME)
            return place
        else:
            # no landmarks or area name defined, just say 'around'
            return 'around'


class Notifier:

    def __init__(self, spawns):
        self.spawns = spawns
        self.recent_notifications = deque(maxlen=config.NOTIFICATION_CACHE)
        self.notify_ranking = config.NOTIFY_RANKING
        self.session = Session(autoflush=False)
        self.initial_score = config.INITIAL_SCORE
        self.minimum_score = config.MINIMUM_SCORE
        self.last_notification = monotonic() - (config.FULL_TIME / 2)
        self.always_notify = []
        self.logger = getLogger('notifier')
        self.never_notify = config.NEVER_NOTIFY_IDS or tuple()
        self.rarity_override = config.RARITY_OVERRIDE or {}
        if self.notify_ranking:
            self.set_pokemon_ranking(loadpickle=True)
            self.set_notify_ids()
            self.auto = True
        elif config.NOTIFY_IDS or config.ALWAYS_NOTIFY_IDS:
            self.notify_ids = config.NOTIFY_IDS or config.ALWAYS_NOTIFY_IDS
            self.always_notify = config.ALWAYS_NOTIFY_IDS
            self.notify_ranking = len(self.notify_ids)
            self.auto = False
        if WEBHOOK:
            self.wh_session = requests.Session()

    def set_notify_ids(self):
        self.notify_ids = self.pokemon_ranking[0:self.notify_ranking]
        self.always_notify = set(self.pokemon_ranking[0:config.ALWAYS_NOTIFY])
        self.always_notify |= set(config.ALWAYS_NOTIFY_IDS)

    def set_pokemon_ranking(self, loadpickle=False):
        self.ranking_time = monotonic()
        if loadpickle:
            self.pokemon_ranking = load_pickle('ranking')
            if self.pokemon_ranking:
                return
        try:
            self.pokemon_ranking = get_pokemon_ranking(self.session)
            with open('pickles/ranking.pickle', 'wb') as f:
                pickle.dump(self.pokemon_ranking, f, pickle.HIGHEST_PROTOCOL)
        except Exception:
            self.session.rollback()
            self.logger.exception('An exception occurred while trying to update rankings.')

    def get_rareness_score(self, pokemon_id):
        if pokemon_id in self.rarity_override:
            return self.rarity_override[pokemon_id]
        exclude = len(self.always_notify)
        total = self.notify_ranking - exclude
        ranking = self.notify_ids.index(pokemon_id) - exclude
        percentile = 1 - (ranking / total)
        return percentile

    def get_iv_score(self, iv):
        try:
            return sum(iv) / 45
        except TypeError:
            return None

    def get_required_score(self, now=None):
        if self.initial_score == self.minimum_score or config.FULL_TIME == 0:
            return self.initial_score
        now = now or monotonic()
        time_passed = now - self.last_notification
        subtract = self.initial_score - self.minimum_score
        if time_passed < config.FULL_TIME:
            subtract *= (time_passed / config.FULL_TIME)
        return self.initial_score - subtract

    def eligible(self, pokemon):
        pokemon_id = pokemon['pokemon_id']

        if (pokemon_id in self.never_notify
                or pokemon['encounter_id'] in self.recent_notifications):
            return False
        if pokemon_id in self.always_notify:
            return True
        if pokemon_id not in self.notify_ids:
            return False
        if config.IGNORE_RARITY:
            return True

        rareness = self.get_rareness_score(pokemon_id)
        highest_score = (rareness + 1) / 2
        score_required = self.get_required_score()
        return highest_score > score_required

    def notify(self, pokemon, time_of_day):
        """Send a PushBullet notification and/or a Tweet, depending on if their
        respective API keys have been set in config.
        """

        spawn_id = pokemon['spawn_id']
        coordinates = (pokemon['lat'], pokemon['lon'])
        pokemon_id = pokemon['pokemon_id']
        encounter_id = pokemon['encounter_id']
        name = POKEMON_NAMES[pokemon_id]

        if encounter_id in self.recent_notifications:
            # skip duplicate
            return False

        if pokemon['valid']:
            time_till_hidden = pokemon['time_till_hidden_ms'] / 1000
        else:
            time_till_hidden = None

        now = monotonic()
        if self.auto:
            if now - self.ranking_time > 3600:
                self.set_pokemon_ranking()
                self.set_notify_ids()

        if pokemon_id in self.always_notify:
            score_required = 0
        else:
            if time_till_hidden and time_till_hidden < config.TIME_REQUIRED:
                self.logger.info('{n} has only {s} seconds remaining.'.format(
                    n=name, s=time_till_hidden))
                return False
            score_required = self.get_required_score(now)

        iv = (pokemon.get('individual_attack'),
              pokemon.get('individual_defense'),
              pokemon.get('individual_stamina'))
        moves = (MOVES.get(pokemon.get('move_1'), {}).get('name'),
                 MOVES.get(pokemon.get('move_2'), {}).get('name'))

        iv_score = self.get_iv_score(iv)
        if score_required:
            if config.IGNORE_RARITY:
                score = iv_score
            elif config.IGNORE_IVS or iv_score is None:
                score = self.get_rareness_score(pokemon_id)
            else:
                rareness = self.get_rareness_score(pokemon_id)
                try:
                    score = (iv_score + rareness) / 2
                except TypeError:
                    self.logger.warning('Failed to calculate score for {}.'.format(name))
                    return False
        else:
            score = None

        if score_required and score < score_required:
            self.logger.info("{n}'s score was {s:.3f} (iv: {i:.3f}),"
                             " but {r:.3f} was required.".format(
                             n=name, s=score, i=iv_score, r=score_required))
            return False

        if not time_till_hidden:
            seen = pokemon['seen'] % 3600
            try:
                time_till_hidden = estimate_remaining_time(self.session, spawn_id, seen)
            except Exception:
                self.session.rollback()
                self.logger.exception('An exception occurred while trying to esimate remaining time.')
            mean = sum(time_till_hidden) / 2

            if mean < config.TIME_REQUIRED and pokemon_id not in self.always_notify:
                self.logger.info('{n} has only around {s} seconds remaining.'.format(
                    n=name, s=mean))
                return False

        whpushed = False
        if WEBHOOK:
            whpushed = self.webhook(pokemon, time_till_hidden)

        notified = False
        if NATIVE:
            notified = Notification(pokemon_id, coordinates, time_till_hidden, iv, moves, iv_score, time_of_day).notify()

        if notified or whpushed:
            self.last_notification = monotonic()
            self.recent_notifications.append(encounter_id)
        return notified or whpushed

    def webhook(self, pokemon, time_till_hidden):
        """ Send a notification via webhook
        """
        if isinstance(time_till_hidden, (tuple, list)):
            time_till_hidden = time_till_hidden[0]

        data = {
            'type': "pokemon",
            'message': {
                "encounter_id": pokemon['encounter_id'],
                "pokemon_id": pokemon['pokemon_id'],
                "last_modified_time": pokemon['seen'] * 1000,
                "spawnpoint_id": pokemon['spawn_id'],
                "latitude": pokemon['lat'],
                "longitude": pokemon['lon'],
                "disappear_time": pokemon['seen'] + time_till_hidden,
                "time_until_hidden_ms": time_till_hidden * 1000
            }
        }

        try:
            data['message']['individual_attack'] = pokemon['individual_attack']
            data['message']['individual_defense'] = pokemon['individual_defense']
            data['message']['individual_stamina'] = pokemon['individual_stamina']
            data['message']['move_1'] = pokemon['move_1']
            data['message']['move_2'] = pokemon['move_2']
        except KeyError:
            pass

        ret = False
        for w in config.WEBHOOKS:
            try:
                self.wh_session.post(w, json=data, timeout=(1, 1))
                ret = True
            except requests.exceptions.Timeout:
                self.logger.warning('Response timeout on webhook endpoint {}'.format(w))
            except requests.exceptions.RequestException as e:
                self.logger.warning('Request Error: {}'.format(e))
        return ret
