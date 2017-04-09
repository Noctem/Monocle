from datetime import datetime, timedelta, timezone
from collections import deque
from math import sqrt
from time import monotonic, time
from pkg_resources import resource_stream
from tempfile import TemporaryFile
from asyncio import gather, TimeoutError

from aiohttp import ClientError, DisconnectedError, HttpProcessingError

from .utils import load_pickle, dump_pickle
from .db import session_scope, get_pokemon_ranking, estimate_remaining_time
from .names import MOVES, POKEMON
from .shared import get_logger, SessionManager, LOOP, run_threaded

from . import sanitized as conf


WEBHOOK = False
if conf.NOTIFY:
    TWITTER = False
    PUSHBULLET = False
    TELEGRAM = False

    if all((conf.TWITTER_CONSUMER_KEY, conf.TWITTER_CONSUMER_SECRET,
            conf.TWITTER_ACCESS_KEY, conf.TWITTER_ACCESS_SECRET)):
        try:
            import peony.utils
            def _get_image_metadata(file_):
                return 'image/png', 'tweet_image', True, file_
            peony.utils.get_image_metadata = _get_image_metadata
            from peony import PeonyClient
        except ImportError as e:
            raise ImportError("You specified a TWITTER_ACCESS_KEY but you don't have peony-twitter installed.") from e
        TWITTER=True

        if conf.TWEET_IMAGES:
            if not conf.ENCOUNTER:
                raise ValueError('You enabled TWEET_IMAGES but ENCOUNTER is not set.')
            try:
                import cairo
            except ImportError as e:
                raise ImportError('You enabled TWEET_IMAGES but Cairo could not be imported.') from e

    if conf.PB_API_KEY:
        try:
            from asyncpushbullet import AsyncPushbullet
        except ImportError as e:
            raise ImportError("You specified a PB_API_KEY but you don't have asyncpushbullet installed.") from e
        PUSHBULLET=True

    if conf.WEBHOOKS:
        from aiopogo import json_dumps, json_loads

        if len(conf.WEBHOOKS) == 1:
            HOOK_POINT = next(iter(conf.WEBHOOKS))
            WEBHOOK = 1
        else:
            HOOK_POINTS = conf.WEBHOOKS
            WEBHOOK = 2

    if conf.TELEGRAM_BOT_TOKEN and conf.TELEGRAM_CHAT_ID:
        TELEGRAM=True

    NATIVE = TWITTER or PUSHBULLET or TELEGRAM

    if not (NATIVE or WEBHOOK):
        raise ValueError('NOTIFY is enabled but no keys or webhook address were provided.')

    try:
        if conf.INITIAL_SCORE < conf.MINIMUM_SCORE:
            raise ValueError('INITIAL_SCORE should be greater than or equal to MINIMUM_SCORE.')
    except TypeError:
        raise AttributeError('INITIAL_SCORE or MINIMUM_SCORE are not set.')

    if conf.NOTIFY_RANKING and conf.NOTIFY_IDS:
        raise ValueError('Only set NOTIFY_RANKING or NOTIFY_IDS, not both.')
    elif not any((conf.NOTIFY_RANKING, conf.NOTIFY_IDS, conf.ALWAYS_NOTIFY_IDS)):
        raise ValueError('Must set either NOTIFY_RANKING, NOTIFY_IDS, or ALWAYS_NOTIFY_IDS.')


class NotificationCache:
    def __init__(self):
        self.store = set()

    def __contains__(self, item):
        return item in self.store

    def add(self, item, delay):
        self.store.add(item)
        return LOOP.call_later(delay, self.remove, item)

    def remove(self, item):
        self.store.discard(item)


class PokeImage:
    def __init__(self, pokemon, move1, move2, time_of_day=0):
        self.pokemon_id = pokemon['pokemon_id']
        self.name = POKEMON[self.pokemon_id]
        try:
            self.attack = pokemon['individual_attack']
            self.defense = pokemon['individual_defense']
            self.stamina = pokemon['individual_stamina']
        except KeyError:
            pass
        self.move1 = move1
        self.move2 = move2
        self.time_of_day = time_of_day

    def create(self):
        if self.time_of_day > 1:
            bg = resource_stream('monocle', 'static/monocle-icons/assets/notification-bg-night.png')
        else:
            bg = resource_stream('monocle', 'static/monocle-icons/assets/notification-bg-day.png')
        ims = cairo.ImageSurface.create_from_png(bg)
        self.context = cairo.Context(ims)
        pokepic = resource_stream('monocle', 'static/monocle-icons/original-icons/{}.png'.format(self.pokemon_id))
        self.draw_stats()
        self.draw_image(pokepic, 204, 224)
        self.draw_name()
        image = TemporaryFile(suffix='.png')
        ims.write_to_png(image)
        return image

    def draw_stats(self):
        """Draw the Pokemon's IV's and moves."""

        self.context.set_line_width(1.75)
        text_x = 240

        try:
            self.context.select_font_face(conf.IV_FONT or "monospace")
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
        except AttributeError:
            pass

        if self.move1 or self.move2:
            self.context.select_font_face(conf.MOVE_FONT or "sans-serif")
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
        self.context.select_font_face(conf.NAME_FONT or "sans-serif")
        self.context.set_font_size(32)
        self.context.move_to(text_x, text_y)
        self.context.set_source_rgba(0, 0, 0)
        self.context.text_path(self.name)
        self.context.stroke()
        self.context.move_to(text_x, text_y)
        self.context.set_source_rgba(1, 1, 1)
        self.context.show_text(self.name)


class Notification:
    def __init__(self, pokemon, score, time_of_day):
        self.pokemon = pokemon
        self.name = POKEMON[pokemon['pokemon_id']]
        self.coordinates = pokemon['lat'], pokemon['lon']
        self.score = score
        self.time_of_day = time_of_day
        self.log = get_logger('notifier')
        self.description = 'wild'
        try:
            self.move1 = MOVES[pokemon['move_1']]
            self.move2 = MOVES[pokemon['move_2']]
        except KeyError:
            self.move1 = None
            self.move2 = None

        try:
            if self.score == 1:
                self.description = 'perfect'
            elif self.score > .83:
                self.description = 'great'
            elif self.score > .6:
                self.description = 'good'
        except TypeError:
            pass

        if conf.TZ_OFFSET:
            _tz = timezone(timedelta(hours=conf.TZ_OFFSET))
        else:
            _tz = None
        now = datetime.fromtimestamp(pokemon['seen'], _tz)

        if TWITTER and conf.HASHTAGS:
            self.hashtags = conf.HASHTAGS.copy()
        else:
            self.hashtags = set()

        # check if expiration time is known, or a range
        try:
            self.tth = pokemon['time_till_hidden']
            delta = timedelta(seconds=self.tth)
            self.expire_time = (now + delta).strftime('%I:%M %p').lstrip('0')
        except KeyError:
            self.earliest_tth = pokemon['earliest_tth']
            self.latest_tth = pokemon['latest_tth']
            min_delta = timedelta(seconds=self.earliest_tth)
            max_delta = timedelta(seconds=self.latest_tth)
            self.earliest = now + min_delta
            self.latest = now + max_delta

            # check if the two TTHs end on same minute
            if (self.earliest.minute == self.latest.minute
                    and self.earliest.hour == self.latest.hour):
                self.tth = (self.earliest_tth + self.latest_tth) / 2
                self.delta = timedelta(seconds=self.tth)
                self.expire_time = (
                    now + self.delta).strftime('%I:%M %p').lstrip('0')
            else:
                self.min_expire_time = (
                    now + min_delta).strftime('%I:%M').lstrip('0')
                self.max_expire_time = (
                    now + max_delta).strftime('%I:%M %p').lstrip('0')

        self.map_link = 'https://maps.google.com/maps?q={0[0]:.5f},{0[1]:.5f}'.format(self.coordinates)
        self.place = None

    async def notify(self):
        if conf.LANDMARKS and (TWITTER or PUSHBULLET):
            self.landmark = conf.LANDMARKS.find_landmark(self.coordinates)

        try:
            self.place = self.landmark.generate_string(self.coordinates)
            if TWITTER and self.landmark.hashtags:
                self.hashtags.update(self.landmark.hashtags)
        except AttributeError:
            self.place = self.generic_place_string()

        if PUSHBULLET or TELEGRAM:
            try:
                self.attack = self.pokemon['individual_attack']
                self.defense = self.pokemon['individual_defense']
                self.stamina = self.pokemon['individual_stamina']
            except KeyError:
                pass

        tweeted = False
        pushed = False
        telegram = False

        notifications = []
        if PUSHBULLET:
            notifications.append(self.pbpush())

        if TWITTER:
            notifications.append(self.tweet())

        if TELEGRAM:
            notifications.append(self.sendToTelegram())

        results = await gather(*notifications, loop=LOOP)
        return True in results

    async def sendToTelegram(self):
        session = SessionManager.get()
        TELEGRAM_BASE_URL = "https://api.telegram.org/bot{token}/sendVenue".format(token=conf.TELEGRAM_BOT_TOKEN)
        title = self.name
        try:
            minutes, seconds = divmod(self.tth, 60)
            description = 'Expires at: {} ({:.0f}m{:.0f}s left)'.format(self.expire_time, minutes, seconds)
        except AttributeError:
            description = "It'll expire between {} & {}.".format(self.min_expire_time, self.max_expire_time)

        try:
            title += ' ({}/{}/{})'.format(self.attack, self.defense, self.stamina)
        except AttributeError:
            pass

        payload = {
            'chat_id': conf.TELEGRAM_CHAT_ID,
            'latitude': self.coordinates[0],
            'longitude': self.coordinates[1],
            'title' : title,
            'address' : description,
        }

        try:
            async with session.post(TELEGRAM_BASE_URL, data=payload) as resp:
                try:
                    resp.raise_for_status()
                except HttpProcessingError as e:
                    try:
                        response = await resp.json(loads=json_loads)
                        self.log.error('Error {} from Telegram: {}', e.code, response['description'])
                    except Exception:
                        self.log.error('Error {} from Telegram: {}', e.code, e.message)
                    return False
                self.log.info('Sent a Telegram notification about {}.', self.name)
                return True
        except (ClientError, DisconnectedError) as e:
            err = e.__cause__ or e
            self.log.error('{} during Telegram notification.', err.__class__.__name__)
            return False
        except Exception:
            self.log.exception('Exception caught in Telegram notification.')
            return False

    async def pbpush(self):
        """ Send a PushBullet notification either privately or to a channel,
        depending on whether or not PB_CHANNEL is set in config.
        """

        pb = self.get_pushbullet_client()

        description = self.description
        try:
            if self.score < .45:
                description = 'weak'
            elif self.score < .35:
                description = 'bad'
        except TypeError:
            pass

        try:
            expiry = 'until {}'.format(self.expire_time)
            minutes, seconds = divmod(self.tth, 60)
            remaining = 'for {:.0f}m{:.0f}s'.format(minutes, seconds)
        except AttributeError:
            expiry = 'until between {} and {}'.format(self.min_expire_time, self.max_expire_time)
            minutes, seconds = divmod(self.earliest_tth, 60)
            min_remaining = '{:.0f}m{:.0f}s'.format(minutes, seconds)
            minutes, seconds = divmod(self.latest_tth, 60)
            max_remaining = '{:.0f}m{:.0f}s'.format(minutes, seconds)
            remaining = 'for between {} and {}'.format(min_remaining, max_remaining)

        title = 'A {} {} will be in {} {}!'.format(description, self.name, conf.AREA_NAME, expiry)

        body = 'It will be {} {}.\n\n'.format(self.place, remaining)
        try:
            body += ('Attack: {}\n'
                     'Defense: {}\n'
                     'Stamina: {}\n'
                     'Move 1: {}\n'
                     'Move 2: {}\n\n').format(self.attack, self.defense, self.stamina, self.move1, self.move2)
        except AttributeError:
            pass

        try:
            try:
                channel = pb.channels[conf.PB_CHANNEL]
            except (IndexError, KeyError):
                channel = None
            await pb.async_push_link(title, self.map_link, body, channel=channel)
        except Exception:
            self.log.exception('Failed to send a PushBullet notification about {}.', self.name)
            return False
        else:
            self.log.info('Sent a PushBullet notification about {}.', self.name)
            return True

    def shorten_tweet(self, tweet_text):
        tweet_text = tweet_text.replace(' meters ', 'm ')

        # remove hashtags until length is short enough
        while len(tweet_text) > 116:
            if self.hashtags:
                hashtag = self.hashtags.pop()
                tweet_text = tweet_text.replace(' #' + hashtag, '')
            else:
                break

        try:
            if len(tweet_text) > 116:
                tweet_text = tweet_text.replace(self.landmark.name,
                                                self.landmark.shortname)
            else:
                return tweet_text

            if len(tweet_text) > 116:
                place = self.landmark.shortname or self.landmark.name
                phrase = self.landmark.phrase
                if self.place.startswith(phrase):
                    place_string = '{} {}'.format(phrase, place)
                else:
                    place_string = 'near {}'.format(place)
                tweet_text = tweet_text.replace(self.place, place_string)
            else:
                return tweet_text
        except AttributeError:
            pass

        if len(tweet_text) > 116:
            try:
                tweet_text = 'A {d} {n} will be {p} until {e}.'.format(
                             d=self.description, n=self.name,
                             p=place_string, e=self.expire_time)
            except AttributeError:
                tweet_text = (
                    "A {d} {n} appeared {p}! It'll expire between {e1} & {e2}."
                    ).format(d=self.description, n=self.name, p=place_string,
                             e1=self.min_expire_time, e2=self.max_expire_time)
        else:
            return tweet_text

        if len(tweet_text) > 116:
            try:
                tweet_text = 'A {d} {n} will expire at {e}.'.format(
                             n=self.name, e=self.expire_time)
            except AttributeError:
                tweet_text = (
                    'A {d} {n} will expire between {e1} & {e2}.').format(
                    d=self.description, n=self.name, e1=self.min_expire_time,
                    e2=self.max_expire_time)
        else:
            return tweet_text

    async def tweet(self):
        """ Create message, reduce it until it fits in a tweet, and then tweet
        it with a link to Google maps and tweet location included.
        """

        tag_string = ''
        try:
            for hashtag in self.hashtags:
                tag_string += ' #{}'.format(hashtag)
        except TypeError:
            pass

        try:
            tweet_text = (
                'A {d} {n} appeared! It will be {p} until {e}. {t}').format(
                d=self.description, n=self.name, p=self.place,
                e=self.expire_time, t=tag_string)
        except AttributeError:
            tweet_text = (
                'A {d} {n} appeared {p}! It will expire sometime between '
                '{e1} and {e2}. {t}').format(
                d=self.description, n=self.name, p=self.place,
                e1=self.min_expire_time, e2=self.max_expire_time,
                t=tag_string)

        if len(tweet_text) > 116:
            tweet_text = self.shorten_tweet(tweet_text)

        tweet_text += ' ' + self.map_link

        media_id = None
        client = self.get_twitter_client()
        if conf.TWEET_IMAGES:
            try:
                image = PokeImage(self.pokemon, self.move1, self.move2, self.time_of_day).create()
            except Exception:
                self.log.exception('Failed to create a Tweet image.')
            else:
                try:
                    media = await client.upload_media(image, auto_convert=False)
                    media_id = media['media_id']
                except Exception:
                    self.log.exception('Failed to upload Tweet image.')
        try:
            await client.api.statuses.update.post(
                status=tweet_text,
                media_ids=media_id,
                lat=str(self.coordinates[0]),
                long=str(self.coordinates[1]),
                display_coordinates=True)
        except Exception:
            self.log.exception('Failed to tweet about {}.', self.name)
            return False
        else:
            self.log.info('Sent a tweet about {}.', self.name)
            return True
        finally:
            try:
                image.close()
            except AttributeError:
                pass

    @staticmethod
    def generic_place_string():
        """ Create a place string with area name (if available)"""
        # no landmarks defined, just use area name
        place = 'in {}'.format(conf.AREA_NAME)
        return place

    @classmethod
    def get_pushbullet_client(cls):
        try:
            return cls._pushbullet_client
        except AttributeError:
            cls._pushbullet_client = AsyncPushbullet(
                api_key=conf.PB_API_KEY,
                loop=LOOP)
            return cls._pushbullet_client

    @classmethod
    def get_twitter_client(cls):
        try:
            return cls._twitter_client
        except AttributeError:
            cls._twitter_client = PeonyClient(
                consumer_key=conf.TWITTER_CONSUMER_KEY,
                consumer_secret=conf.TWITTER_CONSUMER_SECRET,
                access_token=conf.TWITTER_ACCESS_KEY,
                access_token_secret=conf.TWITTER_ACCESS_SECRET,
                session=SessionManager.get(),
                loop=LOOP)
            return cls._twitter_client


class Notifier:

    def __init__(self):
        self.cache = NotificationCache()
        self.notify_ranking = conf.NOTIFY_RANKING
        self.initial_score = conf.INITIAL_SCORE
        self.minimum_score = conf.MINIMUM_SCORE
        self.last_notification = monotonic() - (conf.FULL_TIME / 2)
        self.always_notify = []
        self.log = get_logger('notifier')
        self.never_notify = conf.NEVER_NOTIFY_IDS
        self.rarity_override = conf.RARITY_OVERRIDE
        self.sent = 0
        if self.notify_ranking:
            self.initialize_ranking()
            LOOP.call_later(3600, self.set_notify_ids)
        elif conf.NOTIFY_IDS or conf.ALWAYS_NOTIFY_IDS:
            self.notify_ids = conf.NOTIFY_IDS or conf.ALWAYS_NOTIFY_IDS
            self.always_notify = conf.ALWAYS_NOTIFY_IDS
            self.notify_ranking = len(self.notify_ids)

    def set_notify_ids(self):
        LOOP.create_task(self._set_notify_ids())
        LOOP.call_later(3600, self.set_notify_ids)

    async def _set_notify_ids(self):
        await run_threaded(self.set_ranking)
        self.notify_ids = self.pokemon_ranking[0:self.notify_ranking]
        self.always_notify = set(self.pokemon_ranking[0:conf.ALWAYS_NOTIFY])
        self.always_notify |= set(conf.ALWAYS_NOTIFY_IDS)
        self.log.info('Updated Pokemon rankings.')

    def initialize_ranking(self):
        self.pokemon_ranking = load_pickle('ranking')
        if self.pokemon_ranking:
            self.notify_ids = self.pokemon_ranking[0:self.notify_ranking]
            self.always_notify = set(self.pokemon_ranking[0:conf.ALWAYS_NOTIFY])
            self.always_notify |= set(conf.ALWAYS_NOTIFY_IDS)
        else:
            LOOP.run_until_complete(self._set_notify_ids())

    def set_ranking(self):
        try:
            with session_scope() as session:
                self.pokemon_ranking = get_pokemon_ranking(session)
        except Exception:
            self.log.exception('An exception occurred while trying to update rankings.')
        else:
            dump_pickle('ranking', self.pokemon_ranking)

    def get_rareness_score(self, pokemon_id):
        if pokemon_id in self.rarity_override:
            return self.rarity_override[pokemon_id]
        exclude = len(self.always_notify)
        total = self.notify_ranking - exclude
        ranking = self.notify_ids.index(pokemon_id) - exclude
        percentile = 1 - (ranking / total)
        return percentile

    def get_required_score(self, now=None):
        if self.initial_score == self.minimum_score or conf.FULL_TIME == 0:
            return self.initial_score
        now = now or monotonic()
        time_passed = now - self.last_notification
        subtract = self.initial_score - self.minimum_score
        if time_passed < conf.FULL_TIME:
            subtract *= (time_passed / conf.FULL_TIME)
        return self.initial_score - subtract

    def eligible(self, pokemon):
        pokemon_id = pokemon['pokemon_id']
        encounter_id = pokemon['encounter_id']

        if pokemon_id in self.never_notify:
            return False
        if pokemon_id in self.always_notify:
            return encounter_id not in self.cache
        if (pokemon_id not in self.notify_ids
                and pokemon_id not in self.rarity_override):
            return False
        if conf.IGNORE_RARITY:
            return encounter_id not in self.cache
        try:
            if pokemon['time_till_hidden'] < conf.TIME_REQUIRED:
                return False
        except KeyError:
            pass
        if encounter_id in self.cache:
            return False

        rareness = self.get_rareness_score(pokemon_id)
        highest_score = (rareness + 1) / 2
        score_required = self.get_required_score()
        return highest_score > score_required

    def cleanup(self, encounter_id, handle):
        self.cache.remove(encounter_id)
        handle.cancel()
        return False

    async def notify(self, pokemon, time_of_day):
        """Send a PushBullet notification and/or a Tweet, depending on if their
        respective API keys have been set in config.
        """
        whpushed = False
        notified = False

        pokemon_id = pokemon['pokemon_id']
        name = POKEMON[pokemon_id]

        encounter_id = pokemon['encounter_id']
        if encounter_id in self.cache:
            self.log.info("{} was already notified about.", name)
            return False

        now = monotonic()
        if pokemon_id in self.always_notify:
            score_required = 0
        else:
            score_required = self.get_required_score(now)

        try:
            iv_score = (pokemon['individual_attack'] + pokemon['individual_defense'] + pokemon['individual_stamina']) / 45
        except KeyError:
            if conf.IGNORE_IVS:
                iv_score = None
            else:
                self.log.warning('IVs are supposed to be considered but were not found.')
                return False

        if score_required:
            if conf.IGNORE_RARITY:
                score = iv_score
            elif conf.IGNORE_IVS:
                score = self.get_rareness_score(pokemon_id)
            else:
                rareness = self.get_rareness_score(pokemon_id)
                score = (iv_score + rareness) / 2
        else:
            score = 1

        if score < score_required:
            try:
                self.log.info("{}'s score was {:.3f} (iv: {:.3f}),"
                                 " but {:.3f} was required.",
                                 name, score, iv_score, score_required)
            except TypeError:
                pass
            return False

        if 'time_till_hidden' not in pokemon:
            seen = pokemon['seen'] % 3600
            self.cache.store.add(pokemon['encounter_id'])
            try:
                with session_scope() as session:
                    tth = await run_threaded(estimate_remaining_time, session, pokemon['spawn_id'], seen)
            except Exception:
                self.log.exception('An exception occurred while trying to estimate remaining time.')
                now_epoch = time()
                tth = (pokemon['seen'] + 90 - now_epoch, pokemon['seen'] + 3600 - now_epoch)
            LOOP.call_later(tth[1], self.cache.remove, pokemon['encounter_id'])
            if pokemon_id not in self.always_notify:
                mean = sum(tth) / 2
                if mean < conf.TIME_REQUIRED:
                    self.log.info('{} has only around {} seconds remaining.', name, mean)
                    return False
            pokemon['earliest_tth'], pokemon['latest_tth'] = tth
        else:
            cache_handle = self.cache.add(pokemon['encounter_id'], pokemon['time_till_hidden'])

        if WEBHOOK and NATIVE:
            notified, whpushed = await gather(
                Notification(pokemon, iv_score, time_of_day).notify(),
                self.webhook(pokemon),
                loop=LOOP)
        elif NATIVE:
            notified = await Notification(pokemon, iv_score, time_of_day).notify()
        elif WEBHOOK:
            whpushed = await self.webhook(pokemon)

        if notified or whpushed:
            self.last_notification = monotonic()
            self.sent += 1
            return True
        else:
            return self.cleanup(encounter_id, cache_handle)

    async def webhook(self, pokemon):
        """ Send a notification via webhook
        """
        try:
            tth = pokemon['time_till_hidden']
            ts = pokemon['expire_timestamp']
        except KeyError:
            tth = pokemon['earliest_tth']
            ts = pokemon['seen'] + tth

        data = {
            'type': "pokemon",
            'message': {
                "encounter_id": pokemon['encounter_id'],
                "pokemon_id": pokemon['pokemon_id'],
                "last_modified_time": pokemon['seen'] * 1000,
                "spawnpoint_id": pokemon['spawn_id'],
                "latitude": pokemon['lat'],
                "longitude": pokemon['lon'],
                "disappear_time": ts,
                "time_until_hidden_ms": tth * 1000
            }
        }

        try:
            data['message']['individual_attack'] = pokemon['individual_attack']
            data['message']['individual_defense'] = pokemon['individual_defense']
            data['message']['individual_stamina'] = pokemon['individual_stamina']
            data['message']['move_1'] = pokemon['move_1']
            data['message']['move_2'] = pokemon['move_2']
            data['message']['height'] = pokemon['height']
            data['message']['weight'] = pokemon['weight']
            data['message']['gender'] = pokemon['gender']
        except KeyError:
            pass

        payload = json_dumps(data)
        session = SessionManager.get()
        return await self.wh_send(session, payload)

    if WEBHOOK > 1:
        async def wh_send(self, session, payload):
            results = await gather(*tuple(self.hook_post(w, session, payload) for w in HOOK_POINTS), loop=LOOP)
            return True in results
    else:
        async def wh_send(self, session, payload):
            return await self.hook_post(HOOK_POINT, session, payload)

    async def hook_post(self, w, session, payload, headers={'content-type': 'application/json'}):
        try:
            async with session.post(w, data=payload, timeout=3, headers=headers) as resp:
                resp.raise_for_status()
                return True
        except HttpProcessingError as e:
            self.log.error('Error {} from webook {}: {}', e.code, w, e.message)
            return False
        except TimeoutError:
            self.log.error('Response timeout from webhook: {}', w)
            return False
        except (ClientError, DisconnectedError) as e:
            err = e.__cause__ or e
            self.log.error('{} on webhook: {}', err.__class__.__name__, w)
            return False
        except CancelledError:
            raise
        except Exception:
            self.log.exception('Error from webhook: {}', w)
            return False
