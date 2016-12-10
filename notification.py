from datetime import datetime, timedelta, timezone
from collections import deque
from os import makedirs
from math import sqrt

import time
import pickle

from db import Session, get_pokemon_ranking, get_despawn_time, estimate_remaining_time
from names import POKEMON_NAMES, MOVES

import config

# set unset config options to None
for variable_name in ('PB_API_KEY', 'PB_CHANNEL', 'TWITTER_CONSUMER_KEY',
                      'TWITTER_CONSUMER_SECRET', 'TWITTER_ACCESS_KEY',
                      'TWITTER_ACCESS_SECRET', 'LANDMARKS', 'AREA_NAME',
                      'HASHTAGS', 'TZ_OFFSET', 'NOTIFY_RANKING', 'NOTIFY_IDS'
                      'ENCOUNTER', 'INITIAL_RANKING'):
    if not hasattr(config, variable_name):
        setattr(config, variable_name, None)

OPTIONAL_SETTINGS = {
    'ALWAYS_NOTIFY': 0,
    'FULL_TIME': 1800,
    'TIME_REQUIRED': 300
}
# set defaults for unset config options
for setting_name, default in OPTIONAL_SETTINGS.items():
    if not hasattr(config, setting_name):
        setattr(config, setting_name, default)

if config.ENCOUNTER in ('all', 'notifying'):
    import cairo

PERFECT_SCORE = 15 + (sqrt(15) * 2)

def draw_image(ctx, image, height, width):
    """Draw a scaled image on a given context."""
    image_surface = cairo.ImageSurface.create_from_png(image)
    # calculate proportional scaling
    img_height = image_surface.get_height()
    img_width = image_surface.get_width()
    width_ratio = float(width) / float(img_width)
    height_ratio = float(height) / float(img_height)
    scale_xy = min(height_ratio, width_ratio)
    # scale image and add it
    ctx.save()
    if scale_xy < 1:
        ctx.scale(scale_xy, scale_xy)
        if scale_xy != width_ratio:
            new_width = img_width * scale_xy
            left = (width - new_width) / 2
            ctx.translate(left + 8, 8)
        elif scale_xy != height_ratio:
            new_height = img_height * scale_xy
            top = (height - new_height) / 2
            ctx.translate(8, top + 8)
    else:
        left = (width - img_width) / 2
        top = (height - img_height) / 2
        ctx.translate(left + 8, top + 8)
    ctx.set_source_surface(image_surface)

    ctx.paint()
    ctx.restore()


def draw_stats(cr, attack, defense, stamina, move1=None, move2=None):
    """Draw the Pokemon's IV's and moves."""

    cr.set_line_width(1.75)

    text_x = 240
    if attack is not None:
        cr.select_font_face("SF Mono Semibold")
        cr.set_font_size(22)
        cr.move_to(text_x, 90)
        cr.text_path("Attack:  {:>2}/15".format(attack))
        cr.move_to(text_x, 116)
        cr.text_path("Defense: {:>2}/15".format(defense))
        cr.move_to(text_x, 142)
        cr.text_path("Stamina: {:>2}/15".format(stamina))
        cr.set_source_rgba(0, 0, 0)
        cr.stroke()

        cr.move_to(text_x, 90)
        cr.text_path("Attack:  {:>2}/15".format(attack))
        cr.move_to(text_x, 116)
        cr.text_path("Defense: {:>2}/15".format(defense))
        cr.move_to(text_x, 142)
        cr.text_path("Stamina: {:>2}/15".format(stamina))
        cr.set_source_rgba(1, 1, 1)
        cr.fill()

    if move1 or move2:
        cr.select_font_face("SF UI Text Semibold")
        cr.set_font_size(16)
        if move1:
            cr.move_to(text_x, 170)
            cr.text_path("Move 1: {}".format(move1))
        if move2:
            cr.move_to(text_x, 188)
            cr.text_path("Move 2: {}".format(move2))
        cr.set_source_rgba(0, 0, 0)
        cr.stroke()

        if move1:
            cr.move_to(text_x, 170)
            cr.text_path("Move 1: {}".format(move1))
        if move2:
            cr.move_to(text_x, 188)
            cr.text_path("Move 2: {}".format(move2))
        cr.set_source_rgba(1, 1, 1)
        cr.fill()


def draw_name(cr, name):
    """Draw the Pokemon's name."""
    cr.set_line_width(2.5)
    text_x = 240
    text_y = 50
    cr.select_font_face("SF UI Display Bold")
    cr.set_font_size(32)
    cr.move_to(text_x, text_y)
    cr.set_source_rgba(0, 0, 0)
    cr.text_path(name)
    cr.stroke()
    cr.move_to(text_x, text_y)
    cr.set_source_rgba(1, 1, 1)
    cr.show_text(name)


def create_image(pokemon_id, iv, move1, move2):
    try:
        attack, defense, stamina = iv
        name = POKEMON_NAMES[pokemon_id]
        if config.TZ_OFFSET:
            now = datetime.now(timezone(timedelta(hours=config.TZ_OFFSET)))
        else:
            now = datetime.now()
        hour = now.hour
        if hour > 6 and hour < 18:
            image = 'static/img/notification-bg-day.png'
        else:
            image = 'static/img/notification-bg-night.png'
        ims = cairo.ImageSurface.create_from_png(image)
        context = cairo.Context(ims)

        context.set_source_rgba(1, 1, 1)
        height = 204
        width = 224
        image = 'static/original-icons/{}.png'.format(pokemon_id)
        draw_image(context, image, height, width)
        draw_stats(context, attack, defense, stamina, move1, move2)
        draw_name(context, name)
        image = 'notification-images/{}-notification.png'.format(name)
        try:
            makedirs('notification-images')
        except OSError:
            pass
        ims.write_to_png(image)
        return image
    except Exception:
        return None


def generic_place_string():
    """ Create a place string with area name (if available)"""
    if config.AREA_NAME:
        # no landmarks defined, just use area name
        place = 'in ' + config.AREA_NAME
        return place
    else:
        # no landmarks or area name defined, just say 'around'
        return 'around'


class Notification:

    def __init__(self, name, coordinates, time_till_hidden, iv, score, image):
        self.name = name
        self.coordinates = coordinates
        self.image = image
        self.score = score
        self.attack, self.defense, self.stamina = iv

        if self.score == 1:
            self.description = 'perfect'
        elif self.score > .83:
            self.description = 'great'
        elif self.score > .6:
            self.description = 'good'
        else:
            self.description = 'wild'

        if config.TZ_OFFSET:
            now = datetime.now(timezone(timedelta(hours=config.TZ_OFFSET)))
        else:
            now = datetime.now()

        if config.HASHTAGS:
            self.hashtags = config.HASHTAGS.copy()
        else:
            self.hashtags = set()

        if isinstance(time_till_hidden, (tuple, list)):
            soonest, latest = time_till_hidden
            self.min_delta = timedelta(seconds=soonest)
            self.max_delta = timedelta(seconds=latest)
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
            if self.landmark.hashtags:
                self.hashtags.update(self.landmark.hashtags)
        else:
            self.place = generic_place_string()

        tweeted = False
        pushed = False

        if config.PB_API_KEY:
            pushed = self.pbpush()

        if (config.TWITTER_CONSUMER_KEY and
                config.TWITTER_CONSUMER_SECRET and
                config.TWITTER_ACCESS_KEY and
                config.TWITTER_ACCESS_SECRET):
            tweeted = self.tweet()

        return tweeted, pushed

    def pbpush(self):
        """ Send a PushBullet notification either privately or to a channel,
        depending on whether or not PB_CHANNEL is set in config.
        """

        from pushbullet import Pushbullet
        pb = Pushbullet(config.PB_API_KEY)

        if self.score < .47:
            description = 'weak'
        elif self.score < .35:
            description = 'bad'
        else:
            description = self.description

        area = config.AREA_NAME
        if self.delta:
            expiry = 'until ' + self.expire_time

            minutes, seconds = divmod(self.delta.total_seconds(), 60)
            time_remaining = '{m}m{s:.0f}s'.format(m=int(minutes), s=seconds)

            remaining = 'for ' + time_remaining
        else:
            expiry = 'until between {t1} and {t2}'.format(
                     t1=self.min_expire_time, t2=self.max_expire_time)

            minutes, seconds = divmod(self.min_delta.total_seconds(), 60)
            min_remaining = '{m}m{s:.0f}s'.format(m=int(minutes), s=seconds)
            minutes, seconds = divmod(self.max_delta.total_seconds(), 60)
            max_remaining = '{m}m{s:.0f}s'.format(m=int(minutes), s=seconds)

            remaining = 'for between {r1} and {r2}'.format(
                        r1=min_remaining, r2=max_remaining)

        title = ('A {desc} {name} will be in {area} {expiry}!'
                 ).format(desc=description, name=self.name, area=area,
                          expiry=expiry)

        body = ('It will be {p} {r}.\n'
                'Attack: {a}\n'
                'Defense: {d}\n'
                'Stamina: {s}').format(
            p=self.place, r=remaining,
            a=self.attack, d=self.defense, s=self.stamina)

        try:
            channel = pb.channels[config.PB_CHANNEL]
            channel.push_link(title, self.map_link, body)
        except (IndexError, KeyError):
            pb.push_link(title, self.map_link, body)
        return True

    def tweet(self):
        """ Create message, reduce it until it fits in a tweet, and then tweet
        it with a link to Google maps and tweet location included.
        """
        import twitter
        from twitter.twitter_utils import calc_expected_status_length

        def generate_tag_string(hashtags):
            '''create hashtag string'''
            tag_string = ''
            if hashtags:
                for hashtag in hashtags:
                    tag_string += ' #{}'.format(hashtag)
            return tag_string
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

        try:
            api = twitter.Api(consumer_key=config.TWITTER_CONSUMER_KEY,
                              consumer_secret=config.TWITTER_CONSUMER_SECRET,
                              access_token_key=config.TWITTER_ACCESS_KEY,
                              access_token_secret=config.TWITTER_ACCESS_SECRET)
            api.PostUpdate(tweet_text,
                           media=self.image,
                           latitude=self.coordinates[0],
                           longitude=self.coordinates[1],
                           display_coordinates=True)
        except Exception as e:
            print('Exception:', e)
            return False
        return True


class Notifier:

    def __init__(self, spawns):
        self.spawns = spawns
        self.recent_notifications = deque(maxlen=100)
        self.notify_ranking = config.NOTIFY_RANKING
        self.session = Session()
        self.initial_score = config.INITIAL_SCORE
        self.minimum_score = config.MINIMUM_SCORE
        self.last_notification = time.time() - (config.FULL_TIME / 2)
        self.always_notify = []
        if self.notify_ranking:
            self.set_pokemon_ranking(loadpickle=True)
            self.set_notify_ids()
            self.auto = True
        elif config.NOTIFY_IDS:
            self.pokemon_ranking = config.NOTIFY_IDS
            self.notify_ranking = len(self.pokemon_ranking)
            self.auto = False

    def set_notify_ids(self):
        self.notify_ids = self.pokemon_ranking[0:self.notify_ranking]
        self.always_notify = self.pokemon_ranking[0:config.ALWAYS_NOTIFY]

    def set_pokemon_ranking(self, loadpickle=False):
        self.ranking_time = time.time()
        if loadpickle:
            try:
                with open('pickles/ranking.pickle', 'rb') as f:
                    self.pokemon_ranking = pickle.load(f)
                    config.NOTIFY_IDS = []
                    for pokemon_id in self.pokemon_ranking[0:config.NOTIFY_RANKING]:
                        config.NOTIFY_IDS.append(pokemon_id)
                    return
            except (FileNotFoundError, EOFError):
                pass
        self.pokemon_ranking = get_pokemon_ranking(self.session)
        config.NOTIFY_IDS = []
        for pokemon_id in self.pokemon_ranking[0:config.NOTIFY_RANKING]:
            config.NOTIFY_IDS.append(pokemon_id)
        with open('pickles/ranking.pickle', 'wb') as f:
            pickle.dump(self.pokemon_ranking, f, pickle.HIGHEST_PROTOCOL)

    def evaluate_pokemon(self, pokemon_id, iv):
        attack, defense, stamina = iv
        exclude = config.ALWAYS_NOTIFY
        total = self.notify_ranking - exclude
        ranking = self.notify_ids.index(pokemon_id) - exclude
        percentile = 1 - (ranking / total)
        weighted = (attack + sqrt(defense) + sqrt(stamina)) / PERFECT_SCORE
        raw = sum(iv) / 45
        iv_score = (weighted + raw) / 2
        score = (percentile + iv_score) / 2
        return score, iv_score

    def notify(self, pokemon):
        """Send a PushBullet notification and/or a Tweet, depending on if their
        respective API keys have been set in config.
        """

        # skip if no API keys have been set in config
        if not (config.PB_API_KEY or config.TWITTER_CONSUMER_KEY):
            return False, 'Did not notify, no Twitter/PushBullet keys set.'

        spawn_id = pokemon['spawn_id']
        coordinates = (pokemon['lat'], pokemon['lon'])
        pokemon_id = pokemon['pokemon_id']
        encounter_id = pokemon['encounter_id']
        name = POKEMON_NAMES[pokemon_id]

        if encounter_id in self.recent_notifications:
            # skip duplicate
            return False, 'Already notified about {}.'.format(name)

        if pokemon['valid']:
            time_till_hidden = pokemon['time_till_hidden_ms'] / 1000
        else:
            time_till_hidden = self.spawns.get_time_till_hidden(spawn_id)

        if time_till_hidden:
            rem = time_till_hidden
        else:
            time_till_hidden = estimate_remaining_time(self.session, spawn_id)
            rem = sum(time_till_hidden) / 2

        now = time.time()
        if self.auto:
            time_passed = now - self.ranking_time
            if time_passed > 3600:
                self.set_pokemon_ranking()
                self.set_notify_ids()

        if pokemon_id in self.always_notify:
            score_required = 0
        else:
            if rem < config.TIME_REQUIRED:
                return False, '{n} has only {s} seconds remaining.'.format(
                    n=name, s=time_till_hidden
                )
            time_passed = now - self.last_notification
            if time_passed < config.FULL_TIME:
                fraction = time_passed / config.FULL_TIME
            else:
                fraction = 1
            subtract = (self.initial_score - self.minimum_score) * fraction
            score_required = self.initial_score - subtract


        if pokemon.get('individual_attack') is None:
            return False, '{} has no IVs.'.format(name)

        iv = (pokemon.get('individual_attack'),
              pokemon.get('individual_defense'),
              pokemon.get('individual_stamina'))
        move1 = MOVES.get(pokemon.get('move_1'), {}).get('name')
        move2 = MOVES.get(pokemon.get('move_2'), {}).get('name')

        score, iv_score = self.evaluate_pokemon(pokemon_id, iv)
        with open('pokemon_scores.txt', 'at') as f:
            f.write('{n}, a: {iv[0]}, d: {iv[1]}, s: {iv[2]}, iv: {i:.3f}, score: {sc:.3f}, required: {r:.3f}\n'.format(
                n=name, iv=iv, i=iv_score, sc=score, r=score_required
            ))

        if score < score_required:
            return False, "{n}'s score was {s:.3f} (iv: {i:.3f}), but {r:.3f} was required.".format(
                n=name, s=score, i=iv_score, r=score_required)

        image = create_image(pokemon_id, iv, move1, move2)

        tweeted, pushed = Notification(
            name, coordinates, time_till_hidden, iv, iv_score, image).notify()

        if tweeted or pushed:
            self.last_notification = time.time()
            self.recent_notifications.append(encounter_id)
            if tweeted and pushed:
                explanation = 'Tweeted and pushed '
            elif tweeted:
                explanation = 'Tweeted '
            else:
                explanation = 'Pushed '
        else:
            explanation = 'Failed to notify '

        explanation += 'about {}.'.format(name)
        return tweeted or pushed, explanation
