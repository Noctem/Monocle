from datetime import datetime, timedelta, timezone
from collections import deque
from os import makedirs

import time
import cairo

from db import Session, get_pokemon_ranking, get_despawn_time, estimate_remaining_time
from names import POKEMON_NAMES, MOVES

import config

# set unset config options to None
for variable_name in ['PB_API_KEY', 'PB_CHANNEL', 'TWITTER_CONSUMER_KEY',
                      'TWITTER_CONSUMER_SECRET', 'TWITTER_ACCESS_KEY',
                      'TWITTER_ACCESS_SECRET', 'LANDMARKS', 'AREA_NAME',
                      'HASHTAGS', 'TZ_OFFSET', 'NOTIFY_RANKING', 'NOTIFY_IDS']:
    if not hasattr(config, variable_name):
        setattr(config, variable_name, None)

# set defaults for unset config options
if not hasattr(config, 'ALWAYS_NOTIFY'):
    setattr(config, 'ALWAYS_NOTIFY', 0)
if not hasattr(config, 'FULL_TIME'):
    setattr(config, 'FULL_TIME', 1800)

def draw_image(ctx, image, top, left, height, width):
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
            ctx.translate(left+10, 10)
        elif scale_xy != height_ratio:
            new_height = img_height * scale_xy
            top = (height - new_height) / 2
            ctx.translate(10, top+10)
    else:
        left = (width - img_width) / 2 + 10
        top = (height - img_height) / 2 + 10
        ctx.translate(left, top)
    ctx.set_source_surface(image_surface)

    ctx.paint()
    ctx.restore()

def draw_text(cr, attack, defense, stamina, move1=None, move2=None):
    """Draw a box on a given context."""
    attack = str(attack)
    defense = str(defense)
    stamina = str(stamina)

    if not (len(attack) == 2 and len(defense) == 1 and len(stamina) == 1):
        attack = attack.rjust(2)
        defense = defense.rjust(2)
        stamina = stamina.rjust(2)

    cr.set_line_width(1.5)
    cr.select_font_face("SF Mono Semibold")
    cr.set_font_size(24)

    cr.move_to(300, 96)
    cr.text_path("Attack:  {}/15".format(attack))
    cr.move_to(300, 124)
    cr.text_path("Defense: {}/15".format(defense))
    cr.move_to(300, 152)
    cr.text_path("Stamina: {}/15".format(stamina))
    cr.set_source_rgba(0,0,0)
    cr.stroke()

    cr.move_to(300, 96)
    cr.text_path("Attack:  {}/15".format(attack))
    cr.move_to(300, 124)
    cr.text_path("Defense: {}/15".format(defense))
    cr.move_to(300, 152)
    cr.text_path("Stamina: {}/15".format(stamina))
    cr.set_source_rgba(1,1,1)
    cr.fill()

    if move1 or move2:
        cr.select_font_face("SF UI Text Semibold")
        cr.set_font_size(16)
        if move1:
            cr.move_to(300, 184)
            cr.text_path("Move 1: {}".format(move1))
        if move2:
            cr.move_to(300, 204)
            cr.text_path("Move 2: {}".format(move2))
        cr.set_source_rgba(0,0,0)
        cr.stroke()

        if move1:
            cr.move_to(300, 184)
            cr.text_path("Move 1: {}".format(move1))
        if move2:
            cr.move_to(300, 204)
            cr.text_path("Move 2: {}".format(move2))
        cr.set_source_rgba(1,1,1)
        cr.fill()

def draw_text2(cr, name):
    """Draw a box on a given context."""
    cr.set_line_width(2)
    cr.select_font_face("SF UI Display Bold")
    cr.set_font_size(38)
    cr.move_to(300, 50)
    cr.set_source_rgba(0,0,0)
    cr.text_path(name)
    cr.stroke()
    cr.move_to(300, 50)
    cr.set_source_rgba(1,1,1)
    cr.show_text(name)

def create_image(pokemon_id, iv, move1, move2):
    attack, defense, stamina = iv['attack'], iv['defense'], iv['stamina']
    number = pokemon_id
    name = POKEMON_NAMES[number]
    pokemon_id = str(pokemon_id).zfill(3)
    hour = datetime.now().hour
    if hour > 6 and hour < 18:
        ims = cairo.ImageSurface.create_from_png('static/img/notification-bg-day.png')
    else:
        ims = cairo.ImageSurface.create_from_png('static/img/notification-bg-night.png')
    context = cairo.Context(ims)

    context.set_source_rgba(1,1,1)
    height = 236
    width = 280
    margin = 10
    image = 'static/original-icons/{}.png'.format(pokemon_id)
    draw_image(context, image, margin, margin, height, width)
    draw_text(context, attack, defense, stamina, move1, move2)
    draw_text2(context, name)
    try:
        makedirs('notification-images')
    except OSError:
        pass
    ims.write_to_png('notification-images/{}-notification.png'.format(name))

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

    def __init__(self, name, coordinates, time_till_hidden, iv):
        self.name = name
        self.coordinates = coordinates
        self.attack, self.defense, self.stamina = iv['attack'], iv['defense'], iv['stamina']
        self.iv_sum = self.attack + self.defense + self.stamina

        if self.iv_sum == 45:
            self.description = 'perfect'
        elif self.iv_sum > 36:
            self.description = 'great'
        elif self.iv_sum > 24 and self.attack > 8:
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
                self.expire_time = (now + self.delta).strftime('%I:%M %p').lstrip('0')
            else:
                self.delta = None
                self.expire_time = None
                self.min_expire_time = (now + self.min_delta).strftime('%I:%M').lstrip('0')
                self.max_expire_time = (now + self.max_delta).strftime('%I:%M %p').lstrip('0')
        else:
            self.delta = timedelta(seconds=time_till_hidden)
            self.expire_time = (now + self.delta).strftime('%I:%M %p').lstrip('0')
            self.min_delta = None


        self.map_link = ('https://maps.google.com/maps?q=' +
                         str(round(self.coordinates[0], 5)) + ',' +
                         str(round(self.coordinates[1], 5)))
        self.place = None

    def notify(self):
        if config.LANDMARKS:
            landmark = config.LANDMARKS.find_landmark(self.coordinates)
        else:
            landmark = None

        if landmark:
            self.place = landmark.generate_string(self.coordinates)
            if landmark.hashtags:
                self.hashtags.update(landmark.hashtags)
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

        if tweeted and pushed:
            return (True, 'Tweeted and pushed about ' + self.name + '.')
        elif tweeted:
            return (True, 'Tweeted about ' + self.name + '.')
        elif pushed:
            return (True, 'Pushed about ' + self.name + '.')
        else:
            return (False, 'Failed to notify about ' + self.name + '.')

    def pbpush(self):
        """ Send a PushBullet notification either privately or to a channel,
        depending on whether or not PB_CHANNEL is set in config.
        """

        from pushbullet import Pushbullet
        pb = Pushbullet(config.PB_API_KEY)

        if self.iv_sum < 18:
            description = 'weak'
        elif self.iv_sum < 12:
            description = 'bad'
        else:
            description = self.description

        area = config.AREA_NAME
        if self.delta:
            expiry = 'until ' + self.expire_time

            minutes, seconds = divmod(self.delta.total_seconds(), 60)
            time_remaining = str(int(minutes)) + 'm' + str(round(seconds)) + 's'

            remaining = 'for ' + time_remaining
        else:
            expiry = 'until between {t1} and {t2}'.format(
                     t1=self.min_expire_time, t2=self.max_expire_time)

            min_minutes, min_seconds = divmod(self.min_delta.total_seconds(), 60)
            max_minutes, max_seconds = divmod(self.max_delta.total_seconds(), 60)
            min_remaining = str(int(min_minutes)) + 'm' + str(round(min_seconds)) + 's'
            max_remaining = str(int(max_minutes)) + 'm' + str(round(max_seconds)) + 's'

            remaining = 'for between {r1} and {r2}'.format(
                        r1=min_remaining, r2=max_remaining)

        title = ('A {desc} {name} will be in {area} {expiry}!'
                ).format(desc=description, name=self.name, area=area,
                expiry=expiry)

        body = ('It will be {place} {r}.\n'
                'Attack: {a}\n'
                'Defense: {d}\n'
                'Stamina: {s}').format(
                place=self.place, r=remaining,
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
            tweet_text = ('A {d} {n} appeared! It will be {p} until {e}. {t}'
                         ' {u}').format(d=self.description, n=self.name,
                         p=self.place, e=self.expire_time, t=tag_string,
                         u=self.map_link)
        else:
            tweet_text = ('A {d} {n} appeared {p}! It will expire sometime'
                          ' between {e1} and {e2}. {t} {u}').format(
                          d=self.description, n=self.name, p=self.place,
                          e1=self.min_expire_time, e2=self.max_expire_time,
                          t=tag_string, u=self.map_link)

        # remove hashtags until length is short enough
        while calc_expected_status_length(tweet_text) > 140:
            if self.hashtags:
                hashtag = self.hashtags.pop()
                tweet_text = tweet_text.replace(' #' + hashtag, '')
            else:
                break

        if calc_expected_status_length(tweet_text) > 140:
            if self.expire_time:
                tweet_text = 'A {d} {n} will be in {a} until {e}. {u}'.format(
                             d=self.description, n=self.name,
                             a=config.AREA_NAME, e=self.expire_time,
                             u=self.map_link)
            else:
                tweet_text = ("A {d} {n} appeared {p}! It'll expire between {e1}"
                             ' & {e2}. {u}').format(d=self.description,
                             n=self.name, p=self.place, e1=self.min_expire_time,
                             e2=self.max_expire_time, u=self.map_link)

        if calc_expected_status_length(tweet_text) > 140:
            if self.expire_time:
                tweet_text = 'A wild {n} will be around until {e}. {u}'.format(
                             n=self.name, e=self.expire_time, u=self.map_link)
            else:
                tweet_text = ('A {d} {n} will expire between {e1}'
                             ' & {e2}. {u}').format(d=self.description,
                             n=self.name, e1=self.min_expire_time,
                             e2=self.max_expire_time, u=self.map_link)

        image = 'notification-images/{}-notification.png'.format(self.name)
        try:
            api = twitter.Api(consumer_key=config.TWITTER_CONSUMER_KEY,
                              consumer_secret=config.TWITTER_CONSUMER_SECRET,
                              access_token_key=config.TWITTER_ACCESS_KEY,
                              access_token_secret=config.TWITTER_ACCESS_SECRET)
            api.PostUpdate(tweet_text,
                          media='notification-images/{}-notification.png'.format(self.name),
                          latitude=self.coordinates[0],
                          longitude=self.coordinates[1],
                          display_coordinates=True,
                          verify_status_length=False)
        except Exception as e:
            print('Exception:', e)
            return False
        return True

class Notifier:

    def __init__(self, spawns):
        self.spawns = spawns
        self.recent_notifications = deque(maxlen=100)
        self.notify_ranking = config.INITIAL_RANKING
        self.session = Session()
        self.set_pokemon_ranking()
        self.differences = deque(maxlen=10)
        self.last_notification = None
        self.always_notify = []
        if self.notify_ranking:
            setattr(config, 'NOTIFY_IDS', [])
            for pokemon_id in self.pokemon_ranking[0:config.NOTIFY_RANKING]:
                config.NOTIFY_IDS.append(pokemon_id)
            self.set_notify_ids()

    def set_notify_ids(self):
        self.notify_ids = []
        for pokemon_id in self.pokemon_ranking[0:self.notify_ranking]:
            self.notify_ids.append(pokemon_id)
        for pokemon_id in self.pokemon_ranking[0:config.ALWAYS_NOTIFY]:
            self.always_notify.append(pokemon_id)

    def set_pokemon_ranking(self):
        if self.notify_ranking:
            self.pokemon_ranking = get_pokemon_ranking(self.session)
        else:
            raise ValueError('Must configure NOTIFY_RANKING.')

    def notify(self, pokemon):
        """Send a PushBullet notification and/or a Tweet, depending on if their
        respective API keys have been set in config.
        """

        # skip if no API keys have been set in config
        if not (config.PB_API_KEY or config.TWITTER_CONSUMER_KEY):
            return (False, 'Did not notify, no Twitter/PushBullet keys set.')

        spawn_id = pokemon['spawn_id']
        coordinates = (pokemon['lat'], pokemon['lon'])
        pokemon_id = pokemon['pokemon_id']
        encounter_id = pokemon['encounter_id']
        name = POKEMON_NAMES[pokemon_id]

        if encounter_id in self.recent_notifications:
            # skip duplicate
            return (False, 'Already notified about ' + name + '.')

        if self.last_notification:
            time_passed = time.time() - self.last_notification
            if time_passed < config.FULL_TIME:
                fraction = time_passed / config.FULL_TIME
            else:
                fraction = 1
            dynamic_range = config.NOTIFY_RANKING - config.ALWAYS_ELIGIBLE
            self.notify_ranking = round(config.ALWAYS_ELIGIBLE + (dynamic_range * fraction))
            self.set_notify_ids()

        if pokemon_id not in self.notify_ids:
            return (False, name + ' is not in the top ' + str(self.notify_ranking))

        if pokemon['valid']:
            time_till_hidden = pokemon['time_till_hidden_ms'] / 1000
        else:
            time_till_hidden = self.spawns.get_time_till_hidden(spawn_id)

        if pokemon_id not in self.always_notify and time_till_hidden and time_till_hidden < 420:
            return (False, name + ' has only ' + str(time_till_hidden) + ' seconds remaining.')

        if not time_till_hidden:
            time_till_hidden = estimate_remaining_time(self.session, spawn_id)

        move1 = MOVES.get(pokemon.get('move_1'), {}).get('name')
        move2 = MOVES.get(pokemon.get('move_2'), {}).get('name')

        iv = {}
        iv['attack'], iv['defense'], iv['stamina'] = pokemon['individual_attack'], pokemon['individual_defense'], pokemon['individual_stamina']
        create_image(pokemon_id, iv, move1, move2)

        code, explanation = Notification(name, coordinates, time_till_hidden, iv).notify()
        if code:
            self.last_notification = time.time()
            self.recent_notifications.append(encounter_id)
            self.set_pokemon_ranking()
        return (code, explanation)
