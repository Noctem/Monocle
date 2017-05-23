from hashlib import sha256
from logging import getLogger

from aiopogo import json_dumps
from pogeo import Location, Loop, Rectangle
from pogeo.geocoder import geocode

from .utils import dump_pickle, load_pickle


class Landmark:
    ''' Contains information about user-defined landmarks.'''
    log = getLogger('landmarks')

    def __init__(self, name, shortname=None, points=None, query=None,
                 hashtags=None, phrase=None, is_area=False, query_suffix=None):
        self.name = name
        self.shortname = shortname
        self.is_area = is_area

        if not points and not query:
            query = name.lstrip('the ')

        # append query suffix if it's not already present in query
        if ((query_suffix and query) and
                query_suffix.lower() not in query.lower()):
            query = '{} {}'.format(query, query_suffix)

        self.location = None
        if query:
            self.location = geocode(query, self.log)
        elif points:
            try:
                length = len(points)
                if length > 2:
                    self.location = Loop(points)
                elif length == 2:
                    self.location = Rectangle(*points)
                elif length == 1:
                    self.location = Location(*points[0])
            except TypeError:
                raise ValueError('points must be a list/tuple of lists/tuples'
                                 ' containing 2 coordinates each')

        if not self.location:
            raise ValueError('No location provided for {}. Must provide'
                             ' either points, or query.'.format(self.name))

        # square kilometers
        self.size = self.location.area

        if phrase:
            self.phrase = phrase
        elif is_area:
            self.phrase = 'in'
        else:
            self.phrase = 'at'

        self.hashtags = hashtags

    def __repr__(self):
        center = self.location if isinstance(self.location, Location) else self.location.center
        return '<Landmark | {} | {} | {} | {:.5f}km²>'.format(self.name, center, type(self.location), self.size)

    def __contains__(self, loc):
        """determine if a point is within this object range"""
        return loc in self.location

    def generate_string(self, loc):
        if loc in self.location:
            return '{} {}'.format(self.phrase, self.name)
        distance = self.location.distance(loc)
        if distance < 50 or (self.is_area and distance < 100):
            return '{} {}'.format(self.phrase, self.name)
        else:
            return '{:.0f} meters from {}'.format(distance, self.name)


class Landmarks:
    __slots__ = ('points_of_interest', 'areas')

    def __init__(self, landmarks, query_suffix):
        self.areas = []
        self.points_of_interest = []

        sha = sha256(
            json_dumps(landmarks,
                       ensure_ascii=False,
                       sort_keys=True).encode('utf-8')
        ).digest()

        if not self.unpickle(sha):
            for kwargs in landmarks:
                if 'query_suffix' not in kwargs and 'query' not in kwargs:
                    kwargs['query_suffix'] = query_suffix

                landmark = Landmark(**kwargs)
                if landmark.is_area:
                    self.areas.append(landmark)
                else:
                    self.points_of_interest.append(landmark)

            self.pickle(sha)

    def __bool__(self):
        return self.points_of_interest or self.areas

    def pickle(self, sha):
        dump_pickle('landmarks', {
            'areas': self.areas,
            'points_of_interest': self.points_of_interest,
            'sha': sha})

    def unpickle(self, sha):
        try:
            pickled = load_pickle('landmarks', raise_exception=True)
            if sha == pickled['sha']:
                self.areas = pickled['areas']
                self.points_of_interest = pickled['points_of_interest']
                return True
            else:
                return False
        except (FileNotFoundError, KeyError):
            return False

    def find_landmark(self, coords, max_distance=750):
        landmark = self.find_within(self.points_of_interest, coords)
        if landmark:
            return landmark
        landmark, distance = self.find_closest(self.points_of_interest, coords)
        try:
            if distance < max_distance:
                return landmark
        except TypeError:
            pass

        area = self.find_within(self.areas, coords)
        if area:
            return area

        area, area_distance = self.find_closest(self.areas, coords)

        try:
            if area and area_distance < distance:
                return area
            else:
                return landmark
        except TypeError:
            return area

    @staticmethod
    def find_within(landmarks, coordinates):
        within = [landmark for landmark in landmarks if coordinates in landmark]
        found = len(within)
        if found == 1:
            return within[0]
        if found:
            landmarks = iter(within)
            smallest = next(landmarks)
            smallest_size = landmark.size
            for landmark in landmarks:
                if landmark.size < smallest_size:
                    smallest = landmark
                    smallest_size = landmark.size
            return smallest
        return None

    @staticmethod
    def find_closest(landmarks, coordinates):
        landmarks = iter(landmarks)
        try:
            closest_landmark = next(landmarks)
        except StopIteration:
            return None, None
        shortest_distance = closest_landmark.location.distance(coordinates)
        for landmark in landmarks:
            distance = landmark.location.distance(coordinates)
            if distance <= shortest_distance:
                if (distance == shortest_distance
                        and landmark.size > closest_landmark.size):
                    continue
                shortest_distance = distance
                closest_landmark = landmark
        return closest_landmark, shortest_distance
