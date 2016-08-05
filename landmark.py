from geopy import Point

import config


class Landmark:
    ''' Contains information about user-defined landmarks.'''
    def __init__(self, name, north=None, south=None, west=None, east=None,
                 center=None, hashtags=set(), phrase='at', secondary=None):
        if center:
            self.center = Point(latitude=center[0], longitude=center[1])
        else:
            self.center = Point(latitude=(north+south)/2,
                                longitude=(east+west)/2)
        self.north = north
        self.south = south
        self.west = west
        self.east = east
        try:
            self.hashtags = hashtags
            self.hashtags.update(config.HASHTAGS)
        except NameError:
            self.hashtags = hashtags
        self.name = name
        self.phrase = phrase

    def is_within(self, coordinates):
        """determine if a point is within this object range"""
        return bool(self.north > coordinates.latitude > self.south and
                    self.east > coordinates.longitude > self.west)
