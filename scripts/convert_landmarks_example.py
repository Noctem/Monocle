#!/usr/bin/env python3


class Landmarks:
    args = 'name', 'shortname', 'points', 'query', 'hashtags', 'phrase', 'is_area', 'query_suffix'

    def __init__(self, query_suffix=None):
        self.query_suffix = query_suffix

        self.landmarks = []

    def add(self, *args, **kwargs):
        dictionary = {self.args[num]: arg for num, arg in enumerate(args)}
        dictionary.update(kwargs)

        self.landmarks.append(dictionary)

    def print_config(self):
        print('Replace your old Landmarks config with the following:\n')

        if self.query_suffix:
            print("QUERY_SUFFIX = '{}'".format(self.query_suffix))

        print('LANDMARKS =', tuple(self.landmarks))


### replace example below with your own old-style landmarks config ###
LANDMARKS = Landmarks(query_suffix='Salt Lake City')

LANDMARKS.add('Rice Eccles Stadium', hashtags={'Utes'})
LANDMARKS.add('the Salt Lake Temple', hashtags={'TempleSquare'})
LANDMARKS.add('City Creek Center', points=((40.769210, -111.893901), (40.767231, -111.888275)), hashtags={'CityCreek'})
LANDMARKS.add('the State Capitol', query='Utah State Capitol Building')
LANDMARKS.add('the University of Utah', hashtags={'Utes'}, phrase='at', is_area=True)
LANDMARKS.add('Yalecrest', points=((40.750263, -111.836502), (40.750377, -111.851108), (40.751515, -111.853833), (40.741212, -111.853909), (40.741188, -111.836519)), is_area=True)
### replace example above with your own old-style landmarks config ###


LANDMARKS.print_config()
