from logging import getLogger

from shapely.geometry import Point, Polygon, shape, box, LineString
from shapely import speedups
from geopy import Nominatim
from pogeo import get_distance

if speedups.available:
    speedups.enable()


class FailedQuery(Exception):
    """Raised when no location is found."""


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

        if ((query_suffix and query) and
                query_suffix.lower() not in query.lower()):
            query = '{} {}'.format(query, query_suffix)

        self.location = None
        if query:
            self.query_location(query)
        elif points:
            try:
                length = len(points)
                if length > 2:
                    self.location = Polygon(points)
                elif length == 2:
                    self.location = box(points[0][0], points[0][1],
                                        points[1][0], points[1][1])
                elif length == 1:
                    self.location = Point(*points[0])
            except TypeError:
                raise ValueError('points must be a list/tuple of lists/tuples'
                                 ' containing 2 coordinates each')

        if not self.location:
            raise ValueError('No location provided for {}. Must provide'
                             ' either points, or query.'.format(self.name))
        elif not isinstance(self.location, (Point, Polygon, LineString)):
            raise NotImplementedError('{} is a {} which is not supported'
                                      .format(self.name, self.location.type))
        self.south, self.west, self.north, self.east = self.location.bounds

        # very imprecise conversion to square meters
        self.size = self.location.area * 12100000000

        if phrase:
            self.phrase = phrase
        elif is_area:
            self.phrase = 'in'
        else:
            self.phrase = 'at'

        self.hashtags = hashtags

    def __contains__(self, coordinates):
        """determine if a point is within this object range"""
        lat, lon = coordinates
        if (self.south <= lat <= self.north and
                self.west <= lon <= self.east):
            return self.location.contains(Point(lat, lon))
        return False

    def query_location(self, query):
        def swap_coords(geojson):
            out = []
            for x in geojson:
                if isinstance(x, list):
                    out.append(swap_coords(x))
                else:
                    return geojson[1], geojson[0]
            return out

        nom = Nominatim()
        try:
            geo = nom.geocode(query=query, geometry='geojson', timeout=3).raw
            geojson = geo['geojson']
        except (AttributeError, KeyError):
            raise FailedQuery('Query for {} did not return results.'.format(query))
        self.log.info('Nominatim returned {} for {}'.format(geo['display_name'], query))
        geojson['coordinates'] = swap_coords(geojson['coordinates'])
        self.location = shape(geojson)

    def get_coordinates(self):
        if isinstance(self.location, Polygon):
            return tuple(self.location.exterior.coordinates)
        else:
            return self.location.coords[0]

    def generate_string(self, coordinates):
        if coordinates in self:
            return '{} {}'.format(self.phrase, self.name)
        distance = self.distance_from_point(coordinates)
        if distance < 50 or (self.is_area and distance < 100):
            return '{} {}'.format(self.phrase, self.name)
        else:
            return '{:.0f} meters from {}'.format(distance, self.name)

    def distance_from_point(self, coordinates):
        point = Point(*coordinates)
        if isinstance(self.location, Point):
            nearest = self.location
        else:
            nearest = self.nearest_point(point)
        return get_distance(coordinates, nearest.coords[0])

    def nearest_point(self, point):
        '''Find nearest point in geometry, measured from given point.'''
        if isinstance(self.location, Polygon):
            segs = self.pairs(self.location.exterior.coords)
        elif isinstance(self.location, LineString):
            segs = self.pairs(self.location.coords)
        else:
            raise NotImplementedError('project_point_to_object not implemented'
                                      "for geometry type '{}'.".format(
                                      self.location.type))

        nearest_point = None
        min_dist = float("inf")

        for seg_start, seg_end in segs:
            line_start = Point(seg_start)
            line_end = Point(seg_end)

            intersection_point = self.project_point_to_line(
                point, line_start, line_end)
            cur_dist = point.distance(intersection_point)

            if cur_dist < min_dist:
                min_dist = cur_dist
                nearest_point = intersection_point
        return nearest_point

    @staticmethod
    def pairs(lst):
        """Iterate over a list in overlapping pairs."""
        i = iter(lst)
        prev = next(i)
        for item in i:
            yield prev, item
            prev = item

    @staticmethod
    def project_point_to_line(point, line_start, line_end):
        '''Find nearest point on a straight line,
           measured from given point.'''
        line_magnitude = line_start.distance(line_end)

        u = (((point.x - line_start.x) * (line_end.x - line_start.x) +
              (point.y - line_start.y) * (line_end.y - line_start.y))
             / (line_magnitude ** 2))

        # closest point does not fall within the line segment,
        # take the shorter distance to an endpoint
        if u < 0.00001 or u > 1:
            ix = point.distance(line_start)
            iy = point.distance(line_end)
            if ix > iy:
                return line_end
            else:
                return line_start
        else:
            ix = line_start.x + u * (line_end.x - line_start.x)
            iy = line_start.y + u * (line_end.y - line_start.y)
            return Point([ix, iy])


class Landmarks:

    def __init__(self, query_suffix=None):
        self.points_of_interest = set()
        self.areas = set()
        self.query_suffix = query_suffix

    def add(self, *args, **kwargs):
        if ('query_suffix' not in kwargs) and self.query_suffix and (
                'query' not in kwargs):
            kwargs['query_suffix'] = self.query_suffix
        landmark = Landmark(*args, **kwargs)
        if landmark.is_area:
            self.areas.add(landmark)
        else:
            self.points_of_interest.add(landmark)
        if landmark.size < 1:
            print(landmark.name, type(landmark.location), '\n')
        else:
            print(landmark.name, landmark.size, type(landmark.location), '\n')

    def find_landmark(self, coords, max_distance=750):
        landmark = find_within(self.points_of_interest, coords)
        if landmark:
            return landmark
        landmark, distance = find_closest(self.points_of_interest, coords)
        try:
            if distance < max_distance:
                return landmark
        except TypeError:
            pass

        area = find_within(self.areas, coords)
        if area:
            return area

        area, area_distance = find_closest(self.areas, coords)

        try:
            if area and area_distance < distance:
                return area
            else:
                return landmark
        except TypeError:
            return area


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


def find_closest(landmarks, coordinates):
    landmarks = iter(landmarks)
    try:
        closest_landmark = next(landmarks)
    except StopIteration:
        return None, None
    shortest_distance = closest_landmark.distance_from_point(coordinates)
    for landmark in landmarks:
        distance = landmark.distance_from_point(coordinates)
        if distance <= shortest_distance:
            if (distance == shortest_distance
                    and landmark.size > closest_landmark.size):
                continue
            shortest_distance = distance
            closest_landmark = landmark
    return closest_landmark, shortest_distance
