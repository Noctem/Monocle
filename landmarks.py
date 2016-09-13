from shapely.geometry import Point, Polygon, shape, box, LineString
from geopy import Nominatim
from geopy.distance import vincenty


class FailedQuery(Exception):
    """Raised when no location is found."""


class Landmark:
    ''' Contains information about user-defined landmarks.'''

    def __init__(self, name, points=None, query=None, hashtags=None,
                 phrase=None, is_area=False, query_suffix=None):
        self.name = name
        self.is_area = is_area

        if not points and not query:
            query = name.lstrip('the ')

        if ((query_suffix and query) and
                query_suffix.lower() not in query.lower()):
            query = query + ' ' + query_suffix

        self.location = None
        if query:
            self.query_location(query)
        elif points:
            try:
                if len(points) > 2:
                    self.location = Polygon(points)
                elif len(points) == 2:
                    self.location = box(points[0][0], points[0][1],
                                        points[1][0], points[1][1])
                elif len(points) == 1:
                    self.location = Point(*points[0])
            except (TypeError, IndexError):
                print('points must be a list/tuple of lists/tuples'
                      ' containing 2 coordinates each')
                raise

        if not self.location:
            raise ValueError('No location provided for ' + name +
                             '. Must provide either points, or query.')
        elif not isinstance(self.location, (Point, Polygon, LineString)):
            raise NotImplementedError(name + 'is a ' + self.location.type +
                                      ' which is not supported.')

        # very imprecise conversion to square meters
        self.size = round(self.location.area * 12100000000)

        if not phrase:
            if is_area:
                self.phrase = 'in'
            else:
                self.phrase = 'at'

        self.hashtags = hashtags

    def query_location(self, query):
        def swap_coords(coordinates):
            out = []
            for iterable in coordinates:
                if isinstance(iterable, list):
                    out.append(swap_coords(iterable))
                else:
                    return (coordinates[1], coordinates[0])
            return out

        nom = Nominatim()
        try:
            geo = nom.geocode(query=query, geometry='geojson').raw
            geojson = geo['geojson']
        except (AttributeError, KeyError):
            raise FailedQuery('Query for ' + query + ' did not return results.')
        print(geo['display_name'])
        geojson['coordinates'] = swap_coords(geojson['coordinates'])
        self.location = shape(geojson)

    def get_coordinates(self):
        if isinstance(self.location, Polygon):
            return tuple(self.location.exterior.coordinates)
        elif isinstance(self.location, Point):
            return self.location.coords[0]
        else:
            return self.location

    def generate_string(self, coordinates):
        if self.contains(coordinates):
            return self.phrase + ' ' + self.name
        distance = round(self.distance_from_point(coordinates))
        if (self.is_area and distance < 100) or distance < 20:
            return self.phrase + ' ' + self.name
        else:
            return str(distance) + ' meters from ' + self.name

    def contains(self, coordinates):
        """determine if a point is within this object range"""
        return self.location.contains(Point(*coordinates))

    def distance_from_point(self, coordinates):
        if self.contains(coordinates):
            return 0
        point = Point(*coordinates)
        if isinstance(self.location, Point):
            nearest = self.location
        else:
            nearest = self.nearest_point(point)
        dist = vincenty(point.coords[0], nearest.coords[0])
        return dist.meters

    def nearest_point(self, point):
        '''Find nearest point in geometry, measured from given point.'''
        def pairs(lst):
            """Iterate over a list in overlapping pairs."""
            i = iter(lst)
            prev = next(i)
            for item in i:
                yield prev, item
                prev = item

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

        nearest_point = None
        min_dist = float("inf")

        if isinstance(self.location, Polygon):
            for seg_start, seg_end in pairs(
                    tuple(self.location.exterior.coords)):
                line_start = Point(seg_start)
                line_end = Point(seg_end)

                intersection_point = project_point_to_line(
                    point, line_start, line_end)
                cur_dist = point.distance(intersection_point)

                if cur_dist < min_dist:
                    min_dist = cur_dist
                    nearest_point = intersection_point
        elif isinstance(self.location, LineString):
            for seg_start, seg_end in pairs(list(self.location.coords)):
                line_start = Point(seg_start)
                line_end = Point(seg_end)

                intersection_point = project_point_to_line(
                    point, line_start, line_end)
                cur_dist = point.distance(intersection_point)

                if cur_dist < min_dist:
                    min_dist = cur_dist
                    nearest_point = intersection_point
        else:
            raise NotImplementedError('project_point_to_object not ' +
                                      "implemented for geometry type '" +
                                      self.location.type + "'.")
        return nearest_point


class Landmarks:

    def __init__(self, query_suffix=None):
        self.all_landmarks = []
        self.points_of_interest = []
        self.areas = []
        self.query_suffix = query_suffix

    def add(self, *args, **kwargs):
        if ('query_suffix' not in kwargs) and (self.query_suffix) and (
                'query' not in kwargs):
            kwargs['query_suffix'] = self.query_suffix
        landmark = Landmark(*args, **kwargs)
        self.all_landmarks.append(landmark)
        if landmark.is_area:
            self.areas.append(landmark)
        else:
            self.points_of_interest.append(landmark)
        if landmark.size < 1:
            print(landmark.name, type(landmark.location), '\n')
        else:
            print(landmark.name, landmark.size, type(landmark.location), '\n')

    def find_landmark(self, coords, max_distance=750):
        if self.points_of_interest:
            found = find_within(self.points_of_interest, coords)
            if found:
                return found
            found, distance = find_closest(self.points_of_interest, coords)
            if distance < max_distance:
                return found
        if self.all_landmarks:
            found = find_within(self.areas, coords)
            if found:
                return found
            found, distance = find_closest(self.all_landmarks, coords)
            if found:
                return found
        return None


def find_within(landmark_list, coordinates):
    within = []
    for landmark in landmark_list:
        if landmark.contains(coordinates):
            within.append(landmark)
    if within:
        if len(within) == 1:
            return within[0]
        smallest_size = float('inf')
        smallest = None
        for landmark in within:
            if landmark.size < smallest_size:
                smallest.size = landmark.size
                smallest = landmark
        return smallest
    return None


def find_closest(landmark_list, coordinates):
    closest_landmark = None
    shortest_distance = float("inf")
    for landmark in landmark_list:
        distance = landmark.distance_from_point(coordinates)
        if distance < shortest_distance:
            shortest_distance = distance
            closest_landmark = landmark
        elif distance == shortest_distance:
            if landmark.size < closest_landmark.size:
                shortest_distance = distance
                closest_landmark = landmark
    return (closest_landmark, shortest_distance)
