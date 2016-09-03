from shapely.geometry import Point, Polygon, shape, box, LineString
from geopy import Nominatim
from geopy.distance import vincenty

#from config import MAP_START, MAP_END
#SCAN_RANGE = box(MAP_START[0], MAP_START[1], MAP_END[0], MAP_END[1])

class FailedQuery(Exception):
    """Raised when no location is found."""

class Landmarks:
    def __init__(self, query_suffix=None, hashtags=set()):
        self.all_landmarks = []
        self.points_of_interest = []
        self.areas = []
        self.query_suffix = query_suffix
        self.hashtags = hashtags

    class Landmark:
        ''' Contains information about user-defined landmarks.'''
        def __init__(self, name, points=None, query=None, hashtags=set(), phrase=None, is_area=False, query_suffix=None):
            self.name = name
            self.is_area = is_area

            if not points and not query:
                query = name.lstrip('the ')

            if query_suffix and query:
                if query_suffix.lower() not in query.lower():
                    query = query + ' ' + query_suffix

            self.location = None
            if query:
                self.query_location(query)
            elif points:
                try:
                    if len(points) > 2:
                        self.location = Polygon(points)
                    elif len(points) == 2:
                        self.location = box(points[0][0], points[0][1], points[1][0], points[1][1])
                    elif len(points) == 1:
                        self.location = Point(*points)
                except (TypeError, IndexError):
                    print("points must be a list/tuple of lists/tuples containing 2 coordinates each")
                    raise

            if not self.location:
                raise ValueError('No location provide for ' + name + '. Must provide either points, or query.')
            # very imprecise conversion to square meters
            self.size = round(self.location.area * 12100000000)

            self.center = self.location.centroid
            #if not SCAN_RANGE.contains(self.center):
            #    print('Warning: the center of ' + self.name + ' is outside of your scan range. ' + str(self.center.coords[0]))

            if not phrase:
                if is_area:
                    self.phrase = 'in'
                else:
                    self.phrase = 'at'

            self.hashtags = hashtags

        def query_location(self, query):
            def swap_coords(coordinates):
                out = []
                for iter in coordinates:
                    if isinstance(iter, list):
                        out.append(swap_coords(iter))
                    else:
                        return (coordinates[1], coordinates[0])
                return out

            def boundingbox(bounds):
                bounds = (float(bounds[0]), float(bounds[2]), float(bounds[1]), float(bounds[3]))
                if (bounds[0] != bounds[2]) and (bounds[1] != bounds[3]):
                    print('Warning: using bounding box for ' + query)
                    return bounds
                print(bounds)
                return False

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
            nearest = self.nearest_point(point)
            dist = vincenty(point.coords[0], nearest.coords[0])
            return dist.meters

        def nearest_point(self, point):
            """Find nearest point in geometry, measured from given point."""
            def pairs(lst):
                """Iterate over a list in overlapping pairs."""
                i = iter(lst)
                prev = next(i)
                for item in i:
                    yield prev, item
                    prev = item

            def project_point_to_line(point, line_start, line_end):
                """Find nearest point on a straight line, measured from given point."""
                line_magnitude = line_start.distance(line_end)

                u = ((point.x - line_start.x) * (line_end.x - line_start.x) +
                     (point.y - line_start.y) * (line_end.y - line_start.y)) \
                     / (line_magnitude ** 2)

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
                for seg_start, seg_end in pairs(list(self.location.exterior.coords)):
                    line_start = Point(seg_start)
                    line_end = Point(seg_end)

                    intersection_point = project_point_to_line(point, line_start, line_end)
                    cur_dist =  point.distance(intersection_point)

                    if cur_dist < min_dist:
                        min_dist = cur_dist
                        nearest_point = intersection_point
            elif isinstance(self.location, LineString):
                for seg_start, seg_end in pairs(list(self.location.coords)):
                    line_start = Point(seg_start)
                    line_end = Point(seg_end)

                    intersection_point = project_point_to_line(point, line_start, line_end)
                    cur_dist =  point.distance(intersection_point)

                    if cur_dist < min_dist:
                        min_dist = cur_dist
                        nearest_point = intersection_point
            else:
                raise NotImplementedError("project_point_to_object not implemented for"+
                                          " geometry type '" + self.location.type + "'.")
            return nearest_point

    def add(self, *args, **kwargs):
        if ('query_suffix' not in kwargs) and (self.query_suffix) and ('query' not in kwargs):
            kwargs['query_suffix'] = self.query_suffix
        if 'hashtags' in kwargs:
            kwargs['hashtags'].update(self.hashtags)
        else:
            kwargs['hashtags'] = self.hashtags
        landmark = self.Landmark(*args, **kwargs)
        self.all_landmarks.append(landmark)
        if landmark.is_area:
            self.areas.append(landmark)
        else:
            self.points_of_interest.append(landmark)
        if landmark.size < 1:
            print(landmark.name, type(landmark.location), '\n')
        else:
            print(landmark.name, landmark.size, type(landmark.location), '\n')


    def find_within(self, landmark_list, coordinates):
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

    def find_closest(self, landmark_list, coordinates):
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

    def find_landmark(self, coordinates, max_distance=1000):
        if self.points_of_interest:
            found = self.find_within(self.points_of_interest, coordinates)
            if found:
                return found
            found, distance = self.find_closest(self.points_of_interest, coordinates)
            if distance < max_distance:
                return found
        if self.all_landmarks:
            found = self.find_within(self.areas, coordinates)
            if found:
                return found
            found, distance = self.find_closest(self.all_landmarks, coordinates)
            if found:
                return found
        return None

