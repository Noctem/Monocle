import sys

from . import sanitized as conf
from .utils import get_distance


class Bounds:
    def __init__(self):
        self.north = max(conf.MAP_START[0], conf.MAP_END[0])
        self.south = min(conf.MAP_START[0], conf.MAP_END[0])
        self.east = max(conf.MAP_START[1], conf.MAP_END[1])
        self.west = min(conf.MAP_START[1], conf.MAP_END[1])
        self.center = ((self.north + self.south) / 2,
                       (self.west + self.east) / 2)
        self.multi = False

    def __bool__(self):
        """Are boundaries a polygon?"""
        return False

    def __contains__(self, p):
        return True

    def __hash__(self):
        return 0

    @property
    def area(self):
        """Returns the square kilometers for configured scan area"""
        width = get_distance((self.center[0], self.west), (self.center[0], self.east), 2)
        height = get_distance((self.south, 0), (self.north, 0), 2)
        return round(width * height)


class PolyBounds(Bounds):
    def __init__(self, polygon=conf.BOUNDARIES):
        self.boundaries = prep(polygon)
        self.south, self.west, self.north, self.east = polygon.bounds
        self.center = polygon.centroid.coords[0]
        self.multi = False
        self.polygon = polygon

    def __bool__(self):
        """Are boundaries a polygon?"""
        return True

    def __contains__(self, p):
        return self.boundaries.contains(Point(p))

    def __hash__(self):
        return hash((self.south, self.west, self.north, self.east))


class MultiPolyBounds(PolyBounds):
    def __init__(self):
        super().__init__()
        self.multi = True
        self.polygons = [PolyBounds(polygon) for polygon in self.polygon]

    def __hash__(self):
        return hash(tuple(hash(x) for x in self.polygons))

    @property
    def area(self):
        return sum(x.area for x in self.polygons)


class RectBounds(Bounds):
    def __contains__(self, p):
        lat, lon = p
        return (self.south <= lat <= self.north and
                self.west <= lon <= self.east)

    def __hash__(self):
        return hash((self.north, self.east, self.south, self.west))


if conf.BOUNDARIES:
    try:
        from shapely.geometry import MultiPolygon, Point, Polygon
        from shapely.prepared import prep
    except ImportError as e:
        raise ImportError('BOUNDARIES is set but shapely is not available.') from e

    if isinstance(conf.BOUNDARIES, Polygon):
        sys.modules[__name__] = PolyBounds()
    elif isinstance(conf.BOUNDARIES, MultiPolygon):
        sys.modules[__name__] = MultiPolyBounds()
    else:
        raise TypeError('BOUNDARIES must be a shapely Polygon.')
elif conf.STAY_WITHIN_MAP:
    sys.modules[__name__] = RectBounds()
else:
    sys.modules[__name__] = Bounds()
