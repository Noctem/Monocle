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
    def __init__(self):
        self.boundaries = conf.BOUNDARIES
        self.south, self.west, self.north, self.east = self.boundaries.bounds
        self.center = self.boundaries.centroid.coords[0]

    def __contains__(self, p):
        return self.boundaries.contains(Point(p))

    def __hash__(self):
        return hash((self.south, self.west, self.north, self.east))


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

        assert isinstance(conf.BOUNDARIES, (Polygon, MultiPolygon))
    except AssertionError:
        raise TypeError('BOUNDARIES must be a shapely Polygon.')
    except ImportError as e:
        raise ImportError('BOUNDARIES is set but shapely is not available.') from e

    sys.modules[__name__] = PolyBounds()
elif conf.STAY_WITHIN_MAP:
    sys.modules[__name__] = RectBounds()
else:
    sys.modules[__name__] = Bounds()
