import sys

from . import sanitized as conf
from .utils import get_distance


if conf.MULTI_BOUNDARIES:
    from pogeo import Polygon

    sys.modules[__name__] = Polygon(conf.MULTI_BOUNDARIES, conf.HOLES)
elif conf.BOUNDARIES:
    if conf.HOLES:
        from pogeo import Polygon

        sys.modules[__name__] = Polygon(conf.BOUNDARIES, conf.HOLES)
    else:
        from pogeo import Loop

        sys.modules[__name__] = Loop(conf.BOUNDARIES)
else:
    from pogeo import Rectangle

    sys.modules[__name__] = Rectangle(conf.MAP_START, conf.MAP_END, conf.STAY_WITHIN_MAP)
