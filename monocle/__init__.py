__title__ = 'monocle'
__version__ = '0.8b2'
__author__ = 'David Christenson'
__license__ = 'MIT License'
__copyright__ = 'Copyright (c) 2017 David Christenson <https://github.com/Noctem>'

from . import sanitized

if sanitized.SPAWN_ID_INT:
    from pogeo import cellid_to_location as spawnid_to_loc
    from pogeo import cellid_to_coords as spawnid_to_coords
else:
    from pogeo import token_to_location as spawnid_to_loc
    from pogeo import token_to_coords as spawnid_to_coords
