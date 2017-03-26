#!/usr/bin/env python3

import sys
from pathlib import Path

monocle_dir = Path(__file__).resolve().parents[1]
sys.path.append(str(monocle_dir))

from monocle.db import Base, _engine

Base.metadata.create_all(_engine)
print('Done!')
