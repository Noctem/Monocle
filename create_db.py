#!/usr/bin/env python3

from monocle import db

db.Base.metadata.create_all(db.get_engine())
