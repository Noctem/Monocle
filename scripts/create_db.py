#!/usr/bin/env python3

from pokeminer import db

db.Base.metadata.create_all(db.get_engine())
