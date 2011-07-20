#!/usr/bin/env python

import os
import sys
import web

abspath = os.path.dirname(__file__)
if abspath not in sys.path:
    sys.path.append(abspath)

from api import Lock, MachineLock

urls = (
    '/lock', 'Lock',
    '/lock/(.*)', 'MachineLock',
    )

app = web.application(urls, globals())
application = app.wsgifunc()
