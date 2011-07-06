#!/usr/bin/env python

import web

from api import Lock, MachineLock

urls = (
    '/lock', 'Lock',
    '/lock/(.*)', 'MachineLock',
    )

app = web.application(urls, globals())

if __name__ == "__main__":
    app.run()
