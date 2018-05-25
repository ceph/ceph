from __future__ import absolute_import


class MetricsField(object):
    def __init__(self):
        self.tags = {}
        self.fields = {}
        self.timestamp = None

    def __str__(self):
        return str({
            'tags': self.tags,
            'fields': self.fields,
            'timestamp': self.timestamp
        })
