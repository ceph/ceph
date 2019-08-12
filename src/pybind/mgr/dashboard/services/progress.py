# -*- coding: utf-8 -*-
'''
Progress Mgr Module Helper

This python module implements helper methods to retrieve the
executing and completed tasks tacked by the progress mgr module
using the same structure of dashboard tasks
'''

from __future__ import absolute_import

from datetime import datetime

from .. import mgr, logger


def get_progress_tasks():
    executing_t = []
    finished_t = []
    progress_events = mgr.remote('progress', "_json")

    for ev in progress_events['events']:
        logger.debug("[Progress] event=%s", ev)
        executing_t.append({
            # we're prepending the "progress/" prefix to tag tasks that come
            # from the progress module
            'name': "progress/{}".format(ev['message']),
            'metadata': dict(ev['refs']),
            'begin_time': "{}Z".format(datetime.fromtimestamp(ev["started_at"])
                                       .isoformat()),
            'progress': int(100 * ev['progress'])
        })

    for ev in progress_events['completed']:
        logger.debug("[Progress] finished event=%s", ev)
        finished_t.append({
            'name': "progress/{}".format(ev['message']),
            'metadata': dict(ev['refs']),
            'begin_time': "{}Z".format(datetime.fromtimestamp(ev["started_at"])
                                       .isoformat()),
            'end_time': "{}Z".format(datetime.fromtimestamp(ev['finished_at'])
                                     .isoformat()),
            'duration': ev['finished_at'] - ev['started_at'],
            'progress': 100,
            'success': True,
            'ret_value': None,
            'exception': None
        })

    return executing_t, finished_t
