# -*- coding: utf-8 -*-
'''
Progress Mgr Module Helper

This python module implements helper methods to retrieve the
executing and completed tasks tacked by the progress mgr module
using the same structure of dashboard tasks
'''

from __future__ import absolute_import

from datetime import datetime
import logging

from . import rbd
from .. import mgr


logger = logging.getLogger('progress')


def _progress_event_to_dashboard_task_common(event, task):
    if event['refs'] and isinstance(event['refs'], dict):
        refs = event['refs']
        if refs['origin'] == "rbd_support":
            # rbd mgr module event, we can transform this event into an rbd dashboard task
            action_map = {
                'remove': "delete",
                'flatten': "flatten",
                'trash remove': "trash/remove"
            }
            action = action_map.get(refs['action'], refs['action'])
            metadata = {}
            if 'image_name' in refs:
                metadata['image_spec'] = rbd.get_image_spec(refs['pool_name'],
                                                            refs['pool_namespace'],
                                                            refs['image_name'])
            else:
                metadata['image_id_spec'] = rbd.get_image_spec(refs['pool_name'],
                                                               refs['pool_namespace'],
                                                               refs['image_id'])
            task.update({
                'name': "rbd/{}".format(action),
                'metadata': metadata,
                'begin_time': "{}Z".format(datetime.fromtimestamp(event["started_at"])
                                           .isoformat()),
            })
            return

    task.update({
        # we're prepending the "progress/" prefix to tag tasks that come
        # from the progress module
        'name': "progress/{}".format(event['message']),
        'metadata': dict(event.get('refs', {})),
        'begin_time': "{}Z".format(datetime.fromtimestamp(event["started_at"])
                                   .isoformat()),
    })


def _progress_event_to_dashboard_task(event, completed=False):
    task = {}
    _progress_event_to_dashboard_task_common(event, task)
    if not completed:
        task.update({
            'progress': int(100 * event['progress'])
        })
    else:
        task.update({
            'end_time': "{}Z".format(datetime.fromtimestamp(event['finished_at'])
                                     .isoformat()),
            'duration': event['finished_at'] - event['started_at'],
            'progress': 100,
            'success': 'failed' not in event,
            'ret_value': None,
            'exception': {'detail': event['failure_message']} if 'failed' in event else None
        })
    return task


def get_progress_tasks():
    executing_t = []
    finished_t = []
    progress_events = mgr.remote('progress', "_json")

    for ev in progress_events['events']:
        logger.debug("event=%s", ev)
        executing_t.append(_progress_event_to_dashboard_task(ev))

    for ev in progress_events['completed']:
        logger.debug("finished event=%s", ev)
        finished_t.append(_progress_event_to_dashboard_task(ev, True))

    return executing_t, finished_t
