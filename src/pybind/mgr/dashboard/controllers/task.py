# -*- coding: utf-8 -*-
from __future__ import absolute_import

from . import ApiController, RESTController
from ..tools import TaskManager
from ..services import progress


@ApiController('/task')
class Task(RESTController):
    def list(self, name=None):
        executing_t, finished_t = TaskManager.list_serializable(name)

        e, f = progress.get_progress_tasks()
        executing_t.extend(e)
        finished_t.extend(f)

        executing_t.sort(key=lambda t: t['begin_time'], reverse=True)
        finished_t.sort(key=lambda t: t['end_time'], reverse=True)

        return {
            'executing_tasks': executing_t,
            'finished_tasks': finished_t
        }
