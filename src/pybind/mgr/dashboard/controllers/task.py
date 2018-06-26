# -*- coding: utf-8 -*-
from __future__ import absolute_import

from . import ApiController, RESTController
from ..tools import TaskManager


@ApiController('/task')
class Task(RESTController):
    def list(self, name=None):
        executing_t, finished_t = TaskManager.list_serializable(name)
        return {
            'executing_tasks': executing_t,
            'finished_tasks': finished_t
        }
