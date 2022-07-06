# -*- coding: utf-8 -*-

from ..services import progress
from ..tools import TaskManager
from . import APIDoc, APIRouter, EndpointDoc, RESTController

TASK_SCHEMA = {
    "executing_tasks": (str, "ongoing executing tasks"),
    "finished_tasks": ([{
        "name": (str, "finished tasks name"),
        "metadata": ({
            "pool": (int, "")
        }, ""),
        "begin_time": (str, "Task begin time"),
        "end_time": (str, "Task end time"),
        "duration": (int, ""),
        "progress": (int, "Progress of tasks"),
        "success": (bool, ""),
        "ret_value": (bool, ""),
        "exception": (bool, "")
    }], "")
}


@APIRouter('/task')
@APIDoc("Task Management API", "Task")
class Task(RESTController):
    @EndpointDoc("Display Tasks",
                 parameters={
                     'name': (str, 'Task Name'),
                 },
                 responses={200: TASK_SCHEMA})
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
