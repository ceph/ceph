import errno
import json

from datetime import datetime
from unittest import mock

import pytest

import rbd_support.task as rbd_task
from rbd_support.task import (MAX_COMPLETED_TASKS, TASK_MAX_RETRY_INTERVAL,
                              TASK_RETRY_INTERVAL, Task, TaskHandler)

from .conftest import ImageBusy, ImageNotFound


def remove_refs(image='img', pool='rbd', namespace=''):
    return {'action': 'remove',
            'pool_name': pool,
            'pool_namespace': namespace,
            'image_name': image}


@pytest.fixture
def handler(module):
    h = TaskHandler(module)
    # TaskHandler keeps its queue in class attributes; give each test its
    # own so that tasks don't leak from one test into the next
    h.tasks_by_sequence = {}
    h.tasks_by_id = {}
    h.completed_tasks = []
    h.sequence = 0
    h.in_progress_task = None
    return h


@pytest.fixture
def ioctx():
    return mock.MagicMock()


@pytest.fixture
def image():
    # rbd.Image() as used by queue_remove() and queue_flatten()
    img = mock.MagicMock()
    img.id.return_value = 'abc'
    img.list_snaps.return_value = []
    rbd_task.rbd.Image.return_value.__enter__.return_value = img
    return img


def add_task(handler, ioctx, refs, message='Removing image'):
    with handler.lock:
        return Task.from_json(handler.add_task(ioctx, message, refs))


def test_task_json_round_trip():
    task = Task(7, 'id-7', 'Removing image rbd/img', remove_refs())
    task.retry_attempts = 2
    task.retry_message = 'boom'
    task.retry_time = datetime(2026, 1, 1, 12, 0)
    task.in_progress = True
    task.progress = 0.5

    d = json.loads(task.to_json())
    assert d == {'sequence': 7,
                 'id': 'id-7',
                 'message': 'Removing image rbd/img',
                 'refs': remove_refs(),
                 'retry_attempts': 2,
                 'retry_message': 'boom',
                 'retry_time': '2026-01-01T12:00:00',
                 'in_progress': True,
                 'progress': 0.5}

    loaded = Task.from_json(task.to_json())
    assert (loaded.sequence, loaded.task_id, loaded.message, loaded.refs) == \
        (7, 'id-7', 'Removing image rbd/img', remove_refs())
    assert loaded.sequence_key == b'0000000000000007'


@pytest.mark.parametrize('val', [
    'not json',
    json.dumps({'sequence': 1, 'id': 'x', 'message': 'm',
                'refs': {'action': 'unknown'}}),
    json.dumps({'sequence': 1, 'id': 'x', 'refs': {'action': 'remove'}}),
])
def test_task_from_json_invalid(val):
    with pytest.raises(ValueError):
        Task.from_json(val)


def test_extract_image_spec(handler):
    assert handler.extract_image_spec('img') == ('rbd', '', 'img')
    assert handler.extract_image_spec('p/img') == ('p', '', 'img')
    assert handler.extract_image_spec('p/ns/img') == ('p', 'ns', 'img')
    for spec in ['', 'p/ns/img/x', 'img@snap']:
        with pytest.raises(ValueError):
            handler.extract_image_spec(spec)

    assert handler.extract_pool_spec('') == ('rbd', '')
    assert handler.extract_pool_spec('p') == ('p', '')
    assert handler.extract_pool_spec('p/ns') == ('p', 'ns')


def test_format_image_spec():
    assert TaskHandler.format_image_spec(('p', '', 'img')) == 'p/img'
    assert TaskHandler.format_image_spec(('p', 'ns', 'img')) == 'p/ns/img'


def test_add_task_persists_to_omap(handler, ioctx):
    first = add_task(handler, ioctx, remove_refs('a'))
    second = add_task(handler, ioctx, remove_refs('b'))

    assert (first.sequence, second.sequence) == (1, 2)
    assert first.task_id != second.task_id
    assert sorted(handler.tasks_by_sequence) == [1, 2]
    assert set(handler.tasks_by_id) == {first.task_id, second.task_id}

    keys, vals = ioctx.set_omap.call_args[0][1:]
    assert keys == (b'0000000000000002', )
    assert Task.from_json(vals[0].decode()).task_id == second.task_id
    ioctx.operate_write_op.assert_called_with(mock.ANY, 'rbd_task')


def test_queue_remove_returns_the_queued_task_for_a_duplicate(handler,
                                                              image):
    with handler.lock:
        r, first, _ = handler.queue_remove('rbd/img')
        assert r == 0
        r, second, _ = handler.queue_remove('img')
        assert r == 0
    assert json.loads(first) == json.loads(second)
    assert len(handler.tasks_by_id) == 1
    assert json.loads(first)['refs'] == dict(remove_refs(), image_id='abc')
    assert json.loads(first)['message'] == 'Removing image rbd/img'


def test_queue_remove_errors(handler, image):
    image.list_snaps.return_value = [{'name': 'snap1'}]
    with handler.lock, pytest.raises(ImageBusy, match='has snapshots'):
        handler.queue_remove('rbd/img')

    rbd_task.rbd.Image.side_effect = ImageNotFound()
    with handler.lock, pytest.raises(ImageNotFound, match='does not exist'):
        handler.queue_remove('rbd/img')
    assert not handler.tasks_by_id


def test_find_task_matches_completed_task_on_replay(handler, ioctx):
    task = add_task(handler, ioctx, dict(remove_refs(), image_id='abc'))
    task = handler.tasks_by_id[task.task_id]

    # a request without image_id still matches a task that recorded one
    assert handler.find_task(remove_refs()) is task
    assert handler.find_task(remove_refs('other')) is None

    handler.remove_task(ioctx, task)
    assert not handler.tasks_by_id
    assert handler.completed_tasks == [task]
    # a replayed command finds the finished task instead of queueing it again
    assert handler.find_task(remove_refs()) is task


def test_remove_task_keeps_a_bounded_history(handler, ioctx):
    tasks = [add_task(handler, ioctx, remove_refs(str(i)))
             for i in range(MAX_COMPLETED_TASKS + 5)]
    for task in tasks:
        handler.remove_task(ioctx, handler.tasks_by_id[task.task_id])

    assert len(handler.completed_tasks) == MAX_COMPLETED_TASKS
    assert handler.completed_tasks[-1].task_id == tasks[-1].task_id
    assert handler.completed_tasks[0].task_id == tasks[5].task_id
    ioctx.remove_omap_keys.assert_called_with(mock.ANY,
                                              (tasks[-1].sequence_key, ))


def test_remove_task_does_not_record_failed_tasks(handler, ioctx):
    task = add_task(handler, ioctx, remove_refs())
    task = handler.tasks_by_id[task.task_id]
    task.fail('Image does not exist')
    handler.remove_task(ioctx, task)
    assert not handler.completed_tasks
    assert handler.find_task(remove_refs()) is None


def test_task_cancel(handler, ioctx, module):
    task = add_task(handler, ioctx, remove_refs())
    r, _, _ = handler.task_cancel(task.task_id)
    assert r == 0
    assert not handler.tasks_by_id
    assert not handler.completed_tasks
    module.remote.assert_called_with('progress', 'fail', task.task_id,
                                     'Operation canceled')

    r, _, err = handler.task_cancel(task.task_id)
    assert r == -errno.ENOENT
    assert 'No such task' in err


def test_task_cancel_in_progress_task_stops_the_operation(handler, ioctx):
    task = add_task(handler, ioctx, remove_refs())
    task = handler.tasks_by_id[task.task_id]
    handler.in_progress_task = task

    r, _, _ = handler.task_cancel(task.task_id)
    assert r == 0
    # the running task stays queued until execute_task() finishes it, but the
    # next progress callback tells librbd to abort
    assert handler.tasks_by_id == {task.task_id: task}
    assert handler.progress_callback(task, 1, 2) == -errno.ECANCELED


def test_progress_callback_updates_progress(handler, ioctx, module):
    task = add_task(handler, ioctx, remove_refs())
    task = handler.tasks_by_id[task.task_id]
    handler.in_progress_task = task

    assert handler.progress_callback(task, 1, 4) == 0
    assert task.progress == 0.25
    assert task.progress_posted
    module.remote.assert_called_with('progress', 'update', task.task_id,
                                     task.message, 0.25, mock.ANY)


def test_task_cancel_and_list_respect_authorization(handler, ioctx, module):
    allowed = add_task(handler, ioctx, remove_refs('a', pool='rbd'))
    denied = add_task(handler, ioctx, remove_refs('b', pool='other'))
    module.is_authorized.side_effect = lambda spec: spec['pool'] == 'rbd'

    r, out, _ = handler.task_list(None)
    assert r == 0
    assert [t['id'] for t in json.loads(out)] == [allowed.task_id]

    r, out, _ = handler.task_list(allowed.task_id)
    assert (r, json.loads(out)['id']) == (0, allowed.task_id)
    r, _, _ = handler.task_list(denied.task_id)
    assert r == -errno.ENOENT

    r, _, _ = handler.task_cancel(denied.task_id)
    assert r == -errno.ENOENT
    assert denied.task_id in handler.tasks_by_id


def test_execute_task_success(handler, ioctx, module):
    task = add_task(handler, ioctx, remove_refs())
    with handler.lock:
        handler.execute_task(task.sequence)

    rbd_task.rbd.RBD.return_value.remove.assert_called_once_with(
        mock.ANY, 'img', on_progress=mock.ANY)
    assert not handler.tasks_by_sequence
    assert [t.task_id for t in handler.completed_tasks] == [task.task_id]
    module.remote.assert_called_with('progress', 'complete', task.task_id)


def test_execute_task_retries_with_backoff(handler, ioctx):
    task = add_task(handler, ioctx, remove_refs())
    task = handler.tasks_by_sequence[task.sequence]
    rbd_task.rbd.RBD.return_value.remove.side_effect = \
        rbd_task.rados.Error('connection timed out')

    for attempt in [1, 2, 20]:
        task.retry_attempts = attempt - 1
        before = datetime.now()
        with handler.lock:
            handler.execute_task(task.sequence)
        after = datetime.now()

        # the task stays queued and is retried later, waiting longer after
        # every failed attempt but never more than the maximum interval
        assert handler.tasks_by_sequence == {task.sequence: task}
        assert task.retry_attempts == attempt
        assert task.retry_message == 'connection timed out'
        assert not task.in_progress
        delay = min(TASK_RETRY_INTERVAL * attempt, TASK_MAX_RETRY_INTERVAL)
        assert before + delay <= task.retry_time <= after + delay


def test_execute_task_drops_task_when_pool_is_gone(handler, ioctx, module):
    task = add_task(handler, ioctx, remove_refs())
    module.rados.open_ioctx.side_effect = rbd_task.rados.ObjectNotFound()
    with handler.lock:
        handler.execute_task(task.sequence)
    assert not handler.tasks_by_sequence
    rbd_task.rbd.RBD.return_value.remove.assert_not_called()


def test_execute_task_image_not_found_fails_task(handler, ioctx, module):
    task = add_task(handler, ioctx, remove_refs())
    rbd_task.rbd.RBD.return_value.remove.side_effect = ImageNotFound()
    with handler.lock:
        handler.execute_task(task.sequence)
    assert not handler.tasks_by_sequence
    assert not handler.completed_tasks
    module.remote.assert_called_with('progress', 'fail', task.task_id,
                                     'Image does not exist')


def test_init_task_queue_loads_tasks_and_sequence(handler, module):
    good = Task(41, 'id-41', 'Removing image rbd/img', remove_refs())
    ioctx = module.rados.open_ioctx2.return_value.__enter__.return_value
    ioctx.nspace = ''
    ioctx.get_omap_vals.side_effect = [
        (iter([(good.sequence_key, good.to_json().encode()),
               (b'0000000000000042', b'not a task')]), 0),
        (iter([]), 0),
        (iter([]), 0),
        (iter([]), 0),
    ]
    rbd_task.rbd.RBD.return_value.namespace_list.return_value = []

    handler.init_task_queue()

    # pool 3 has no rbd application and is not scanned
    assert [c[0][0] for c in module.rados.open_ioctx2.call_args_list] == [1, 2]
    assert list(handler.tasks_by_id) == ['id-41']
    assert handler.sequence == 41
    module.log.error.assert_called_once()

    # new tasks continue the sequence of the loaded queue
    task = add_task(handler, mock.MagicMock(), remove_refs('new'))
    assert task.sequence == 42
