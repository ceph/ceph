import argparse
import httplib2
import json
import logging
import os
import urllib

from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def _lock_url():
    return os.getenv('TEUTHOLOGY_LOCK_SERVER', 'http://localhost:8080/lock')

def send_request(method, url, body=None):
    http = httplib2.Http()
    resp, content = http.request(url, method=method, body=body)
    if resp.status == 200:
        return (True, content)
    log.info("%s request to '%s' with body '%s' failed with response code %d",
             method, url, body, resp.status)
    return (False, None)

def lock_many(num, user=None):
    if user is None:
        user = teuthology.get_user()
    success, content = send_request('POST', _lock_url(),
                                    urllib.urlencode(dict(user=user, num=num)))
    if success:
        machines = json.loads(content)
        log.debug('locked {machines}'.format(machines=', '.join(machines)))
        return machines
    log.warn('Could not lock %d nodes', num)
    return []

def lock(name, user=None):
    if user is None:
        user = teuthology.get_user()
    success, _ = send_request('POST', _lock_url() + '/' + name,
                              urllib.urlencode(dict(user=user)))
    if success:
        log.debug('locked %s as %s', name, user)
    else:
        log.error('failed to lock %s', name)
    return success

def unlock(name, user=None):
    if user is None:
        user = teuthology.get_user()
    success, _ = send_request('DELETE', _lock_url() + '/' + name + '?' + \
                                  urllib.urlencode(dict(user=user)))
    if success:
        log.debug('unlocked %s', name)
    else:
        log.error('failed to unlock %s', name)
    return success

def get_status(name):
    success, content = send_request('GET', _lock_url() + '/' + name)
    if success:
        return json.loads(content)
    return None

def list_locks():
    success, content = send_request('GET', _lock_url())
    if success:
        return json.loads(content)
    return None

def update_lock(name, description=None, status=None):
    updated = {}
    if description is not None:
        updated['desc'] = description
    if status is not None:
        updated['status'] = status

    if updated:
        success, _ = send_request('PUT', _lock_url() + '/' + name + '?' + \
                                      urllib.urlencode(updated))
        return success
    return True

def _positive_int(string):
    value = int(string)
    if value < 1:
        raise argparse.ArgumentTypeError(
            '{string} is not positive'.format(string=string))
    return value

def main():
    parser = argparse.ArgumentParser(description="""
Lock, unlock, or query lock status of machines.
""")
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        default=False,
        help='be more verbose',
        )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--list',
        action='store_true',
        default=False,
        help='show lock info for all machines, or only machines specified',
        )
    group.add_argument(
        '--lock',
        action='store_true',
        default=False,
        help='lock particular machines',
        )
    group.add_argument(
        '--unlock',
        action='store_true',
        default=False,
        help='unlock particular machines',
        )
    group.add_argument(
        '--lock-many',
        dest='num_to_lock',
        type=_positive_int,
        help='lock this many machines',
        )
    group.add_argument(
        '--update',
        action='store_true',
        default=False,
        help='update the description or status of some machines',
        )
    parser.add_argument(
        '--owner',
        default=None,
        help='who will own locked machines',
        )
    parser.add_argument(
        '-f',
        action='store_true',
        default=False,
        help='don\'t exit after the first error, continue locking or unlocking other machines',
        )
    parser.add_argument(
        '--desc',
        default=None,
        help='update description',
        )
    parser.add_argument(
        '--status',
        default=None,
        choices=['up', 'down'],
        help='update status',
        )
    parser.add_argument(
        'machines',
        metavar='MACHINE',
        default=[],
        nargs='*',
        help='machines to operate on',
        )

    ctx = parser.parse_args()

    loglevel = logging.ERROR
    if ctx.verbose:
        loglevel = logging.DEBUG

    logging.basicConfig(
        level=loglevel,
        )

    ret = 0
    user = ctx.owner
    machines = ctx.machines
    machines_to_update = []

    if ctx.list:
        if ctx.status is not None or ctx.desc is not None:
            log.error('--up and --desc do nothing with --list')
            return 1

        if machines:
            statuses = [get_status(machine) for machine in machines]
        else:
            statuses = list_locks()

        if statuses:
            print json.dumps(statuses, indent=4)
        else:
            log.error('error retrieving lock statuses')
            ret = 1
    elif ctx.lock:
        assert machines, 'You must specify machines to lock.'
        for machine in machines:
            if not lock(machine, user):
                ret = 1
                if not ctx.f:
                    return ret
            else:
                machines_to_update.append(machine)
    elif ctx.unlock:
        assert machines, 'You must specify machines to unlock.'
        for machine in machines:
            if not unlock(machine, user):
                ret = 1
                if not ctx.f:
                    return ret
            else:
                machines_to_update.append(machine)
    elif ctx.num_to_lock:
        assert not machines, \
            'The --lock-many option does not support specifying machines'
        result = lock_many(ctx.num_to_lock, user)
        if not result:
            ret = 1
        else:
            machines_to_update = result
            print json.dumps(result, indent=4)
    elif ctx.update:
        assert machines, 'You must specify machines to update'
        machines_to_update = machines

    if ctx.desc is not None or ctx.status is not None:
        for machine in machines_to_update:
            update_lock(machine, ctx.desc, ctx.status)

    return ret
