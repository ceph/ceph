import logging
import os

import requests

from teuthology import misc
from teuthology.config import config
from teuthology.util.compat import urlencode


log = logging.getLogger(__name__)


def get_status(name):
    name = misc.canonicalize_hostname(name, user=None)
    uri = os.path.join(config.lock_server, 'nodes', name, '')
    response = requests.get(uri)
    success = response.ok
    if success:
        return response.json()
    log.warning(
        "Failed to query lock server for status of {name}".format(name=name))
    return None


def get_statuses(machines):
    if machines:
        statuses = []
        for machine in machines:
            machine = misc.canonicalize_hostname(machine)
            status = get_status(machine)
            if status:
                statuses.append(status)
            else:
                log.error("Lockserver doesn't know about machine: %s" %
                          machine)
    else:
        statuses = list_locks()
    return statuses


def is_vm(name=None, status=None):
    if status is None:
        if name is None:
            raise ValueError("Must provide either name or status, or both")
        name = misc.canonicalize_hostname(name)
        status = get_status(name)
    return status.get('is_vm', False)


def list_locks(keyed_by_name=False, **kwargs):
    uri = os.path.join(config.lock_server, 'nodes', '')
    for key, value in kwargs.items():
        if kwargs[key] is False:
            kwargs[key] = '0'
        if kwargs[key] is True:
            kwargs[key] = '1'
    if kwargs:
        if 'machine_type' in kwargs:
            kwargs['machine_type'] = kwargs['machine_type'].replace(',','|')
        uri += '?' + urlencode(kwargs)
    try:
        response = requests.get(uri)
    except requests.ConnectionError:
        success = False
        log.exception("Could not contact lock server: %s", config.lock_server)
    else:
        success = response.ok
    if success:
        if not keyed_by_name:
            return response.json()
        else:
            return {node['name']: node
                    for node in response.json()}
    return dict()


def find_stale_locks(owner=None):
    """
    Return a list of node dicts corresponding to nodes that were locked to run
    a job, but the job is no longer running. The purpose of this is to enable
    us to nuke nodes that were left locked due to e.g. infrastructure failures
    and return them to the pool.

    :param owner: If non-None, return nodes locked by owner. Default is None.
    """
    def might_be_stale(node_dict):
        """
        Answer the question: "might this be a stale lock?"

        The answer is yes if:
            It is locked
            It has a non-null description containing multiple '/' characters

        ... because we really want "nodes that were locked for a particular job
        and are still locked" and the above is currently the best way to guess.
        """
        desc = node_dict['description']
        if (node_dict['locked'] is True and
            desc is not None and desc.startswith('/') and
                desc.count('/') > 1):
            return True
        return False

    # Which nodes are locked for jobs?
    nodes = list_locks(locked=True)
    if owner is not None:
        nodes = [node for node in nodes if node['locked_by'] == owner]
    nodes = filter(might_be_stale, nodes)

    def node_job_is_active(node, cache):
        """
        Is this node's job active (e.g. running or waiting)?

        :param node:  The node dict as returned from the lock server
        :param cache: A set() used for caching results
        :returns:     True or False
        """
        description = node['description']
        if description in cache:
            return True
        (name, job_id) = description.split('/')[-2:]
        url = os.path.join(config.results_server, 'runs', name, 'jobs', job_id,
                           '')
        resp = requests.get(url)
        if not resp.ok:
            return False
        job_info = resp.json()
        if job_info['status'] in ('running', 'waiting'):
            cache.add(description)
            return True
        return False

    result = list()
    # Here we build the list of of nodes that are locked, for a job (as opposed
    # to being locked manually for random monkeying), where the job is not
    # running
    active_jobs = set()
    for node in nodes:
        if node_job_is_active(node, active_jobs):
            continue
        result.append(node)
    return result
