import json
import logging
import requests
import socket
import re

from datetime import datetime
from paramiko import SSHException
from paramiko.ssh_exception import NoValidConnectionsError

import teuthology.orchestra

from teuthology.config import config
from teuthology.contextutil import safe_while
from teuthology.exceptions import MaxWhileTries
from teuthology import misc

log = logging.getLogger(__name__)


def enabled(warn=False):
    """
    Check for required FOG settings

    :param warn: Whether or not to log a message containing unset parameters
    :returns: True if they are present; False if they are not
    """
    fog_conf = config.get('fog', dict())
    params = ['endpoint', 'api_token', 'user_token', 'machine_types']
    unset = [param for param in params if not fog_conf.get(param)]
    if unset and warn:
        log.warn(
            "FOG disabled; set the following config options to enable: %s",
            ' '.join(unset),
        )
    return (unset == [])


def get_types():
    """
    Fetch and parse config.fog['machine_types']

    :returns: The list of FOG-configured machine types. An empty list if FOG is
              not configured.
    """
    if not enabled():
        return []
    fog_conf = config.get('fog', dict())
    types = fog_conf.get('machine_types', '')
    if not isinstance(types, list):
        types = types.split(',')
    return [type_ for type_ in types if type_]


class FOG(object):
    """
    Reimage bare-metal machines with https://fogproject.org/
    """
    timestamp_format = '%Y-%m-%d %H:%M:%S'

    def __init__(self, name, os_type, os_version):
        self.remote = teuthology.orchestra.remote.Remote(
            misc.canonicalize_hostname(name))
        self.name = self.remote.hostname
        self.shortname = self.remote.shortname
        self.os_type = os_type
        self.os_version = os_version
        self.log = log.getChild(self.shortname)

    def create(self):
        """
        Initiate deployment and wait until completion
        """
        if not enabled():
            raise RuntimeError("FOG is not configured!")
        host_data = self.get_host_data()
        host_id = int(host_data['id'])
        self.set_image(host_id)
        task_id = self.schedule_deploy_task(host_id)
        try:
            # Use power_off/power_on because other methods call
            # _wait_for_login, which will not work here since the newly-imaged
            # host will have an incorrect hostname
            self.remote.console.power_off()
            self.remote.console.power_on()
            self.wait_for_deploy_task(task_id)
        except Exception:
            self.cancel_deploy_task(task_id)
            raise
        self._wait_for_ready()
        self._fix_hostname()
        self.log.info("Deploy complete!")

    def do_request(self, url_suffix, data=None, method='GET', verify=True):
        """
        A convenience method to submit a request to the FOG server
        :param url_suffix: The portion of the URL to append to the endpoint,
                           e.g.  '/system/info'
        :param data: Optional JSON data to submit with the request
        :param method: The HTTP method to use for the request (default: 'GET')
        :param verify: Whether or not to raise an exception if the request is
                       unsuccessful (default: True)
        :returns: A requests.models.Response object
        """
        req_kwargs = dict(
            headers={
                'fog-api-token': config.fog['api_token'],
                'fog-user-token': config.fog['user_token'],
            },
        )
        if data is not None:
            req_kwargs['data'] = data
        req = requests.Request(
            method,
            config.fog['endpoint'] + url_suffix,
            **req_kwargs
        )
        prepped = req.prepare()
        resp = requests.Session().send(prepped)
        if not resp.ok and resp.text:
            self.log.error("%s: %s", resp.status_code, resp.text)
        if verify:
            resp.raise_for_status()
        return resp

    def get_host_data(self):
        """
        Locate the host we want to use, and return the FOG object which
        represents it
        :returns: A dict describing the host
        """
        resp = self.do_request(
            '/host',
            data=json.dumps(dict(name=self.shortname)),
        )
        obj = resp.json()
        if obj['count'] == 0:
            raise RuntimeError("Host %s not found!" % self.shortname)
        if obj['count'] > 1:
            raise RuntimeError(
                "More than one host found for %s" % self.shortname)
        return obj['hosts'][0]

    def get_image_data(self):
        """
        Locate the image we want to use, and return the FOG object which
        represents it
        :returns: A dict describing the image
        """
        name = '_'.join([
            self.remote.machine_type, self.os_type.lower(), self.os_version])
        resp = self.do_request(
            '/image',
            data=json.dumps(dict(name=name)),
        )
        obj = resp.json()
        if not obj['count']:
            raise RuntimeError(
                "Could not find an image for %s %s" %
                (self.os_type, self.os_version))
        return obj['images'][0]

    def set_image(self, host_id):
        """
        Tell FOG to use the proper image on the next deploy
        :param host_id: The id of the host to deploy
        """
        image_data = self.get_image_data()
        image_id = int(image_data['id'])
        self.do_request(
            '/host/%s' % host_id,
            method='PUT',
            data=json.dumps(dict(imageID=image_id)),
        )

    def schedule_deploy_task(self, host_id):
        """
        :param host_id: The id of the host to deploy
        :returns: The id of the scheduled task
        """
        self.log.info(
            "Scheduling deploy of %s %s",
            self.os_type, self.os_version)
        # First, let's find and cancel any existing deploy tasks for the host.
        for task in self.get_deploy_tasks():
            self.cancel_deploy_task(task['id'])
        # Next, we need to find the right tasktype ID
        resp = self.do_request(
            '/tasktype',
            data=json.dumps(dict(name='deploy')),
        )
        tasktypes = [obj for obj in resp.json()['tasktypes']
                     if obj['name'].lower() == 'deploy']
        deploy_id = int(tasktypes[0]['id'])
        # Next, schedule the task
        resp = self.do_request(
            '/host/%i/task' % host_id,
            method='POST',
            data='{"taskTypeID": %i}' % deploy_id,
        )
        host_tasks = self.get_deploy_tasks()
        for task in host_tasks:
            timestamp = task['createdTime']
            time_delta = (
                datetime.utcnow() - datetime.strptime(
                    timestamp, self.timestamp_format)
            ).total_seconds()
            # There should only be one deploy task matching our host. Just in
            # case there are multiple, select a very recent one.
            if time_delta < 5:
                return task['id']

    def get_deploy_tasks(self):
        """
        :returns: A list of deploy tasks which are active on our host
        """
        resp = self.do_request('/task/active')
        try:
            tasks = resp.json()['tasks']
        except Exception:
            self.log.exception("Failed to get deploy tasks!")
            return list()
        host_tasks = [obj for obj in tasks
                      if obj['host']['name'] == self.shortname]
        return host_tasks

    def deploy_task_active(self, task_id):
        """
        :param task_id: The id of the task to query
        :returns: True if the task is active
        """
        host_tasks = self.get_deploy_tasks()
        return any(
            [task['id'] == task_id for task in host_tasks]
        )

    def wait_for_deploy_task(self, task_id):
        """
        Wait until the specified task is no longer active (i.e., it has
        completed)
        """
        self.log.info("Waiting for deploy to finish")
        with safe_while(sleep=15, tries=60) as proceed:
            while proceed():
                if not self.deploy_task_active(task_id):
                    break

    def cancel_deploy_task(self,  task_id):
        """ Cancel an active deploy task """
        resp = self.do_request(
            '/task/cancel',
            method='DELETE',
            data='{"id": %s}' % task_id,
        )
        resp.raise_for_status()

    def _wait_for_ready(self):
        """ Attempt to connect to the machine via SSH """
        with safe_while(sleep=6, tries=100) as proceed:
            while proceed():
                try:
                    self.remote.connect()
                    break
                except (
                    socket.error,
                    SSHException,
                    NoValidConnectionsError,
                    MaxWhileTries,
                    EOFError,
                ):
                    pass
        sentinel_file = config.fog.get('sentinel_file', None)
        if sentinel_file:
            cmd = "while [ ! -e '%s' ]; do sleep 5; done" % sentinel_file
            self.remote.run(args=cmd, timeout=600)
        self.log.info("Node is ready")

    def _fix_hostname(self):
        """
        After a reimage, the host will still have the hostname of the machine
        used to create the image initially. Fix that by making a call to
        /binhostname and tweaking /etc/hosts.
        """
        wrong_hostname = self.remote.sh('hostname').strip()
        etc_hosts = self.remote.sh(
            'grep %s /etc/hosts' % wrong_hostname,
            check_status=False,
        ).strip()
        if etc_hosts:
            wrong_ip = re.split(r'\s+', etc_hosts.split('\n')[0].strip())[0]
            self.remote.run(args="sudo hostname %s" % self.shortname)
            self.remote.run(
                args="sudo sed -i -e 's/%s/%s/g' /etc/hosts" % (
                    wrong_hostname, self.shortname),
            )
            self.remote.run(
                args="sudo sed -i -e 's/%s/%s/g' /etc/hosts" % (
                    wrong_ip, self.remote.ip_address),
            )
        self.remote.run(
            args="sudo sed -i -e 's/%s/%s/g' /etc/hostname" % (
                wrong_hostname, self.shortname),
            check_status=False,
        )
        self.remote.run(
            args="sudo hostname %s" % self.shortname,
            check_status=False,
        )

    def destroy(self):
        """A no-op; we just leave idle nodes as-is"""
        pass
