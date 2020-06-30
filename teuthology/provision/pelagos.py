
import logging
import requests
import re
import time

from teuthology.config import config
from teuthology.contextutil import safe_while
from teuthology.misc import canonicalize_hostname
from teuthology.util.compat import  HTTPError

log = logging.getLogger(__name__)
config_section = 'pelagos'

# Provisioner configuration section description see in
# docs/siteconfig.rst

def enabled(warn=False):
    """
    Check for required Pelagos settings

    :param warn: Whether or not to log a message containing unset parameters
    :returns: True if they are present; False if they are not
    """
    conf = config.get(config_section, dict())
    params = ['endpoint', 'machine_types']
    unset = [_ for _ in params if not conf.get(_)]
    if unset and warn:
        log.warn(
            "Pelagos is disabled; set the following config options to enable: %s",
            ' '.join(unset),
        )
    return (unset == [])


def get_types():
    """
    Fetch and parse config.pelagos['machine_types']

    :returns: The list of Pelagos-configured machine types. An empty list if Pelagos is
              not configured.
    """
    if not enabled():
        return []
    conf = config.get(config_section, dict())
    types = conf.get('machine_types', '')
    if not isinstance(types, list):
        types = [_ for _ in types.split(',') if _]
    return [_ for _ in types if _]

def park_node(name):
        p = Pelagos(name, "maintenance_image")
        p.create()


class Pelagos(object):

    def __init__(self, name, os_type, os_version=""):
        #for service should be a hostname, not a user@host
        split_uri = re.search(r'(\w*)@(.+)', canonicalize_hostname(name))
        if split_uri is not None:
            self.name = split_uri.groups()[1]
        else:
            self.name = name

        self.os_type = os_type
        self.os_version = os_version
        if os_version:
            self.os_name = os_type + "-" + os_version
        else:
            self.os_name = os_type
        self.log = log.getChild(self.name)

    def create(self):
        """
        Initiate deployment via REST requests and wait until completion

        """
        if not enabled():
            raise RuntimeError("Pelagos is not configured!")
        location = None
        try:
            params=dict(os=self.os_name, node=self.name)
            response = self.do_request('node/provision',
                                data=params, method='POST')
            location = response.headers.get('Location')
            self.log.debug("provision task: '%s'", location)
            # gracefully wait till provision task gets created on pelagos
            time.sleep(2)
            self.log.info("Waiting for deploy to finish")
            sleep_time=15
            with safe_while(sleep=sleep_time, tries=60) as proceed:
                while proceed():
                    if not self.is_task_active(location):
                        break
                    self.log.info('Sleeping %s seconds' % sleep_time)
        except Exception as e:
            if location:
                self.cancel_deploy_task(location)
            else:
                self.log.error("Failed to start deploy tasks!")
            raise e
        self.log.info("Deploy complete!")
        if self.task_status_response.status_code != 200:
            raise Exception("Deploy failed")
        return self.task_status_response

    def cancel_deploy_task(self,  task_id):
        # TODO implement it
        return

    def is_task_active(self, task_url):
        try:
            status_response = self.do_request('', url=task_url, verify=False)
        except HTTPError as err:
            self.log.error("Task fail reason: '%s'", err.reason)
            if err.status_code == 404:
                self.log.error(err.reason)
                self.task_status_response = 'failed'
                return False
            else:
                raise HTTPError(err.code, err.reason)
            self.log.debug("Response code '%s'",
                                str(status_response.status_code))
        self.task_status_response = status_response
        if status_response.status_code == 202:
            status = status_response.headers['status']
            self.log.debug("Status response: '%s'", status)
            if status == 'not completed':
                return True
        return False

    def do_request(self, url_suffix, url="" , data=None, method='GET', verify=True):
        """
        A convenience method to submit a request to the Pelagos server
        :param url_suffix: The portion of the URL to append to the endpoint,
                           e.g.  '/system/info'
        :param data: Optional JSON data to submit with the request
        :param method: The HTTP method to use for the request (default: 'GET')
        :param verify: Whether or not to raise an exception if the request is
                       unsuccessful (default: True)
        :returns: A requests.models.Response object
        """
        prepared_url = url or config.pelagos['endpoint'] + url_suffix
        self.log.debug("Sending %s request to: '%s'", method, prepared_url)
        if data:
            self.log.debug("Using data: '%s'", str(data))
        req = requests.Request(
            method,
            prepared_url,
            data=data
        )
        prepared = req.prepare()
        resp = requests.Session().send(prepared)
        if not resp.ok and resp.text:
            self.log.error("Returned status code: '%s', text: %s",
                           resp.status_code, resp.text or 'Empty')
        if verify:
            resp.raise_for_status()
        return resp

    def destroy(self):
        """A no-op; we just leave idle nodes as-is"""
        pass

