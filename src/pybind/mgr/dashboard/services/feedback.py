# -*- coding: utf-8 -*-

import json

import requests
from requests.auth import AuthBase

from ..model.feedback import Feedback
from ..rest_client import RequestException, RestClient
from ..settings import Settings


class config:
    url = 'tracker.ceph.com'
    port = 443


class RedmineAuth(AuthBase):
    def __init__(self):
        try:
            self.access_key = Settings.ISSUE_TRACKER_API_KEY
        except KeyError:
            self.access_key = None

    def __call__(self, r):
        r.headers['X-Redmine-API-Key'] = self.access_key
        return r


class CephTrackerClient(RestClient):
    access_key = ''

    def __init__(self):
        super().__init__(config.url, config.port, client_name='CephTracker',
                         ssl=True, auth=RedmineAuth(), ssl_verify=True)

    @staticmethod
    def get_api_key():
        try:
            access_key = Settings.ISSUE_TRACKER_API_KEY
        except KeyError:
            raise KeyError("Key not set")
        if access_key == '':
            raise KeyError("Empty key")
        return access_key

    def get_issues(self, issue_number):
        '''
        Fetch an issue from the Ceph Issue tracker
        '''
        headers = {
            'Content-Type': 'application/json',
        }
        response = requests.get(
            f'https://tracker.ceph.com/issues/{issue_number}.json', headers=headers)
        if not response.ok:
            raise RequestException(
                "Request failed with status code {}\n"
                .format(response.status_code),
                self._handle_response_status_code(response.status_code),
                response.content)
        return {"message": response.text}

    def create_issue(self, feedback: Feedback):
        '''
        Create an issue in the Ceph Issue tracker
        '''
        try:
            headers = {
                'Content-Type': 'application/json',
                'X-Redmine-API-Key': self.get_api_key(),
            }
        except KeyError:
            raise Exception("Ceph Tracker API Key not set")
        data = json.dumps(feedback.as_dict())
        response = requests.post(
            f'https://tracker.ceph.com/projects/{feedback.project_id}/issues.json',
            headers=headers, data=data)
        if not response.ok:
            raise RequestException(
                "Request failed with status code {}\n"
                .format(response.status_code),
                self._handle_response_status_code(response.status_code),
                response.content)
        return {"message": response.text}
