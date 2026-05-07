# -*- coding: utf-8 -*-

import json
import requests
from requests.exceptions import RequestException

from .model import Feedback

DEFAULT_TRACKER_URL = 'tracker.ceph.com'


class CephTrackerClient():

    def __init__(self, tracker_url: str = DEFAULT_TRACKER_URL):
        self.tracker_url = tracker_url

    def list_issues(self):
        '''
        Fetch an issue from the Ceph Issue tracker
        '''
        headers = {
            'Content-Type': 'application/json',
        }
        response = requests.get(
            f'https://{self.tracker_url}/issues.json', headers=headers)
        if not response.ok:
            if response.status_code == 404:
                raise FileNotFoundError
            raise RequestException(response.status_code)
        return {"message": response.json()}

    def create_issue(self, feedback: Feedback, api_key: str):
        '''
        Create an issue in the Ceph Issue tracker
        '''
        try:
            headers = {
                'Content-Type': 'application/json',
                'X-Redmine-API-Key': api_key,
            }
        except KeyError:
            raise Exception("Ceph Tracker API Key not set")
        data = json.dumps(feedback.as_dict())
        response = requests.post(
            f'https://{self.tracker_url}/projects/{feedback.project_id}/issues.json',
            headers=headers, data=data)
        if not response.ok:
            if response.status_code == 401:
                raise RequestException("Unauthorized. Invalid issue tracker API key")
            raise RequestException(response.reason)
        return {"message": response.json()}
