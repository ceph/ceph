# # -*- coding: utf-8 -*-
import os
from collections import defaultdict

from .. import mgr
from ..services import feedback
from . import ApiController, ControllerDoc, RESTController
from ..plugins import PLUGIN_MANAGER as PM

@ApiController('/feedback', secure=False)
@ControllerDoc("Feedback API", "Report")
class Feedback(RESTController):
    issueAPIkey = None

    def __init__(self):  # pragma: no cover
        super(Feedback, self).__init__()
        self.cephTrackerClient = feedback.CephTrackerClient()


    def create(self, project_id, tracker_id, subject, description, category_id, severity):
        """
        Create an issue.
        :param component: The buggy ceph component.
        :param title: The title of the issue.
        :param description: The description of the issue.
        """
        return self.cephTrackerClient.create_issue(project_id, tracker_id, subject, description, category_id, severity)

    
    def get(self, issue_number):
        """
        Validate the issue tracker API given by user.
        :param issueAPI: The issue tracker API access key.
        """
        return self.cephTrackerClient.get_issues(issue_number)
