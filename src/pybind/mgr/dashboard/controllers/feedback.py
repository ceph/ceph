# # -*- coding: utf-8 -*-

from ..exceptions import DashboardException
from ..model.feedback import Feedback
from ..rest_client import RequestException
from ..security import Scope
from ..services import feedback
from . import APIDoc, APIRouter, RESTController


@APIRouter('/feedback', Scope.CONFIG_OPT)
@APIDoc("Feedback API", "Report")
class FeedbackController(RESTController):
    issueAPIkey = None

    def __init__(self):  # pragma: no cover
        super().__init__()
        self.tracker_client = feedback.CephTrackerClient()

    def create(self, project, tracker, subject, description):
        """
        Create an issue.
        :param project: The affected ceph component.
        :param tracker: The tracker type.
        :param subject: The title of the issue.
        :param description: The description of the issue.
        """
        try:
            new_issue = Feedback(Feedback.Project[project].value,
                                 Feedback.TrackerType[tracker].value, subject, description)
        except KeyError:
            raise DashboardException(msg=f'{"Invalid arguments"}', component='feedback')
        try:
            return self.tracker_client.create_issue(new_issue)
        except RequestException as error:
            if error.status_code == 401:
                raise DashboardException(msg=f'{"Invalid API key"}',
                                         http_status_code=error.status_code,
                                         component='feedback')
            raise error
        except Exception:
            raise DashboardException(msg=f'{"API key not set"}',
                                     http_status_code=401,
                                     component='feedback')

    def get(self, issue_number):
        """
        Fetch issue details.
        :param issueAPI: The issue tracker API access key.
        """
        try:
            return self.tracker_client.get_issues(issue_number)
        except RequestException as error:
            if error.status_code == 404:
                raise DashboardException(msg=f'Issue {issue_number} not found',
                                         http_status_code=error.status_code,
                                         component='feedback')
            raise error
