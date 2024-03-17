
"""
Feedback module

See doc/mgr/feedback.rst for more info.
"""

from requests.exceptions import RequestException

from mgr_module import CLIReadCommand, HandleCommandResult, MgrModule
import errno

from .service import CephTrackerClient
from .model import Feedback


class FeedbackModule(MgrModule):

    # there are CLI commands we implement
    @CLIReadCommand('feedback set api-key')
    def _cmd_feedback_set_api_key(self, key: str) -> HandleCommandResult:
        """
        Set Ceph Issue Tracker API key
        """
        try:
            self.set_store('api_key', key)
        except Exception as error:
            return HandleCommandResult(stderr=f'Exception in setting API key : {error}')
        return HandleCommandResult(stdout="Successfully updated API key")

    @CLIReadCommand('feedback delete api-key')
    def _cmd_feedback_delete_api_key(self) -> HandleCommandResult:
        """
        Delete Ceph Issue Tracker API key
        """
        try:
            self.set_store('api_key', None)
        except Exception as error:
            return HandleCommandResult(stderr=f'Exception in deleting API key : {error}')
        return HandleCommandResult(stdout="Successfully deleted key")

    @CLIReadCommand('feedback get api-key')
    def _cmd_feedback_get_api_key(self) -> HandleCommandResult:
        """
        Get Ceph Issue Tracker API key
        """
        try:
            key = self.get_store('api_key')
            if key is None:
                return HandleCommandResult(stderr='Issue tracker key is not set. Set key with `ceph feedback api-key set <your_key>`')
        except Exception as error:
            return HandleCommandResult(stderr=f'Error in retreiving issue tracker API key: {error}')
        return HandleCommandResult(stdout=f'Your key: {key}')

    @CLIReadCommand('feedback issue list')
    def _cmd_feedback_issue_list(self) -> HandleCommandResult:
        """
        Fetch issue list
        """
        key = self.get_store('api_key')
        tracker_client = CephTrackerClient()
        try:
            response = tracker_client.list_issues(key)
        except Exception:
            return HandleCommandResult(stderr="Error occurred. Try again later")
        return HandleCommandResult(stdout=str(response))

    @CLIReadCommand('feedback issue report')
    def _cmd_feedback_issue_report(self, project: str, tracker: str, subject: str, description: str) -> HandleCommandResult:
        """
        Create an issue
        """
        try:
            feedback = Feedback(Feedback.Project[project].value,
                                Feedback.TrackerType[tracker].value, subject, description)
        except KeyError:
            return -errno.EINVAL, '', 'Invalid arguments'
        try:
            current_api_key = self.get_store('api_key')
            if current_api_key is None:
                return HandleCommandResult(stderr='Issue tracker key is not set. Set key with `ceph set issue_key <your_key>`')
        except Exception as error:
            return HandleCommandResult(stderr=f'Error in retreiving issue tracker API key: {error}')
        tracker_client = CephTrackerClient()
        try:
            response = tracker_client.create_issue(feedback, current_api_key)
        except RequestException as error:
            return HandleCommandResult(stderr=f'Error in creating issue: {str(error)}. Please set valid API key.')
        return HandleCommandResult(stdout=f'{str(response)}')

    def set_api_key(self, key: str):
        try:
            self.set_store('api_key', key)
        except Exception as error:
            raise RequestException(f'Exception in setting API key : {error}')
        return 'Successfully updated API key'

    def get_api_key(self):
        try:
            key = self.get_store('api_key')
        except Exception as error:
            raise RequestException(f'Error in retreiving issue tracker API key : {error}')
        return key
    
    def is_api_key_set(self):
        try:
            key = self.get_store('api_key')
        except Exception as error:
            raise RequestException(f'Error in retreiving issue tracker API key : {error}')
        if key is None:
            return False
        return key != ''

    def delete_api_key(self):
        try:
            self.set_store('api_key', None)
        except Exception as error:
            raise RequestException(f'Exception in deleting API key : {error}')
        return 'Successfully deleted API key'

    def get_issues(self):
        key = self.get_store('api_key')
        tracker_client = CephTrackerClient()
        return tracker_client.list_issues(key)

    def validate_and_create_issue(self, project: str, tracker: str, subject: str, description: str, api_key=None):
        feedback = Feedback(Feedback.Project[project].value,
                                Feedback.TrackerType[tracker].value, subject, description)
        tracker_client = CephTrackerClient()
        stored_api_key = self.get_store('api_key')
        try:
            if api_key:
                result = tracker_client.create_issue(feedback, api_key)
            else:
                result = tracker_client.create_issue(feedback, stored_api_key)
        except RequestException:
            self.set_store('api_key', None)
            raise
        if not stored_api_key:
            self.set_store('api_key', api_key)
        return result
