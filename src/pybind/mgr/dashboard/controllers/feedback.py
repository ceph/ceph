# # -*- coding: utf-8 -*-

from typing import Any, Dict

from .. import mgr
from ..exceptions import DashboardException
from ..security import Scope
from . import APIDoc, APIRouter, BaseController, Endpoint, ReadPermission, RESTController, UIRouter
from ._version import APIVersion


@APIRouter('/feedback', Scope.CONFIG_OPT)
@APIDoc("Feedback API", "Report")
class FeedbackController(RESTController):

    @RESTController.MethodMap(version=APIVersion.EXPERIMENTAL)
    def list(self):
        """
        List all issues details.
        """
        try:
            response = mgr.remote('feedback', 'get_issues')
        except RuntimeError as error:
            raise DashboardException(msg=f'Error in fetching issue list: {str(error)}',
                                         http_status_code=error.status_code,
                                         component='feedback')
        return response

    @RESTController.MethodMap(version=APIVersion.EXPERIMENTAL)
    def create(self, project, tracker, subject, description, api_key=None):
        """
        Create an issue.
        :param project: The affected ceph component.
        :param tracker: The tracker type.
        :param subject: The title of the issue.
        :param description: The description of the issue.
        :param api_key: Ceph tracker api key.
        """
        try:
            response = mgr.remote('feedback', 'validate_and_create_issue',
                                  project, tracker, subject, description, api_key)
        except RuntimeError as error:
            if "Invalid issue tracker API key" in str(error):
                raise DashboardException(msg='Error in creating tracker issue: Invalid API key',
                                         component='feedback')
            if "KeyError" in str(error):
                raise DashboardException(msg=f'Error in creating tracker issue: {error}',
                                         component='feedback')
            raise DashboardException(msg=f'{error}',
                                     http_status_code=500,
                                     component='feedback')

        return response


@APIRouter('/feedback/api_key', Scope.CONFIG_OPT)
@APIDoc(group="Report")
class FeedbackApiController(RESTController):

    @RESTController.MethodMap(version=APIVersion.EXPERIMENTAL)
    def list(self):
        """
        Returns Ceph tracker API key.
        """
        try:
            api_key = mgr.remote('feedback', 'get_api_key')
        except ImportError:
            raise DashboardException(msg='Feedback module not found.',
                                     http_status_code=404,
                                     component='feedback')
        except RuntimeError as error:
            raise DashboardException(msg=f'{error}',
                                     http_status_code=500,
                                     component='feedback')
        if api_key is None:
            raise DashboardException(msg='Issue tracker API key is not set',
                                     component='feedback')
        return api_key

    @RESTController.MethodMap(version=APIVersion.EXPERIMENTAL)
    def create(self, api_key):
        """
        Sets Ceph tracker API key.
        :param api_key: The Ceph tracker API key.
        """
        try:
            response = mgr.remote('feedback', 'set_api_key', api_key)
        except RuntimeError as error:
            raise DashboardException(msg=f'{error}',
                                     component='feedback')
        return response

    @RESTController.MethodMap(version=APIVersion.EXPERIMENTAL)
    def bulk_delete(self):
        """
        Deletes Ceph tracker API key.
        """
        try:
            response = mgr.remote('feedback', 'delete_api_key')
        except RuntimeError as error:
            raise DashboardException(msg=f'{error}',
                                     http_status_code=500,
                                     component='feedback')
        return response


@UIRouter('/feedback/api_key', Scope.CONFIG_OPT)
class FeedbackUiController(BaseController):
    def _get_key(self):
        try:
            return mgr.remote('feedback', 'get_api_key')
        except RuntimeError:
            raise DashboardException(msg='Feedback module is not enabled',
                                     http_status_code=404,
                                     component='feedback')

    @Endpoint()
    @ReadPermission
    def exist(self):
        """
        Checks if Ceph tracker API key is stored.
        """
        return self._get_key()

    @Endpoint()
    @ReadPermission
    def status(self):
        """
        Returns the status of the Ceph tracker API key.
        """
        status: Dict[str, Any] = {'available': True, 'message': None}
        response = self._get_key()
        if not response:
            status['available'] = False
            status['message'] = """Redmine API key is not set.
                                Please set the Redmine API key to manage issues
                                through the dashboard."""
            return status
        return status
