import logging
from teuthology import misc
from teuthology.task import Task

log = logging.getLogger(__name__)


class TeuthologyIntegration(Task):

    def begin(self):
        misc.sh("""
        set -x
        pip install tox
        tox
        # tox -e py27-integration
        tox -e openstack-integration
        """)

task = TeuthologyIntegration
