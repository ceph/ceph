from logging import getLogger

from tasks.cephfs.xfstests_dev import XFSTestsDev


log = getLogger(__name__)


class TestXFSTestsDev(XFSTestsDev):

    def test_generic(self):
        self.run_generic_tests()
