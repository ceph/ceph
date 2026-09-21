from io import StringIO
from logging import getLogger

from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = getLogger(__name__)


class TestLargeReaddir(CephFSTestCase):
    """
    readdir over a directory too large to be returned in one MDS reply.
    """

    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    # A readdir reply is bounded by max_bytes, which defaults to 512KB. Each
    # entry spends that budget on its name, a lease and an encoded inode, so a
    # few thousand entries take several replies to return. The MDS stops a page
    # when the next entry does not fit and the client resumes from there, so a
    # slip in that accounting drops entries rather than failing outright --
    # which is what these tests are here to catch.
    FILES = 5000
    NAME = "large-readdir-%06d"

    def _populate(self, path):
        self.mount_a.run_shell_payload(f"""
set -e
mkdir -p {path}
cd {path}
seq 1 {self.FILES} | awk '{{printf "{self.NAME}\\n", $1}}' | xargs -r -n 500 touch
""")

    def _readdirs_issued(self):
        c = self.fs.mds_asok(['perf', 'dump', 'mds_server', 'req_readdir_latency'])
        return c['mds_server']['req_readdir_latency']['avgcount']

    def _assert_listing_complete(self, path):
        expected = {self.NAME % i for i in range(1, self.FILES + 1)}

        # Read it back through a cold client cache, or the listing is served
        # locally and the MDS is never asked.
        self.mount_a.umount_wait()
        self.mount_a.mount_wait()

        before = self._readdirs_issued()
        p = self.mount_a.run_shell_payload(f"ls -U -1 {path}", stdout=StringIO())
        names = p.stdout.getvalue().split()
        after = self._readdirs_issued()

        # Without this the test would still pass if the whole directory came
        # back in one reply, silently covering none of the paging logic.
        self.assertGreater(after - before, 1,
                           "directory was returned in a single reply, so the "
                           "multi-reply path was not exercised")
        self.assertEqual(len(names), len(set(names)),
                         "readdir returned duplicate entries")
        self.assertEqual(set(names), expected,
                         "readdir did not return every entry")

    def test_large_readdir(self):
        """
        That a readdir spanning several replies returns every entry exactly
        once for a directory with no charmap -- the path taken by every
        CephFS readdir.
        """
        self._populate("largedir")
        self._assert_listing_complete("largedir")
