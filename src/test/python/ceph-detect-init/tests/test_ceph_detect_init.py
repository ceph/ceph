import ceph_detect_init
import types
import platform


class TestCephDetectInit(object):
    def test_basic(self):
        # This tests nothing except for being able to import ceph_detect_init
        # correctly and thus ensuring somewhat that it will work under
        # different Python versions.
        assert True

    def test_init_get_is_string(self):
        init_value = ceph_detect_init.init_get()
        assert type(init_value) == types.StringType

    def test_init_get_opensuse_13_1(self):
        distro, release, codename = platform.linux_distribution()
        if distro != "openSUSE ":
            return
        if release != "13.1":
            return
        if codename != "x86_64":
            return
        init_value = ceph_detect_init.init_get()
        assert init_value == "systemd"

    def test_init_get_opensuse_13_2(self):
        distro, release, codename = platform.linux_distribution()
        if distro != "openSUSE ":
            return
        if release != "13.2":
            return
        if codename != "x86_64":
            return
        init_value = ceph_detect_init.init_get()
        assert init_value == "systemd"


if __name__ == "__main__":
    import logging
    import nose
    logging.basicConfig()
    LoggingLevel = logging.WARNING
    logging.basicConfig(level=LoggingLevel)
    log = logging.getLogger("main")
    nose.runmodule()

