from tasks.cephfs.cephfs_test_case import CephFSTestCase
import os
import ioctl_linux
import fcntl
import time
import random

class TestRstatFlush(CephFSTestCase):
    MDSS_REQUIRED = 3

    def test_rstat_flush_aggresively(self):
        CEPH_IOC_RSTATFLUSH = ioctl_linux.IO(0x97, 6)
	self.fs.set_max_mds(3)
	
	# wait for MDSes to become active
	time.sleep(5)

	for i in range(1,40):
	  self.mount_a.run_shell(["mkdir", "-p", "test_%d_%d/a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q" % (os.getpid(), i)])
	  self.mount_a.run_shell(["touch", "test_%d_%d/a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/test.f" % (os.getpid(), i)])
	  f = self.mount_a.open_file("test_%d_%d" % (os.getpid(), i))

	  dirs = str("test_%d_%d/a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/test.f" % (os.getpid(), i)).split("/")
	  del dirs[-1]

	  base_path = ""
	  for d in dirs:
	    base_path = base_path + d + "/"
	    pin = random.randint(0, 2)
	    self.mount_a.setfattr(base_path, "ceph.dir.pin", str(pin))

	  # wait for the export to happen
	  secs = random.randint(1, 20)
	  time.sleep(secs)

	  self.mount_a.run_shell(["truncate", "-s", "10", "test_%d_%d/a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/test.f" % (os.getpid(), i)])
	  self.mount_a.run_shell(["sync"])
	  self.mount_a.run_shell(["truncate", "-s", "20", "test_%d_%d/a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/test.f" % (os.getpid(), i)])
	  self.mount_a.run_shell(["sync"])

	  fcntl.ioctl(f, CEPH_IOC_RSTATFLUSH)

	  self.mount_a.close_file(f)

	  rctime = self.mount_a.getfattr("test_%d_%d" % (os.getpid(), i), "ceph.dir.rctime")
	  mtime = self.mount_a.stat("test_%d_%d/a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/test.f" % (os.getpid(), i))["st_mtime"]
	  print("rctime: %f, mtime: %f" % (float(rctime), mtime))
	  self.assertEqual(float(rctime), mtime)
	  print("%dth test succeeded" % i)

