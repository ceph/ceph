// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Tests for Ceph delegation handling
 *
 * (c) 2017, Jeff Layton <jlayton@redhat.com>
 */

#include "gtest/gtest.h"
#include "include/compat.h"
#include "include/cephfs/libcephfs.h"
#include "include/fs_types.h"
#include "include/stat.h"
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>
#include <sys/stat.h>
#include <dirent.h>
#include <sys/uio.h>
#include <libgen.h>
#include <stdlib.h>
#include <cstring>

#ifdef __linux__
#include <sys/xattr.h>
#include <limits.h>
#endif

#include <map>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <future>

#define	CEPHFS_RECLAIM_TIMEOUT		60

/* execl argv[1] for ReclaimResetAfterMDSFailover child */
static const char DYING_FAILOVER[] = "--failover";

static int dying_client(int argc, char **argv)
{
  struct ceph_mount_info *cmount;

  /* Caller must pass in the uuid */
  if (argc < 2)
    return 1;

  if (ceph_create(&cmount, nullptr) != 0)
    return 1;

  if (ceph_conf_read_file(cmount, nullptr) != 0)
    return 1;

  if (ceph_conf_parse_env(cmount, nullptr) != 0)
    return 1;

  if (ceph_init(cmount) != 0)
    return 1;

  ceph_set_session_timeout(cmount, CEPHFS_RECLAIM_TIMEOUT);

  if (ceph_start_reclaim(cmount, argv[1], CEPH_RECLAIM_RESET) != -ENOENT)
    return 1;

  ceph_set_uuid(cmount, argv[1]);

  if (ceph_mount(cmount, "/") != 0)
    return 1;

  Inode *root, *file;
  if (ceph_ll_lookup_root(cmount, &root) != 0)
    return 1;

  Fh *fh;
  struct ceph_statx stx;
  UserPerm *perms = ceph_mount_perms(cmount);

  if (ceph_ll_create(cmount, root, argv[1], 0666, O_RDWR|O_CREAT|O_EXCL,
		      &file, &fh, &stx, 0, 0, perms) != 0)
    return 1;

  return 0;
}

static int dying_client_failover(const char *uuid)
{
  struct ceph_mount_info *cmount;

  if (ceph_create(&cmount, nullptr) != 0) {
    return 1;
  }
  if (ceph_conf_read_file(cmount, nullptr) != 0) {
    return 1;
  }
  if (ceph_conf_parse_env(cmount, nullptr) != 0) {
    return 1;
  }
  if (ceph_init(cmount) != 0) {
    return 1;
  }

  ceph_set_session_timeout(cmount, 300);

  if (ceph_start_reclaim(cmount, uuid, CEPH_RECLAIM_RESET) != -ENOENT) {
    return 1;
  }

  ceph_set_uuid(cmount, uuid);

  if (ceph_mount(cmount, "/") != 0) {
    return 1;
  }

  if (ceph_mkdir(cmount, "/reclaim_failover_test", 0755) != 0 && errno != EEXIST) {
    return 1;
  }

  int fd = ceph_open(cmount, "/reclaim_failover_test/testfile",
    O_RDWR|O_CREAT, 0644);
  if (fd < 0) {
    return 1;
  }

  if (ceph_write(cmount, fd, "hello", 5, 0) < 0) {
    return 1;
  }

  if (ceph_fsync(cmount, fd, 0) != 0) {
    return 1;
  }

  /*since this is called from the child inside an execl(), returning from here
    without cloing fd would mimic client crash*/
  return 0;
}

TEST(LibCephFS, ReclaimReset) {
  pid_t		pid;
  char		uuid[256];
  const char	*exe = "/proc/self/exe";

  sprintf(uuid, "simplereclaim:%x", getpid());

  pid = fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    errno = 0;
    execl(exe, exe, uuid, nullptr);
    /* It won't be zero of course, which is sort of the point... */
    ASSERT_EQ(errno, 0);
  }

  /* parent - wait for child to exit */
  int ret;
  pid_t wp = wait(&ret);
  ASSERT_GE(wp, 0);
  ASSERT_EQ(WIFEXITED(ret), true);
  ASSERT_EQ(WEXITSTATUS(ret), 0);

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, nullptr), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, nullptr), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, nullptr));
  ASSERT_EQ(ceph_init(cmount), 0);
  ceph_set_session_timeout(cmount, CEPHFS_RECLAIM_TIMEOUT);
  ASSERT_EQ(ceph_start_reclaim(cmount, uuid, CEPH_RECLAIM_RESET), 0);
  ceph_set_uuid(cmount, uuid);
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  Inode *root, *file;
  ASSERT_EQ(ceph_ll_lookup_root(cmount, &root), 0);
  UserPerm *perms = ceph_mount_perms(cmount);
  struct ceph_statx stx;
  ASSERT_EQ(ceph_ll_lookup(cmount, root, uuid, &file, &stx, 0, 0, perms), 0);
  Fh *fh;
  ASSERT_EQ(ceph_ll_open(cmount, file, O_WRONLY, &fh, perms), 0);

  ceph_unmount(cmount);
  ceph_release(cmount);
}

TEST(LibCephFS, ReclaimResetAfterMDSFailover) {
  char uuid[256];
  const char *exe = "/proc/self/exe";

  sprintf(uuid, "reclaim_failover:%x", getpid());

  pid_t pid = fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    errno = 0;
    execl(exe, exe, DYING_FAILOVER, uuid, nullptr);
    ASSERT_EQ(errno, 0);
  }

  int status = 0;
  ASSERT_EQ(waitpid(pid, &status, 0), pid);
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_EQ(WEXITSTATUS(status), 0);

  /* Get a separate client to kill mds because the same client cannot be used
     to reclaim since it needs to be done before mounting. */
  {
    struct ceph_mount_info *admin;

    ASSERT_EQ(ceph_create(&admin, nullptr), 0);
    ASSERT_EQ(ceph_conf_read_file(admin, nullptr), 0);
    ASSERT_EQ(ceph_conf_parse_env(admin, nullptr), 0);
    ASSERT_EQ(ceph_init(admin), 0);
    ASSERT_EQ(ceph_mount(admin, "/"), 0);

    /* Respawn rank 0 so that once it boots up it replays the ESession events
       in journal and recreates the old open session. */  
    const char *cmd[1] = {"{\"prefix\": \"respawn\"}"};
    char *outbuf = nullptr, *outs = nullptr;
    size_t outbuf_len = 0, outs_len = 0;
    int ret = ceph_mds_command(admin, "0", cmd, 1, nullptr, 0,
                              &outbuf, &outbuf_len, &outs, &outs_len);

    bool exiting = (ret == 0 && outbuf && outbuf_len > 0 &&
                    std::string(outbuf, outbuf_len).find("Respawning") != std::string::npos &&
                    outs_len == 0);
    ASSERT_EQ(exiting, true);

    if (outbuf) ceph_buffer_free(outbuf);
    if (outs) ceph_buffer_free(outs);
    ceph_unmount(admin);
    ceph_release(admin);
  }

  sleep(60);

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, nullptr), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, nullptr), 0);
  ASSERT_EQ(ceph_conf_parse_env(cmount, nullptr), 0);
  ASSERT_EQ(ceph_init(cmount), 0);
  ceph_set_session_timeout(cmount, 300);
  /* Now reclaim the dead client's session. If auth_name was not preserved
     across journal replay, this will fail with -EPERM. */
  ASSERT_EQ(ceph_start_reclaim(cmount, uuid, CEPH_RECLAIM_RESET), 0);
  ceph_finish_reclaim(cmount);
  ceph_set_uuid(cmount, uuid);
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  /* Verify we can still see the file the dead client had created */
  struct ceph_statx stx;
  ASSERT_EQ(ceph_statx(cmount, "/reclaim_failover_test/testfile", &stx, 0, 0), 0);

  ceph_unmount(cmount);
  ceph_release(cmount);
}

/* Evict a client session from MDS rank 0 by its global id. */
static int evict_session(struct ceph_mount_info *admin, uint64_t id)
{
  std::string cmd = "{\"prefix\": \"client evict\", \"filters\": [\"id=" +
                    std::to_string(id) + "\"]}";
  const char *cmdv[1] = {cmd.c_str()};
  char *outbuf = nullptr, *outs = nullptr;
  size_t outbuf_len = 0, outs_len = 0;
  int ret = ceph_mds_command(admin, "0", cmdv, 1, nullptr, 0,
                             &outbuf, &outbuf_len, &outs, &outs_len);
  if (outbuf) ceph_buffer_free(outbuf);
  if (outs) ceph_buffer_free(outs);
  return ret;
}

/* Wait until the client can create files again, i.e. it has reconnected. */
static bool wait_reconnected(struct ceph_mount_info *cmount,
                             const std::string& dir, int timeout)
{
  for (int i = 0; i < timeout; ++i) {
    std::string path = dir + "/probe" + std::to_string(i);
    auto f = std::async(std::launch::async, [&] {
      return ceph_open(cmount, path.c_str(), O_CREAT|O_EXCL|O_WRONLY, 0644);
    });
    if (f.wait_for(std::chrono::seconds(timeout - i)) != std::future_status::ready)
      return false;
    int fd = f.get();
    if (fd >= 0) {
      ceph_close(cmount, fd);
      return true;
    }
    sleep(1);
  }
  return false;
}

static void write_all(int fd, const void *buf, size_t len)
{
  ASSERT_EQ(write(fd, buf, len), (ssize_t)len);
}

static void read_all(int fd, void *buf, size_t len)
{
  ASSERT_EQ(read(fd, buf, len), (ssize_t)len);
}

static const size_t discard_dirty_len = 65536;

/*
 * Client A of ReconnectStaleDiscardsDirtyData, run in a process of its
 * own (a fork would share the parent's messenger nonce). Skipped unless
 * started by that test, which passes the pipes and directory in the
 * environment. A buffers a write, reports its id, waits to be told to go
 * on, then reports whether it reconnected and what fsync() returned.
 */
TEST(LibCephFS, ReconnectStaleDiscardsDirtyDataClientA) {
  const char *fds = getenv("CEPHFS_TEST_DISCARD_DIRTY_FDS");
  const char *dir = getenv("CEPHFS_TEST_DISCARD_DIRTY_DIR");
  if (!fds || !dir) {
    GTEST_SKIP() << "only run by ReconnectStaleDiscardsDirtyData";
  }
  int rfd, wfd;
  ASSERT_EQ(sscanf(fds, "%d,%d", &rfd, &wfd), 2);
  std::string path = std::string(dir) + "/f";

  struct ceph_mount_info *a;
  ASSERT_EQ(ceph_create(&a, nullptr), 0);
  ASSERT_EQ(ceph_conf_read_file(a, nullptr), 0);
  ASSERT_EQ(ceph_conf_parse_env(a, nullptr), 0);
  ASSERT_EQ(ceph_conf_set(a, "client_reconnect_stale", "true"), 0);
  // keep the write in A's cache until the eviction
  ASSERT_EQ(ceph_conf_set(a, "client_oc_max_dirty_age", "600"), 0);
  ASSERT_EQ(ceph_mount(a, "/"), 0);
  ASSERT_EQ(ceph_mkdir(a, dir, 0755), 0);

  std::string olddata(discard_dirty_len, 'A');
  int fd = ceph_open(a, path.c_str(), O_CREAT|O_RDWR, 0644);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(ceph_write(a, fd, olddata.data(), discard_dirty_len, 0),
            (int)discard_dirty_len);
  uint64_t id = ceph_get_instance_id(a);
  write_all(wfd, &id, sizeof(id));

  char go;
  read_all(rfd, &go, 1);
  int64_t res[2];
  res[0] = wait_reconnected(a, dir, 60);
  res[1] = ceph_fsync(a, fd, 0);
  write_all(wfd, res, sizeof(res));
  ceph_close(a, fd);
  ceph_unmount(a);
  ceph_release(a);
}

/*
 * A client that reconnects on its own after being blocklisted
 * (client_reconnect_stale) must not write data it buffered before the
 * eviction: another client may have written the file since. The stale
 * data is discarded, and fsync() of the old handle reports EIO.
 *
 * Client A is stopped (SIGSTOP) while it is evicted, as a client is that
 * hangs or is partitioned: when it resumes it sees its MDS session reset
 * before it learns that it is blocklisted, the order in which the dirty
 * data used to survive.
 *
 * The first automatic reconnect of a client is only allowed once the
 * host's monotonic clock is past 30 minutes (see
 * client_reconnect_stale_interval); on a freshly booted host this test
 * is skipped.
 */
TEST(LibCephFS, ReconnectStaleDiscardsDirtyData) {
  if (getenv("CEPHFS_TEST_DISCARD_DIRTY_FDS")) {
    GTEST_SKIP() << "running as client A";
  }
  struct timespec ts;
  ASSERT_EQ(clock_gettime(CLOCK_MONOTONIC, &ts), 0);
  if (ts.tv_sec < 31 * 60) {
    GTEST_SKIP() << "host up for less than 30 minutes";
  }

  const size_t len = discard_dirty_len;
  static int run = 0;  // --gtest_repeat
  std::string dir = "reconnect_discard_dirty." + std::to_string(getpid()) +
                    "." + std::to_string(run++);
  std::string path = dir + "/f";

  int to_child[2], to_parent[2];
  ASSERT_EQ(pipe(to_child), 0);
  ASSERT_EQ(pipe(to_parent), 0);

  pid_t pid = fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    close(to_child[1]);
    close(to_parent[0]);
    std::string fds = std::to_string(to_child[0]) + "," +
                      std::to_string(to_parent[1]);
    setenv("CEPHFS_TEST_DISCARD_DIRTY_FDS", fds.c_str(), 1);
    setenv("CEPHFS_TEST_DISCARD_DIRTY_DIR", dir.c_str(), 1);
    const char *argv[] = {
      "ceph_test_libcephfs_reclaim",
      "--gtest_filter=LibCephFS.ReconnectStaleDiscardsDirtyDataClientA",
      nullptr};
    execv("/proc/self/exe", const_cast<char **>(argv));
    _exit(127);
  }
  close(to_child[0]);
  close(to_parent[1]);

  uint64_t id;
  read_all(to_parent[0], &id, sizeof(id));

  struct ceph_mount_info *b;
  ASSERT_EQ(ceph_create(&b, nullptr), 0);
  ASSERT_EQ(ceph_conf_read_file(b, nullptr), 0);
  ASSERT_EQ(ceph_conf_parse_env(b, nullptr), 0);
  ASSERT_EQ(ceph_mount(b, "/"), 0);

  // A is stopped and evicted (and blocklisted); B writes the same range.
  ASSERT_EQ(kill(pid, SIGSTOP), 0);
  ASSERT_EQ(evict_session(b, id), 0);
  std::string newdata(len, 'B');
  int fdb = ceph_open(b, path.c_str(), O_WRONLY, 0);
  ASSERT_GE(fdb, 0);
  ASSERT_EQ(ceph_write(b, fdb, newdata.data(), len, 0), (int)len);
  ASSERT_EQ(ceph_fsync(b, fdb, 0), 0);
  ASSERT_EQ(ceph_close(b, fdb), 0);

  // A resumes, reconnects and flushes its old handle.
  ASSERT_EQ(kill(pid, SIGCONT), 0);
  write_all(to_child[1], "g", 1);
  int64_t res[2];
  read_all(to_parent[0], res, sizeof(res));
  int status;
  ASSERT_EQ(waitpid(pid, &status, 0), pid);
  ASSERT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0) << status;
  ASSERT_EQ(res[0], 1) << "A did not reconnect";
  EXPECT_EQ(res[1], -EIO) << "fsync of A's old handle";

  ceph_unmount(b);
  ceph_release(b);

  // Read back through a new client: B's cache would still show B's data
  // even if A's write had reached the OSDs (A wrote without caps, so
  // nothing invalidated it).
  struct ceph_mount_info *c;
  ASSERT_EQ(ceph_create(&c, nullptr), 0);
  ASSERT_EQ(ceph_conf_read_file(c, nullptr), 0);
  ASSERT_EQ(ceph_conf_parse_env(c, nullptr), 0);
  ASSERT_EQ(ceph_mount(c, "/"), 0);
  std::string got(len, '\0');
  int fdc = ceph_open(c, path.c_str(), O_RDONLY, 0);
  ASSERT_GE(fdc, 0);
  ASSERT_EQ(ceph_read(c, fdc, got.data(), len, 0), (int)len);
  ceph_close(c, fdc);
  EXPECT_TRUE(got == newdata) << "stale data from the evicted client: "
                              << got.substr(0, 16) << "...";
  ceph_unmount(c);
  ceph_release(c);
}

static int update_root_mode()
{
  struct ceph_mount_info *admin;
  int r = ceph_create(&admin, nullptr);
  if (r < 0)
    return r;
  ceph_conf_read_file(admin, nullptr);
  ceph_conf_parse_env(admin, nullptr);
  ceph_conf_set(admin, "client_permissions", "false");
  r = ceph_mount(admin, "/");
  if (r < 0)
    goto out;
  r = ceph_chmod(admin, "/", 01777);
out:
  ceph_shutdown(admin);
  return r;
}

int main(int argc, char **argv)
{
  int r = update_root_mode();
  if (r < 0)
    exit(1);

  ::testing::InitGoogleTest(&argc, argv);

  if (argc == 2)
    return dying_client(argc, argv);

  if (argc == 3 && strcmp(argv[1], DYING_FAILOVER) == 0) {
    r = dying_client_failover(argv[2]);
    if (r == 0) {
      return r;
    } else {
      exit(1);
    }
  }

  srand(getpid());

  return RUN_ALL_TESTS();
}
