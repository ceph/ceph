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
