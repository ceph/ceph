#include <string>
#include <unistd.h>
#include <include/fs_types.h>
#include <mds/mdstypes.h>
#include <include/cephfs/libcephfs.h>
#include <include/ceph_fs.h>
#include <gtest/gtest.h>
#include "common/JSONFormatter.h"
#include "json_spirit/json_spirit.h"
#include "boost/format/alt_sstream.hpp"


#define MAX_CEPH_FILES	1000
#define DIRNAME		"ino_release_cb"

using namespace std;

json_spirit::mValue tell_rank0(ceph_mount_info* cmount, const std::string& prefix, cmdmap_t&& cmdmap = {}) {
  cmdmap["prefix"] = prefix;
  cmdmap["format"] = std::string("json");

  JSONFormatter jf;
  jf.open_object_section("");
  ceph::common::cmdmap_dump(cmdmap, &jf);
  jf.close_section();

  boost::io::basic_oaltstringstream<char> oss;
  jf.flush(oss);

  const char *cmdv[] = {oss.begin()};

  char *outb, *outs;
  size_t outb_len, outs_len;
  int status = ceph_mds_command(cmount, "0", cmdv, sizeof(cmdv)/sizeof(cmdv[0]), nullptr, 0, &outb, &outb_len, &outs, &outs_len);
  if (status < 0)
  {
    outs[outs_len] = 0;
    std::cout << "couldn't tell rank 0 '" << oss.begin() << "'\n" << strerror(-status) << ": " << outs << std::endl;
    return json_spirit::mValue::null;
  }

  json_spirit::mValue dump;
  if (!json_spirit::read(outb, dump))
  {
    std::cout << "couldn't parse '" << prefix << "'response json" << std::endl;
    return json_spirit::mValue::null;
  }
  return dump;
}

bool tell_rank0_config(ceph_mount_info* cmount, const std::string &var, const std::optional<const std::string> val = {}) {
  cmdmap_t cmdmap;
  std::string prefix;
  cmdmap["var"] = var;

  if (val.has_value()) {
    cmdmap["val"] = std::vector{val.value()};
    prefix = "config set";
  }
  else {
    prefix = "config unset";
  }

  return !tell_rank0(cmount, prefix, std::move(cmdmap)).is_null();
}

static std::atomic<bool> cb_done = false;
static void cb(void *hdl, vinodeno_t vino)
{
	cb_done = true;
}
TEST(LibCephFS, InoReleaseCb) {
  inodeno_t inos[MAX_CEPH_FILES];
  struct ceph_mount_info *cmount = NULL;

  ceph_create(&cmount, "admin");
  ceph_conf_read_file(cmount, NULL);
  ceph_init(cmount);

  [[maybe_unused]] int ret = ceph_mount(cmount, NULL);
  assert(ret >= 0);

  char test_file[NAME_MAX];
  int i;
  for (i = 0; i < MAX_CEPH_FILES; ++i) {
    int fd;
    struct ceph_statx stx;


    sprintf(test_file, "test_file_%d_%d", i, getpid());
    fd = ceph_open(cmount, test_file, O_RDWR|O_CREAT, 0644);
    assert(fd >= 0);

    ret = ceph_fstatx(cmount, fd, &stx, CEPH_STATX_INO, 0);
    assert(ret >= 0);

    inos[i] = stx.stx_ino;
    ceph_close(cmount, fd);
  }

  ceph_unmount(cmount);
  ceph_release(cmount);
  ceph_create(&cmount, "admin");
  ceph_conf_read_file(cmount, NULL);
  ceph_init(cmount);

  struct ceph_client_callback_args args = { 0 };
  args.ino_release_cb = cb;
  ret = ceph_ll_register_callbacks2(cmount, &args);
  assert(ret == 0);

  ret = ceph_mount(cmount, NULL);
  assert(ret >= 0);

  Inode *inodes[MAX_CEPH_FILES];

  for (i = 0; i < MAX_CEPH_FILES; ++i) {
    if (cb_done)
      break;
    ret = ceph_ll_lookup_inode(cmount, inos[i], &inodes[i]);
    assert(ret >= 0);
  }

  sleep(15);

  tell_rank0_config(cmount, "mds_min_caps_per_client", "1");
  tell_rank0_config(cmount, "mds_max_caps_per_client", "1");

  sleep(30);

  assert(cb_done);

  //reset to default at the time, anything but 1 is close enough
  tell_rank0_config(cmount, "mds_min_caps_per_client", "100");
  tell_rank0_config(cmount, "mds_max_caps_per_client", "1000000");
  for (i = 0; i < MAX_CEPH_FILES; ++i) {
    sprintf(test_file, "test_file_%d_%d", i, getpid());
    ASSERT_EQ(0, ceph_unlink(cmount, test_file));
  }

  ceph_unmount(cmount);
  ceph_release(cmount);
}

bool cb1_done = false;
void cb1(void *hdl, vinodeno_t vino, int64_t off, int64_t len)
{
  cb1_done = true;
};
TEST(LibCephFS, InoInvalidateCb) {
  struct ceph_mount_info *cmount = NULL;

  ceph_create(&cmount, "admin");
  ceph_conf_read_file(cmount, NULL);
  ceph_init(cmount);
  struct ceph_client_callback_args args = { 0 };
  args.ino_cb = cb1;
  auto ret = ceph_ll_register_callbacks2(cmount, &args);
  assert(ret == 0);

  ret = ceph_mount(cmount, NULL);
  assert(ret >= 0);
  char test_file[NAME_MAX];
  sprintf(test_file, "test_file_%d", getpid());
  int fd = ceph_open(cmount, test_file, O_RDWR|O_CREAT, 0666);
  ASSERT_LE(0, fd);

  auto data = std::string("contents");
  ASSERT_EQ(ceph_write(cmount, fd, data.c_str(), data.size(), 0), (int)data.size());;

  ASSERT_EQ(0,ceph_close(cmount, fd));

  ceph_truncate(cmount, test_file, 1);

  assert(cb1_done);

  ASSERT_EQ(0, ceph_unlink(cmount, test_file));
  ASSERT_EQ(0, ceph_unmount(cmount));
  ASSERT_EQ(0, ceph_release(cmount));
}

bool cb2_done = false;
void cb2(void *hdl, void *data)
{
  cb2_done = true;
};
TEST(LibCephFS, SwitchIntrCb) {
  struct ceph_mount_info *cmount = NULL;

  ceph_create(&cmount, "admin");
  ceph_conf_read_file(cmount, NULL);
  ceph_init(cmount);

  struct ceph_client_callback_args args = { 0 };
  args.switch_intr_cb = cb2;
  auto ret = ceph_ll_register_callbacks2(cmount, &args);
  assert(ret == 0);

  ret = ceph_mount(cmount, NULL);
  assert(ret >= 0);
  char test_file[NAME_MAX];
  sprintf(test_file, "test_file_%d", getpid());
  int fd = ceph_open(cmount, test_file, O_RDWR|O_CREAT, 0666);
  ASSERT_LE(0, fd);

  ASSERT_EQ(0, ceph_flock(cmount, fd, LOCK_EX, 42));

  assert(cb2_done);

  ASSERT_EQ(0, ceph_flock(cmount, fd, LOCK_UN, 42));
  ASSERT_EQ(0, ceph_close(cmount, fd));

  ASSERT_EQ(0, ceph_unlink(cmount, test_file));
  ASSERT_EQ(0, ceph_unmount(cmount));
  ASSERT_EQ(0, ceph_release(cmount));
}

bool cb3_done = false;
mode_t cb3(void *hdl)
{
  cb3_done = true;
  return 0;
};
TEST(LibCephFS, UmaskCb) {
  struct ceph_mount_info *cmount = NULL;

  ceph_create(&cmount, "admin");
  ceph_conf_read_file(cmount, NULL);
  ceph_conf_set(cmount, "client_acl_type", "posix_acl");
  ceph_init(cmount);

  struct ceph_client_callback_args args = { 0 };
  args.umask_cb = cb3;
  auto ret = ceph_ll_register_callbacks2(cmount, &args);
  assert(ret == 0);

  ret = ceph_mount(cmount, NULL);
  assert(ret >= 0);
  char test_file[NAME_MAX];
  sprintf(test_file, "test_file_%d", getpid());

  assert(!cb3_done);

  int fd = ceph_open(cmount, test_file, O_RDWR|O_CREAT, 0666);
  ASSERT_LE(0, fd);

  assert(cb3_done);


  ASSERT_EQ(0, ceph_close(cmount, fd));

  ASSERT_EQ(0, ceph_unlink(cmount, test_file));
  ASSERT_EQ(0, ceph_unmount(cmount));
  ASSERT_EQ(0, ceph_release(cmount));
}

bool cb4_done = false;
void cb4(void *hdl, vinodeno_t dir, vinodeno_t ino, const char *name, size_t len)
{
  cb4_done = true;
};
TEST(LibCephFS, DentryCb) {
  struct ceph_mount_info *cmount = NULL;
  struct ceph_mount_info *tcmount = NULL;

  ceph_create(&cmount, "admin");
  ceph_conf_read_file(cmount, NULL);
  ceph_init(cmount);

  struct ceph_client_callback_args args = { 0 };
  args.dentry_cb = cb4;
  auto ret = ceph_ll_register_callbacks2(cmount, &args);
  assert(ret == 0);

  ret = ceph_mount(cmount, NULL);
  assert(ret >= 0);
  char test_file[NAME_MAX];
  sprintf(test_file, "test_file_%d", getpid());

  assert(!cb4_done);

  assert(!cb4_done);
  Inode *root = nullptr;
  Inode *root2, *file = nullptr;

  Fh *fh;
  struct ceph_statx stx;
  UserPerm *perms = ceph_mount_perms(cmount);

  ASSERT_EQ(ceph_ll_lookup_root(cmount, &root), 0);
  ASSERT_EQ(ceph_ll_create(cmount, root, test_file, 0666,
            O_RDWR|O_CREAT|O_EXCL, &file, &fh, &stx, 0, 0, perms), 0);

  ASSERT_EQ(ceph_create(&tcmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(tcmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(tcmount, NULL));
  ASSERT_EQ(ceph_mount(tcmount, "/"), 0);

  ASSERT_EQ(ceph_ll_lookup_root(tcmount, &root2), 0);

  assert(!cb4_done);

  ASSERT_EQ(0, ceph_ll_unlink(tcmount, root2, test_file, perms));
  ASSERT_EQ(ceph_ll_close(cmount, fh), 0);

  std::this_thread::sleep_for(std::chrono::seconds(10));

  assert(cb4_done);

  ceph_shutdown(tcmount);
  ceph_shutdown(cmount);
}

bool cb5_done = false;
int cb5(void *hdl)
{
  cb5_done = true;
  return 0;
};
