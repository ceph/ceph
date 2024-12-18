// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <fcntl.h>
#include <iostream>
#include <string.h>
#include <string>

#include "include/Context.h"
#include "auth/Crypto.h"
#include "client/Client.h"
#include "client/Inode.h"
#include "librados/RadosClient.h"
#include "common/async/context_pool.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/version.h"
#include "mon/MonClient.h"
#include "include/str_list.h"
#include "include/stringify.h"
#include "include/object.h"
#include "messages/MMonMap.h"
#include "msg/Messenger.h"
#include "include/ceph_assert.h"
#include "mds/MDSMap.h"

#include "include/cephfs/libcephfs.h"

#define DEFAULT_UMASK 002

using namespace std;

static mode_t umask_cb(void *);
namespace {
// Set things up this way so we don't start up threads until mount and
// kill them off when the last mount goes away, but are tolerant to
// multiple mounts of overlapping duration.
std::shared_ptr<ceph::async::io_context_pool> get_icp(CephContext* cct)
{
  static std::mutex m;
  static std::weak_ptr<ceph::async::io_context_pool> icwp;


  std::unique_lock l(m);

  auto icp = icwp.lock();
  if (icp)
    return icp;

  icp = std::make_shared<ceph::async::io_context_pool>();
  icwp = icp;
  icp->start(cct->_conf.get_val<std::uint64_t>("client_asio_thread_count"));
  return icp;
}
}

struct ceph_mount_info
{
  mode_t umask = DEFAULT_UMASK;
  std::shared_ptr<ceph::async::io_context_pool> icp;
public:
  explicit ceph_mount_info(CephContext *cct_)
    : default_perms(),
      mounted(false),
      inited(false),
      client(nullptr),
      monclient(nullptr),
      messenger(nullptr),
      cct(cct_)
  {
    if (cct_) {
      cct->get();
    }
  }

  ~ceph_mount_info()
  {
    try {
      shutdown();
      if (cct) {
	cct->put();
	cct = nullptr;
      }
    }
    catch (const std::exception& e) {
      // we shouldn't get here, but if we do, we want to know about it.
      lderr(cct) << "ceph_mount_info::~ceph_mount_info: caught exception: "
	         << e.what() << dendl;
    }
    catch (...) {
      // ignore
    }
  }

  int init()
  {
    int ret;

    if (!cct->_log->is_started()) {
      cct->_log->start();
    }
    icp = get_icp(cct);

    {
      MonClient mc_bootstrap(cct, icp->get_io_context());
      ret = mc_bootstrap.get_monmap_and_config();
      if (ret < 0)
	return ret;
    }

    common_init_finish(cct);

    //monmap
    monclient = new MonClient(cct, icp->get_io_context());
    ret = -CEPHFS_ERROR_MON_MAP_BUILD; //defined in libcephfs.h;
    if (monclient->build_initial_monmap() < 0)
      goto fail;

    //network connection
    messenger = Messenger::create_client_messenger(cct, "client");

    //at last the client
    ret = -CEPHFS_ERROR_NEW_CLIENT; //defined in libcephfs.h;
    client = new StandaloneClient(messenger, monclient, icp->get_io_context());
    if (!client)
      goto fail;

    ret = -CEPHFS_ERROR_MESSENGER_START; //defined in libcephfs.h;
    if (messenger->start() != 0)
      goto fail;

    ret = client->init();
    if (ret)
      goto fail;

    {
      ceph_client_callback_args args = {};
      args.handle = this;
      args.umask_cb = umask_cb;
      client->ll_register_callbacks(&args);
    }

    default_perms = Client::pick_my_perms(cct);
    inited = true;
    return 0;

    fail:
    shutdown();
    return ret;
  }

  int select_filesystem(const std::string &fs_name_)
  {
    if (mounted) {
      return -CEPHFS_EISCONN;
    }

    fs_name = fs_name_;
    return 0;
  }

  const std::string& get_filesystem(void)
  {
    return fs_name;
  }

  int mount(const std::string &mount_root, const UserPerm& perms)
  {
    int ret;
    
    if (mounted)
      return -CEPHFS_EISCONN;

    if (!inited) {
      ret = init();
      if (ret != 0) {
        return ret;
      }
    }

    ret = client->mount(mount_root, perms, false, fs_name);
    if (ret) {
      shutdown();
      return ret;
    } else {
      mounted = true;
      return 0;
    }
  }

  int unmount()
  {
    if (!mounted)
      return -CEPHFS_ENOTCONN;
    shutdown();
    return 0;
  }
  int abort_conn()
  {
    if (mounted) {
      client->abort_conn();
      mounted = false;
    }
    return 0;
  }

  void shutdown()
  {
    if (mounted) {
      client->unmount();
      mounted = false;
    }
    if (inited) {
      client->shutdown();
      inited = false;
    }
    if (messenger) {
      messenger->shutdown();
      messenger->wait();
      delete messenger;
      messenger = nullptr;
    }
    if (monclient) {
      delete monclient;
      monclient = nullptr;
    }
    if (client) {
      delete client;
      client = nullptr;
    }
    icp.reset();
  }

  bool is_initialized() const
  {
    return inited;
  }

  bool is_mounted()
  {
    return mounted;
  }

  mode_t set_umask(mode_t umask)
  {
    this->umask = umask;
    return umask;
  }

  std::string getaddrs()
  {
    CachedStackStringStream cos;
    *cos << messenger->get_myaddrs();
    return std::string(cos->strv());
  }

  int conf_read_file(const char *path_list)
  {
    int ret = cct->_conf.parse_config_files(path_list, nullptr, 0);
    if (ret)
      return ret;
    cct->_conf.apply_changes(nullptr);
    cct->_conf.complain_about_parse_error(cct);
    return 0;
  }

  int conf_parse_argv(int argc, const char **argv)
  {
    auto args = argv_to_vec(argc, argv);
    int ret = cct->_conf.parse_argv(args);
    if (ret)
	return ret;
    cct->_conf.apply_changes(nullptr);
    return 0;
  }

  int conf_parse_env(const char *name)
  {
    auto& conf = cct->_conf;
    conf.parse_env(cct->get_module_type(), name);
    conf.apply_changes(nullptr);
    return 0;
  }

  int conf_set(const char *option, const char *value)
  {
    int ret = cct->_conf.set_val(option, value);
    if (ret)
      return ret;
    cct->_conf.apply_changes(nullptr);
    return 0;
  }

  int conf_get(const char *option, char *buf, size_t len)
  {
    char *tmp = buf;
    return cct->_conf.get_val(option, &tmp, len);
  }

  Client *get_client()
  {
    return client;
  }

  const char *get_cwd(const UserPerm& perms)
  {
    client->getcwd(cwd, perms);
    return cwd.c_str();
  }

  int chdir(const char *to, const UserPerm& perms)
  {
    return client->chdir(to, cwd, perms);
  }

  CephContext *get_ceph_context() const {
    return cct;
  }

  UserPerm default_perms;
private:
  bool mounted;
  bool inited;
  StandaloneClient *client;
  MonClient *monclient;
  Messenger *messenger;
  CephContext *cct;
  std::string cwd;
  std::string fs_name;
};

static mode_t umask_cb(void *handle)
{
  return ((struct ceph_mount_info *)handle)->umask;
}

static void do_out_buffer(bufferlist& outbl, char **outbuf, size_t *outbuflen)
{
  if (outbuf) {
    if (outbl.length() > 0) {
      *outbuf = (char *)malloc(outbl.length());
      memcpy(*outbuf, outbl.c_str(), outbl.length());
    } else {
      *outbuf = nullptr;
    }
  }
  if (outbuflen)
    *outbuflen = outbl.length();
}

static void do_out_buffer(string& outbl, char **outbuf, size_t *outbuflen)
{
  if (outbuf) {
    if (outbl.length() > 0) {
      *outbuf = (char *)malloc(outbl.length());
      memcpy(*outbuf, outbl.c_str(), outbl.length());
    } else {
      *outbuf = nullptr;
    }
  }
  if (outbuflen)
    *outbuflen = outbl.length();
}

extern "C" UserPerm *ceph_userperm_new(uid_t uid, gid_t gid, int ngids,
				       gid_t *gidlist)
{
  return new (std::nothrow) UserPerm(uid, gid, ngids, gidlist);
}

extern "C" void ceph_userperm_destroy(UserPerm *perm)
{
  delete perm;
}

extern "C" const char *ceph_version(int *pmajor, int *pminor, int *ppatch)
{
  int major, minor, patch;
  const char *v = ceph_version_to_str();

  int n = sscanf(v, "%d.%d.%d", &major, &minor, &patch);
  if (pmajor)
    *pmajor = (n >= 1) ? major : 0;
  if (pminor)
    *pminor = (n >= 2) ? minor : 0;
  if (ppatch)
    *ppatch = (n >= 3) ? patch : 0;
  return PROJECT_VERSION;
}

extern "C" int ceph_create_with_context(struct ceph_mount_info **cmount, CephContext *cct)
{
  *cmount = new struct ceph_mount_info(cct);
  return 0;
}

extern "C" int ceph_create_from_rados(struct ceph_mount_info **cmount,
    rados_t cluster)
{
  auto rados = (librados::RadosClient *) cluster;
  auto cct = rados->cct;
  return ceph_create_with_context(cmount, cct);
}

extern "C" int ceph_create(struct ceph_mount_info **cmount, const char * const id)
{
  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
  if (id) {
    iparams.name.set(CEPH_ENTITY_TYPE_CLIENT, id);
  }

  CephContext *cct = common_preinit(iparams, CODE_ENVIRONMENT_LIBRARY, 0);
  cct->_conf.parse_env(cct->get_module_type()); // environment variables coverride
  cct->_conf.apply_changes(nullptr);
  int ret = ceph_create_with_context(cmount, cct);
  cct->put();
  cct = nullptr;
  return ret;
}

extern "C" int ceph_unmount(struct ceph_mount_info *cmount)
{
  return cmount->unmount();
}

extern "C" int ceph_abort_conn(struct ceph_mount_info *cmount)
{
  return cmount->abort_conn();
}

extern "C" int ceph_release(struct ceph_mount_info *cmount)
{
  if (cmount->is_mounted())
    return -CEPHFS_EISCONN;
  delete cmount;
  cmount = nullptr;
  return 0;
}

extern "C" void ceph_shutdown(struct ceph_mount_info *cmount)
{
  cmount->shutdown();
  delete cmount;
  cmount = nullptr;
}

extern "C" uint64_t ceph_get_instance_id(struct ceph_mount_info *cmount)
{
  if (cmount->is_initialized())
    return cmount->get_client()->get_nodeid().v;
  return 0;
}

extern "C" int ceph_getaddrs(struct ceph_mount_info *cmount, char** addrs)
{
  if (!cmount->is_initialized())
    return -CEPHFS_ENOTCONN;
  auto s = cmount->getaddrs();
  *addrs = strdup(s.c_str());
  return 0;
}

extern "C" int ceph_conf_read_file(struct ceph_mount_info *cmount, const char *path)
{
  return cmount->conf_read_file(path);
}

extern "C" mode_t ceph_umask(struct ceph_mount_info *cmount, mode_t mode)
{
  return cmount->set_umask(mode);
}

extern "C" int ceph_conf_parse_argv(struct ceph_mount_info *cmount, int argc,
				     const char **argv)
{
  return cmount->conf_parse_argv(argc, argv);
}

extern "C" int ceph_conf_parse_env(struct ceph_mount_info *cmount, const char *name)
{
  return cmount->conf_parse_env(name);
}

extern "C" int ceph_conf_set(struct ceph_mount_info *cmount, const char *option,
			     const char *value)
{
  return cmount->conf_set(option, value);
}

extern "C" int ceph_conf_get(struct ceph_mount_info *cmount, const char *option,
			     char *buf, size_t len)
{
  if (!buf) {
    return -CEPHFS_EINVAL;
  }
  return cmount->conf_get(option, buf, len);
}

extern "C" int ceph_set_mount_timeout(struct ceph_mount_info *cmount, uint32_t timeout) {
  if (cmount->is_mounted()) {
    return -CEPHFS_EINVAL;
  }

  auto timeout_str = stringify(timeout);
  return ceph_conf_set(cmount, "client_mount_timeout", timeout_str.c_str());
}

extern "C" int ceph_mds_command(struct ceph_mount_info *cmount,
    const char *mds_spec,
    const char **cmd,
    size_t cmdlen,
    const char *inbuf, size_t inbuflen,
    char **outbuf, size_t *outbuflen,
    char **outsbuf, size_t *outsbuflen)
{
  bufferlist inbl;
  bufferlist outbl;
  std::vector<string> cmdv;
  std::string outs;

  if (!cmount->is_initialized()) {
    return -CEPHFS_ENOTCONN;
  }

  // Construct inputs
  for (size_t i = 0; i < cmdlen; ++i) {
    cmdv.push_back(cmd[i]);
  }
  inbl.append(inbuf, inbuflen);

  // Issue remote command
  C_SaferCond cond;
  int r = cmount->get_client()->mds_command(
      mds_spec,
      cmdv, inbl,
      &outbl, &outs,
      &cond);

  if (r != 0) {
    goto out;
  }

  // Wait for completion
  r = cond.wait();

  // Construct outputs
  do_out_buffer(outbl, outbuf, outbuflen);
  do_out_buffer(outs, outsbuf, outsbuflen);

out:
  return r;
}

extern "C" int ceph_init(struct ceph_mount_info *cmount)
{
  return cmount->init();
}

extern "C" int ceph_select_filesystem(struct ceph_mount_info *cmount,
                                      const char *fs_name)
{
  if (fs_name == nullptr) {
    return -CEPHFS_EINVAL;
  }

  return cmount->select_filesystem(fs_name);
}

extern "C" int ceph_mount(struct ceph_mount_info *cmount, const char *root)
{
  std::string mount_root;
  if (root)
    mount_root = root;
  return cmount->mount(mount_root, cmount->default_perms);
}

extern "C" int ceph_is_mounted(struct ceph_mount_info *cmount)
{
  return cmount->is_mounted() ? 1 : 0;
}

extern "C" struct UserPerm *ceph_mount_perms(struct ceph_mount_info *cmount)
{
  return &cmount->default_perms;
}

extern "C" int64_t ceph_get_fs_cid(struct ceph_mount_info *cmount)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->get_fs_cid();
}

extern "C" int ceph_mount_perms_set(struct ceph_mount_info *cmount,
				    struct UserPerm *perms)
{
  if (cmount->is_mounted())
    return -CEPHFS_EISCONN;
  cmount->default_perms = *perms;
  return 0;
}

extern "C" int ceph_statfs(struct ceph_mount_info *cmount, const char *path,
			   struct statvfs *stbuf)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->statfs(path, stbuf, cmount->default_perms);
}

extern "C" int ceph_get_local_osd(struct ceph_mount_info *cmount)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->get_local_osd();
}

extern "C" const char* ceph_getcwd(struct ceph_mount_info *cmount)
{
  return cmount->get_cwd(cmount->default_perms);
}

extern "C" int ceph_chdir (struct ceph_mount_info *cmount, const char *s)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->chdir(s, cmount->default_perms);
}

extern "C" int ceph_opendir(struct ceph_mount_info *cmount,
			    const char *name, struct ceph_dir_result **dirpp)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->opendir(name, (dir_result_t **)dirpp, cmount->default_perms);
}

extern "C" int ceph_fdopendir(struct ceph_mount_info *cmount, int dirfd,
                              struct ceph_dir_result **dirpp)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->fdopendir(dirfd, (dir_result_t **)dirpp, cmount->default_perms);
}

extern "C" int ceph_closedir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->closedir(reinterpret_cast<dir_result_t*>(dirp));
}

extern "C" struct dirent * ceph_readdir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp)
{
  if (!cmount->is_mounted()) {
    /* Client::readdir also sets errno to signal errors. */
    errno = CEPHFS_ENOTCONN;
    return nullptr;
  }
  return cmount->get_client()->readdir(reinterpret_cast<dir_result_t*>(dirp));
}

extern "C" int ceph_readdir_r(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp, struct dirent *de)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->readdir_r(reinterpret_cast<dir_result_t*>(dirp), de);
}

extern "C" int ceph_readdirplus_r(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp,
				  struct dirent *de, struct ceph_statx *stx, unsigned want,
				  unsigned flags, struct Inode **out)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  if (flags & ~CEPH_REQ_FLAG_MASK)
    return -CEPHFS_EINVAL;
  return cmount->get_client()->readdirplus_r(reinterpret_cast<dir_result_t*>(dirp), de, stx, want, flags, out);
}

extern "C" int ceph_open_snapdiff(struct ceph_mount_info* cmount,
                                  const char* root_path,
                                  const char* rel_path,
                                  const char* snap1,
                                  const char* snap2,
                                  struct ceph_snapdiff_info* out)
{
  if (!cmount->is_mounted()) {
    /* we set errno to signal errors. */
    errno = ENOTCONN;
    return -errno;
  }
  if (!out || !root_path || !rel_path ||
      !snap1 || !*snap1 || !snap2 || !*snap2) {
    errno = EINVAL;
    return -errno;
  }
  out->cmount = cmount;
  out->dir1 = out->dir_aux = nullptr;

  char full_path1[PATH_MAX];
  char snapdir[PATH_MAX];
  cmount->conf_get("client_snapdir", snapdir, sizeof(snapdir) - 1);
  int n = snprintf(full_path1, PATH_MAX,
    "%s/%s/%s/%s", root_path, snapdir, snap1, rel_path);
  if (n < 0 || n == PATH_MAX) {
    errno = ENAMETOOLONG;
    return -errno;
  }
  char full_path2[PATH_MAX];
  n = snprintf(full_path2, PATH_MAX,
    "%s/%s/%s/%s", root_path, snapdir, snap2, rel_path);
  if (n < 0 || n == PATH_MAX) {
    errno = ENAMETOOLONG;
    return -errno;
  }

  int r = ceph_opendir(cmount, full_path1, &(out->dir1));
  if (r != 0) {
    //it's OK to have one of the snap paths absent - attempting another one
    r = ceph_opendir(cmount, full_path2, &(out->dir1));
    if (r != 0) {
      // both snaps are absent, giving up
      errno = ENOENT;
      return -errno;
    }
    std::swap(snap1, snap2); // will use snap1 to learn snap_other below
  } else {
    // trying to open second snapshot to learn snapid and
    // get the entry loaded into the client cache if any.
    r = ceph_opendir(cmount, full_path2, &(out->dir_aux));
    //paranoic, rely on this value below
    out->dir_aux = r == 0 ? out->dir_aux : nullptr;
  }
  if (!out->dir_aux) {
    // now trying to learn the second snapshot's id by using snapshot's root
    n = snprintf(full_path2, PATH_MAX,
        "%s/%s/%s", root_path, snapdir, snap2);
    ceph_assert(n > 0 && n < PATH_MAX); //we've already checked above
                                        //that longer string fits.
                                        // Hence unlikely to assert
    r = ceph_opendir(cmount, full_path2, &(out->dir_aux));
    if (r != 0) {
      goto close_err;
    }
  }
  return 0;

close_err:
  ceph_close_snapdiff(out);
  return r;
}

extern "C" int ceph_readdir_snapdiff(struct ceph_snapdiff_info* snapdiff,
                                     struct ceph_snapdiff_entry_t* out)
{
  if (!snapdiff->cmount->is_mounted()) {
    /* also sets errno to signal errors. */
    errno = ENOTCONN;
    return -errno;
  }
  dir_result_t* d1 = reinterpret_cast<dir_result_t*>(snapdiff->dir1);
  dir_result_t* d2 = reinterpret_cast<dir_result_t*>(snapdiff->dir_aux);
  if (!d1 || !d2 || !d1->inode || !d2->inode) {
    errno = EINVAL;
    return -errno;
  }
  snapid_t snapid;
  int r = snapdiff->cmount->get_client()->readdir_snapdiff(
    d1,
    d2->inode->snapid,
    &(out->dir_entry),
    &snapid);
  if (r >= 0) {
    // converting snapid_t to uint64_t to avoid snapid_t exposure
    out->snapid = snapid;
  }
  return r;
}

extern "C" int ceph_close_snapdiff(struct ceph_snapdiff_info* snapdiff)
{
  if (!snapdiff->cmount || !snapdiff->cmount->is_mounted()) {
    /* also sets errno to signal errors. */
    errno = ENOTCONN;
    return -errno;
  }
  if (snapdiff->dir_aux) {
    ceph_closedir(snapdiff->cmount, snapdiff->dir_aux);
  }
  if (snapdiff->dir1) {
    ceph_closedir(snapdiff->cmount, snapdiff->dir1);
  }
  snapdiff->cmount = nullptr;
  snapdiff->dir1 = snapdiff->dir_aux = nullptr;
  return 0;
}

extern "C" int ceph_getdents(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp,
			     char *buf, int buflen)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->getdents(reinterpret_cast<dir_result_t*>(dirp), buf, buflen);
}

extern "C" int ceph_getdnames(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp,
			      char *buf, int buflen)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->getdnames(reinterpret_cast<dir_result_t*>(dirp), buf, buflen);
}

extern "C" void ceph_rewinddir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp)
{
  if (!cmount->is_mounted())
    return;
  cmount->get_client()->rewinddir(reinterpret_cast<dir_result_t*>(dirp));
}

extern "C" int64_t ceph_telldir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->telldir(reinterpret_cast<dir_result_t*>(dirp));
}

extern "C" void ceph_seekdir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp, int64_t offset)
{
  if (!cmount->is_mounted())
    return;
  cmount->get_client()->seekdir(reinterpret_cast<dir_result_t*>(dirp), offset);
}

extern "C" int ceph_may_delete(struct ceph_mount_info *cmount, const char *path)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->may_delete(path, cmount->default_perms);
}

extern "C" int ceph_link (struct ceph_mount_info *cmount, const char *existing,
			  const char *newname)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->link(existing, newname, cmount->default_perms);
}

extern "C" int ceph_unlink(struct ceph_mount_info *cmount, const char *path)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->unlink(path, cmount->default_perms);
}

extern "C" int ceph_unlinkat(struct ceph_mount_info *cmount, int dirfd, const char *relpath, int flags)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->unlinkat(dirfd, relpath, flags, cmount->default_perms);
}

extern "C" int ceph_rename(struct ceph_mount_info *cmount, const char *from,
			   const char *to)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->rename(from, to, cmount->default_perms);
}

// dirs
extern "C" int ceph_mkdir(struct ceph_mount_info *cmount, const char *path, mode_t mode)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->mkdir(path, mode, cmount->default_perms);
}

extern "C" int ceph_mkdirat(struct ceph_mount_info *cmount, int dirfd, const char *relpath,
                            mode_t mode)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->mkdirat(dirfd, relpath, mode, cmount->default_perms);
}

extern "C" int ceph_mksnap(struct ceph_mount_info *cmount, const char *path, const char *name,
                           mode_t mode, struct snap_metadata *snap_metadata, size_t nr_snap_metadata)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  size_t i = 0;
  std::map<std::string, std::string> metadata;
  while (i < nr_snap_metadata) {
    metadata.emplace(snap_metadata[i].key, snap_metadata[i].value);
    ++i;
  }
  return cmount->get_client()->mksnap(path, name, cmount->default_perms, mode, metadata);
}

extern "C" int ceph_rmsnap(struct ceph_mount_info *cmount, const char *path, const char *name)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->rmsnap(path, name, cmount->default_perms, true);
}

extern "C" int ceph_mkdirs(struct ceph_mount_info *cmount, const char *path, mode_t mode)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->mkdirs(path, mode, cmount->default_perms);
}

extern "C" int ceph_rmdir(struct ceph_mount_info *cmount, const char *path)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->rmdir(path, cmount->default_perms);
}

// symlinks
extern "C" int ceph_readlink(struct ceph_mount_info *cmount, const char *path,
			     char *buf, int64_t size)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->readlink(path, buf, size, cmount->default_perms);
}

extern "C" int ceph_readlinkat(struct ceph_mount_info *cmount, int dirfd,
                               const char *relpath, char *buf, int64_t size)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->readlinkat(dirfd, relpath, buf, size, cmount->default_perms);
}

extern "C" int ceph_symlink(struct ceph_mount_info *cmount, const char *existing,
			    const char *newname)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->symlink(existing, newname, cmount->default_perms);
}

extern "C" int ceph_symlinkat(struct ceph_mount_info *cmount, const char *existing, int dirfd,
                            const char *newname)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->symlinkat(existing, dirfd, newname, cmount->default_perms);
}

extern "C" int ceph_fstatx(struct ceph_mount_info *cmount, int fd, struct ceph_statx *stx,
                            unsigned int want, unsigned int flags)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  if (flags & ~CEPH_REQ_FLAG_MASK)
    return -CEPHFS_EINVAL;
  return cmount->get_client()->fstatx(fd, stx, cmount->default_perms,
                                      want, flags);
}

extern "C" int ceph_statxat(struct ceph_mount_info *cmount, int dirfd, const char *relpath,
                            struct ceph_statx *stx, unsigned int want, unsigned int flags)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
#ifdef CEPH_AT_EMPTY_PATH
  if (flags & ~CEPH_AT_EMPTY_PATH)
#else
  if (flags & ~CEPH_REQ_FLAG_MASK)
#endif
    return -CEPHFS_EINVAL;
  return cmount->get_client()->statxat(dirfd, relpath, stx, cmount->default_perms,
                                       want, flags);
}

extern "C" int ceph_statx(struct ceph_mount_info *cmount, const char *path,
			  struct ceph_statx *stx, unsigned int want, unsigned int flags)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  if (flags & ~CEPH_REQ_FLAG_MASK)
    return -CEPHFS_EINVAL;
  return cmount->get_client()->statx(path, stx, cmount->default_perms,
				     want, flags);
}

extern "C" int ceph_fsetattrx(struct ceph_mount_info *cmount, int fd,
			      struct ceph_statx *stx, int mask)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->fsetattrx(fd, stx, mask, cmount->default_perms);
}

extern "C" int ceph_setattrx(struct ceph_mount_info *cmount, const char *relpath,
			    struct ceph_statx *stx, int mask, int flags)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  if (flags & ~CEPH_REQ_FLAG_MASK)
    return -CEPHFS_EINVAL;
  return cmount->get_client()->setattrx(relpath, stx, mask,
					cmount->default_perms, flags);
}

// *xattr() calls supporting samba/vfs
extern "C" int ceph_getxattr(struct ceph_mount_info *cmount, const char *path, const char *name, void *value, size_t size)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;

  return cmount->get_client()->getxattr(path, name, value, size, cmount->default_perms);
}

extern "C" int ceph_lgetxattr(struct ceph_mount_info *cmount, const char *path, const char *name, void *value, size_t size)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->lgetxattr(path, name, value, size, cmount->default_perms);
}

extern "C" int ceph_fgetxattr(struct ceph_mount_info *cmount, int fd, const char *name, void *value, size_t size)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->fgetxattr(fd, name, value, size, cmount->default_perms);
}


extern "C" int ceph_listxattr(struct ceph_mount_info *cmount, const char *path, char *list, size_t size)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->listxattr(path, list, size, cmount->default_perms);
}

extern "C" int ceph_llistxattr(struct ceph_mount_info *cmount, const char *path, char *list, size_t size)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->llistxattr(path, list, size, cmount->default_perms);
}

extern "C" int ceph_flistxattr(struct ceph_mount_info *cmount, int fd, char *list, size_t size)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->flistxattr(fd, list, size, cmount->default_perms);
}

extern "C" int ceph_removexattr(struct ceph_mount_info *cmount, const char *path, const char *name)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->removexattr(path, name, cmount->default_perms);
}

extern "C" int ceph_lremovexattr(struct ceph_mount_info *cmount, const char *path, const char *name)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->lremovexattr(path, name, cmount->default_perms);
}

extern "C" int ceph_fremovexattr(struct ceph_mount_info *cmount, int fd, const char *name)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->fremovexattr(fd, name, cmount->default_perms);
}

extern "C" int ceph_setxattr(struct ceph_mount_info *cmount, const char *path, const char *name, const void *value, size_t size, int flags)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->setxattr(path, name, value, size, flags, cmount->default_perms);
}

extern "C" int ceph_lsetxattr(struct ceph_mount_info *cmount, const char *path, const char *name, const void *value, size_t size, int flags)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->lsetxattr(path, name, value, size, flags, cmount->default_perms);
}

extern "C" int ceph_fsetxattr(struct ceph_mount_info *cmount, int fd, const char *name, const void *value, size_t size, int flags)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->fsetxattr(fd, name, value, size, flags, cmount->default_perms);
}
/* end xattr support */

extern "C" int ceph_stat(struct ceph_mount_info *cmount, const char *path, struct stat *stbuf)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->stat(path, stbuf, cmount->default_perms);
}

extern "C" int ceph_fstat(struct ceph_mount_info *cmount, int fd, struct stat *stbuf)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->fstat(fd, stbuf, cmount->default_perms);
}

extern int ceph_lstat(struct ceph_mount_info *cmount, const char *path, struct stat *stbuf)
{
   if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->lstat(path, stbuf, cmount->default_perms);
}

extern "C" int ceph_chmod(struct ceph_mount_info *cmount, const char *path, mode_t mode)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->chmod(path, mode, cmount->default_perms);
}
extern "C" int ceph_lchmod(struct ceph_mount_info *cmount, const char *path, mode_t mode)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->lchmod(path, mode, cmount->default_perms);
}
extern "C" int ceph_fchmod(struct ceph_mount_info *cmount, int fd, mode_t mode)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->fchmod(fd, mode, cmount->default_perms);
}

extern "C" int ceph_chmodat(struct ceph_mount_info *cmount, int dirfd, const char *relpath,
                            mode_t mode, int flags) {
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->chmodat(dirfd, relpath, mode, flags, cmount->default_perms);
}

extern "C" int ceph_chown(struct ceph_mount_info *cmount, const char *path,
			  int uid, int gid)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->chown(path, uid, gid, cmount->default_perms);
}
extern "C" int ceph_fchown(struct ceph_mount_info *cmount, int fd,
			   int uid, int gid)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->fchown(fd, uid, gid, cmount->default_perms);
}
extern "C" int ceph_lchown(struct ceph_mount_info *cmount, const char *path,
			   int uid, int gid)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->lchown(path, uid, gid, cmount->default_perms);
}

extern "C" int ceph_chownat(struct ceph_mount_info *cmount, int dirfd, const char *relpath,
                            uid_t uid, gid_t gid, int flags) {
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->chownat(dirfd, relpath, uid, gid, flags, cmount->default_perms);
}

extern "C" int ceph_utime(struct ceph_mount_info *cmount, const char *path,
			  struct utimbuf *buf)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->utime(path, buf, cmount->default_perms);
}

extern "C" int ceph_futime(struct ceph_mount_info *cmount, int fd,
                           struct utimbuf *buf)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->futime(fd, buf, cmount->default_perms);
}

extern "C" int ceph_utimes(struct ceph_mount_info *cmount, const char *path,
                           struct timeval times[2])
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->utimes(path, times, cmount->default_perms);
}

extern "C" int ceph_lutimes(struct ceph_mount_info *cmount, const char *path,
                            struct timeval times[2])
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->lutimes(path, times, cmount->default_perms);
}

extern "C" int ceph_futimes(struct ceph_mount_info *cmount, int fd,
                            struct timeval times[2])
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->futimes(fd, times, cmount->default_perms);
}

extern "C" int ceph_futimens(struct ceph_mount_info *cmount, int fd,
                            struct timespec times[2])
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->futimens(fd, times, cmount->default_perms);
}

extern "C" int ceph_utimensat(struct ceph_mount_info *cmount, int dirfd, const char *relpath,
                              struct timespec times[2], int flags) {
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->utimensat(dirfd, relpath, times, flags, cmount->default_perms);
}

extern "C" int ceph_flock(struct ceph_mount_info *cmount, int fd, int operation,
			  uint64_t owner)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->flock(fd, operation, owner);
}

extern "C" int ceph_truncate(struct ceph_mount_info *cmount, const char *path,
			     int64_t size)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->truncate(path, size, cmount->default_perms);
}

// file ops
extern "C" int ceph_mknod(struct ceph_mount_info *cmount, const char *path,
			  mode_t mode, dev_t rdev)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->mknod(path, mode, cmount->default_perms, rdev);
}

extern "C" int ceph_open(struct ceph_mount_info *cmount, const char *path,
			 int flags, mode_t mode)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->open(path, flags, cmount->default_perms, mode);
}

extern "C" int ceph_openat(struct ceph_mount_info *cmount, int dirfd, const char *relpath,
                           int flags, mode_t mode)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->openat(dirfd, relpath, flags, cmount->default_perms, mode);
}

extern "C" int ceph_open_layout(struct ceph_mount_info *cmount, const char *path, int flags,
    mode_t mode, int stripe_unit, int stripe_count, int object_size, const char *data_pool)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->open(path, flags, cmount->default_perms, mode,
				    stripe_unit, stripe_count,
				    object_size, data_pool);
}

extern "C" int ceph_close(struct ceph_mount_info *cmount, int fd)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->close(fd);
}

extern "C" int64_t ceph_lseek(struct ceph_mount_info *cmount, int fd,
			     int64_t offset, int whence)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->lseek(fd, offset, whence);
}

extern "C" int ceph_read(struct ceph_mount_info *cmount, int fd, char *buf,
			 int64_t size, int64_t offset)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->read(fd, buf, size, offset);
}

extern "C" int ceph_preadv(struct ceph_mount_info *cmount, int fd,
              const struct iovec *iov, int iovcnt, int64_t offset)
{
  if (!cmount->is_mounted())
      return -CEPHFS_ENOTCONN;
  return cmount->get_client()->preadv(fd, iov, iovcnt, offset);
}

extern "C" int ceph_write(struct ceph_mount_info *cmount, int fd, const char *buf,
			  int64_t size, int64_t offset)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->write(fd, buf, size, offset);
}

extern "C" int ceph_pwritev(struct ceph_mount_info *cmount, int fd,
              const struct iovec *iov, int iovcnt, int64_t offset)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->pwritev(fd, iov, iovcnt, offset);
}

extern "C" int ceph_ftruncate(struct ceph_mount_info *cmount, int fd, int64_t size)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->ftruncate(fd, size, cmount->default_perms);
}

extern "C" int ceph_fsync(struct ceph_mount_info *cmount, int fd, int syncdataonly)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->fsync(fd, syncdataonly);
}

extern "C" int ceph_fallocate(struct ceph_mount_info *cmount, int fd, int mode,
	                      int64_t offset, int64_t length)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->fallocate(fd, mode, offset, length);
}

extern "C" int ceph_lazyio(class ceph_mount_info *cmount,
                           int fd, int enable)
{
  return (cmount->get_client()->lazyio(fd, enable));
}

extern "C" int ceph_lazyio_propagate(class ceph_mount_info *cmount,
                           int fd, int64_t offset, size_t count)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return (cmount->get_client()->lazyio_propagate(fd, offset, count));
}

extern "C" int ceph_lazyio_synchronize(class ceph_mount_info *cmount,
                           int fd, int64_t offset, size_t count)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return (cmount->get_client()->lazyio_synchronize(fd, offset, count));
}


extern "C" int ceph_sync_fs(struct ceph_mount_info *cmount)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->sync_fs();
}

extern "C" int ceph_get_file_stripe_unit(struct ceph_mount_info *cmount, int fh)
{
  file_layout_t l;
  int r;

  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  r = cmount->get_client()->fdescribe_layout(fh, &l);
  if (r < 0)
    return r;
  return l.stripe_unit;
}

extern "C" int ceph_get_path_stripe_unit(struct ceph_mount_info *cmount, const char *path)
{
  file_layout_t l;
  int r;

  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  r = cmount->get_client()->describe_layout(path, &l, cmount->default_perms);
  if (r < 0)
    return r;
  return l.stripe_unit;
}

extern "C" int ceph_get_file_stripe_count(struct ceph_mount_info *cmount, int fh)
{
  file_layout_t l;
  int r;

  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  r = cmount->get_client()->fdescribe_layout(fh, &l);
  if (r < 0)
    return r;
  return l.stripe_count;
}

extern "C" int ceph_get_path_stripe_count(struct ceph_mount_info *cmount, const char *path)
{
  file_layout_t l;
  int r;

  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  r = cmount->get_client()->describe_layout(path, &l, cmount->default_perms);
  if (r < 0)
    return r;
  return l.stripe_count;
}

extern "C" int ceph_get_file_object_size(struct ceph_mount_info *cmount, int fh)
{
  file_layout_t l;
  int r;

  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  r = cmount->get_client()->fdescribe_layout(fh, &l);
  if (r < 0)
    return r;
  return l.object_size;
}

extern "C" int ceph_get_path_object_size(struct ceph_mount_info *cmount, const char *path)
{
  file_layout_t l;
  int r;

  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  r = cmount->get_client()->describe_layout(path, &l, cmount->default_perms);
  if (r < 0)
    return r;
  return l.object_size;
}

extern "C" int ceph_get_file_pool(struct ceph_mount_info *cmount, int fh)
{
  file_layout_t l;
  int r;

  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  r = cmount->get_client()->fdescribe_layout(fh, &l);
  if (r < 0)
    return r;
  return l.pool_id;
}

extern "C" int ceph_get_path_pool(struct ceph_mount_info *cmount, const char *path)
{
  file_layout_t l;
  int r;

  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  r = cmount->get_client()->describe_layout(path, &l, cmount->default_perms);
  if (r < 0)
    return r;
  return l.pool_id;
}

extern "C" int ceph_get_file_pool_name(struct ceph_mount_info *cmount, int fh, char *buf, size_t len)
{
  file_layout_t l;
  int r;

  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  r = cmount->get_client()->fdescribe_layout(fh, &l);
  if (r < 0)
    return r;
  string name = cmount->get_client()->get_pool_name(l.pool_id);
  if (len == 0)
    return name.length();
  if (name.length() > len)
    return -CEPHFS_ERANGE;
  strncpy(buf, name.c_str(), len);
  return name.length();
}

extern "C" int ceph_get_pool_name(struct ceph_mount_info *cmount, int pool, char *buf, size_t len)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  string name = cmount->get_client()->get_pool_name(pool);
  if (len == 0)
    return name.length();
  if (name.length() > len)
    return -CEPHFS_ERANGE;
  strncpy(buf, name.c_str(), len);
  return name.length();
}

extern "C" int ceph_get_path_pool_name(struct ceph_mount_info *cmount, const char *path, char *buf, size_t len)
{
  file_layout_t l;
  int r;

  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  r = cmount->get_client()->describe_layout(path, &l, cmount->default_perms);
  if (r < 0)
    return r;
  string name = cmount->get_client()->get_pool_name(l.pool_id);
  if (len == 0)
    return name.length();
  if (name.length() > len)
    return -CEPHFS_ERANGE;
  strncpy(buf, name.c_str(), len);
  return name.length();
}

extern "C" int ceph_get_default_data_pool_name(struct ceph_mount_info *cmount, char *buf, size_t len)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  int64_t pool_id = cmount->get_client()->get_default_pool_id();
 
  string name = cmount->get_client()->get_pool_name(pool_id);
  if (len == 0)
    return name.length();
  if (name.length() > len)
    return -CEPHFS_ERANGE;
  strncpy(buf, name.c_str(), len);
  return name.length(); 
}

extern "C" int ceph_get_file_layout(struct ceph_mount_info *cmount, int fh, int *stripe_unit, int *stripe_count, int *object_size, int *pg_pool)
{
  file_layout_t l;
  int r;

  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  r = cmount->get_client()->fdescribe_layout(fh, &l);
  if (r < 0)
    return r;
  if (stripe_unit)
    *stripe_unit = l.stripe_unit;
  if (stripe_count)
    *stripe_count = l.stripe_count;
  if (object_size)
    *object_size = l.object_size;
  if (pg_pool)
    *pg_pool = l.pool_id;
  return 0;
}

extern "C" int ceph_get_path_layout(struct ceph_mount_info *cmount, const char *path, int *stripe_unit, int *stripe_count, int *object_size, int *pg_pool)
{
  file_layout_t l;
  int r;

  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  r = cmount->get_client()->describe_layout(path, &l, cmount->default_perms);
  if (r < 0)
    return r;
  if (stripe_unit)
    *stripe_unit = l.stripe_unit;
  if (stripe_count)
    *stripe_count = l.stripe_count;
  if (object_size)
    *object_size = l.object_size;
  if (pg_pool)
    *pg_pool = l.pool_id;
  return 0;
}

extern "C" int ceph_get_file_replication(struct ceph_mount_info *cmount, int fh)
{
  file_layout_t l;
  int r;

  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  r = cmount->get_client()->fdescribe_layout(fh, &l);
  if (r < 0)
    return r;
  int rep = cmount->get_client()->get_pool_replication(l.pool_id);
  return rep;
}

extern "C" int ceph_get_path_replication(struct ceph_mount_info *cmount, const char *path)
{
  file_layout_t l;
  int r;

  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  r = cmount->get_client()->describe_layout(path, &l, cmount->default_perms);
  if (r < 0)
    return r;
  int rep = cmount->get_client()->get_pool_replication(l.pool_id);
  return rep;
}

extern "C" int ceph_set_default_file_stripe_unit(struct ceph_mount_info *cmount,
						 int stripe)
{
  // this option no longer exists
  return -CEPHFS_EOPNOTSUPP;
}

extern "C" int ceph_set_default_file_stripe_count(struct ceph_mount_info *cmount,
						  int count)
{
  // this option no longer exists
  return -CEPHFS_EOPNOTSUPP;
}

extern "C" int ceph_set_default_object_size(struct ceph_mount_info *cmount, int size)
{
  // this option no longer exists
  return -CEPHFS_EOPNOTSUPP;
}

extern "C" int ceph_set_default_file_replication(struct ceph_mount_info *cmount,
						 int replication)
{
  // this option no longer exists
  return -CEPHFS_EOPNOTSUPP;
}

extern "C" int ceph_set_default_preferred_pg(struct ceph_mount_info *cmount, int osd)
{
  // this option no longer exists
  return -CEPHFS_EOPNOTSUPP;
}

extern "C" int ceph_get_file_extent_osds(struct ceph_mount_info *cmount, int fh,
    int64_t offset, int64_t *length, int *osds, int nosds)
{
  if (nosds < 0)
    return -CEPHFS_EINVAL;

  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;

  vector<int> vosds;
  int ret = cmount->get_client()->get_file_extent_osds(fh, offset, length, vosds);
  if (ret < 0)
    return ret;

  if (!nosds)
    return vosds.size();

  if ((int)vosds.size() > nosds)
    return -CEPHFS_ERANGE;

  for (int i = 0; i < (int)vosds.size(); i++)
    osds[i] = vosds[i];

  return vosds.size();
}

extern "C" int ceph_get_osd_crush_location(struct ceph_mount_info *cmount,
    int osd, char *path, size_t len)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;

  if (!path && len)
    return -CEPHFS_EINVAL;

  vector<pair<string, string> > loc;
  int ret = cmount->get_client()->get_osd_crush_location(osd, loc);
  if (ret)
    return ret;

  size_t needed = 0;
  size_t cur = 0;
  vector<pair<string, string> >::iterator it;
  for (it = loc.begin(); it != loc.end(); ++it) {
    string& type = it->first;
    string& name = it->second;
    needed += type.size() + name.size() + 2;
    if (needed <= len) {
      if (path)
	strcpy(path + cur, type.c_str());
      cur += type.size() + 1;
      if (path)
	strcpy(path + cur, name.c_str());
      cur += name.size() + 1;
    }
  }

  if (len == 0)
    return needed;

  if (needed > len)
    return -CEPHFS_ERANGE;

  return needed;
}

extern "C" int ceph_get_osd_addr(struct ceph_mount_info *cmount, int osd,
    struct sockaddr_storage *addr)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;

  if (!addr)
    return -CEPHFS_EINVAL;

  entity_addr_t address;
  int ret = cmount->get_client()->get_osd_addr(osd, address);
  if (ret < 0)
    return ret;

  *addr = address.get_sockaddr_storage();

  return 0;
}

extern "C" int ceph_get_file_stripe_address(struct ceph_mount_info *cmount, int fh,
					    int64_t offset, struct sockaddr_storage *addr, int naddr)
{
  vector<entity_addr_t> address;
  unsigned i;
  int r;

  if (naddr < 0)
    return -CEPHFS_EINVAL;

  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;

  r = cmount->get_client()->get_file_stripe_address(fh, offset, address);
  if (r < 0)
    return r;

  for (i = 0; i < (unsigned)naddr && i < address.size(); i++)
    addr[i] = address[i].get_sockaddr_storage();

  /* naddr == 0: drop through and return actual size */
  if (naddr && (address.size() > (unsigned)naddr))
    return -CEPHFS_ERANGE;

  return address.size();
}

extern "C" int ceph_localize_reads(struct ceph_mount_info *cmount, int val)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  if (!val)
    cmount->get_client()->clear_filer_flags(CEPH_OSD_FLAG_LOCALIZE_READS);
  else
    cmount->get_client()->set_filer_flags(CEPH_OSD_FLAG_LOCALIZE_READS);
  return 0;
}

extern "C" CephContext *ceph_get_mount_context(struct ceph_mount_info *cmount)
{
  return cmount->get_ceph_context();
}

extern "C" int ceph_debug_get_fd_caps(struct ceph_mount_info *cmount, int fd)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->get_caps_issued(fd);
}

extern "C" int ceph_debug_get_file_caps(struct ceph_mount_info *cmount, const char *path)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->get_caps_issued(path, cmount->default_perms);
}

extern "C" int ceph_get_stripe_unit_granularity(struct ceph_mount_info *cmount)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return CEPH_MIN_STRIPE_UNIT;
}

extern "C" int ceph_get_pool_id(struct ceph_mount_info *cmount, const char *pool_name)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;

  if (!pool_name || !pool_name[0])
    return -CEPHFS_EINVAL;

  /* negative range reserved for errors */
  int64_t pool_id = cmount->get_client()->get_pool_id(pool_name);
  if (pool_id > 0x7fffffff)
    return -CEPHFS_ERANGE;

  /* get_pool_id error codes fit in int */
  return (int)pool_id;
}

extern "C" int ceph_get_pool_replication(struct ceph_mount_info *cmount,
					 int pool_id)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->get_pool_replication(pool_id);
}
/* Low-level exports */

extern "C" int ceph_ll_lookup_root(struct ceph_mount_info *cmount,
                  Inode **parent)
{
  *parent = cmount->get_client()->get_root();
  if (*parent)
    return 0;
  return -CEPHFS_EFAULT;
}

extern "C" struct Inode *ceph_ll_get_inode(class ceph_mount_info *cmount,
					   vinodeno_t vino)
{
  return (cmount->get_client())->ll_get_inode(vino);
}


extern "C" int ceph_ll_lookup_vino(
    struct ceph_mount_info *cmount,
    vinodeno_t vino,
    Inode **inode)
{
  return (cmount->get_client())->ll_lookup_vino(vino, cmount->default_perms, inode);
}

/**
 * Populates the client cache with the requested inode, and its
 * parent dentry.
 */
extern "C" int ceph_ll_lookup_inode(
    struct ceph_mount_info *cmount,
    struct inodeno_t ino,
    Inode **inode)
{
  return (cmount->get_client())->ll_lookup_inode(ino, cmount->default_perms, inode);
}

extern "C" int ceph_ll_lookup(struct ceph_mount_info *cmount,
			      Inode *parent, const char *name, Inode **out,
			      struct ceph_statx *stx, unsigned want,
			      unsigned flags, const UserPerm *perms)
{
  if (flags & ~CEPH_REQ_FLAG_MASK)
    return -CEPHFS_EINVAL;
  return (cmount->get_client())->ll_lookupx(parent, name, out, stx, want,
					    flags, *perms);
}

extern "C" int ceph_ll_put(class ceph_mount_info *cmount, Inode *in)
{
  return (cmount->get_client()->ll_put(in));
}

extern "C" int ceph_ll_forget(class ceph_mount_info *cmount, Inode *in,
			      int count)
{
  return (cmount->get_client()->ll_forget(in, count));
}

extern "C" int ceph_ll_walk(struct ceph_mount_info *cmount, const char* name, Inode **i,
		 struct ceph_statx *stx, unsigned int want, unsigned int flags,
		 const UserPerm *perms)
{
  if (flags & ~CEPH_REQ_FLAG_MASK)
    return -CEPHFS_EINVAL;
  return(cmount->get_client()->ll_walk(name, i, stx, want, flags, *perms));
}

extern "C" int ceph_ll_getattr(class ceph_mount_info *cmount,
			       Inode *in, struct ceph_statx *stx,
			       unsigned int want, unsigned int flags,
			       const UserPerm *perms)
{
  if (flags & ~CEPH_REQ_FLAG_MASK)
    return -CEPHFS_EINVAL;
  return (cmount->get_client()->ll_getattrx(in, stx, want, flags, *perms));
}

extern "C" int ceph_ll_setattr(class ceph_mount_info *cmount,
			       Inode *in, struct ceph_statx *stx,
			       int mask, const UserPerm *perms)
{
  return (cmount->get_client()->ll_setattrx(in, stx, mask, *perms));
}

extern "C" int ceph_ll_open(class ceph_mount_info *cmount, Inode *in,
			    int flags, Fh **fh, const UserPerm *perms)
{
  return (cmount->get_client()->ll_open(in, flags, fh, *perms));
}

extern "C" int ceph_ll_read(class ceph_mount_info *cmount, Fh* filehandle,
			    int64_t off, uint64_t len, char* buf)
{
  bufferlist bl;
  int r = 0;

  r = cmount->get_client()->ll_read(filehandle, off, len, &bl);
  if (r >= 0)
    {
      bl.begin().copy(bl.length(), buf);
      r = bl.length();
    }
  return r;
}

extern "C" int ceph_ll_read_block(class ceph_mount_info *cmount,
				  Inode *in, uint64_t blockid,
				  char* buf, uint64_t offset,
				  uint64_t length,
				  struct ceph_file_layout* layout)
{
  file_layout_t l;
  int r = (cmount->get_client()->ll_read_block(in, blockid, buf, offset,
					       length, &l));
  l.to_legacy(layout);
  return r;
}

extern "C" int ceph_ll_write_block(class ceph_mount_info *cmount,
				   Inode *in, uint64_t blockid,
				   char *buf, uint64_t offset,
				   uint64_t length,
				   struct ceph_file_layout *layout,
				   uint64_t snapseq, uint32_t sync)
{
  file_layout_t l;
  int r = (cmount->get_client()->ll_write_block(in, blockid, buf, offset,
						length, &l, snapseq, sync));
  l.to_legacy(layout);
  return r;
}

extern "C" int ceph_ll_commit_blocks(class ceph_mount_info *cmount,
				     Inode *in, uint64_t offset,
				     uint64_t range)
{
  return (cmount->get_client()->ll_commit_blocks(in, offset, range));
}

extern "C" int ceph_ll_fsync(class ceph_mount_info *cmount,
			     Fh *fh, int syncdataonly)
{
  return (cmount->get_client()->ll_fsync(fh, syncdataonly));
}

extern "C" int ceph_ll_sync_inode(class ceph_mount_info *cmount,
			     Inode *in, int syncdataonly)
{
  return (cmount->get_client()->ll_sync_inode(in, syncdataonly));
}

extern "C" int ceph_ll_fallocate(class ceph_mount_info *cmount, Fh *fh,
				 int mode, int64_t offset, int64_t length)
{
  return cmount->get_client()->ll_fallocate(fh, mode, offset, length);
}

extern "C" off_t ceph_ll_lseek(class ceph_mount_info *cmount,
				Fh *fh, off_t offset, int whence)
{
  return (cmount->get_client()->ll_lseek(fh, offset, whence));
}

extern "C" int ceph_ll_write(class ceph_mount_info *cmount,
			     Fh *fh, int64_t off, uint64_t len,
			     const char *data)
{
  return (cmount->get_client()->ll_write(fh, off, len, data));
}

extern "C" int64_t ceph_ll_readv(class ceph_mount_info *cmount,
				 struct Fh *fh, const struct iovec *iov,
				 int iovcnt, int64_t off)
{
  return (cmount->get_client()->ll_readv(fh, iov, iovcnt, off));
}

extern "C" int64_t ceph_ll_writev(class ceph_mount_info *cmount,
				  struct Fh *fh, const struct iovec *iov,
				  int iovcnt, int64_t off)
{
  return (cmount->get_client()->ll_writev(fh, iov, iovcnt, off));
}

class LL_Onfinish : public Context {
public:
  LL_Onfinish(struct ceph_ll_io_info *io_info)
    : io_info(io_info) {}
  bufferlist bl;
private:
  struct ceph_ll_io_info *io_info;
  void finish(int r) override {
    if (!io_info->write && r > 0) {
      copy_bufferlist_to_iovec(io_info->iov, io_info->iovcnt, &bl, r);
    }
    io_info->result = r;
    io_info->callback(io_info);
  }
};

extern "C" int64_t ceph_ll_nonblocking_readv_writev(class ceph_mount_info *cmount,
						    struct ceph_ll_io_info *io_info)
{
  LL_Onfinish *onfinish = new LL_Onfinish(io_info);

  return (cmount->get_client()->ll_preadv_pwritev(
			io_info->fh, io_info->iov, io_info->iovcnt,
			io_info->off, io_info->write, onfinish, &onfinish->bl,
			io_info->fsync, io_info->syncdataonly));
}

extern "C" int ceph_ll_close(class ceph_mount_info *cmount, Fh* fh)
{
  return (cmount->get_client()->ll_release(fh));
}

extern "C" int ceph_ll_create(class ceph_mount_info *cmount,
			      Inode *parent, const char *name, mode_t mode,
			      int oflags, Inode **outp, Fh **fhp,
			      struct ceph_statx *stx, unsigned want,
			      unsigned lflags, const UserPerm *perms)
{
  if (lflags & ~CEPH_REQ_FLAG_MASK)
    return -CEPHFS_EINVAL;
  return (cmount->get_client())->ll_createx(parent, name, mode, oflags, outp,
					    fhp, stx, want, lflags, *perms);
}

extern "C" int ceph_ll_mknod(class ceph_mount_info *cmount, Inode *parent,
			     const char *name, mode_t mode, dev_t rdev,
			     Inode **out, struct ceph_statx *stx,
			     unsigned want, unsigned flags,
			     const UserPerm *perms)
{
  if (flags & ~CEPH_REQ_FLAG_MASK)
    return -CEPHFS_EINVAL;
  return (cmount->get_client())->ll_mknodx(parent, name, mode, rdev,
					   out, stx, want, flags, *perms);
}

extern "C" int ceph_ll_mkdir(class ceph_mount_info *cmount, Inode *parent,
			     const char *name, mode_t mode, Inode **out,
			     struct ceph_statx *stx, unsigned want,
			     unsigned flags, const UserPerm *perms)
{
  if (flags & ~CEPH_REQ_FLAG_MASK)
    return -CEPHFS_EINVAL;
  return cmount->get_client()->ll_mkdirx(parent, name, mode, out, stx, want,
					 flags, *perms);
}

extern "C" int ceph_ll_link(class ceph_mount_info *cmount,
			    Inode *in, Inode *newparent,
			    const char *name, const UserPerm *perms)
{
  return cmount->get_client()->ll_link(in, newparent, name, *perms);
}

extern "C" int ceph_ll_opendir(class ceph_mount_info *cmount,
			       Inode *in,
			       struct ceph_dir_result **dirpp,
			       const UserPerm *perms)
{
  return (cmount->get_client()->ll_opendir(in, O_RDONLY, (dir_result_t**) dirpp,
					   *perms));
}

extern "C" int ceph_ll_releasedir(class ceph_mount_info *cmount,
				  ceph_dir_result *dir)
{
  return cmount->get_client()->ll_releasedir(reinterpret_cast<dir_result_t*>(dir));
}

extern "C" int ceph_ll_rename(class ceph_mount_info *cmount,
			      Inode *parent, const char *name,
			      Inode *newparent, const char *newname,
			      const UserPerm *perms)
{
  return cmount->get_client()->ll_rename(parent, name, newparent,
					 newname, *perms);
}

extern "C" int ceph_ll_unlink(class ceph_mount_info *cmount, Inode *in,
			      const char *name, const UserPerm *perms)
{
  return cmount->get_client()->ll_unlink(in, name, *perms);
}

extern "C" int ceph_ll_statfs(class ceph_mount_info *cmount,
			      Inode *in, struct statvfs *stbuf)
{
  return (cmount->get_client()->ll_statfs(in, stbuf, cmount->default_perms));
}

extern "C" int ceph_ll_readlink(class ceph_mount_info *cmount, Inode *in,
				char *buf, size_t bufsiz,
				const UserPerm *perms)
{
  return cmount->get_client()->ll_readlink(in, buf, bufsiz, *perms);
}

extern "C" int ceph_ll_symlink(class ceph_mount_info *cmount,
			       Inode *in, const char *name,
			       const char *value, Inode **out,
			       struct ceph_statx *stx, unsigned want,
			       unsigned flags, const UserPerm *perms)
{
  if (flags & ~CEPH_REQ_FLAG_MASK)
    return -CEPHFS_EINVAL;
  return (cmount->get_client()->ll_symlinkx(in, name, value, out, stx, want,
					    flags, *perms));
}

extern "C" int ceph_ll_rmdir(class ceph_mount_info *cmount,
			     Inode *in, const char *name,
			     const UserPerm *perms)
{
  return cmount->get_client()->ll_rmdir(in, name, *perms);
}

extern "C" int ceph_ll_getxattr(class ceph_mount_info *cmount,
				Inode *in, const char *name, void *value,
				size_t size, const UserPerm *perms)
{
  return (cmount->get_client()->ll_getxattr(in, name, value, size, *perms));
}

extern "C" int ceph_ll_listxattr(struct ceph_mount_info *cmount,
                              Inode *in, char *list,
                              size_t buf_size, size_t *list_size,
			      const UserPerm *perms)
{
  int res = cmount->get_client()->ll_listxattr(in, list, buf_size, *perms);
  if (res >= 0) {
    *list_size = (size_t)res;
    return 0;
  }
  return res;
}

extern "C" int ceph_ll_setxattr(class ceph_mount_info *cmount,
				Inode *in, const char *name,
				const void *value, size_t size,
				int flags, const UserPerm *perms)
{
  return (cmount->get_client()->ll_setxattr(in, name, value, size, flags, *perms));
}

extern "C" int ceph_ll_removexattr(class ceph_mount_info *cmount,
				   Inode *in, const char *name,
				   const UserPerm *perms)
{
  return (cmount->get_client()->ll_removexattr(in, name, *perms));
}

extern "C" int ceph_ll_getlk(struct ceph_mount_info *cmount,
			     Fh *fh, struct flock *fl, uint64_t owner)
{
  return (cmount->get_client()->ll_getlk(fh, fl, owner));
}

extern "C" int ceph_ll_setlk(struct ceph_mount_info *cmount,
			     Fh *fh, struct flock *fl, uint64_t owner,
			     int sleep)
{
  return (cmount->get_client()->ll_setlk(fh, fl, owner, sleep));
}

extern "C" int ceph_ll_lazyio(class ceph_mount_info *cmount,
			      Fh *fh, int enable)
{
  return (cmount->get_client()->ll_lazyio(fh, enable));
}

extern "C" int ceph_ll_delegation(struct ceph_mount_info *cmount, Fh *fh,
				  unsigned cmd, ceph_deleg_cb_t cb, void *priv)
{
  return (cmount->get_client()->ll_delegation(fh, cmd, cb, priv));
}

extern "C" uint32_t ceph_ll_stripe_unit(class ceph_mount_info *cmount,
					Inode *in)
{
  return (cmount->get_client()->ll_stripe_unit(in));
}

extern "C" uint32_t ceph_ll_file_layout(class ceph_mount_info *cmount,
					Inode *in,
					struct ceph_file_layout *layout)
{
  file_layout_t l;
  int r = (cmount->get_client()->ll_file_layout(in, &l));
  l.to_legacy(layout);
  return r;
}

uint64_t ceph_ll_snap_seq(class ceph_mount_info *cmount, Inode *in)
{
  return (cmount->get_client()->ll_snap_seq(in));
}

extern "C" int ceph_ll_get_stripe_osd(class ceph_mount_info *cmount,
				      Inode *in, uint64_t blockno,
				      struct ceph_file_layout* layout)
{
  file_layout_t l;
  int r = (cmount->get_client()->ll_get_stripe_osd(in, blockno, &l));
  l.to_legacy(layout);
  return r;
}

extern "C" int ceph_ll_num_osds(class ceph_mount_info *cmount)
{
  return (cmount->get_client()->ll_num_osds());
}

extern "C" int ceph_ll_osdaddr(class ceph_mount_info *cmount,
			       int osd, uint32_t *addr)
{
  return (cmount->get_client()->ll_osdaddr(osd, addr));
}

extern "C" uint64_t ceph_ll_get_internal_offset(class ceph_mount_info *cmount,
						Inode *in,
						uint64_t blockno)
{
  return (cmount->get_client()->ll_get_internal_offset(in, blockno));
}

extern "C" void ceph_buffer_free(char *buf)
{
  if (buf) {
    free(buf);
  }
}

extern "C" uint32_t ceph_get_cap_return_timeout(class ceph_mount_info *cmount)
{
  if (!cmount->is_mounted())
    return 0;
  return cmount->get_client()->mdsmap->get_session_autoclose().sec();
}

extern "C" int ceph_set_deleg_timeout(class ceph_mount_info *cmount, uint32_t timeout)
{
  if (!cmount->is_mounted())
    return -CEPHFS_ENOTCONN;
  return cmount->get_client()->set_deleg_timeout(timeout);
}

extern "C" void ceph_set_session_timeout(class ceph_mount_info *cmount, unsigned timeout)
{
  cmount->get_client()->set_session_timeout(timeout);
}

extern "C" void ceph_set_uuid(class ceph_mount_info *cmount, const char *uuid)
{
  cmount->get_client()->set_uuid(std::string(uuid));
}

extern "C" int ceph_start_reclaim(class ceph_mount_info *cmount,
				  const char *uuid, unsigned flags)
{
  if (!cmount->is_initialized()) {
    int ret = cmount->init();
    if (ret != 0)
      return ret;
  }
  return cmount->get_client()->start_reclaim(std::string(uuid), flags,
					     cmount->get_filesystem());
}

extern "C" void ceph_finish_reclaim(class ceph_mount_info *cmount)
{
  cmount->get_client()->finish_reclaim();
}

// This is deprecated, use ceph_ll_register_callbacks2 instead.
extern "C" void ceph_ll_register_callbacks(class ceph_mount_info *cmount,
					   struct ceph_client_callback_args *args)
{
  cmount->get_client()->ll_register_callbacks(args);
}

extern "C" int ceph_ll_register_callbacks2(class ceph_mount_info *cmount,
					    struct ceph_client_callback_args *args)
{
  return cmount->get_client()->ll_register_callbacks2(args);
}


extern "C" int ceph_get_snap_info(struct ceph_mount_info *cmount,
                                  const char *path, struct snap_info *snap_info) {
  Client::SnapInfo info;
  int r = cmount->get_client()->get_snap_info(path, cmount->default_perms, &info);
  if (r < 0) {
    return r;
  }

  size_t i = 0;
  auto nr_metadata = info.metadata.size();

  snap_info->id = info.id.val;
  snap_info->nr_snap_metadata = nr_metadata;
  if (nr_metadata) {
    snap_info->snap_metadata = (struct snap_metadata *)calloc(nr_metadata, sizeof(struct snap_metadata));
    if (!snap_info->snap_metadata) {
      return -CEPHFS_ENOMEM;
    }

    // fill with key, value pairs
    for (auto &[key, value] : info.metadata) {
      // len(key) + '\0' + len(value) + '\0'
      char *kvp = (char *)malloc(key.size() + value.size() + 2);
      if (!kvp) {
        break;
      }
      char *_key = kvp;
      char *_value = kvp + key.size() + 1;

      memcpy(_key, key.c_str(), key.size());
      _key[key.size()] = '\0';
      memcpy(_value, value.c_str(), value.size());
      _value[value.size()] = '\0';

      snap_info->snap_metadata[i].key = _key;
      snap_info->snap_metadata[i].value = _value;
      ++i;
    }
  }

  if (nr_metadata && i != nr_metadata) {
    ceph_free_snap_info_buffer(snap_info);
    return -CEPHFS_ENOMEM;
  }

  return 0;
}

extern "C" void ceph_free_snap_info_buffer(struct snap_info *snap_info) {
  for (size_t i = 0; i < snap_info->nr_snap_metadata; ++i) {
    free((void *)snap_info->snap_metadata[i].key); // malloc'd memory is key+value composite
  }
  free(snap_info->snap_metadata);
}
