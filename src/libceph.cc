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

#include "client/Client.h"
#include "include/ceph/libceph.h"
#include "common/Mutex.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/version.h"
#include "include/str_list.h"
#include "messages/MMonMap.h"
#include "msg/SimpleMessenger.h"

#include <fcntl.h>
#include <iostream>
#include <string.h>
#include <string>

static Mutex libceph_init_mutex("libceph_init_mutex");
static bool libceph_initialized = false; // FIXME! remove this
static int nonce_seed = 0;

class ceph_mount_info
{
public:
  ceph_mount_info(uint64_t msgr_nonce_, md_config_t *conf)
    : msgr_nonce(msgr_nonce_),
      mounted(false),
      client(NULL),
      monclient(NULL),
      messenger(NULL),
      conf(conf)
  {
  }

  ~ceph_mount_info()
  {
    try {
      shutdown();
    // uncomment once conf is de-globalized
//    if (conf) {
//      free(conf);
//      conf = NULL;
//    }
    }
    catch (const std::exception& e) {
      // we shouldn't get here, but if we do, we want to know about it.
      derr << "ceph_mount_info::~ceph_mount_info: caught exception: "
	   << e.what() << dendl;
    }
    catch (...) {
      // ignore
    }
  }

  int mount(const std::string &mount_root)
  {
    if (mounted)
      return -EDOM;

    //monmap
    monclient = new MonClient();
    if (monclient->build_initial_monmap() < 0) {
      shutdown();
      return -1000;
    }

    //network connection
    messenger = new SimpleMessenger();
    if (!messenger->register_entity(entity_name_t::CLIENT())) {
      messenger->destroy();
      messenger = NULL;
      shutdown();
      return -1001;
    }

    //at last the client
    client = new Client(messenger, monclient);
    if (!client) {
      shutdown();
      return -1002;
    }

    if (messenger->start_with_nonce(msgr_nonce) != 0) {
      shutdown();
      return -1003;
    }

    client->init();

    int ret = client->mount(mount_root);
    if (ret) {
      shutdown();
      return ret;
    }

    mounted = true;
    return 0;
  }

  void shutdown()
  {
    if (mounted) {
      client->unmount();
      mounted = false;
    }
    if (client) {
      client->shutdown();
      delete client;
      client = NULL;
    }
    if (messenger) {
      messenger->wait();
      messenger->destroy();
      messenger = NULL;
    }
    if (monclient) {
      delete monclient;
      monclient = NULL;
    }
  }

  int conf_read_file(const char *path)
  {
    if (!path)
      path = CEPH_CONF_FILE_DEFAULT;

    std::list<std::string> conf_files;
    get_str_list(path, conf_files);
    std::deque<std::string> parse_errors;
    int ret = conf->parse_config_files(conf_files, &parse_errors);
    if (ret)
      return ret;
    conf->parse_env(); // environment variables override

    conf->apply_changes();
    complain_about_parse_errors(&parse_errors);
    return 0;
  }

  void conf_parse_argv(int argc, const char **argv)
  {
    vector<const char*> args;
    argv_to_vec(argc, argv, args);
    conf->parse_argv(args);
    conf->apply_changes();
  }

  int conf_set(const char *option, const char *value)
  {
    int ret = conf->set_val(option, value);
    if (ret)
      return ret;
    conf->apply_changes();
    return 0;
  }

  int conf_get(const char *option, char *buf, size_t len)
  {
    char *tmp = buf;
    return conf->get_val(option, &tmp, len);
  }

  Client *get_client()
  {
    return client;
  }

  const char *get_cwd()
  {
    client->getcwd(cwd);
    return cwd.c_str();
  }

private:
  uint64_t msgr_nonce;
  bool mounted;
  Client *client;
  MonClient *monclient;
  SimpleMessenger *messenger;
  md_config_t *conf;
  std::string cwd;
};

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
  return VERSION;
}

static int ceph_create_with_config_impl(struct ceph_mount_info **cmount, md_config_t *conf)
{
  // should hold libceph_init_mutex here
  libceph_initialized = true;
  uint64_t nonce = (uint64_t)++nonce_seed * 1000000ull + (uint64_t)getpid();
  *cmount = new struct ceph_mount_info(nonce, conf);
  return 0;
}

extern "C" int ceph_create(struct ceph_mount_info **cmount, const char * const id)
{
  int ret;
  libceph_init_mutex.Lock();
  md_config_t *conf = &g_conf;
  if (!libceph_initialized) {
    CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT, CEPH_CONF_FILE_DEFAULT);
    iparams.conf_file = "";
    if (id) {
      iparams.name.set(CEPH_ENTITY_TYPE_CLIENT, id);
    }

    conf = common_preinit(iparams, CODE_ENVIRONMENT_LIBRARY, 0);
    conf->parse_env(); // environment variables override
    conf->apply_changes();
  }
  ret = ceph_create_with_config_impl(cmount, conf);
  libceph_init_mutex.Unlock();
  return ret;
}

extern "C" int ceph_create_with_config(struct ceph_mount_info **cmount, md_config_t *conf)
{
  int ret;
  libceph_init_mutex.Lock();
  ret = ceph_create_with_config_impl(cmount, conf);
  libceph_init_mutex.Unlock();
  return ret;
}

extern "C" void ceph_shutdown(struct ceph_mount_info *cmount)
{
  cmount->shutdown();
}

extern "C" int ceph_conf_read_file(struct ceph_mount_info *cmount, const char *path)
{
  return cmount->conf_read_file(path);
}

extern "C" void ceph_conf_parse_argv(struct ceph_mount_info *cmount, int argc,
				     const char **argv)
{
  cmount->conf_parse_argv(argc, argv);
}

extern "C" int ceph_conf_set(struct ceph_mount_info *cmount, const char *option,
			     const char *value)
{
  return cmount->conf_set(option, value);
}

extern "C" int ceph_conf_get(struct ceph_mount_info *cmount, const char *option,
			     char *buf, size_t len)
{
  return cmount->conf_get(option, buf, len);
}

extern "C" int ceph_mount(struct ceph_mount_info *cmount, const char *root)
{
  std::string mount_root;

  keyring_init(&g_conf);

  if (root)
    mount_root = root;
  return cmount->mount(mount_root);
}

extern "C" int ceph_statfs(struct ceph_mount_info *cmount, const char *path,
			   struct statvfs *stbuf)
{
  return cmount->get_client()->statfs(path, stbuf);
}

extern "C" int ceph_get_local_osd(struct ceph_mount_info *cmount)
{
  return cmount->get_client()->get_local_osd();
}

extern "C" const char* ceph_getcwd(struct ceph_mount_info *cmount)
{
  return cmount->get_cwd();
}

extern "C" int ceph_chdir (struct ceph_mount_info *cmount, const char *s)
{
  return cmount->get_client()->chdir(s);
}

extern "C" int ceph_opendir(struct ceph_mount_info *cmount,
			    const char *name, struct ceph_dir_result **dirpp)
{
  return cmount->get_client()->opendir(name, (dir_result_t **)dirpp);
}

extern "C" int ceph_closedir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp)
{
  return cmount->get_client()->closedir((dir_result_t*)dirp);
}

extern "C" struct dirent * ceph_readdir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp)
{
  return cmount->get_client()->readdir((dir_result_t*)dirp);
}

extern "C" int ceph_readdir_r(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp, struct dirent *de)
{
  return cmount->get_client()->readdir_r((dir_result_t*)dirp, de);
}

extern "C" int ceph_readdirplus_r(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp,
				  struct dirent *de, struct stat *st, int *stmask)
{
  return cmount->get_client()->readdirplus_r((dir_result_t*)dirp, de, st, stmask);
}

extern "C" int ceph_getdents(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp,
			     char *buf, int buflen)
{
  return cmount->get_client()->getdents((dir_result_t*)dirp, buf, buflen);
}

extern "C" int ceph_getdnames(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp,
			      char *buf, int buflen)
{
  return cmount->get_client()->getdnames((dir_result_t*)dirp, buf, buflen);
}

extern "C" void ceph_rewinddir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp)
{
  cmount->get_client()->rewinddir((dir_result_t*)dirp);
}

extern "C" loff_t ceph_telldir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp)
{
  return cmount->get_client()->telldir((dir_result_t*)dirp);
}

extern "C" void ceph_seekdir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp, loff_t offset)
{
  cmount->get_client()->seekdir((dir_result_t*)dirp, offset);
}

extern "C" int ceph_link (struct ceph_mount_info *cmount, const char *existing,
			  const char *newname)
{
  return cmount->get_client()->link(existing, newname);
}

extern "C" int ceph_unlink(struct ceph_mount_info *cmount, const char *path)
{
  return cmount->get_client()->unlink(path);
}

extern "C" int ceph_rename(struct ceph_mount_info *cmount, const char *from,
			   const char *to)
{
  return cmount->get_client()->rename(from, to);
}

// dirs
extern "C" int ceph_mkdir(struct ceph_mount_info *cmount, const char *path, mode_t mode)
{
  return cmount->get_client()->mkdir(path, mode);
}

extern "C" int ceph_mkdirs(struct ceph_mount_info *cmount, const char *path, mode_t mode)
{
  return cmount->get_client()->mkdirs(path, mode);
}

extern "C" int ceph_rmdir(struct ceph_mount_info *cmount, const char *path)
{
  return cmount->get_client()->rmdir(path);
}

// symlinks
extern "C" int ceph_readlink(struct ceph_mount_info *cmount, const char *path,
			     char *buf, loff_t size)
{
  return cmount->get_client()->readlink(path, buf, size);
}

extern "C" int ceph_symlink(struct ceph_mount_info *cmount, const char *existing,
			    const char *newname)
{
  return cmount->get_client()->symlink(existing, newname);
}

// inode stuff
extern "C" int ceph_lstat(struct ceph_mount_info *cmount, const char *path,
			  struct stat *stbuf)
{
  return cmount->get_client()->lstat(path, stbuf);
}

extern "C" int ceph_setattr(struct ceph_mount_info *cmount, const char *relpath,
			    struct stat *attr, int mask)
{
  return cmount->get_client()->setattr(relpath, attr, mask);
}

// *xattr() calls supporting samba/vfs
extern "C" int ceph_getxattr(struct ceph_mount_info *cmount, const char *path, const char *name, void *value, size_t size)
{
  return cmount->get_client()->getxattr(path, name, value, size);
}

extern "C" int ceph_lgetxattr(struct ceph_mount_info *cmount, const char *path, const char *name, void *value, size_t size)
{
  return cmount->get_client()->lgetxattr(path, name, value, size);
}

extern "C" int ceph_listxattr(struct ceph_mount_info *cmount, const char *path, char *list, size_t size)
{
  return cmount->get_client()->listxattr(path, list, size);
}

extern "C" int ceph_llistxattr(struct ceph_mount_info *cmount, const char *path, char *list, size_t size)
{
  return cmount->get_client()->llistxattr(path, list, size);
}

extern "C" int ceph_removexattr(struct ceph_mount_info *cmount, const char *path, const char *name)
{
  return cmount->get_client()->removexattr(path, name);
}

extern "C" int ceph_lremovexattr(struct ceph_mount_info *cmount, const char *path, const char *name)
{
  return cmount->get_client()->lremovexattr(path, name);
}

extern "C" int ceph_setxattr(struct ceph_mount_info *cmount, const char *path, const char *name, const void *value, size_t size, int flags)
{
  return cmount->get_client()->setxattr(path, name, value, size, flags);
}

extern "C" int ceph_lsetxattr(struct ceph_mount_info *cmount, const char *path, const char *name, const void *value, size_t size, int flags)
{
  return cmount->get_client()->lsetxattr(path, name, value, size, flags);
}
/* end xattr support */

extern "C" int ceph_chmod(struct ceph_mount_info *cmount, const char *path, mode_t mode)
{
  return cmount->get_client()->chmod(path, mode);
}
extern "C" int ceph_chown(struct ceph_mount_info *cmount, const char *path,
			  uid_t uid, gid_t gid)
{
  return cmount->get_client()->chown(path, uid, gid);
}

extern "C" int ceph_utime(struct ceph_mount_info *cmount, const char *path,
			  struct utimbuf *buf)
{
  return cmount->get_client()->utime(path, buf);
}

extern "C" int ceph_truncate(struct ceph_mount_info *cmount, const char *path,
			     loff_t size)
{
  return cmount->get_client()->truncate(path, size);
}

// file ops
extern "C" int ceph_mknod(struct ceph_mount_info *cmount, const char *path,
			  mode_t mode, dev_t rdev)
{
  return cmount->get_client()->mknod(path, mode, rdev);
}

extern "C" int ceph_open(struct ceph_mount_info *cmount, const char *path,
			 int flags, mode_t mode)
{
  return cmount->get_client()->open(path, flags, mode);
}

extern "C" int ceph_close(struct ceph_mount_info *cmount, int fd)
{
  return cmount->get_client()->close(fd);
}

extern "C" loff_t ceph_lseek(struct ceph_mount_info *cmount, int fd,
			     loff_t offset, int whence)
{
  return cmount->get_client()->lseek(fd, offset, whence);
}

extern "C" int ceph_read(struct ceph_mount_info *cmount, int fd, char *buf,
			 loff_t size, loff_t offset)
{
  return cmount->get_client()->read(fd, buf, size, offset);
}

extern "C" int ceph_write(struct ceph_mount_info *cmount, int fd, const char *buf,
			  loff_t size, loff_t offset)
{
  return cmount->get_client()->write(fd, buf, size, offset);
}

extern "C" int ceph_ftruncate(struct ceph_mount_info *cmount, int fd, loff_t size)
{
  return cmount->get_client()->ftruncate(fd, size);
}

extern "C" int ceph_fsync(struct ceph_mount_info *cmount, int fd, int syncdataonly)
{
  return cmount->get_client()->fsync(fd, syncdataonly);
}

extern "C" int ceph_fstat(struct ceph_mount_info *cmount, int fd, struct stat *stbuf)
{
  return cmount->get_client()->fstat(fd, stbuf);
}

extern "C" int ceph_sync_fs(struct ceph_mount_info *cmount)
{
  return cmount->get_client()->sync_fs();
}


extern "C" int ceph_get_file_stripe_unit(struct ceph_mount_info *cmount, int fh)
{
  struct ceph_file_layout l;
  int r = cmount->get_client()->describe_layout(fh, &l);
  if (r < 0)
    return r;
  return l.fl_stripe_unit;
}

extern "C" int ceph_get_file_pool(struct ceph_mount_info *cmount, int fh)
{
  struct ceph_file_layout l;
  int r = cmount->get_client()->describe_layout(fh, &l);
  if (r < 0)
    return r;
  return l.fl_pg_pool;
}

extern "C" int ceph_get_file_replication(struct ceph_mount_info *cmount, int fh)
{
  struct ceph_file_layout l;
  int r = cmount->get_client()->describe_layout(fh, &l);
  if (r < 0)
    return r;
  int rep = cmount->get_client()->get_pool_replication(l.fl_pg_pool);
  return rep;
}

extern "C" int ceph_set_default_file_stripe_unit(struct ceph_mount_info *cmount,
						 int stripe)
{
  cmount->get_client()->set_default_file_stripe_unit(stripe);
  return 0;
}

extern "C" int ceph_set_default_file_stripe_count(struct ceph_mount_info *cmount,
						  int count)
{
  cmount->get_client()->set_default_file_stripe_unit(count);
  return 0;
}

extern "C" int ceph_set_default_object_size(struct ceph_mount_info *cmount, int size)
{
  cmount->get_client()->set_default_object_size(size);
  return 0;
}

extern "C" int ceph_set_default_file_replication(struct ceph_mount_info *cmount,
						 int replication)
{
  cmount->get_client()->set_default_file_replication(replication);
  return 0;
}

extern "C" int ceph_set_default_preferred_pg(struct ceph_mount_info *cmount, int osd)
{
  cmount->get_client()->set_default_preferred_pg(osd);
  return 0;
}

extern "C" int ceph_get_file_stripe_address(struct ceph_mount_info *cmount, int fh,
					    loff_t offset, char *buf, int buflen)
{
  string address;
  int r = cmount->get_client()->get_file_stripe_address(fh, offset, address);
  if (r < 0)
    return r; 
  int len = address.size()+1;
  if (len > buflen) {
    if (buflen == 0)
      return len;
    return -ERANGE;
  }
  len = address.copy(buf, len, 0);
  buf[len] = '\0'; // write a null char to terminate c-style string
  return 0;
}

extern "C" int ceph_localize_reads(struct ceph_mount_info *cmount, int val)
{
  if (!val)
    cmount->get_client()->clear_filer_flags(CEPH_OSD_FLAG_LOCALIZE_READS);
  else
    cmount->get_client()->set_filer_flags(CEPH_OSD_FLAG_LOCALIZE_READS);
  return 0;
}
