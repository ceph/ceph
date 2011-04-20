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
#include "client/libceph.h"
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

class ceph_cluster_t
{
public:
  ceph_cluster_t(uint64_t msgr_nonce_, md_config_t *conf)
    : msgr_nonce(msgr_nonce_),
      mounted(false),
      client(NULL),
      monclient(NULL),
      messenger(NULL),
      conf(conf)
  {
  }

  ~ceph_cluster_t()
  {
    try {
      disconnect();
    }
    catch (const std::exception& e) {
      // we shouldn't get here, but if we do, we want to know about it.
      derr << "ceph_cluster_t::~ceph_cluster_t: caught exception: "
	   << e.what() << dendl;
    }
    catch (...) {
      // ignore
    }
  }

  int connect()
  {
    //monmap
    monclient = new MonClient();
    if (monclient->build_initial_monmap() < 0) {
      delete monclient;
      monclient = NULL;
      return -1000;
    }

    //network connection
    messenger = new SimpleMessenger();
    messenger->register_entity(entity_name_t::CLIENT());

    //at last the client
    client = new Client(messenger, monclient);

    messenger->start(false, msgr_nonce); // do not daemonize

    client->init();
    return 0;
  }

  void disconnect()
  {
    if (mounted) {
      client->unmount();
      mounted = false;
    }
    if (client) {
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
    // uncomment once conf is de-globalized
//    if (conf) {
//      free(conf);
//      conf = NULL;
//    }
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

  int mount(const std::string &mount_root)
  {
    if (mounted)
      return -EINVAL;
     int ret = client->mount(mount_root);
     if (ret)
       return ret;
    mounted = true;
    return 0;
  }

  void umount()
  {
    if (!mounted)
      return;
    client->unmount();
    mounted = false;
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

static int ceph_create_with_config_impl(ceph_cluster_t **cluster, md_config_t *conf)
{
  // should hold libceph_init_mutex here
  libceph_initialized = true;
  uint64_t nonce = (uint64_t)++nonce_seed * 1000000ull + (uint64_t)getpid();
  *cluster = new ceph_cluster_t(nonce, conf);
  return 0;
}

extern "C" int ceph_create(ceph_cluster_t **cluster, const char * const id)
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
  ret = ceph_create_with_config_impl(cluster, conf);
  libceph_init_mutex.Unlock();
  return ret;
}

extern "C" int ceph_create_with_config(ceph_cluster_t **cluster, md_config_t *conf)
{
  int ret;
  libceph_init_mutex.Lock();
  ret = ceph_create_with_config_impl(cluster, conf);
  libceph_init_mutex.Unlock();
  return ret;
}

extern "C" int ceph_connect(ceph_cluster_t *cluster)
{
  return cluster->connect();
}

extern "C" void ceph_shutdown(ceph_cluster_t *cluster)
{
  cluster->disconnect();
}

extern "C" int ceph_conf_read_file(ceph_cluster_t *cluster, const char *path)
{
  return cluster->conf_read_file(path);
}

extern "C" void ceph_conf_parse_argv(ceph_cluster_t *cluster, int argc,
				     const char **argv)
{
  cluster->conf_parse_argv(argc, argv);
}

extern "C" int ceph_conf_set(ceph_cluster_t *cluster, const char *option,
			     const char *value)
{
  return cluster->conf_set(option, value);
}

extern "C" int ceph_conf_get(ceph_cluster_t *cluster, const char *option,
			     char *buf, size_t len)
{
  return cluster->conf_get(option, buf, len);
}

extern "C" int ceph_mount(ceph_cluster_t *cluster, const char *root)
{
  std::string mount_root;
  if (!root)
    mount_root = root;
  return cluster->mount(mount_root);
}

extern "C" void ceph_umount(ceph_cluster_t *cluster)
{
  cluster->umount();
}

extern "C" int ceph_statfs(ceph_cluster_t *cluster, const char *path,
			   struct statvfs *stbuf)
{
  return cluster->get_client()->statfs(path, stbuf);
}

extern "C" int ceph_get_local_osd(ceph_cluster_t *cluster)
{
  return cluster->get_client()->get_local_osd();
}

extern "C" const char* ceph_getcwd(ceph_cluster_t *cluster)
{
  return cluster->get_cwd();
}

extern "C" int ceph_chdir (ceph_cluster_t *cluster, const char *s)
{
  return cluster->get_client()->chdir(s);
}

extern "C" int ceph_opendir(ceph_cluster_t *cluster,
			    const char *name, DIR **dirpp)
{
  return cluster->get_client()->opendir(name, dirpp);
}

extern "C" int ceph_closedir(ceph_cluster_t *cluster, DIR *dirp)
{
  return cluster->get_client()->closedir(dirp);
}

extern "C" int ceph_readdir_r(ceph_cluster_t *cluster, DIR *dirp, struct dirent *de)
{
  return cluster->get_client()->readdir_r(dirp, de);
}

extern "C" int ceph_readdirplus_r(ceph_cluster_t *cluster, DIR *dirp,
				  struct dirent *de, struct stat *st, int *stmask)
{
  return cluster->get_client()->readdirplus_r(dirp, de, st, stmask);
}

extern "C" int ceph_getdents(ceph_cluster_t *cluster, DIR *dirp,
			     char *buf, int buflen)
{
  return cluster->get_client()->getdents(dirp, buf, buflen);
}

extern "C" int ceph_getdnames(ceph_cluster_t *cluster, DIR *dirp,
			      char *buf, int buflen)
{
  return cluster->get_client()->getdnames(dirp, buf, buflen);
}

extern "C" void ceph_rewinddir(ceph_cluster_t *cluster, DIR *dirp)
{
  cluster->get_client()->rewinddir(dirp);
}

extern "C" loff_t ceph_telldir(ceph_cluster_t *cluster, DIR *dirp)
{
  return cluster->get_client()->telldir(dirp);
}

extern "C" void ceph_seekdir(ceph_cluster_t *cluster, DIR *dirp, loff_t offset)
{
  cluster->get_client()->seekdir(dirp, offset);
}

extern "C" int ceph_link (ceph_cluster_t *cluster, const char *existing,
			  const char *newname)
{
  return cluster->get_client()->link(existing, newname);
}

extern "C" int ceph_unlink(ceph_cluster_t *cluster, const char *path)
{
  return cluster->get_client()->unlink(path);
}

extern "C" int ceph_rename(ceph_cluster_t *cluster, const char *from,
			   const char *to)
{
  return cluster->get_client()->rename(from, to);
}

// dirs
extern "C" int ceph_mkdir(ceph_cluster_t *cluster, const char *path, mode_t mode)
{
  return cluster->get_client()->mkdir(path, mode);
}

extern "C" int ceph_mkdirs(ceph_cluster_t *cluster, const char *path, mode_t mode)
{
  return cluster->get_client()->mkdirs(path, mode);
}

extern "C" int ceph_rmdir(ceph_cluster_t *cluster, const char *path)
{
  return cluster->get_client()->rmdir(path);
}

// symlinks
extern "C" int ceph_readlink(ceph_cluster_t *cluster, const char *path,
			     char *buf, loff_t size)
{
  return cluster->get_client()->readlink(path, buf, size);
}

extern "C" int ceph_symlink(ceph_cluster_t *cluster, const char *existing,
			    const char *newname)
{
  return cluster->get_client()->symlink(existing, newname);
}

// inode stuff
extern "C" int ceph_lstat(ceph_cluster_t *cluster, const char *path,
			  struct stat *stbuf)
{
  return cluster->get_client()->lstat(path, stbuf);
}

extern "C" int ceph_lstat_precise(ceph_cluster_t *cluster, const char *path,
				  stat_precise *stbuf)
{
  return cluster->get_client()->lstat_precise(path, (Client::stat_precise*)stbuf);
}

extern "C" int ceph_setattr(ceph_cluster_t *cluster, const char *relpath,
			    struct stat *attr, int mask)
{
  Client::stat_precise p_attr = Client::stat_precise(*attr);
  return cluster->get_client()->setattr(relpath, &p_attr, mask);
}

extern "C" int ceph_setattr_precise(ceph_cluster_t *cluster, const char *relpath,
				    struct stat_precise *attr, int mask)
{
  return cluster->get_client()->setattr(relpath, (Client::stat_precise*)attr, mask);
}

extern "C" int ceph_chmod(ceph_cluster_t *cluster, const char *path, mode_t mode)
{
  return cluster->get_client()->chmod(path, mode);
}
extern "C" int ceph_chown(ceph_cluster_t *cluster, const char *path,
			  uid_t uid, gid_t gid)
{
  return cluster->get_client()->chown(path, uid, gid);
}

extern "C" int ceph_utime(ceph_cluster_t *cluster, const char *path,
			  struct utimbuf *buf)
{
  return cluster->get_client()->utime(path, buf);
}

extern "C" int ceph_truncate(ceph_cluster_t *cluster, const char *path,
			     loff_t size)
{
  return cluster->get_client()->truncate(path, size);
}

// file ops
extern "C" int ceph_mknod(ceph_cluster_t *cluster, const char *path,
			  mode_t mode, dev_t rdev)
{
  return cluster->get_client()->mknod(path, mode, rdev);
}

extern "C" int ceph_open(ceph_cluster_t *cluster, const char *path,
			 int flags, mode_t mode)
{
  return cluster->get_client()->open(path, flags, mode);
}

extern "C" int ceph_close(ceph_cluster_t *cluster, int fd)
{
  return cluster->get_client()->close(fd);
}

extern "C" loff_t ceph_lseek(ceph_cluster_t *cluster, int fd,
			     loff_t offset, int whence)
{
  return cluster->get_client()->lseek(fd, offset, whence);
}

extern "C" int ceph_read(ceph_cluster_t *cluster, int fd, char *buf,
			 loff_t size, loff_t offset)
{
  return cluster->get_client()->read(fd, buf, size, offset);
}

extern "C" int ceph_write(ceph_cluster_t *cluster, int fd, const char *buf,
			  loff_t size, loff_t offset)
{
  return cluster->get_client()->write(fd, buf, size, offset);
}

extern "C" int ceph_ftruncate(ceph_cluster_t *cluster, int fd, loff_t size)
{
  return cluster->get_client()->ftruncate(fd, size);
}

extern "C" int ceph_fsync(ceph_cluster_t *cluster, int fd, int syncdataonly)
{
  return cluster->get_client()->fsync(fd, syncdataonly);
}

extern "C" int ceph_fstat(ceph_cluster_t *cluster, int fd, struct stat *stbuf)
{
  return cluster->get_client()->fstat(fd, stbuf);
}

extern "C" int ceph_sync_fs(ceph_cluster_t *cluster)
{
  return cluster->get_client()->sync_fs();
}

extern "C" int ceph_get_file_stripe_unit(ceph_cluster_t *cluster, int fh)
{
  return cluster->get_client()->get_file_stripe_unit(fh);
}

extern "C" int ceph_get_file_replication(ceph_cluster_t *cluster,
					 const char *path)
{
  int fd = cluster->get_client()->open(path, O_RDONLY);
  if (fd < 0)
    return fd;
  int rep = cluster->get_client()->get_file_replication(fd);
  cluster->get_client()->close(fd);
  return rep;
}

extern "C" int ceph_get_default_preferred_pg(ceph_cluster_t *cluster, int fd)
{
  return cluster->get_client()->get_default_preferred_pg(fd);
}

extern "C" int ceph_set_default_file_stripe_unit(ceph_cluster_t *cluster,
						 int stripe)
{
  cluster->get_client()->set_default_file_stripe_unit(stripe);
  return 0;
}

extern "C" int ceph_set_default_file_stripe_count(ceph_cluster_t *cluster,
						  int count)
{
  cluster->get_client()->set_default_file_stripe_unit(count);
  return 0;
}

extern "C" int ceph_set_default_object_size(ceph_cluster_t *cluster, int size)
{
  cluster->get_client()->set_default_object_size(size);
  return 0;
}

extern "C" int ceph_set_default_file_replication(ceph_cluster_t *cluster,
						 int replication)
{
  cluster->get_client()->set_default_file_replication(replication);
  return 0;
}

extern "C" int ceph_set_default_preferred_pg(ceph_cluster_t *cluster, int pg)
{
  cluster->get_client()->set_default_preferred_pg(pg);
  return 0;
}

extern "C" int ceph_get_file_stripe_address(ceph_cluster_t *cluster, int fh,
					    loff_t offset, char *buf, int buflen)
{
  string address;
  int r = cluster->get_client()->get_file_stripe_address(fh, offset, address);
  if (r != 0) return r; //at time of writing, method ONLY returns
  // 0 or -EINVAL if there are no known osds
  int len = address.size()+1;
  if (len > buflen) {
    if (buflen == 0) return len;
    else return -ERANGE;
  }
  len = address.copy(buf, len, 0);
  buf[len] = '\0'; // write a null char to terminate c-style string
  return 0;
}

extern "C" int ceph_localize_reads(ceph_cluster_t *cluster, int val)
{
  if (!val)
    cluster->get_client()->clear_filer_flags(CEPH_OSD_FLAG_LOCALIZE_READS);
  else
    cluster->get_client()->set_filer_flags(CEPH_OSD_FLAG_LOCALIZE_READS);
  return 0;
}
