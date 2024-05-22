// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License version 2.1, as published by
 * the Free Software Foundation.  See file COPYING.
 *
 */

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <fmt/format.h>

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <regex>
#include <sstream>
#include <string_view>

#include <limits.h>
#include <string.h>

#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include "include/ceph_assert.h"
#include "include/rados/librados.hpp"

#include "common/Clock.h"
#include "common/Formatter.h"
#include "common/ceph_argparse.h"
#include "common/ceph_mutex.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/version.h"

#include "include/libcephsqlite.h"
#include "SimpleRADOSStriper.h"

#define dout_subsys ceph_subsys_cephsqlite
#undef dout_prefix
#define dout_prefix *_dout << "cephsqlite: " << __func__ << ": "
#define d(cct,cluster,lvl) ldout((cct), (lvl)) << "(client." << cluster->get_instance_id() << ") "
#define dv(lvl) d(cct,cluster,(lvl))
#define df(lvl) d(f->io.cct,f->io.cluster,(lvl)) << f->loc << " "

enum {
  P_FIRST = 0xf0000,
  P_OP_OPEN,
  P_OP_DELETE,
  P_OP_ACCESS,
  P_OP_FULLPATHNAME,
  P_OP_CURRENTTIME,
  P_OPF_CLOSE,
  P_OPF_READ,
  P_OPF_WRITE,
  P_OPF_TRUNCATE,
  P_OPF_SYNC,
  P_OPF_FILESIZE,
  P_OPF_LOCK,
  P_OPF_UNLOCK,
  P_OPF_CHECKRESERVEDLOCK,
  P_OPF_FILECONTROL,
  P_OPF_SECTORSIZE,
  P_OPF_DEVICECHARACTERISTICS,
  P_LAST,
};

using cctptr = boost::intrusive_ptr<CephContext>;
using rsptr = std::shared_ptr<librados::Rados>;

struct cephsqlite_appdata {
  ~cephsqlite_appdata() {
    {
      std::scoped_lock lock(cluster_mutex);
      _disconnect();
    }
    if (logger) {
      cct->get_perfcounters_collection()->remove(logger.get());
    }
    if (striper_logger) {
      cct->get_perfcounters_collection()->remove(striper_logger.get());
    }
  }
  int setup_perf() {
    ceph_assert(cct);
    PerfCountersBuilder plb(cct.get(), "libcephsqlite_vfs", P_FIRST, P_LAST);
    plb.add_time_avg(P_OP_OPEN, "op_open", "Time average of Open operations");
    plb.add_time_avg(P_OP_DELETE, "op_delete", "Time average of Delete operations");
    plb.add_time_avg(P_OP_ACCESS, "op_access", "Time average of Access operations");
    plb.add_time_avg(P_OP_FULLPATHNAME, "op_fullpathname", "Time average of FullPathname operations");
    plb.add_time_avg(P_OP_CURRENTTIME, "op_currenttime", "Time average of Currenttime operations");
    plb.add_time_avg(P_OPF_CLOSE, "opf_close", "Time average of Close file operations");
    plb.add_time_avg(P_OPF_READ, "opf_read", "Time average of Read file operations");
    plb.add_time_avg(P_OPF_WRITE, "opf_write", "Time average of Write file operations");
    plb.add_time_avg(P_OPF_TRUNCATE, "opf_truncate", "Time average of Truncate file operations");
    plb.add_time_avg(P_OPF_SYNC, "opf_sync", "Time average of Sync file operations");
    plb.add_time_avg(P_OPF_FILESIZE, "opf_filesize", "Time average of FileSize file operations");
    plb.add_time_avg(P_OPF_LOCK, "opf_lock", "Time average of Lock file operations");
    plb.add_time_avg(P_OPF_UNLOCK, "opf_unlock", "Time average of Unlock file operations");
    plb.add_time_avg(P_OPF_CHECKRESERVEDLOCK, "opf_checkreservedlock", "Time average of CheckReservedLock file operations");
    plb.add_time_avg(P_OPF_FILECONTROL, "opf_filecontrol", "Time average of FileControl file operations");
    plb.add_time_avg(P_OPF_SECTORSIZE, "opf_sectorsize", "Time average of SectorSize file operations");
    plb.add_time_avg(P_OPF_DEVICECHARACTERISTICS, "opf_devicecharacteristics", "Time average of DeviceCharacteristics file operations");
    logger.reset(plb.create_perf_counters());
    if (int rc = SimpleRADOSStriper::config_logger(cct.get(), "libcephsqlite_striper", &striper_logger); rc < 0) {
      return rc;
    }
    cct->get_perfcounters_collection()->add(logger.get());
    cct->get_perfcounters_collection()->add(striper_logger.get());
    return 0;
  }

  std::pair<cctptr, rsptr> get_cluster() {
    std::scoped_lock lock(cluster_mutex);
    if (!cct) {
      if (int rc = _open(nullptr); rc < 0) {
        ceph_abort("could not open connection to ceph");
      }
    }
    return {cct, cluster};
  }
  int connect() {
    std::scoped_lock lock(cluster_mutex);
    return _connect();
  }
  int reconnect() {
    std::scoped_lock lock(cluster_mutex);
    _disconnect();
    return _connect();
  }
  int maybe_reconnect(rsptr _cluster) {
    std::scoped_lock lock(cluster_mutex);
    if (!cluster || cluster == _cluster) {
      ldout(cct, 10) << "reconnecting to RADOS" << dendl;
      _disconnect();
      return _connect();
    } else {
      ldout(cct, 10) << "already reconnected" << dendl;
      return 0;
    }
  }
  int open(CephContext* _cct) {
    std::scoped_lock lock(cluster_mutex);
    return _open(_cct);
  }

  std::unique_ptr<PerfCounters> logger;
  std::shared_ptr<PerfCounters> striper_logger;

private:
  int _open(CephContext* _cct) {
    if (!_cct) {
      std::vector<const char*> env_args;
      env_to_vec(env_args, "CEPH_ARGS");
      std::string cluster, conf_file_list; // unused
      CephInitParameters iparams = ceph_argparse_early_args(env_args, CEPH_ENTITY_TYPE_CLIENT, &cluster, &conf_file_list);
      cct = cctptr(common_preinit(iparams, CODE_ENVIRONMENT_LIBRARY, 0), false);
      cct->_conf.parse_config_files(nullptr, &std::cerr, 0);
      cct->_conf.parse_env(cct->get_module_type()); // environment variables override
      cct->_conf.apply_changes(nullptr);
      common_init_finish(cct.get());
    } else {
      cct = cctptr(_cct);
    }

    if (int rc = setup_perf(); rc < 0) {
      return rc;
    }

    if (int rc = _connect(); rc < 0) {
      return rc;
    }

    return 0;
  }
  void _disconnect() {
    if (cluster) {
      cluster.reset();
    }
  }
  int _connect() {
    ceph_assert(cct);
    auto _cluster = rsptr(new librados::Rados());
    ldout(cct, 5) << "initializing RADOS handle as " << cct->_conf->name << dendl;
    if (int rc = _cluster->init_with_context(cct.get()); rc < 0) {
      lderr(cct) << "cannot initialize RADOS: " << cpp_strerror(rc) << dendl;
      return rc;
    }
    if (int rc = _cluster->connect(); rc < 0) {
      lderr(cct) << "cannot connect: " << cpp_strerror(rc) << dendl;
      return rc;
    }
    auto s = _cluster->get_addrs();
    ldout(cct, 5) << "completed connection to RADOS with address " << s << dendl;
    cluster = std::move(_cluster);
    return 0;
  }

  ceph::mutex cluster_mutex = ceph::make_mutex("libcephsqlite");;
  cctptr cct;
  rsptr cluster;
};

struct cephsqlite_fileloc {
  std::string pool;
  std::string radosns;
  std::string name;
};

struct cephsqlite_fileio {
  cctptr cct;
  rsptr cluster; // anchor for ioctx
  librados::IoCtx ioctx;
  std::unique_ptr<SimpleRADOSStriper> rs;
};

std::ostream& operator<<(std::ostream &out, const cephsqlite_fileloc& fileloc) {
  return out
    << "["
    << fileloc.pool
    << ":"
    << fileloc.radosns
    << "/"
    << fileloc.name
    << "]"
    ;
}

struct cephsqlite_file {
  sqlite3_file base;
  struct sqlite3_vfs* vfs = nullptr;
  int flags = 0;
  // There are 5 lock states: https://sqlite.org/c3ref/c_lock_exclusive.html
  int lock = 0;
  struct cephsqlite_fileloc loc{};
  struct cephsqlite_fileio io{};
};


#define getdata(vfs) (*((cephsqlite_appdata*)((vfs)->pAppData)))

static int Lock(sqlite3_file *file, int ilock)
{
  auto f = (cephsqlite_file*)file;
  auto start = ceph::coarse_mono_clock::now();
  df(5) << std::hex << ilock << dendl;

  auto& lock = f->lock;
  ceph_assert(!f->io.rs->is_locked() || lock > SQLITE_LOCK_NONE);
  ceph_assert(lock <= ilock);
  if (!f->io.rs->is_locked() && ilock > SQLITE_LOCK_NONE) {
    if (int rc = f->io.rs->lock(0); rc < 0) {
      df(5) << "failed: " << rc << dendl;
      if (rc == -EBLOCKLISTED) {
        getdata(f->vfs).maybe_reconnect(f->io.cluster);
      }
      return SQLITE_IOERR;
    }
  }

  lock = ilock;
  auto end = ceph::coarse_mono_clock::now();
  getdata(f->vfs).logger->tinc(P_OPF_LOCK, end-start);
  return SQLITE_OK;
}

static int Unlock(sqlite3_file *file, int ilock)
{
  auto f = (cephsqlite_file*)file;
  auto start = ceph::coarse_mono_clock::now();
  df(5) << std::hex << ilock << dendl;

  auto& lock = f->lock;
  ceph_assert(lock == SQLITE_LOCK_NONE || (lock > SQLITE_LOCK_NONE && f->io.rs->is_locked()));
  ceph_assert(lock >= ilock);
  if (ilock <= SQLITE_LOCK_NONE && SQLITE_LOCK_NONE < lock) {
    if (int rc = f->io.rs->unlock(); rc < 0) {
      df(5) << "failed: " << rc << dendl;
      if (rc == -EBLOCKLISTED) {
        getdata(f->vfs).maybe_reconnect(f->io.cluster);
      }
      return SQLITE_IOERR;
    }
  }

  lock = ilock;
  auto end = ceph::coarse_mono_clock::now();
  getdata(f->vfs).logger->tinc(P_OPF_UNLOCK, end-start);
  return SQLITE_OK;
}

static int CheckReservedLock(sqlite3_file *file, int *result)
{
  auto f = (cephsqlite_file*)file;
  auto start = ceph::coarse_mono_clock::now();
  df(5) << dendl;
  *result = 0;

  auto& lock = f->lock;
  if (lock > SQLITE_LOCK_SHARED) {
    *result = 1;
  }

  df(10);
  f->io.rs->print_lockers(*_dout);
  *_dout << dendl;

  auto end = ceph::coarse_mono_clock::now();
  getdata(f->vfs).logger->tinc(P_OPF_CHECKRESERVEDLOCK, end-start);
  return SQLITE_OK;
}

static int Close(sqlite3_file *file)
{
  auto f = (cephsqlite_file*)file;
  auto start = ceph::coarse_mono_clock::now();
  df(5) << dendl;
  f->~cephsqlite_file();
  auto end = ceph::coarse_mono_clock::now();
  getdata(f->vfs).logger->tinc(P_OPF_CLOSE, end-start);
  return SQLITE_OK;
}

static int Read(sqlite3_file *file, void *buf, int len, sqlite_int64 off)
{
  auto f = (cephsqlite_file*)file;
  auto start = ceph::coarse_mono_clock::now();
  df(5) << buf << " " << off << "~" << len << dendl;

  if (int rc = f->io.rs->read(buf, len, off); rc < 0) {
    df(5) << "read failed: " << cpp_strerror(rc) << dendl;
    if (rc == -EBLOCKLISTED) {
      getdata(f->vfs).maybe_reconnect(f->io.cluster);
    }
    return SQLITE_IOERR_READ;
  } else {
    df(5) << "= " << rc << dendl;
    auto end = ceph::coarse_mono_clock::now();
    getdata(f->vfs).logger->tinc(P_OPF_READ, end-start);
    if (rc < len) {
      memset((unsigned char*)buf+rc, 0, len-rc);
      return SQLITE_IOERR_SHORT_READ;
    } else {
      return SQLITE_OK;
    }
  }
}

static int Write(sqlite3_file *file, const void *buf, int len, sqlite_int64 off)
{
  auto f = (cephsqlite_file*)file;
  auto start = ceph::coarse_mono_clock::now();
  df(5) << off << "~" << len << dendl;

  if (int rc = f->io.rs->write(buf, len, off); rc < 0) {
    df(5) << "write failed: " << cpp_strerror(rc) << dendl;
    if (rc == -EBLOCKLISTED) {
      getdata(f->vfs).maybe_reconnect(f->io.cluster);
    }
    return SQLITE_IOERR_WRITE;
  } else {
    df(5) << "= " << rc << dendl;
    auto end = ceph::coarse_mono_clock::now();
    getdata(f->vfs).logger->tinc(P_OPF_WRITE, end-start);
    return SQLITE_OK;
  }

}

static int Truncate(sqlite3_file *file, sqlite_int64 size)
{
  auto f = (cephsqlite_file*)file;
  auto start = ceph::coarse_mono_clock::now();
  df(5) << size << dendl;

  if (int rc = f->io.rs->truncate(size); rc < 0) {
    df(5) << "truncate failed: " << cpp_strerror(rc) << dendl;
    if (rc == -EBLOCKLISTED) {
      getdata(f->vfs).maybe_reconnect(f->io.cluster);
    }
    return SQLITE_IOERR;
  }

  auto end = ceph::coarse_mono_clock::now();
  getdata(f->vfs).logger->tinc(P_OPF_TRUNCATE, end-start);
  return SQLITE_OK;
}

static int Sync(sqlite3_file *file, int flags)
{
  auto f = (cephsqlite_file*)file;
  auto start = ceph::coarse_mono_clock::now();
  df(5) << flags << dendl;

  if (int rc = f->io.rs->flush(); rc < 0) {
    df(5) << "failed: " << cpp_strerror(rc) << dendl;
    if (rc == -EBLOCKLISTED) {
      getdata(f->vfs).maybe_reconnect(f->io.cluster);
    }
    return SQLITE_IOERR;
  }

  df(5) << " = 0" << dendl;

  auto end = ceph::coarse_mono_clock::now();
  getdata(f->vfs).logger->tinc(P_OPF_SYNC, end-start);
  return SQLITE_OK;
}


static int FileSize(sqlite3_file *file, sqlite_int64 *osize)
{
  auto f = (cephsqlite_file*)file;
  auto start = ceph::coarse_mono_clock::now();
  df(5) << dendl;

  uint64_t size = 0;
  if (int rc = f->io.rs->stat(&size); rc < 0) {
    df(5) << "stat failed: " << cpp_strerror(rc) << dendl;
    if (rc == -EBLOCKLISTED) {
      getdata(f->vfs).maybe_reconnect(f->io.cluster);
    }
    return SQLITE_NOTFOUND;
  }

  *osize = (sqlite_int64)size;

  df(5) << "= " << size << dendl;

  auto end = ceph::coarse_mono_clock::now();
  getdata(f->vfs).logger->tinc(P_OPF_FILESIZE, end-start);
  return SQLITE_OK;
}


static bool parsepath(std::string_view path, struct cephsqlite_fileloc* fileloc)
{
  static const std::regex re1{"^/*(\\*[[:digit:]]+):([[:alnum:]\\-_.]*)/([[:alnum:]\\-._]+)$"};
  static const std::regex re2{"^/*([[:alnum:]\\-_.]+):([[:alnum:]\\-_.]*)/([[:alnum:]\\-._]+)$"};

  std::cmatch cm;
  if (!std::regex_match(path.data(), cm, re1)) {
    if (!std::regex_match(path.data(), cm, re2)) {
      return false;
    }
  }
  fileloc->pool = cm[1];
  fileloc->radosns = cm[2];
  fileloc->name = cm[3];

  return true;
}

static int makestriper(sqlite3_vfs* vfs, cctptr cct, rsptr cluster, const cephsqlite_fileloc& loc, cephsqlite_fileio* io)
{
  bool gotmap = false;

  d(cct,cluster,10) << loc << dendl;

enoent_retry:
  if (loc.pool[0] == '*') {
    std::string err;
    int64_t id = strict_strtoll(loc.pool.c_str()+1, 10, &err);
    ceph_assert(err.empty());
    if (int rc = cluster->ioctx_create2(id, io->ioctx); rc < 0) {
      if (rc == -ENOENT && !gotmap) {
        cluster->wait_for_latest_osdmap();
        gotmap = true;
        goto enoent_retry;
      }
      d(cct,cluster,1) << "cannot create ioctx: " << cpp_strerror(rc) << dendl;
      return rc;
    }
  } else {
    if (int rc = cluster->ioctx_create(loc.pool.c_str(), io->ioctx); rc < 0) {
      if (rc == -ENOENT && !gotmap) {
        cluster->wait_for_latest_osdmap();
        gotmap = true;
        goto enoent_retry;
      }
      d(cct,cluster,1) << "cannot create ioctx: " << cpp_strerror(rc) << dendl;
      return rc;
    }
  }

  if (!loc.radosns.empty())
    io->ioctx.set_namespace(loc.radosns);

  io->rs = std::make_unique<SimpleRADOSStriper>(io->ioctx, loc.name);
  io->rs->set_logger(getdata(vfs).striper_logger);
  io->rs->set_lock_timeout(cct->_conf.get_val<std::chrono::milliseconds>("cephsqlite_lock_renewal_timeout"));
  io->rs->set_lock_interval(cct->_conf.get_val<std::chrono::milliseconds>("cephsqlite_lock_renewal_interval"));
  io->rs->set_blocklist_the_dead(cct->_conf.get_val<bool>("cephsqlite_blocklist_dead_locker"));
  io->cluster = std::move(cluster);
  io->cct = cct;

  return 0;
}

static int SectorSize(sqlite3_file* sf)
{
  static const int size = 65536;
  auto start = ceph::coarse_mono_clock::now();
  auto f = (cephsqlite_file*)sf;
  df(5) << " = " << size << dendl;
  auto end = ceph::coarse_mono_clock::now();
  getdata(f->vfs).logger->tinc(P_OPF_SECTORSIZE, end-start);
  return size;
}

static int FileControl(sqlite3_file* sf, int op, void *arg)
{
  auto f = (cephsqlite_file*)sf;
  auto start = ceph::coarse_mono_clock::now();
  df(5) << op << ", " << arg << dendl;
  auto end = ceph::coarse_mono_clock::now();
  getdata(f->vfs).logger->tinc(P_OPF_FILECONTROL, end-start);
  return SQLITE_NOTFOUND;
}

static int DeviceCharacteristics(sqlite3_file* sf)
{
  auto f = (cephsqlite_file*)sf;
  auto start = ceph::coarse_mono_clock::now();
  df(5) << dendl;
  static const int c = 0
      |SQLITE_IOCAP_ATOMIC
      |SQLITE_IOCAP_POWERSAFE_OVERWRITE
      |SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN
      |SQLITE_IOCAP_SAFE_APPEND
      ;
  auto end = ceph::coarse_mono_clock::now();
  getdata(f->vfs).logger->tinc(P_OPF_DEVICECHARACTERISTICS, end-start);
  return c;
}

static int Open(sqlite3_vfs *vfs, const char *name, sqlite3_file *file,
                int flags,  int *oflags)
{
  static const sqlite3_io_methods io = {
    1,                        /* iVersion */
    Close,                    /* xClose */
    Read,                     /* xRead */
    Write,                    /* xWrite */
    Truncate,                 /* xTruncate */
    Sync,                     /* xSync */
    FileSize,                 /* xFileSize */
    Lock,                     /* xLock */
    Unlock,                   /* xUnlock */
    CheckReservedLock,        /* xCheckReservedLock */
    FileControl,              /* xFileControl */
    SectorSize,               /* xSectorSize */
    DeviceCharacteristics     /* xDeviceCharacteristics */
  };

  auto start = ceph::coarse_mono_clock::now();
  bool gotmap = false;
  auto [cct, cluster] = getdata(vfs).get_cluster();

  /* we are not going to create temporary files */
  if (name == NULL) {
    dv(-1) << " cannot open temporary database" << dendl;
    return SQLITE_CANTOPEN;
  }
  auto path = std::string_view(name);
  if (path == ":memory:") {
    dv(-1) << " cannot open temporary database" << dendl;
    return SQLITE_IOERR;
  }

  dv(5) << path << " flags=" << std::hex << flags << dendl;

  auto f = new (file)cephsqlite_file();
  f->vfs = vfs;
  if (!parsepath(path, &f->loc)) {
    ceph_assert(0); /* xFullPathname validates! */
  }
  f->flags = flags;

enoent_retry:
  if (int rc = makestriper(vfs, cct, cluster, f->loc, &f->io); rc < 0) {
    f->~cephsqlite_file();
    dv(-1) << "cannot open striper" << dendl;
    return SQLITE_IOERR;
  }

  if (flags & SQLITE_OPEN_CREATE) {
    dv(10) << "OPEN_CREATE" << dendl;
    if (int rc = f->io.rs->create(); rc < 0 && rc != -EEXIST) {
      if (rc == -ENOENT && !gotmap) {
        /* we may have an out of date OSDMap which cancels the op in the
         * Objecter. Try to get a new one and retry. This is mostly noticable
         * in testing when pools are getting created/deleted left and right.
         */
        dv(5) << "retrying create after getting latest OSDMap" << dendl;
        cluster->wait_for_latest_osdmap();
        gotmap = true;
        goto enoent_retry;
      }
      dv(5) << "file cannot be created: " << cpp_strerror(rc) << dendl;
      return SQLITE_IOERR;
    }
  }

  if (int rc = f->io.rs->open(); rc < 0) {
    if (rc == -ENOENT && !gotmap) {
      /* See comment above for create case. */
      dv(5) << "retrying open after getting latest OSDMap" << dendl;
      cluster->wait_for_latest_osdmap();
      gotmap = true;
      goto enoent_retry;
    }
    dv(10) << "cannot open striper: " << cpp_strerror(rc) << dendl;
    return rc;
  }

  if (oflags) {
    *oflags = flags;
  }
  f->base.pMethods = &io;
  auto end = ceph::coarse_mono_clock::now();
  getdata(vfs).logger->tinc(P_OP_OPEN, end-start);
  return SQLITE_OK;
}

/*
** Delete the file identified by argument path. If the dsync parameter
** is non-zero, then ensure the file-system modification to delete the
** file has been synced to disk before returning.
*/
static int Delete(sqlite3_vfs* vfs, const char* path, int dsync)
{
  auto start = ceph::coarse_mono_clock::now();
  auto [cct, cluster] = getdata(vfs).get_cluster();
  dv(5) << "'" << path << "', " << dsync << dendl;

  cephsqlite_fileloc fileloc;
  if (!parsepath(path, &fileloc)) {
    dv(5) << "path does not parse!" << dendl;
    return SQLITE_NOTFOUND;
  }

  cephsqlite_fileio io;
  if (int rc = makestriper(vfs, cct, cluster, fileloc, &io); rc < 0) {
    dv(-1) << "cannot open striper" << dendl;
    return SQLITE_IOERR;
  }

  if (int rc = io.rs->lock(0); rc < 0) {
    return SQLITE_IOERR;
  }

  if (int rc = io.rs->remove(); rc < 0) {
    dv(5) << "= " << rc << dendl;
    return SQLITE_IOERR_DELETE;
  }

  /* No need to unlock */
  dv(5) << "= 0" << dendl;
  auto end = ceph::coarse_mono_clock::now();
  getdata(vfs).logger->tinc(P_OP_DELETE, end-start);

  return SQLITE_OK;
}

/*
** Query the file-system to see if the named file exists, is readable or
** is both readable and writable.
*/
static int Access(sqlite3_vfs* vfs, const char* path, int flags, int* result)
{
  auto start = ceph::coarse_mono_clock::now();
  auto [cct, cluster] = getdata(vfs).get_cluster();
  dv(5) << path << " " << std::hex << flags << dendl;

  cephsqlite_fileloc fileloc;
  if (!parsepath(path, &fileloc)) {
    dv(5) << "path does not parse!" << dendl;
    return SQLITE_NOTFOUND;
  }

  cephsqlite_fileio io;
  if (int rc = makestriper(vfs, cct, cluster, fileloc, &io); rc < 0) {
    dv(-1) << "cannot open striper" << dendl;
    return SQLITE_IOERR;
  }

  if (int rc = io.rs->open(); rc < 0) {
    if (rc == -ENOENT) {
      *result = 0;
      return SQLITE_OK;
    } else {
      dv(10) << "cannot open striper: " << cpp_strerror(rc) << dendl;
      *result = 0;
      return SQLITE_IOERR;
    }
  }

  uint64_t size = 0;
  if (int rc = io.rs->stat(&size); rc < 0) {
    dv(5) << "= " << rc << " (" << cpp_strerror(rc) << ")" << dendl;
    *result = 0;
  } else {
    dv(5) << "= 0" << dendl;
    *result = 1;
  }

  auto end = ceph::coarse_mono_clock::now();
  getdata(vfs).logger->tinc(P_OP_ACCESS, end-start);
  return SQLITE_OK;
}

/* This method is only called once for each database. It provides a chance to
 * reformat the path into a canonical format.
 */
static int FullPathname(sqlite3_vfs* vfs, const char* ipath, int opathlen, char* opath)
{
  auto start = ceph::coarse_mono_clock::now();
  auto path = std::string_view(ipath);
  auto [cct, cluster] = getdata(vfs).get_cluster();
  dv(5) << "1: " <<  path << dendl;

  cephsqlite_fileloc fileloc;
  if (!parsepath(path, &fileloc)) {
    dv(5) << "path does not parse!" << dendl;
    return SQLITE_NOTFOUND;
  }
  dv(5) << " parsed " << fileloc << dendl;

  auto p = fmt::format("{}:{}/{}", fileloc.pool, fileloc.radosns, fileloc.name);
  if (p.size() >= (size_t)opathlen) {
    dv(5) << "path too long!" << dendl;
    return SQLITE_CANTOPEN;
  }
  strcpy(opath, p.c_str());
  dv(5) << " output " << p << dendl;

  auto end = ceph::coarse_mono_clock::now();
  getdata(vfs).logger->tinc(P_OP_FULLPATHNAME, end-start);
  return SQLITE_OK;
}

static int CurrentTime(sqlite3_vfs* vfs, sqlite3_int64* time)
{
  auto start = ceph::coarse_mono_clock::now();
  auto [cct, cluster] = getdata(vfs).get_cluster();
  dv(5) << time << dendl;

  auto t = ceph_clock_now();
  *time = t.to_msec() + 2440587.5*86400000; /* julian days since 1970 converted to ms */

  auto end = ceph::coarse_mono_clock::now();
  getdata(vfs).logger->tinc(P_OP_CURRENTTIME, end-start);
  return SQLITE_OK;
}

LIBCEPHSQLITE_API int cephsqlite_setcct(CephContext* _cct, char** ident)
{
  ldout(_cct, 1) << "cct: " << _cct << dendl;

  if (sqlite3_api == nullptr) {
    lderr(_cct) << "API violation: must have sqlite3 init libcephsqlite" << dendl;
    return -EINVAL;
  }

  auto vfs = sqlite3_vfs_find("ceph");
  if (!vfs) {
    lderr(_cct) << "API violation: must have sqlite3 init libcephsqlite" << dendl;
    return -EINVAL;
  }

  auto& appd = getdata(vfs);
  if (int rc = appd.open(_cct); rc < 0) {
    return rc;
  }

  auto [cct, cluster] = appd.get_cluster();

  auto s = cluster->get_addrs();
  if (ident) {
    *ident = strdup(s.c_str());
  }

  ldout(cct, 1) << "complete" << dendl;

  return 0;
}

static void f_perf(sqlite3_context* ctx, int argc, sqlite3_value** argv)
{
  auto vfs = (sqlite3_vfs*)sqlite3_user_data(ctx);
  auto [cct, cluster] = getdata(vfs).get_cluster();
  dv(10) << dendl;
  auto&& appd = getdata(vfs);
  JSONFormatter f(false);
  f.open_object_section("ceph_perf");
  appd.logger->dump_formatted(&f, false, false);
  appd.striper_logger->dump_formatted(&f, false, false);
  f.close_section();
  {
    CachedStackStringStream css;
    f.flush(*css);
    auto sv = css->strv();
    dv(20) << " = " << sv << dendl;
    sqlite3_result_text(ctx, sv.data(), sv.size(), SQLITE_TRANSIENT);
  }
}

static void f_status(sqlite3_context* ctx, int argc, sqlite3_value** argv)
{
  auto vfs = (sqlite3_vfs*)sqlite3_user_data(ctx);
  auto [cct, cluster] = getdata(vfs).get_cluster();
  dv(10) << dendl;
  JSONFormatter f(false);
  f.open_object_section("ceph_status");
  f.dump_int("id", cluster->get_instance_id());
  f.dump_string("addr", cluster->get_addrs());
  f.close_section();
  {
    CachedStackStringStream css;
    f.flush(*css);
    auto sv = css->strv();
    dv(20) << " = " << sv << dendl;
    sqlite3_result_text(ctx, sv.data(), sv.size(), SQLITE_TRANSIENT);
  }
}

static int autoreg(sqlite3* db, char** err, const struct sqlite3_api_routines* thunk)
{
  auto vfs = sqlite3_vfs_find("ceph");
  if (!vfs) {
    ceph_abort("ceph vfs not found");
  }

  if (int rc = sqlite3_create_function(db, "ceph_perf", 0, SQLITE_UTF8, vfs, f_perf, nullptr, nullptr); rc) {
    return rc;
  }

  if (int rc = sqlite3_create_function(db, "ceph_status", 0, SQLITE_UTF8, vfs, f_status, nullptr, nullptr); rc) {
    return rc;
  }

  return SQLITE_OK;
}

/* You may wonder why we have an atexit handler? After all, atexit/exit creates
 * a mess for multithreaded programs. Well, sqlite3 does not have an API for
 * orderly removal of extensions. And, in fact, any API we might make
 * unofficially (such as "sqlite3_cephsqlite_fini") would potentially race with
 * other threads interacting with sqlite3 + the "ceph" VFS. There is a method
 * for removing a VFS but it's not called by sqlite3 in any error scenario and
 * there is no mechanism within sqlite3 to tell a VFS to unregister itself.
 *
 * This all would be mostly okay if /bin/sqlite3 did not call exit(3), but it
 * does. (This occurs only for the sqlite3 binary, not when used as a library.)
 * exit(3) calls destructors on all static-duration structures for the program.
 * This breaks any outstanding threads created by the librados handle in all
 * sorts of fantastic ways from C++ exceptions to memory faults.  In general,
 * Ceph libraries are not tolerant of exit(3) (_exit(3) is okay!). Applications
 * must clean up after themselves or _exit(3).
 *
 * So, we have an atexit handler for libcephsqlite. This simply shuts down the
 * RADOS handle. We can be assured that this occurs before any ceph library
 * static-duration structures are destructed due to ordering guarantees by
 * exit(3). Generally, we only see this called when the VFS is used by
 * /bin/sqlite3 and only during sqlite3 error scenarios (like I/O errors
 * arrising from blocklisting).
 */

static void cephsqlite_atexit()
{
  if (auto vfs = sqlite3_vfs_find("ceph"); vfs) {
    if (vfs->pAppData) {
      auto&& appd = getdata(vfs);
      delete &appd;
      vfs->pAppData = nullptr;
    }
  }
}

LIBCEPHSQLITE_API int sqlite3_cephsqlite_init(sqlite3* db, char** err, const sqlite3_api_routines* api)
{
  SQLITE_EXTENSION_INIT2(api);

  auto vfs = sqlite3_vfs_find("ceph");
  if (!vfs) {
    vfs = (sqlite3_vfs*) calloc(1, sizeof(sqlite3_vfs));
    auto appd = new cephsqlite_appdata;
    vfs->iVersion = 2;
    vfs->szOsFile = sizeof(struct cephsqlite_file);
    vfs->mxPathname = 4096;
    vfs->zName = "ceph";
    vfs->pAppData = appd;
    vfs->xOpen = Open;
    vfs->xDelete = Delete;
    vfs->xAccess = Access;
    vfs->xFullPathname = FullPathname;
    vfs->xCurrentTimeInt64 = CurrentTime;
    if (int rc = sqlite3_vfs_register(vfs, 0); rc) {
      delete appd;
      free(vfs);
      return rc;
    }
  }

  if (int rc = std::atexit(cephsqlite_atexit); rc) {
    return SQLITE_INTERNAL;
  }

  if (int rc = sqlite3_auto_extension((void(*)(void))autoreg); rc) {
    return rc;
  }
  if (int rc = autoreg(db, err, api); rc) {
    return rc;
  }

  return SQLITE_OK_LOAD_PERMANENTLY;
}
