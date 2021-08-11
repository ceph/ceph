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
#include "client/Client.h"
#include "common/async/context_pool.h"

#include "include/libcephfssqlite.h"

#define dout_subsys ceph_subsys_cephfssqlite
#undef dout_prefix
#define dout_prefix *_dout << "cephfssqlite: " << __func__ << ": "
#define d(vfs,lvl) ldout(getcct(vfs), (lvl)) << "(client.) "
#define dv(lvl) d(vfs,(lvl))
#define df(lvl) d(f->vfs,(lvl)) << f->loc << " "

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

struct cephfssqlite_appdata {
    ~cephfssqlite_appdata() {
        if (logger) {
            cct->get_perfcounters_collection()->remove(logger.get());
        }
        if (!client_map.empty()) {
            client_map.erase(client_map.begin(), client_map.end());
        }
    }
    int setup_perf() {
        ceph_assert(cct);
        PerfCountersBuilder plb(cct.get(), "libcephfssqlite_vfs", P_FIRST, P_LAST);
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
        cct->get_perfcounters_collection()->add(logger.get());
        return 0;
    }
    int init_cluster() {
        ceph_assert(cct);
        return 0;
    }

    boost::intrusive_ptr<CephContext> cct;
    std::unique_ptr<PerfCounters> logger;

    std::map<std::string, std::shared_ptr<Client>> client_map;
};

struct cephfssqlite_fileloc {
    std::string fsname;
    std::string path;
    UserPerm default_perms;
};

struct cephfssqlite_fileio {
    Client *client;
};

std::ostream& operator<<(std::ostream &out, const cephfssqlite_fileloc& fileloc) {
    return out
            << "["
            << fileloc.fsname
            << ":"
            << fileloc.path
            << "]"
            ;
}

struct cephfssqlite_file {
    sqlite3_file base;
    struct sqlite3_vfs* vfs = nullptr;
    int flags = 0;
    int fd = -1;
    // There are 5 lock states: https://sqlite.org/c3ref/c_lock_exclusive.html
    int lock = 0;
    struct cephfssqlite_fileloc loc{};
    struct cephfssqlite_fileio io{};
};


#define getdata(vfs) (*((cephfssqlite_appdata*)((vfs)->pAppData)))

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

static CephContext* getcct(sqlite3_vfs* vfs)
{
    auto&& appd = getdata(vfs);
    auto& cct = appd.cct;
    if (cct) {
        return cct.get();
    }

    /* bootstrap cct */
    std::vector<const char*> env_args;
    env_to_vec(env_args, "CEPH_ARGS");
    std::string cluster, conf_file_list; // unused
    CephInitParameters iparams = ceph_argparse_early_args(env_args, CEPH_ENTITY_TYPE_CLIENT, &cluster, &conf_file_list);
    cct = boost::intrusive_ptr<CephContext>(common_preinit(iparams, CODE_ENVIRONMENT_LIBRARY, 0), false);
    cct->_conf.parse_config_files(nullptr, &std::cerr, 0);
    cct->_conf.parse_env(cct->get_module_type()); // environment variables override
    cct->_conf.apply_changes(nullptr);
    common_init_finish(cct.get());
    cct->put();

    if (int rc = appd.setup_perf(); rc < 0) {
        ceph_abort("cannot setup perf counters");
    }

    if (int rc = appd.init_cluster(); rc < 0) {
        ceph_abort("cannot setup RADOS cluster handle");
    }

    return cct.get();
}

static int Lock(sqlite3_file *file, int ilock)
{
    auto f = (cephfssqlite_file*)file;
    auto start = ceph::coarse_mono_clock::now();
    df(5) << std::hex << ilock << dendl;

    auto& lock = f->lock;
    ceph_assert(lock >= SQLITE_LOCK_NONE);
    ceph_assert(lock <= ilock);
    if (ilock > SQLITE_LOCK_NONE) {
        int operation = (ilock == SQLITE_LOCK_SHARED)? LOCK_SH : LOCK_EX;
        if (int rc = f->io.client->flock(f->fd, operation, getpid()); rc < 0) {
            df(5) << "failed: " << rc << dendl;
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
    auto f = (cephfssqlite_file*)file;
    auto start = ceph::coarse_mono_clock::now();
    df(5) << std::hex << ilock << dendl;

    auto& lock = f->lock;
    ceph_assert(lock >= SQLITE_LOCK_NONE);
    ceph_assert(lock >= ilock);
    if (ilock <= SQLITE_LOCK_NONE && SQLITE_LOCK_NONE < lock) {
        int operation;
        if (ilock < SQLITE_LOCK_SHARED) {
            operation = LOCK_UN;
        } else if (ilock == SQLITE_LOCK_SHARED) {
            operation = LOCK_SH;
        } else {
            operation = LOCK_EX;
        }
        if (int rc = f->io.client->flock(f->fd, operation, getpid()); rc < 0) {
            df(5) << "failed: " << rc << dendl;
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
    auto f = (cephfssqlite_file*)file;
    auto start = ceph::coarse_mono_clock::now();
    df(5) << dendl;

    *result = 0;
    auto& lock = f->lock;
    if (lock > SQLITE_LOCK_SHARED) {
        *result = 1;
    }

    auto end = ceph::coarse_mono_clock::now();
    getdata(f->vfs).logger->tinc(P_OPF_CHECKRESERVEDLOCK, end-start);
    return SQLITE_OK;
}

static int Close(sqlite3_file *file)
{
    auto f = (cephfssqlite_file*)file;
    auto start = ceph::coarse_mono_clock::now();
    df(5) << dendl;
    f->io.client->close(f->fd);
    f->~cephfssqlite_file();
    auto end = ceph::coarse_mono_clock::now();
    getdata(f->vfs).logger->tinc(P_OPF_CLOSE, end-start);
    return SQLITE_OK;
}

static int Read(sqlite3_file *file, void *buf, int len, sqlite_int64 off)
{
    auto f = (cephfssqlite_file*)file;
    auto start = ceph::coarse_mono_clock::now();
    df(5) << buf << " " << off << "~" << len << dendl;

    if (int rc = f->io.client->read(f->fd, (char *)buf, len, off); rc < 0) {
        df(5) << "read failed: " << cpp_strerror(rc) << dendl;
        return SQLITE_IOERR_READ;
    } else {
        df(5) << "= " << rc << dendl;
        auto end = ceph::coarse_mono_clock::now();
        getdata(f->vfs).logger->tinc(P_OPF_READ, end-start);
        if (rc < len) {
            memset(buf, 0, len-rc);
            return SQLITE_IOERR_SHORT_READ;
        } else {
            return SQLITE_OK;
        }
    }
}

static int Write(sqlite3_file *file, const void *buf, int len, sqlite_int64 off)
{
    auto f = (cephfssqlite_file*)file;
    auto start = ceph::coarse_mono_clock::now();
    df(5) << off << "~" << len << dendl;

    if (int rc = f->io.client->write(f->fd, (char *)buf, len, off); rc < 0) {
        df(5) << "write failed: " << cpp_strerror(rc) << dendl;
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
    auto f = (cephfssqlite_file*)file;
    auto start = ceph::coarse_mono_clock::now();
    df(5) << size << dendl;

    if (int rc = f->io.client->truncate(f->loc.path.c_str(), size, f->loc.default_perms); rc < 0) {
        df(5) << "truncate failed: " << cpp_strerror(rc) << dendl;
        return SQLITE_IOERR;
    }

    auto end = ceph::coarse_mono_clock::now();
    getdata(f->vfs).logger->tinc(P_OPF_TRUNCATE, end-start);
    return SQLITE_OK;
}

static int Sync(sqlite3_file *file, int flags)
{
    auto f = (cephfssqlite_file*)file;
    auto start = ceph::coarse_mono_clock::now();
    df(5) << flags << dendl;

    if (int rc = f->io.client->fsync(f->fd, true); rc < 0) {
        df(5) << "failed: " << cpp_strerror(rc) << dendl;
        return SQLITE_IOERR;
    }

    df(5) << " = 0" << dendl;

    auto end = ceph::coarse_mono_clock::now();
    getdata(f->vfs).logger->tinc(P_OPF_SYNC, end-start);
    return SQLITE_OK;
}


static int FileSize(sqlite3_file *file, sqlite_int64 *osize)
{
    auto f = (cephfssqlite_file*)file;
    auto start = ceph::coarse_mono_clock::now();
    df(5) << dendl;

    struct stat stbuf;
    if (int rc = f->io.client->stat(f->loc.path.c_str(), &stbuf, f->loc.default_perms); rc < 0) {
        df(5) << "stat failed: " << cpp_strerror(rc) << dendl;
        return SQLITE_NOTFOUND;
    }
    uint64_t size = stbuf.st_size;
    *osize = (sqlite_int64)size;

    df(5) << "= " << size << dendl;

    auto end = ceph::coarse_mono_clock::now();
    getdata(f->vfs).logger->tinc(P_OPF_FILESIZE, end-start);
    return SQLITE_OK;
}


static bool parsepath(std::string_view path, struct cephfssqlite_fileloc* fileloc)
{
    std::regex regex{"^/*([[:alnum:]-_.]+)/([[:alnum:]-._/]+)$"};

    std::cmatch cm;
    if (std::regex_match(path.data(), cm, regex)) {
        fileloc->fsname = cm[1];
        fileloc->path = cm[2];
        return true;
    }
    return false;
}

static int makecephfs(sqlite3_vfs* vfs, cephfssqlite_fileloc* loc, cephfssqlite_fileio* io)
{
    auto&& appd = getdata(vfs);
    auto& cct = appd.cct;

    dv(10) << loc << dendl;

    auto it = appd.client_map.find(loc->fsname);
    if (it != appd.client_map.end()) {
        io->client = it->second.get();
        loc->default_perms = Client::pick_my_perms(cct.get());
    } else {
        if (!cct->_log->is_started()) {
            cct->_log->start();
        }
        std::shared_ptr<ceph::async::io_context_pool> icp = get_icp(cct.get());
        {
            MonClient mc_bootstrap(cct.get(), icp->get_io_context());
            int ret = mc_bootstrap.get_monmap_and_config();
            if (ret < 0) {
                return ret;
            }
        }
        common_init_finish(cct.get());

        MonClient *monclient = new MonClient(cct.get(), icp->get_io_context());
        if (monclient->build_initial_monmap() < 0) {
            return -1;
        }

        Messenger *messenger = Messenger::create_client_messenger(cct.get(), "client");

        std::shared_ptr<Client> client =
                std::make_shared<StandaloneClient>(messenger, monclient, icp->get_io_context());
        if (!client.get()) {
            return -1;
        }
        if (messenger->start() != 0) {
            return -1;
        }

        int ret = client.get()->init();
        if (ret) {
            return ret;
        }
        loc->default_perms = Client::pick_my_perms(cct.get());
        ret = client.get()->mount("/", loc->default_perms, false, "");
        if (ret) {
            ceph_assert(0);
        }

        appd.client_map[loc->fsname] = client;
        io->client = client.get();
    }
    return 0;
}

static int SectorSize(sqlite3_file* sf)
{
    static const int size = 65536;
    auto start = ceph::coarse_mono_clock::now();
    auto f = (cephfssqlite_file*)sf;
    df(5) << " = " << size << dendl;
    auto end = ceph::coarse_mono_clock::now();
    getdata(f->vfs).logger->tinc(P_OPF_SECTORSIZE, end-start);
    return size;
}

static int FileControl(sqlite3_file* sf, int op, void *arg)
{
    auto f = (cephfssqlite_file*)sf;
    auto start = ceph::coarse_mono_clock::now();
    df(5) << op << ", " << arg << dendl;
    auto end = ceph::coarse_mono_clock::now();
    getdata(f->vfs).logger->tinc(P_OPF_FILECONTROL, end-start);
    return SQLITE_NOTFOUND;
}

static int DeviceCharacteristics(sqlite3_file* sf)
{
    auto f = (cephfssqlite_file*)sf;
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

    /* we are not going to create temporary files */
    if (name == NULL) {
        dv(-1) << " cannot open temporary database" << dendl;
        return SQLITE_CANTOPEN;
    }
    auto path = std::string_view(name);
    if (path == ":memory:"sv) {
        dv(-1) << " cannot open temporary database" << dendl;
        return SQLITE_IOERR;
    }

    dv(5) << path << " flags=" << std::hex << flags << dendl;

    auto f = new (file)cephfssqlite_file();
    f->vfs = vfs;
    if (!parsepath(path, &f->loc)) {
        ceph_assert(0); /* xFullPathname validates! */
    }
    f->flags = flags;

    if (int rc = makecephfs(vfs, &f->loc, &f->io); rc < 0) {
        f->~cephfssqlite_file();
        dv(5) << "cannot open striper" << dendl;
        return SQLITE_IOERR;
    }

    int rc = f->io.client->open(f->loc.path.c_str(), O_CREAT | O_RDWR, f->loc.default_perms);
    if (rc < 0) {
        dv(10) << "cannot open fs: " << cpp_strerror(rc) << dendl;
        return rc;
    }
    f->fd = rc;

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
    dv(5) << "'" << path << "', " << dsync << dendl;

    cephfssqlite_fileloc fileloc;
    if (!parsepath(path, &fileloc)) {
        dv(5) << "path does not parse!" << dendl;
        return SQLITE_NOTFOUND;
    }

    cephfssqlite_fileio io;
    if (int rc = makecephfs(vfs, &fileloc, &io); rc < 0) {
        dv(5) << "cannot open striper" << dendl;
        return SQLITE_IOERR;
    }

    if (int rc = io.client->unlink(fileloc.path.c_str(), fileloc.default_perms); rc < 0) {
        return SQLITE_IOERR;
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
    dv(5) << path << " " << std::hex << flags << dendl;

    cephfssqlite_fileloc fileloc;
    if (!parsepath(path, &fileloc)) {
        dv(5) << "path does not parse!" << dendl;
        return SQLITE_NOTFOUND;
    }

    cephfssqlite_fileio io;
    if (int rc = makecephfs(vfs, &fileloc, &io); rc < 0) {
        dv(5) << "cannot open striper" << dendl;
        return SQLITE_IOERR;
    }

    if (int rc = io.client->open(fileloc.path.c_str(), flags, fileloc.default_perms); rc < 0) {
        if (rc == -ENOENT) {
            *result = 0;
            return SQLITE_OK;
        } else {
            dv(10) << "cannot open striper: " << cpp_strerror(rc) << dendl;
            *result = 0;
            return SQLITE_IOERR;
        }
    }

    struct stat stbuf;
    if (int rc = io.client->stat(fileloc.path.c_str(), &stbuf, fileloc.default_perms); rc < 0) {
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

    dv(5) << "1: " <<  path << dendl;

    cephfssqlite_fileloc fileloc;
    if (!parsepath(path, &fileloc)) {
        dv(5) << "path does not parse!" << dendl;
        return SQLITE_NOTFOUND;
    }
    dv(5) << " parsed " << fileloc << dendl;

    auto p = fmt::format("{}/{}", fileloc.fsname, fileloc.path);
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
    dv(5) << time << dendl;

    auto t = ceph_clock_now();
    *time = t.to_msec() + 2440587.5*86400000; /* julian days since 1970 converted to ms */

    auto end = ceph::coarse_mono_clock::now();
    getdata(vfs).logger->tinc(P_OP_CURRENTTIME, end-start);
    return SQLITE_OK;
}

LIBCEPHFSSQLITE_API int cephfssqlite_setcct(CephContext* cct, char** ident)
{
    ldout(cct, 1) << "cct: " << cct << dendl;

    if (sqlite3_api == nullptr) {
        lderr(cct) << "API violation: must have sqlite3 init libcephfssqlite" << dendl;
        return -EINVAL;
    }

    auto vfs = sqlite3_vfs_find("cephfs");
    if (!vfs) {
        lderr(cct) << "API violation: must have sqlite3 init libcephfssqlite" << dendl;
        return -EINVAL;
    }

    auto& appd = getdata(vfs);
    appd.cct = cct;
    if (int rc = appd.setup_perf(); rc < 0) {
        appd.cct = nullptr;
        return rc;
    }
    if (int rc = appd.init_cluster(); rc < 0) {
        appd.cct = nullptr;
        return rc;
    }

    ldout(cct, 1) << "complete" << dendl;

    return 0;
}

static void f_perf(sqlite3_context* ctx, int argc, sqlite3_value** argv)
{
    auto vfs = (sqlite3_vfs*)sqlite3_user_data(ctx);
    dv(10) << dendl;
    auto&& appd = getdata(vfs);
    JSONFormatter f(false);
    f.open_object_section("ceph_perf");
    appd.logger->dump_formatted(&f, false);
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
    dv(10) << dendl;
    JSONFormatter f(false);
    f.open_object_section("ceph_status");
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
    auto vfs = sqlite3_vfs_find("cephfs");
    if (!vfs) {
        ceph_abort("cephfs vfs not found");
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

static void cephfssqlite_atexit()
{
    if (auto vfs = sqlite3_vfs_find("cephfs"); vfs) {
        if (vfs->pAppData) {
//            auto&& appd = getdata(vfs);
//            delete &appd;
            vfs->pAppData = nullptr;
        }
    }
}

LIBCEPHFSSQLITE_API int sqlite3_cephfssqlite_init(sqlite3* db, char** err, const sqlite3_api_routines* api)
{
    SQLITE_EXTENSION_INIT2(api);

    auto vfs = sqlite3_vfs_find("cephfs");
    if (!vfs) {
        vfs = (sqlite3_vfs*) calloc(1, sizeof(sqlite3_vfs));
        auto appd = new cephfssqlite_appdata;
        vfs->iVersion = 2;
        vfs->szOsFile = sizeof(struct cephfssqlite_file);
        vfs->mxPathname = 4096;
        vfs->zName = "cephfs";
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

    if (int rc = std::atexit(cephfssqlite_atexit); rc) {
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