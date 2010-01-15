// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "Common/Compat.h"
#include <cerrno>
#include <string>

extern "C" {
#include <fcntl.h>
#include <poll.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
}

#include "Common/FileUtils.h"
#include "Common/System.h"
#include "CephBroker.h"

using namespace Hypertable;

atomic_t CephBroker::ms_next_fd = ATOMIC_INIT(0);

CephBroker::CephBroker(PropertiesPtr& cfg) {
  m_verbose = cfg->get_bool("Hypertable.Verbose");
  m_root_dir = "";
  //construct an arguments array
  const char *argv[10];
  int argc = 0;
  argv[argc++] = "cephBroker";
  argv[argc++] = "-m";
  argv[argc++] = (cfg->get_str("CephBroker.MonAddr").c_str());
  /*
  // For Ceph debugging, uncomment these lines
  argv[argc++] = "--debug_client";
  argv[argc++] = "0";
  argv[argc++] = "--debug_ms";
  argv[argc++] = "0";
  argv[argc++] = "--lockdep";
  argv[argc++] = "0"; */

  HT_INFO("Calling ceph_initialize");
  ceph_initialize(argc, argv);
  HT_INFO("Calling ceph_mount");
  ceph_mount();
  HT_INFO("Returning from constructor");
}

CephBroker::~CephBroker() {
  ceph_deinitialize();
}

void CephBroker::open(ResponseCallbackOpen *cb, const char *fname, uint32_t bufsz) {
  int fd, ceph_fd;
  String abspath;
  HT_DEBUGF("open file='%s' bufsz=%d", fname, bufsz);

  make_abs_path(fname, abspath);

  fd = atomic_inc_return(&ms_next_fd);

  if ((ceph_fd = ceph_open(abspath.c_str(), O_RDONLY)) < 0) {
    report_error(cb, -ceph_fd);
    return;
  }
  HT_INFOF("open (%s) fd=%d ceph_fd=%d", fname, fd, ceph_fd);

  {
    struct sockaddr_in addr;
    OpenFileDataCephPtr fdata (new OpenFileDataCeph(abspath, ceph_fd, O_RDONLY));

    cb->get_address(addr);

    m_open_file_map.create(fd, addr, fdata);

    cb->response(fd);
  }
}

void CephBroker::create(ResponseCallbackOpen *cb, const char *fname, bool overwrite,
			int32_t bufsz, int16_t replication, int64_t blksz){
  int fd, ceph_fd;
  int flags;
  String abspath;

  make_abs_path(fname, abspath);
  HT_DEBUGF("create file='%s' overwrite=%d bufsz=%d replication=%d blksz=%lld",
            fname, (int)overwrite, bufsz, (int)replication, (Lld)blksz);

  fd = atomic_inc_return(&ms_next_fd);

  if (overwrite)
    flags = O_WRONLY | O_CREAT | O_TRUNC;
  else
    flags = O_WRONLY | O_CREAT | O_APPEND;

  //make sure the directories in the path exist
  String directory = abspath.substr(0, abspath.rfind('/'));
  int r;
  HT_INFOF("Calling mkdirs on %s", directory.c_str());
  if((r=ceph_mkdirs(directory.c_str(), 0644)) < 0 && r!=-EEXIST) {
    HT_ERRORF("create failed on mkdirs: dname='%s' - %d", directory.c_str(), -r);
    report_error(cb, -r);
    return;
  }

  //create file
  if ((ceph_fd = ceph_open(abspath.c_str(), flags, 0644)) < 0) {
    HT_ERRORF("open failed: file=%s - %s",  abspath.c_str(), strerror(-ceph_fd));
    report_error(cb, ceph_fd);
    return;
  }

  HT_INFOF("create %s  = %d", fname, ceph_fd);

  {
    struct sockaddr_in addr;
    OpenFileDataCephPtr fdata (new OpenFileDataCeph(fname, ceph_fd, O_WRONLY));

    cb->get_address(addr);

    m_open_file_map.create(fd, addr, fdata);

    cb->response(fd);
  }
}

void CephBroker::close(ResponseCallback *cb, uint32_t fd) {
  if (m_verbose) {
    HT_INFOF("close fd=%d", fd);
  }
  OpenFileDataCephPtr fdata;
  m_open_file_map.get(fd, fdata);
  m_open_file_map.remove(fd);
  cb->response_ok();
}

void CephBroker::read(ResponseCallbackRead *cb, uint32_t fd, uint32_t amount) {
  OpenFileDataCephPtr fdata;
  ssize_t nread;
  uint64_t offset;
  StaticBuffer buf(new uint8_t [amount], amount);

  HT_DEBUGF("read fd=%d amount = %d", fd, amount);

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    snprintf(errbuf, sizeof(errbuf), "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    HT_ERRORF("bad file handle: %d", fd);
    return;
  }

  if ((offset = ceph_lseek(fdata->fd, 0, SEEK_CUR)) < 0) {
    HT_ERRORF("lseek failed: fd=%d ceph_fd=%d offset=0 SEEK_CUR - %s", fd, fdata->fd, strerror(-offset));
    report_error(cb, offset);
    return;
  }

  if ((nread = ceph_read(fdata->fd, (char *)buf.base, amount)) < 0 ) {
    HT_ERRORF("read failed: fd=%d ceph_fd=%d amount=%d", fd, fdata->fd, amount);
    report_error(cb, -nread);
    return;
  }

  buf.size = nread;
  cb->response(offset, buf);
}

void CephBroker::append(ResponseCallbackAppend *cb, uint32_t fd,
			uint32_t amount, const void *data, bool sync)
{
  OpenFileDataCephPtr fdata;
  ssize_t nwritten;
  uint64_t offset;

  HT_DEBUG_OUT << "append fd="<< fd <<" amount="<< amount <<" data='"
	       << format_bytes(20, data, amount) <<" sync="<< sync << HT_END;

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    snprintf(errbuf, sizeof(errbuf), "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((offset = (uint64_t)ceph_lseek(fdata->fd, 0, SEEK_CUR)) < 0) {
    HT_ERRORF("lseek failed: fd=%d ceph_fd=%d offset=0 SEEK_CUR - %s", fd, fdata->fd,
              strerror(-offset));
    report_error(cb, offset);
    return;
  }

  if ((nwritten = ceph_write(fdata->fd, (const char *)data, amount)) < 0) {
    HT_ERRORF("write failed: fd=%d ceph_fd=%d amount=%d - %s", fd, fdata->fd, amount,
              strerror(-nwritten));
    report_error(cb, -nwritten);
    return;
  }

  int r;
  if (sync && ((r = ceph_fsync(fdata->fd, true)) != 0)) {
    HT_ERRORF("flush failed: fd=%d ceph_fd=%d - %s", fd, fdata->fd, strerror(errno));
    report_error(cb, r);
    return;
  }

  cb->response(offset, nwritten);
}

void CephBroker::seek(ResponseCallback *cb, uint32_t fd, uint64_t offset) {
  OpenFileDataCephPtr fdata;

  HT_DEBUGF("seek fd=%lu offset=%llu", (Lu)fd, (Llu)offset);

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    snprintf(errbuf, sizeof(errbuf), "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }
  int r;
  if ((r = (uint64_t)ceph_lseek(fdata->fd, offset, SEEK_SET)) < 0) {
    HT_ERRORF("lseek failed: fd=%d ceph_fd=%d offset=%llu - %s", fd, fdata->fd,
	      (Llu)offset, strerror(-r));
    report_error(cb, offset);
    return;
  }

  cb->response_ok();
}

void CephBroker::remove(ResponseCallback *cb, const char *fname) {
  String abspath;
  
  HT_DEBUGF("remove file='%s'", fname);
  
  make_abs_path(fname, abspath);
  
  int r;
  if ((r = ceph_unlink(abspath.c_str())) < 0) {
    HT_ERRORF("unlink failed: file='%s' - %s", abspath.c_str(), strerror(-r));
    report_error(cb, r);
    return;
  }
  cb->response_ok();
}

void CephBroker::length(ResponseCallbackLength *cb, const char *fname) {
  int r;
  struct stat statbuf;

  HT_DEBUGF("length file='%s'", fname);

  if ((r = ceph_lstat(fname, &statbuf)) < 0) {
    String abspath;
    make_abs_path(fname, abspath);
    HT_ERRORF("length (stat) failed: file='%s' - %s", abspath.c_str(), strerror(-r));
    report_error(cb,- r);
    return;
  }
  cb->response(statbuf.st_size);
}

void CephBroker::pread(ResponseCallbackRead *cb, uint32_t fd, uint64_t offset,
		       uint32_t amount) {
  OpenFileDataCephPtr fdata;
  ssize_t nread;
  StaticBuffer buf(new uint8_t [amount], amount);

  HT_DEBUGF("pread fd=%d offset=%llu amount=%d", fd, (Llu)offset, amount);

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    snprintf(errbuf, sizeof(errbuf), "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((nread = ceph_read(fdata->fd, (char *)buf.base, amount, offset)) < 0) {
    HT_ERRORF("pread failed: fd=%d ceph_fd=%d amount=%d offset=%llu - %s", fd, fdata->fd,
              amount, (Llu)offset, strerror(-nread));
    report_error(cb, nread);
    return;
  }

  buf.size = nread;

  cb->response(offset, buf);
}

void CephBroker::mkdirs(ResponseCallback *cb, const char *dname) {
  String absdir;

  HT_DEBUGF("mkdirs dir='%s'", dname);

  make_abs_path(dname, absdir);
  int r;
  if((r=ceph_mkdirs(absdir.c_str(), 0644)) < 0 && r!=-EEXIST) {
    HT_ERRORF("mkdirs failed: dname='%s' - %d", absdir.c_str(), -r);
    report_error(cb, -r);
    return;
  }
  cb->response_ok();
}

void CephBroker::rmdir(ResponseCallback *cb, const char *dname) {
  String absdir;
  int r;

  make_abs_path(dname, absdir);
  if((r = rmdir_recursive(absdir.c_str())) < 0) {
      HT_ERRORF("failed to remove dir %s, got error %d", absdir.c_str(), r);
      report_error(cb, -r);
      return;
  }
  cb->response_ok();
}

int CephBroker::rmdir_recursive(const char *directory) {
  DIR *dirp;
  struct dirent de;
  struct stat st;
  int r;
  if ((r = ceph_opendir(directory, &dirp) < 0))
    return r; //failed to open
  while ((r = ceph_readdirplus_r(dirp, &de, &st, 0)) > 0) {
    String new_dir = de.d_name;
    if(!(new_dir.compare(".")==0 || new_dir.compare("..")==0)) {
      new_dir = directory;
      new_dir += '/';
      new_dir += de.d_name;
      if (S_ISDIR(st.st_mode)) { //it's a dir, clear it out...
	if((r=rmdir_recursive(new_dir.c_str())) < 0) return r;
      } else { //delete this file
	if((r=ceph_unlink(new_dir.c_str())) < 0) return r;
      }
    }
  }
  if (r < 0) return r; //we got an error
  if ((r = ceph_closedir(dirp)) < 0) return r;
  return ceph_rmdir(directory);
}

void CephBroker::flush(ResponseCallback *cb, uint32_t fd) {
  OpenFileDataCephPtr fdata;

  HT_DEBUGF("flush fd=%d", fd);

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    snprintf(errbuf, sizeof(errbuf), "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  int r;
  if ((r = ceph_fsync(fdata->fd, true)) != 0) {
    HT_ERRORF("flush failed: fd=%d  ceph_fd=%d - %s", fd, fdata->fd, strerror(-r));
    report_error(cb, -r);
    return;
  }

  cb->response_ok();
}

void CephBroker::status(ResponseCallback *cb) {
  cb->response_ok();
  /*perhaps a total cheat, but both the local and Kosmos brokers
    included in Hypertable also do this. */
}

void CephBroker::shutdown(ResponseCallback *cb) {
  m_open_file_map.remove_all();
  cb->response_ok();
  poll(0, 0, 2000);
}

void CephBroker::readdir(ResponseCallbackReaddir *cb, const char *dname) {
  std::vector<String> listing;
  String absdir;

  HT_DEBUGF("Readdir dir='%s'", dname);

  //get from ceph in a buffer
  make_abs_path(dname, absdir);

  DIR *dirp;
  ceph_opendir(absdir.c_str(), &dirp);
  int r;
  int buflen = 100; //good default?
  char *buf = new char[buflen];
  String *ent;
  int bufpos;
  while (1) {
    r = ceph_getdnames(dirp, buf, buflen);
    if (r==-ERANGE) { //expand the buffer
      delete buf;
      buflen *= 2;
      buf = new char[buflen];
      continue;
    }
    if (r<=0) break;

    //if we make it here, we got at least one name, maybe more
    bufpos = 0;
    while (bufpos<r) {//make new strings and add them to listing
      ent = new String(buf+bufpos);
      if (ent->compare(".") && ent->compare(".."))
	listing.push_back(*ent);
      bufpos+=ent->size()+1;
      delete ent;
    }
  }
  delete buf;
  ceph_closedir(dirp);

  if (r < 0) report_error(cb, -r); //Ceph shouldn't return r<0 on getdnames
  //(except for ERANGE) so if it happens this is bad
  cb->response(listing);
}

void CephBroker::exists(ResponseCallbackExists *cb, const char *fname) {
  String abspath;
  struct stat statbuf;
  
  HT_DEBUGF("exists file='%s'", fname);
  make_abs_path(fname, abspath);
  cb->response(ceph_lstat(abspath.c_str(), &statbuf) == 0);
}

void CephBroker::rename(ResponseCallback *cb, const char *src, const char *dst) {
  String src_abs;
  String dest_abs;
  int r;

  make_abs_path(src, src_abs);
  make_abs_path(dst, dest_abs);
  if ((r = ceph_rename(src_abs.c_str(), dest_abs.c_str())) <0 ) {
    report_error(cb, r);
    return;
  }
  cb->response_ok();
}

void CephBroker::debug(ResponseCallback *cb, int32_t command,
		       StaticBuffer &serialized_parameters) {
  HT_ERROR("debug commands not implemented!");
  cb->error(Error::NOT_IMPLEMENTED, format("Debug commands not supported"));
}

void CephBroker::report_error(ResponseCallback *cb, int error) {
  char errbuf[128];
  errbuf[0] = 0;

  strerror_r(error, errbuf, 128);

  cb->error(Error::DFSBROKER_IO_ERROR, errbuf);
}


