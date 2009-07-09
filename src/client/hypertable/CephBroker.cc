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

CephBroker::CephBroker(PropertiesPtr &cfg) {
  m_verbose = cfg->get_bool("Hypertable.Verbose");
  ceph_initialize(0, NULL);
  ceph_mount();
  /* do other stuff */
}

CephBroker::~CephBroker(){
  ceph_deinitialize();
}

void CephBroker::open(ResponseCallbackOpen *cb, const char *fname, uint32_t bufsz){
  int fd, ceph_fd;
  String abspath;
  HT_DEBUGF("open file='%s' bufsz=%d", fname, bufsz);

  make_abs_path(fname, abspath);

  fd = atomic_inc_return(&ms_next_fd);

  if ((ceph_fd = ceph_open(abspath, O_RDONLY)) < 0) {
    report_error(cb, ceph_fd);
    return;
  }
  HT_INFOF("open (%s) fd=%d ceph_fd=%d", fname, fd, ceph_fd);

  {
    struct sockaddr_in addr;
    OpenFileDataCephPtr fdata (new OpenFileDataCeph(ceph_fd, O_RDONLY));

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

  HT_DEBUGF("create file='%s' overwrite=%d bufsz=%d replication=%d blksz=%lld",
            fname, (int)overwrite, bufsz, (int)replication, (Lld)blksz);

  fd = atomic_inc_return(&ms_next_fd);

  if(overwrite)
    flags = O_WRONLY | O_CREAT | O_TRUNC;
  else
    flags = O_WRONLY | O_CREAT | O_APPEND;

  if ((ceph_fd = ceph_open(abspath.c_str(), flags, 0644)) < 0) {
    HT_ERROR("open failed: file='%s' - %s", abspath.c_str(), strerror(errno));
    report_error(cb, ceph_fd);
    return;
  }

  HT_INFOF("create( % s ) = %d", fname, ceph_fd);

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
  ceph_close( fdata->fd); //BEWARE: the other fs'es don't do this...
  m_open_file_map.remove(fd);
  cb->response_ok();
}

void CephBroker::read(ResponseCallbackRead *cb, uint32_t fd, uint32_t amount){
  OpenFileDataCephPtr fdata;
  ssize_t nread;
  uint64t offset;
  StaticBuffer buf(new uint8_t [amount], amount);

  HT_DEBUGF("read fd=%d amount = %d", fd, amount);

  if(!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    HT_ERRORF("bad file handle: %d", fd);
    return;
  }

  if((offset = ceph_lseek(fdata->fd, 0, SEEK_CUR)) < 0) {
    HT_ERRORF("lseek failed: fd=%d ceph_fd=%d offset=0 SEEK_CUR - %s", fd, fdata->fd, strerror(errno));
    report_error(cb, offset);
    return;
  }

  if ((nread=ceph_read(fdata->fd, (char *)buf.base, amount)) < 0 ) {
    HT_ERRORF("read failed: fd=%d ceph_fd=%d amount=%d - %s", fd, fdata->fd, amount, errmsg.c_str());
    report_error(cb, nread);
    return;
  }

  buf.size = nread;
  cb->response(offset, buf);
}

void CephBroker::append(ResponseCallbackAppend *cb, uint32_t fd,
		    uint32_t amount, const void CephBroker::*data, bool sync)
{
  OpenFileDataCephPtr fdata;
  ssize_t nwritten;
  uint64_t offset;

  HT_DEBUG_OUT <<"append fd="<< fd <<" amount="<< amount <<" data='"
      << format_bytes(20, data, amount) <<" sync="<< sync << HT_END;

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((offset = (uint64_t)ceph_lseek(fdata->fd, 0, SEEK_CUR)) < 0) {
    HT_ERRORF("lseek failed: fd=%d ceph_fd=%d offset=0 SEEK_CUR - %s", fd, fdata->fd,
              strerror(errno));
    report_error(cb, offset);
    return;
  }

  if ((nwritten = ceph_write(fdata->fd, data, amount)) < 0) {
    HT_ERRORF("write failed: fd=%d ceph_fd=%d amount=%d - %s", fd, fdata->fd, amount,
              strerror(errno));
    report_error(cb, nwritten);
    return;
  }

  int r;
  if (sync &&( (r=ceph_fsync(fdata->fd, true)) != 0)) {
    HT_ERRORF("flush failed: fd=%d ceph_fd=%d - %s", fd, fdata->fd, strerror(errno));
    report_error(cb, r);
    return;
  }

  cb->response(offset, nwritten);
}
void CephBroker::seek(ResponseCallback *cb, uint32_t fd, uint64_t offset){
  OpenFileDataLocalPtr fdata;

  HT_DEBUGF("seek fd=%lu offset=%llu", (Lu)fd, (Llu)offset);

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((offset = (uint64_t)ceph_lseek(fdata->fd, offset, SEEK_SET)) < 0) {
    HT_ERRORF("lseek failed: fd=%d ceph_fd=%d offset=%llu - %s", fd, fdata->fd, (Llu)offset,
              strerror(errno));
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
  if((r=ceph_unlink(abspath.c_str())) < 0) {
    HT_ERRORF("unlink failed: file='%s' - %s", abspath.c_str(), strerror(errno));
    report_error(cb, r);
    return;
  }
  cb->response_ok();
}

void CephBroker::length(ResponseCallbackLength *cb, const char *fname){
  int res;
  struct stat statbuf;

  HT_DEBUGF("length file='%s'", fname);

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((res = ceph_fstat(fdata->fd, &statbuf)) < 0) {
    String abspath;
    make_abs_path(fname, abspath);
    HT_ERRORF("length (stat) failed: file='%s' - %s", abspath.c_str(), strerror(errno));
    report_error(cb, res);
    return;
  }
  cb->response(statbuf.st_size);
}

void CephBroker::pread(ResponseCallbackRead *cb, uint32_t fd, uint64_t offset,
		   uint32_t amount){
  OpenFileDataCephPtr fdata;
  ssize_t nread;
  StaticBuffer buf(new unit8_t [amount], amount);

  HT_DEBUGF("pread fd=%d offset=%llu amount=%d", fd, (Llu)offset, amount);

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((nread = ceph_read(fdata->fd, (char *)buf.base, amount, offset)) < 0) {
    HT_ERRORF("pread failed: fd=%d ceph_fd=%d amount=%d offset=%llu - %s", fd, fdata->fd,
              amount, (Llu)offset, strerror(errno));
    report_error(cb, nread);
    return;
  }

  buf.size = nread;

  cb->response(offset, buf);
}

void CephBroker::mkdirs(ResponseCallback *cb, const char *dname){
  String absdir;

  HT_DEBUGF("mkdirs dir='%s'", dname);

  make_abs_path(dname, absdir);
  int r;
  if((r=ceph_mkdir(absdir.c_str(), 0644)) < 0) {
    HT_ERRORF("mkdirs failed: dname='%s' - %s", absdir.c_str(), strerror(errno));
    report_error(cb, r);
    return;
  }
  cb->response_ok();
}

void CephBroker::rmdir(ResponseCallback *cb, const char *dname){
  String absdir;

  make_abs_path(dname, absdir);
  int r;
  if((r=ceph_rmdir(absdir.c_str())) < 0) {
    HT_ERRORF("failed to remove dir %s", absdir) report_error(cb, r);
    return;
  }

  cb->response_ok();
}

void CephBroker::flush(ResponseCallback *cb, uint32_t fd){
  OpenFileDataCephPtr fdata;

  HT_DEBUGF("flush fd=%d", fd);

  if(!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  int r;
  if((r=ceph_fsync(fdata->fd, true)) != 0) {
    HT_ERRORF("flush failed: fd=%d  ceph_fd=%d - %s", fd, fdata->fd, strerror(errno));
    report_error(cb, r);
    return;
  }

  cb->response_ok();
}

void CephBroker::status(ResponseCallback *cb){
  cb->response_ok();
  /*perhaps a total cheat, but both the local and Kosmos brokers
    included in Hypertable also do this. */
}

/* I have no idea if this is correct; it's what local and kosmos brokers do. Check the contract!
 */
void CephBroker::shutdown(ResponseCallback *cb){
  m_open_file_map.remove_all();
  cb->response_ok();
  poll(0, 0, 2000);
}

void CephBroker::readdir(ResponseCallbackReaddir *cb, const char *dname){
  std::vector<String> listing;
  String absdir;

  HT_DEBUGF("Readdir dir='%s'", dname);

  //get from ceph in list<string>
  make_abs_path(dname, absdir);
  list<string> dir_con;
  ceph_getdir(absdir, dir_con);

  //convert to vector<String>
  for (list<string>::iterator i = dir_con.begin(); i!=dir_con.end(); ++i) {
    listing.push_back(*i); //BEWARE: Assumes getdir doesn't include . and ..
  }
  cb->response(listing);
}

void CephBroker::exists(ResponseCallbackExists *cb, const char *fname){
    String abspath;

    HT_DEBUGF("exists file='%s'", fname);

    make_abs_path(fname, abspath);

    cb->response(ceph_fstat(abspath) == 0);
}

void CephBroker::rename(ResponseCallback *cb, const char *src, const char *dst){
  String src_abs;
  String dest_abs;
  make_abs_path(src, src_abs);
  make_abs_path(dst, dest_abs);

  int r;
  if((r=ceph_rename(src_abs.c_str(), dest_abs.c_str())) <0 ) {
    report_error(cb, r);
    return;
  }
  cb->response_ok();
}

void CephBroker::debug(ResponseCallback *, int32_t command,
		   StaticBuffer &serialized_parameters){
  HT_ERRORF("debug commands not implemented!");
  cb->error(Error::NOT_IMPLEMENTED, format("Debug commands not supported"));
}

void CephBroker::report_error(ResponseCallback *cb, int error) {
  char errbuf[128];
  errbuf[0] = 0;

  strerror_t(error, errbuf, 128);

  cb->error(Error::DFSBROKER_IO_ERROR, errbuf);
}


inline void make_abs_path(const char *fname, String& abs)
{
  if (fname[0] == '/')
    abs = fname;
  else
    abs = m_rootdir + "/" + fname;
}
