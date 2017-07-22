/** -*- C++ -*-
 * Copyright (C) 2009-2011 New Dream Network
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 *
 * Authors:
 * Gregory Farnum <gfarnum@gmail.com>
 * Colin McCabe <cmccabe@alumni.cmu.edu>
 */

#include "Common/Compat.h"

#include "CephBroker.h"
#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Filesystem.h"
#include "Common/System.h"

#include <cephfs/libcephfs.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <string>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

using namespace Hypertable;

std::atomic<int> CephBroker::ms_next_fd{0};

/* A thread-safe version of strerror */
static std::string cpp_strerror(int err)
{
  char buf[128];
  if (err < 0)
    err = -err;
  std::ostringstream oss;
  oss << strerror_r(err, buf, sizeof(buf));
  return oss.str();
}

OpenFileDataCeph::OpenFileDataCeph(struct ceph_mount_info *cmount_, const String& fname,
				   int _fd, int _flags) 
  : cmount(cmount_), fd(_fd), flags(_flags), filename(fname)
{
}

OpenFileDataCeph::~OpenFileDataCeph() {
  ceph_close(cmount, fd);
}

CephBroker::CephBroker(PropertiesPtr& cfg)
  : cmount(NULL)
{
  int ret;
  String id(cfg->get_str("CephBroker.Id"));
  m_verbose = cfg->get_bool("Hypertable.Verbose");
  m_root_dir = cfg->get_str("CephBroker.RootDir");
  String mon_addr(cfg->get_str("CephBroker.MonAddr"));

  HT_INFO("Calling ceph_create");
  ret = ceph_create(&cmount, id.empty() ? NULL : id.c_str());
  if (ret) {
    throw Hypertable::Exception(ret, "ceph_create failed");
  }
  ret = ceph_conf_set(cmount, "mon_host", mon_addr.c_str());
  if (ret) {
    ceph_shutdown(cmount);
    throw Hypertable::Exception(ret, "ceph_conf_set(mon_addr) failed");
  }

  // For Ceph debugging, uncomment these lines
  //ceph_conf_set(cmount, "debug_client", "1");
  //ceph_conf_set(cmount, "debug_ms", "1");

  HT_INFO("Calling ceph_mount");
  ret = ceph_mount(cmount, m_root_dir.empty() ? NULL : m_root_dir.c_str());
  if (ret) {
    ceph_shutdown(cmount);
    throw Hypertable::Exception(ret, "ceph_mount failed");
  }
  HT_INFO("Mounted Ceph filesystem.");
}

CephBroker::~CephBroker()
{
  ceph_shutdown(cmount);
  cmount = NULL;
}

void CephBroker::open(ResponseCallbackOpen *cb, const char *fname,
		      uint32_t flags, uint32_t bufsz) {
  int fd, ceph_fd;
  String abspath;
  HT_DEBUGF("open file='%s' bufsz=%d", fname, bufsz);

  make_abs_path(fname, abspath);

  fd = atomic_inc_return(&ms_next_fd);

  if ((ceph_fd = ceph_open(cmount, abspath.c_str(), O_RDONLY, 0)) < 0) {
    report_error(cb, -ceph_fd);
    return;
  }
  HT_INFOF("open (%s) fd=%d ceph_fd=%d", fname, fd, ceph_fd);

  {
    struct sockaddr_in addr;
    OpenFileDataCephPtr fdata(new OpenFileDataCeph(cmount, abspath, ceph_fd, O_RDONLY));

    cb->get_address(addr);

    m_open_file_map.create(fd, addr, fdata);

    cb->response(fd);
  }
}

void CephBroker::create(ResponseCallbackOpen *cb, const char *fname, uint32_t flags,
			int32_t bufsz, int16_t replication, int64_t blksz){
  int fd, ceph_fd;
  int oflags;
  String abspath;

  make_abs_path(fname, abspath);
  HT_DEBUGF("create file='%s' flags=%u bufsz=%d replication=%d blksz=%lld",
            fname, flags, bufsz, (int)replication, (Lld)blksz);

  fd = atomic_inc_return(&ms_next_fd);

  if (flags & Filesystem::OPEN_FLAG_OVERWRITE)
    oflags = O_WRONLY | O_CREAT | O_TRUNC;
  else
    oflags = O_WRONLY | O_CREAT | O_APPEND;

  //make sure the directories in the path exist
  String directory = abspath.substr(0, abspath.rfind('/'));
  int r;
  HT_INFOF("Calling mkdirs on %s", directory.c_str());
  if((r=ceph_mkdirs(cmount, directory.c_str(), 0644)) < 0 && r!=-EEXIST) {
    HT_ERRORF("create failed on mkdirs: dname='%s' - %d", directory.c_str(), -r);
    report_error(cb, -r);
    return;
  }

  //create file
  if ((ceph_fd = ceph_open(cmount, abspath.c_str(), oflags, 0644)) < 0) {
    std::string errs(cpp_strerror(-ceph_fd));
    HT_ERRORF("open failed: file=%s - %s",  abspath.c_str(), errs.c_str());
    report_error(cb, ceph_fd);
    return;
  }

  HT_INFOF("create %s  = %d", fname, ceph_fd);

  {
    struct sockaddr_in addr;
    OpenFileDataCephPtr fdata (new OpenFileDataCeph(cmount, fname, ceph_fd, O_WRONLY));

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
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    HT_ERRORF("bad file handle: %d", fd);
    return;
  }

  if ((offset = ceph_lseek(cmount, fdata->fd, 0, SEEK_CUR)) < 0) {
    std::string errs(cpp_strerror((int)-offset));
    HT_ERRORF("lseek failed: fd=%d ceph_fd=%d offset=0 SEEK_CUR - %s",
	      fd, fdata->fd, errs.c_str());
    report_error(cb, offset);
    return;
  }

  if ((nread = ceph_read(cmount, fdata->fd, (char *)buf.base, amount, 0)) < 0 ) {
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
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((offset = (uint64_t)ceph_lseek(cmount, fdata->fd, 0, SEEK_CUR)) < 0) {
    std::string errs(cpp_strerror((int)-offset));
    HT_ERRORF("lseek failed: fd=%d ceph_fd=%d offset=0 SEEK_CUR - %s", fd, fdata->fd,
              errs.c_str());
    report_error(cb, offset);
    return;
  }

  if ((nwritten = ceph_write(cmount, fdata->fd, (const char *)data, amount, 0)) < 0) {
    std::string errs(cpp_strerror(nwritten));
    HT_ERRORF("write failed: fd=%d ceph_fd=%d amount=%d - %s",
	      fd, fdata->fd, amount, errs.c_str());
    report_error(cb, -nwritten);
    return;
  }

  int r;
  if (sync && ((r = ceph_fsync(cmount, fdata->fd, true)) != 0)) {
    std::string errs(cpp_strerror(errno));
    HT_ERRORF("flush failed: fd=%d ceph_fd=%d - %s", fd, fdata->fd, errs.c_str());
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
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }
  loff_t res = ceph_lseek(cmount, fdata->fd, offset, SEEK_SET);
  if (res < 0) {
    std::string errs(cpp_strerror((int)res));
    HT_ERRORF("lseek failed: fd=%d ceph_fd=%d offset=%llu - %s",
	      fd, fdata->fd, (Llu)offset, errs.c_str());
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
  if ((r = ceph_unlink(cmount, abspath.c_str())) < 0) {
    std::string errs(cpp_strerror(r));
    HT_ERRORF("unlink failed: file='%s' - %s", abspath.c_str(), errs.c_str());
    report_error(cb, r);
    return;
  }
  cb->response_ok();
}

void CephBroker::length(ResponseCallbackLength *cb, const char *fname, bool) {
  int r;
  struct ceph_statx stx;

  HT_DEBUGF("length file='%s'", fname);

  if ((r = ceph_statx(cmount, fname, &stx, CEPH_STATX_SIZE, AT_SYMLINK_NOFOLLOW)) < 0) {
    String abspath;
    make_abs_path(fname, abspath);
    std::string errs(cpp_strerror(r));
    HT_ERRORF("length (stat) failed: file='%s' - %s", abspath.c_str(), errs.c_str());
    report_error(cb,- r);
    return;
  }
  cb->response(stx.stx_size);
}

void CephBroker::pread(ResponseCallbackRead *cb, uint32_t fd, uint64_t offset,
		       uint32_t amount, bool) {
  OpenFileDataCephPtr fdata;
  ssize_t nread;
  StaticBuffer buf(new uint8_t [amount], amount);

  HT_DEBUGF("pread fd=%d offset=%llu amount=%d", fd, (Llu)offset, amount);

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  if ((nread = ceph_read(cmount, fdata->fd, (char *)buf.base, amount, offset)) < 0) {
    std::string errs(cpp_strerror(nread));
    HT_ERRORF("pread failed: fd=%d ceph_fd=%d amount=%d offset=%llu - %s", fd, fdata->fd,
              amount, (Llu)offset, errs.c_str());
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
  if((r=ceph_mkdirs(cmount, absdir.c_str(), 0644)) < 0 && r!=-EEXIST) {
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
  struct ceph_dir_result *dirp;
  struct dirent de;
  struct ceph_statx stx;
  int r;
  if ((r = ceph_opendir(cmount, directory, &dirp)) < 0)
    return r; //failed to open
  while ((r = ceph_readdirplus_r(cmount, dirp, &de, &stx, CEPH_STATX_INO, AT_NO_ATTR_SYNC, NULL)) > 0) {
    String new_dir = de.d_name;
    if(!(new_dir.compare(".")==0 || new_dir.compare("..")==0)) {
      new_dir = directory;
      new_dir += '/';
      new_dir += de.d_name;
      if (S_ISDIR(stx.stx_mode)) { //it's a dir, clear it out...
	if((r=rmdir_recursive(new_dir.c_str())) < 0) return r;
      } else { //delete this file
	if((r=ceph_unlink(cmount, new_dir.c_str())) < 0) return r;
      }
    }
  }
  if (r < 0) return r; //we got an error
  if ((r = ceph_closedir(cmount, dirp)) < 0) return r;
  return ceph_rmdir(cmount, directory);
}

void CephBroker::flush(ResponseCallback *cb, uint32_t fd) {
  OpenFileDataCephPtr fdata;

  HT_DEBUGF("flush fd=%d", fd);

  if (!m_open_file_map.get(fd, fdata)) {
    char errbuf[32];
    sprintf(errbuf, "%d", fd);
    cb->error(Error::DFSBROKER_BAD_FILE_HANDLE, errbuf);
    return;
  }

  int r;
  if ((r = ceph_fsync(cmount, fdata->fd, true)) != 0) {
    std::string errs(cpp_strerror(r));
    HT_ERRORF("flush failed: fd=%d  ceph_fd=%d - %s", fd, fdata->fd, errs.c_str());
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

  struct ceph_dir_result *dirp;
  ceph_opendir(cmount, absdir.c_str(), &dirp);
  int r;
  int buflen = 100; //good default?
  char *buf = new char[buflen];
  String *ent;
  int bufpos;
  while (1) {
    r = ceph_getdnames(cmount, dirp, buf, buflen);
    if (r==-ERANGE) { //expand the buffer
      delete [] buf;
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
  delete [] buf;
  ceph_closedir(cmount, dirp);

  if (r < 0) report_error(cb, -r); //Ceph shouldn't return r<0 on getdnames
  //(except for ERANGE) so if it happens this is bad
  cb->response(listing);
}

void CephBroker::exists(ResponseCallbackExists *cb, const char *fname) {
  String abspath;
  struct ceph_statx stx;
  
  HT_DEBUGF("exists file='%s'", fname);
  make_abs_path(fname, abspath);
  cb->response(ceph_statx(cmount, abspath.c_str(), &stx, 0, AT_SYMLINK_NOFOLLOW) == 0);
}

void CephBroker::rename(ResponseCallback *cb, const char *src, const char *dst) {
  String src_abs;
  String dest_abs;
  int r;

  make_abs_path(src, src_abs);
  make_abs_path(dst, dest_abs);
  if ((r = ceph_rename(cmount, src_abs.c_str(), dest_abs.c_str())) <0 ) {
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


