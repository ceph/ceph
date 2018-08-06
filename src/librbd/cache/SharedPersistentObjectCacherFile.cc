// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "SharedPersistentObjectCacherFile.h"
#include "include/Context.h"
#include "common/dout.h"
#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <aio.h>
#include <errno.h>
#include <fcntl.h>
#include <utility>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::SyncFile: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {

SyncFile::SyncFile(CephContext *cct, const std::string &name)
  : cct(cct) {
  m_name = cct->_conf.get_val<std::string>("rbd_shared_cache_path") + "/rbd_cache." + name;
  ldout(cct, 20) << "file path=" << m_name << dendl;
}

SyncFile::~SyncFile() {
  // TODO force proper cleanup
  if (m_fd != -1) {
    ::close(m_fd);
  }
}

void SyncFile::open(Context *on_finish) {
  while (true) {
    m_fd = ::open(m_name.c_str(), O_CREAT | O_DIRECT | O_NOATIME | O_RDWR | O_SYNC,
                  S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (m_fd == -1) {
      int r = -errno;
      if (r == -EINTR) {
        continue;
      }
      on_finish->complete(r);
      return;
    }
    break;
  }

  on_finish->complete(0);
}

void SyncFile::open() {
  while (true) 
  {
    m_fd = ::open(m_name.c_str(), O_CREAT | O_NOATIME | O_RDWR | O_SYNC,
                  S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (m_fd == -1) 
    {
      int r = -errno;
      if (r == -EINTR) {
        continue;
      }
      return;
    }
    break;
  }
}

void SyncFile::read(uint64_t offset, uint64_t length, ceph::bufferlist *bl, Context *on_finish) {
  on_finish->complete(read_object_from_file(bl, offset, length));
}

void SyncFile::write(uint64_t offset, ceph::bufferlist &&bl, bool fdatasync, Context *on_finish) {
  on_finish->complete(write_object_to_file(bl, bl.length()));
}

int SyncFile::write_object_to_file(ceph::bufferlist read_buf, uint64_t object_len) {

  ldout(cct, 20) << "cache file name:" << m_name
                 << ", length:" << object_len <<  dendl;

  // TODO(): aio
  int ret = pwrite(m_fd, read_buf.c_str(), object_len, 0); 
  if(ret < 0) {
    lderr(cct)<<"write file fail:" << std::strerror(errno) << dendl;
    return ret;
  }

  return ret;
}

int SyncFile::read_object_from_file(ceph::bufferlist* read_buf, uint64_t object_off, uint64_t object_len) {

  ldout(cct, 20) << "offset:" << object_off
                 << ", length:" << object_len <<  dendl;

  bufferptr buf(object_len);

  // TODO(): aio
  int ret = pread(m_fd, buf.c_str(), object_len, object_off); 
  if(ret < 0) {
    lderr(cct)<<"read file fail:" << std::strerror(errno) << dendl;
    return ret;
  }
  read_buf->append(std::move(buf));

  return ret;
}

} // namespace cache
} // namespace librbd
