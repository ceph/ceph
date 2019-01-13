// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ObjectCacheFile.h"
#include "include/Context.h"
#include "common/dout.h"
#include "common/WorkQueue.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <aio.h>
#include <errno.h>
#include <fcntl.h>
#include <utility>

#define dout_subsys ceph_subsys_immutable_obj_cache
#undef dout_prefix
#define dout_prefix *_dout << "ceph::cache::ObjectCacheFile: " << this << " " \
                           <<  __func__ << ": "

namespace ceph {
namespace immutable_obj_cache {

ObjectCacheFile::ObjectCacheFile(CephContext *cct, const std::string &name)
  : cct(cct), m_fd(-1) {
  m_name = cct->_conf.get_val<std::string>("immutable_object_cache_path") + "/ceph_immutable_obj_cache/" + name;
  ldout(cct, 20) << "file path=" << m_name << dendl;
}

ObjectCacheFile::~ObjectCacheFile() {
  // TODO force proper cleanup
}

void ObjectCacheFile::read(uint64_t offset, uint64_t length, ceph::bufferlist *bl, Context *on_finish) {
  on_finish->complete(read_object_from_file(bl, offset, length));
}

void ObjectCacheFile::write(uint64_t offset, ceph::bufferlist &&bl, bool fdatasync, Context *on_finish) {
  on_finish->complete(write_object_to_file(bl, bl.length()));
}

int ObjectCacheFile::write_object_to_file(ceph::bufferlist read_buf, uint64_t object_len) {
  ldout(cct, 20) << "cache file name:" << m_name
                 << ", length:" << object_len <<  dendl;

  int ret = read_buf.write_file(m_name.c_str()); 
  if(ret < 0) {
    lderr(cct)<<"write file fail:" << std::strerror(errno) << dendl;
    return ret;
  } 

  return object_len;
}

int ObjectCacheFile::read_object_from_file(ceph::bufferlist* read_buf, uint64_t object_off, uint64_t object_len) {

  ldout(cct, 20) << "offset:" << object_off
                 << ", length:" << object_len << dendl;

  bufferlist temp_bl;
  std::string error_str;

  // TODO : current implements will drop sharely performance.
  int ret = temp_bl.read_file(m_name.c_str(), &error_str);
  if (ret < 0) {
    lderr(cct)<<"read file fail:" << error_str << dendl;
    return -1;
  }

  if(object_off >= temp_bl.length()) {
    return 0;
  }

  if((temp_bl.length() - object_off) < object_len) {
    object_len = temp_bl.length() - object_off;
  }

  read_buf->substr_of(temp_bl, object_off, object_len);

  return read_buf->length();
}

uint64_t ObjectCacheFile::get_file_size() {
  struct stat buf;
  int temp_fd = ::open(m_name.c_str(), O_RDONLY);
  if(temp_fd < 0) {
    lderr(cct)<<"get_file_size fail: open file is fails." << std::strerror(errno) << dendl;
    return -1;
  }

  int ret = fstat(temp_fd, &buf);
  if(ret == -1) {
    lderr(cct)<<"fstat fail:" << std::strerror(errno) << dendl;
    return -1;
  }

  ret = buf.st_size; 
  ::close(temp_fd);
  return ret;
}

} // namespace immutable_obj_cache
} // namespace cache
