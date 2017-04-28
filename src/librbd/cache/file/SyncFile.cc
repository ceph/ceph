// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/file/SyncFile.h"
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
#define dout_prefix *_dout << "librbd::file::SyncFile: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {
namespace file {

template <typename I>
SyncFile<I>::SyncFile(I &image_ctx, ContextWQ &work_queue,
                    const std::string &name)
  : m_image_ctx(image_ctx), m_work_queue(work_queue){
  CephContext *cct = m_image_ctx.cct;
  m_name = cct->_conf->rbd_persistent_cache_path + "/rbd_cache." + name;
}

template <typename I>
SyncFile<I>::~SyncFile() {
  // TODO force proper cleanup
  if (m_fd != -1) {
    ::close(m_fd);
  }
}

template <typename I>
void SyncFile<I>::open(Context *on_finish) {
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

template <typename I>
void SyncFile<I>::close(Context *on_finish) {
  assert(m_fd >= 0);
  while (true) {
    int r = ::close(m_fd);
    if (r == -1) {
      r = -errno;
      if (r == -EINTR) {
        continue;
      }
      on_finish->complete(r);
      return;
    }
    break;
  }

  m_fd = -1;
  on_finish->complete(0);
}

template <typename I>
void SyncFile<I>::read(uint64_t offset, uint64_t length, ceph::bufferlist *bl,
                      Context *on_finish) {
  on_finish->complete(read(offset, length, bl));
}

template <typename I>
void SyncFile<I>::write(uint64_t offset, ceph::bufferlist &&bl,
                       bool fdatasync, Context *on_finish) {
  on_finish->complete(write(offset, bl, fdatasync));
  //on_finish->complete(0);
}

template <typename I>
void SyncFile<I>::discard(uint64_t offset, uint64_t length, bool fdatasync,
                         Context *on_finish) {
  on_finish->complete(discard(offset, length, fdatasync));
}

template <typename I>
void SyncFile<I>::truncate(uint64_t length, bool fdatasync, Context *on_finish) {
  on_finish->complete(truncate(length, fdatasync));
}

template <typename I>
void SyncFile<I>::fsync(Context *on_finish) {
  int r = ::fsync(m_fd);
  if (r == -1) {
    r = -errno;
    on_finish->complete(r);
    return;
  }
  on_finish->complete(0);
}

template <typename I>
void SyncFile<I>::fdatasync(Context *on_finish) {
  on_finish->complete(fdatasync());
}

template <typename I>
int SyncFile<I>::write(uint64_t offset, const ceph::bufferlist &bl,
                      bool sync) {
  sync = false;
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "offset=" << offset << ", "
                 << "length=" << bl.length() << dendl;

  //int r = bl.write_fd(m_fd, offset);
  //todo: bl is not aligned in one 4096 block
  char *aligned_buffer = (char*)aligned_alloc(4096, sizeof(char)*4096);
  int r;
  /*r = posix_memalign((void**)&aligned_buffer, sizeof(char)*4096, 1);
  if( r < 0 ){
    return -r;
  }*/
  bl.copy( 0, bl.length(), aligned_buffer );
  r = pwrite(m_fd, aligned_buffer, 4096, offset); 
  if (r < 0) {
    free(aligned_buffer);
    return -r;
  }

  if (sync) {
    r = fdatasync();
  }
  free(aligned_buffer);
  return r;
}

template <typename I>
int SyncFile<I>::read(uint64_t offset, uint64_t length, ceph::bufferlist *bl) {

  CephContext *cct = m_image_ctx.cct;
  bufferptr bp = buffer::create(length);
  bl->push_back(bp);

  int r = 0;
  char *buffer = reinterpret_cast<char *>(bp.c_str());
  char *aligned_buffer = (char*)aligned_alloc(4096, sizeof(char)*4096);
  /*char *aligned_buffer;
  r = posix_memalign((void**)&aligned_buffer, sizeof(char)*4096, 1);
  if( r < 0 ){
    return -r;
  }*/
  uint64_t left = length;
  ssize_t ret_val;
  uint64_t count = 0;
  uint64_t cpy_length;
  do {
    //todo: offset is not aligned.
    ret_val = pread64(m_fd, aligned_buffer, 4096, offset);
    if( ret_val < 0 ){
      r = -ret_val;
      if (r == -EINTR) {
        continue;
      }
      free(aligned_buffer);
      return r;
    }
    //ret_val = pread64(m_fd, buffer, length - count, offset + count);
    cpy_length = left <= 4096 ? left : 4096;
    memcpy(buffer + count, aligned_buffer, cpy_length);
    count += cpy_length;
    left -= cpy_length;
  } while ( left );
  free(aligned_buffer);
  return count;
}

template <typename I>
int SyncFile<I>::discard(uint64_t offset, uint64_t length, bool sync) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "offset=" << offset << ", "
                 << "length=" << length << dendl;

  int r;
  while (true) {
    r = fallocate(m_fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                  offset, length);
    if (r == -1) {
      r = -errno;
      if (r == -EINTR) {
        continue;
      }
      return r;
    }
    break;
  }

  if (sync) {
    r = fdatasync();
  }
  return r;
}

template <typename I>
int SyncFile<I>::truncate(uint64_t length, bool sync) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "length=" << length << dendl;

  int r;
  while (true) {
    r = ftruncate(m_fd, length);
    if (r == -1) {
      r = -errno;
      if (r == -EINTR) {
        continue;
      }
      return r;
    }
    break;
  }

  if (sync) {
    r = fdatasync();
  }
  return r;
}

template <typename I>
int SyncFile<I>::fdatasync() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  int r = ::fdatasync(m_fd);
  if (r == -1) {
    r = -errno;
    return r;
  }
  return 0;
}

template <typename I>
uint64_t SyncFile<I>::filesize() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  struct stat file_st;
  memset(&file_st, 0, sizeof(file_st));
  fstat(m_fd, &file_st);
  return file_st.st_size;
}

} // namespace file
} // namespace cache
} // namespace librbd

template class librbd::cache::file::SyncFile<librbd::ImageCtx>;
