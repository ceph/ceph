// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_REPLAY_BUFFER_READER_H
#define CEPH_RBD_REPLAY_BUFFER_READER_H

#include "include/int_types.h"
#include "include/buffer.h"

namespace rbd_replay {

class BufferReader {
public:
  static const size_t DEFAULT_MIN_BYTES = 1<<20;
  static const size_t DEFAULT_MAX_BYTES = 1<<22;

  BufferReader(int fd, size_t min_bytes = DEFAULT_MIN_BYTES,
               size_t max_bytes = DEFAULT_MAX_BYTES);

  int fetch(bufferlist::iterator **it);

private:
  int m_fd;
  size_t m_min_bytes;
  size_t m_max_bytes;
  bufferlist m_bl;
  bufferlist::iterator m_bl_it;
  bool m_eof_reached;

};

} // namespace rbd_replay

#endif // CEPH_RBD_REPLAY_BUFFER_READER_H
