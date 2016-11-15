// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rbd_replay/BufferReader.h"
#include "include/assert.h"
#include "include/intarith.h"

namespace rbd_replay {

BufferReader::BufferReader(int fd, size_t min_bytes, size_t max_bytes)
  : m_fd(fd), m_min_bytes(min_bytes), m_max_bytes(max_bytes),
    m_bl_it(m_bl.begin()), m_eof_reached(false) {
  assert(m_min_bytes <= m_max_bytes);
}

int BufferReader::fetch(bufferlist::iterator **it) {
  if (m_bl_it.get_remaining() < m_min_bytes) {
    ssize_t bytes_to_read = ROUND_UP_TO(m_max_bytes - m_bl_it.get_remaining(),
                                        CEPH_PAGE_SIZE);
    while (!m_eof_reached && bytes_to_read > 0) {
      int r = m_bl.read_fd(m_fd, CEPH_PAGE_SIZE);
      if (r < 0) {
        return r;
      }
      if (r == 0) {
	m_eof_reached = true;
      }
      assert(r <= bytes_to_read);
      bytes_to_read -= r;
    }
  }

  *it = &m_bl_it;
  return 0;
}

} // namespace rbd_replay
