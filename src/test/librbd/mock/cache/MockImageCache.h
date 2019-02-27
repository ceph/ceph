// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_CACHE_MOCK_IMAGE_CACHE_H
#define CEPH_TEST_LIBRBD_CACHE_MOCK_IMAGE_CACHE_H

#include "gmock/gmock.h"
#include <vector>

namespace librbd {
namespace cache {

struct MockImageCache {
  typedef std::vector<std::pair<uint64_t,uint64_t> > Extents;

  MOCK_METHOD4(aio_read_mock, void(const Extents &, ceph::bufferlist*, int,
                                   Context *));
  void aio_read(Extents&& image_extents, ceph::bufferlist* bl,
                int fadvise_flags, Context *on_finish) {
    aio_read_mock(image_extents, bl, fadvise_flags, on_finish);
  }


  MOCK_METHOD4(aio_write_mock, void(const Extents &, const ceph::bufferlist &,
                                    int, Context *));
  void aio_write(Extents&& image_extents, ceph::bufferlist&& bl,
                 int fadvise_flags, Context *on_finish) {
    aio_write_mock(image_extents, bl, fadvise_flags, on_finish);
  }

  MOCK_METHOD4(aio_discard, void(uint64_t, uint64_t, uint32_t, Context *));
  MOCK_METHOD1(aio_flush, void(Context *));
  MOCK_METHOD5(aio_writesame_mock, void(uint64_t, uint64_t, ceph::bufferlist& bl,
                                        int, Context *));
  void aio_writesame(uint64_t off, uint64_t len, ceph::bufferlist&& bl,
                     int fadvise_flags, Context *on_finish) {
    aio_writesame_mock(off, len, bl, fadvise_flags, on_finish);
  }

  MOCK_METHOD6(aio_compare_and_write_mock, void(const Extents &,
                                                const ceph::bufferlist &,
                                                const ceph::bufferlist &,
                                                uint64_t *, int, Context *));

  void aio_compare_and_write(Extents&& image_extents, ceph::bufferlist&& cmp_bl,
                             ceph::bufferlist&& bl, uint64_t *mismatch_offset,
                             int fadvise_flags, Context *on_finish) {
    aio_compare_and_write_mock(image_extents, cmp_bl, bl, mismatch_offset,
                               fadvise_flags, on_finish);
  }
};

} // namespace cache
} // namespace librbd

#endif // CEPH_TEST_LIBRBD_CACHE_MOCK_IMAGE_CACHE_H
