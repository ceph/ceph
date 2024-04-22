// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
      
#ifndef CEPH_LIBRBD_CACHE_SSD_TYPES_H
#define CEPH_LIBRBD_CACHE_SSD_TYPES_H
  
#include "acconfig.h"
    
#include "librbd/io/Types.h"
#include "librbd/cache/pwl/Types.h"

namespace librbd {
namespace cache {
namespace pwl {
namespace ssd {

struct SuperBlock{
  WriteLogPoolRoot root;

  DENC(SuperBlock, v, p) {
    DENC_START(1, 1, p);
    denc(v.root, p);
    DENC_FINISH(p);
  }

  void dump(Formatter *f) const {
    f->dump_object("super", root);
  }

  static void generate_test_instances(std::list<SuperBlock*>& ls) {
    ls.push_back(new SuperBlock());
    ls.push_back(new SuperBlock);
    ls.back()->root.layout_version = 3;
    ls.back()->root.cur_sync_gen = 1;
    ls.back()->root.pool_size = 10737418240;
    ls.back()->root.flushed_sync_gen = 1;
    ls.back()->root.block_size = 4096;
    ls.back()->root.num_log_entries = 0;
    ls.back()->root.first_free_entry = 30601;
    ls.back()->root.first_valid_entry = 2;
  }
};

} // namespace ssd
} // namespace pwl
} // namespace cache
} // namespace librbd

WRITE_CLASS_DENC(librbd::cache::pwl::ssd::SuperBlock)

#endif // CEPH_LIBRBD_CACHE_SSD_TYPES_H
