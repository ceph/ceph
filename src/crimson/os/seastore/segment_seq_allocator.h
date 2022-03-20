// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore::journal {
class SegmentedJournal;
}

namespace crimson::os::seastore {

class SegmentSeqAllocator {
public:
  segment_seq_t get_and_inc_next_segment_seq() {
    return next_segment_seq++;
  }
private:
  void set_next_segment_seq(segment_seq_t seq) {
    LOG_PREFIX(SegmentSeqAllocator::set_next_segment_seq);
    SUBINFO(seastore_journal, "next_segment_seq={}", segment_seq_printer_t{seq});
    next_segment_seq = seq;
  }
  segment_seq_t next_segment_seq = 0;
  friend class journal::SegmentedJournal;
};

using SegmentSeqAllocatorRef =
  std::unique_ptr<SegmentSeqAllocator>;

};
