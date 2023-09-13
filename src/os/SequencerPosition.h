// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __CEPH_OS_SEQUENCERPOSITION_H
#define __CEPH_OS_SEQUENCERPOSITION_H

#include "include/types.h"
#include "include/encoding.h"
#include "common/Formatter.h"

#include <ostream>

/**
 * transaction and op offset
 */
struct SequencerPosition {
  uint64_t seq;  ///< seq
  uint32_t trans; ///< transaction in that seq (0-based)
  uint32_t op;    ///< op in that transaction (0-based)

  SequencerPosition(uint64_t s=0, int32_t t=0, int32_t o=0) : seq(s), trans(t), op(o) {}

  auto operator<=>(const SequencerPosition&) const = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(seq, bl);
    encode(trans, bl);
    encode(op, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(1, p);
    decode(seq, p);
    decode(trans, p);
    decode(op, p);
    DECODE_FINISH(p);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("seq", seq);
    f->dump_unsigned("trans", trans);
    f->dump_unsigned("op", op);
  }
  static void generate_test_instances(std::list<SequencerPosition*>& o) {
    o.push_back(new SequencerPosition);
    o.push_back(new SequencerPosition(1, 2, 3));
    o.push_back(new SequencerPosition(4, 5, 6));
  }
};
WRITE_CLASS_ENCODER(SequencerPosition)

inline std::ostream& operator<<(std::ostream& out, const SequencerPosition& t) {
  return out << t.seq << "." << t.trans << "." << t.op;
}

#endif
