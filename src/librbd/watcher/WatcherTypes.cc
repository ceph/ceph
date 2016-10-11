// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/watcher/WatcherTypes.h"
#include "common/Formatter.h"

namespace librbd {
namespace watcher {

void ResponseMessage::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(result, bl);
  ENCODE_FINISH(bl);
}

void ResponseMessage::decode(bufferlist::iterator& iter) {
  DECODE_START(1, iter);
  ::decode(result, iter);
  DECODE_FINISH(iter);
}

void ResponseMessage::dump(Formatter *f) const {
  f->dump_int("result", result);
}

void ResponseMessage::generate_test_instances(std::list<ResponseMessage *> &o) {
  o.push_back(new ResponseMessage(1));
}

} // namespace watcher
} // namespace librbd
