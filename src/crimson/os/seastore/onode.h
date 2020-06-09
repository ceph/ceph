// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <limits>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "include/buffer.h"
#include "include/denc.h"

namespace crimson::os::seastore {

// in-memory onode, in addition to the stuff that should be persisted to disk,
// it may contain intrusive hooks for LRU, rw locks etc
class Onode : public boost::intrusive_ref_counter<
  Onode,
  boost::thread_unsafe_counter>
{
public:
  Onode(std::string_view s)
    : payload{s}
  {}
  size_t size() const;
  const std::string& get() const {
    return payload;
  }
  void encode(void* buffer, size_t len);
  DENC(Onode, v, p) {
    DENC_START(1, 1, p);
    denc(v.payload, p);
    DENC_FINISH(p);
  }

private:
  // dummy payload
  std::string payload;
};

bool operator==(const Onode& lhs, const Onode& rhs);
std::ostream& operator<<(std::ostream &out, const Onode &rhs);
using OnodeRef = boost::intrusive_ptr<Onode>;
}

WRITE_CLASS_DENC(crimson::os::seastore::Onode)
