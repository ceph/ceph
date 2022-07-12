// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "osd/osd_types.h"

namespace crimson::os {
class FuturizedStore;

class FuturizedCollection
  : public boost::intrusive_ref_counter<FuturizedCollection,
                                        boost::thread_unsafe_counter>
{
public:
  FuturizedCollection(const coll_t& cid)
    : cid{cid} {}
  virtual ~FuturizedCollection() {}
  virtual seastar::future<> flush() {
    return seastar::make_ready_future<>();
  }
  virtual seastar::future<bool> flush_commit() {
    return seastar::make_ready_future<bool>(true);
  }
  const coll_t& get_cid() const {
    return cid;
  }
private:
  const coll_t cid;
};

using CollectionRef =  boost::intrusive_ptr<FuturizedCollection>;
}
