// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "osd/osd_types.h"

namespace crimson::os {
class FuturizedStore;

class FuturizedCollection
  : public boost::intrusive_ref_counter<FuturizedCollection,
                                        boost::thread_safe_counter>
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

// Provide intrusive_ptr ADL functions in the global namespace.
// boost::intrusive_ref_counter defines them in boost::sp_adl_block,
// which is not reachable by ADL when FuturizedCollection is in crimson::os.
inline void intrusive_ptr_add_ref(const FuturizedCollection* p) noexcept {
  boost::sp_adl_block::intrusive_ptr_add_ref(
    static_cast<const boost::intrusive_ref_counter<
      FuturizedCollection, boost::thread_safe_counter>*>(p));
}
inline void intrusive_ptr_release(const FuturizedCollection* p) noexcept {
  boost::sp_adl_block::intrusive_ptr_release(
    static_cast<const boost::intrusive_ref_counter<
      FuturizedCollection, boost::thread_safe_counter>*>(p));
}

using CollectionRef = boost::intrusive_ptr<FuturizedCollection>;
} // namespace crimson::os
