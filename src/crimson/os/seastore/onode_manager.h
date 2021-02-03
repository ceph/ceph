// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "include/buffer_fwd.h"
#include "include/ceph_assert.h"
#include "common/hobject.h"

#include "crimson/os/seastore/onode.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/osd/exceptions.h"

namespace crimson::os::seastore {

class OnodeManager {
public:
  using open_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  virtual open_ertr::future<OnodeRef> get_or_create_onode(
    Transaction &trans,
    const ghobject_t &hoid) {
    return open_ertr::make_ready_future<OnodeRef>();
  }
  virtual open_ertr::future<std::vector<OnodeRef>> get_or_create_onodes(
    Transaction &trans,
    const std::vector<ghobject_t> &hoids) {
    return open_ertr::make_ready_future<std::vector<OnodeRef>>();
  }

  using write_ertr= crimson::errorator<
    crimson::ct_error::input_output_error>;
  virtual write_ertr::future<> write_dirty(
    Transaction &trans,
    const std::vector<OnodeRef> &onodes) {
    return write_ertr::now();
  }
  virtual ~OnodeManager() {}
};
using OnodeManagerRef = std::unique_ptr<OnodeManager>;

namespace onode_manager {

OnodeManagerRef create_ephemeral() {
  return OnodeManagerRef();
}

}

}
