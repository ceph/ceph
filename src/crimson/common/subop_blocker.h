// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "osd/osd_op_util.h"
#include "crimson/osd/osd_operation.h"

namespace crimson::osd {

using interruptor =
  ::crimson::interruptible::interruptor<
    ::crimson::osd::IOInterruptCondition>;

// bases on 998cb8c141bb89aafae298a9d5e130fbd78fe5f2
template <typename T>
struct SubOpBlocker : crimson::BlockerT<SubOpBlocker<T>> {
  static constexpr const char* type_name = "CompoundOpBlocker";

  using id_done_t = std::pair<crimson::OperationRef, T>;

  void dump_detail(Formatter *f) const final {
    f->open_array_section("dependent_operations");
    {
      for (const auto &kv : subops) {
        f->dump_unsigned("op_id", kv.first->get_id());
      }
    }
    f->close_section();
  }

  template <class... Args>
  void emplace_back(Args&&... args) {
      subops.emplace_back(std::forward<Args>(args)...);
  };

  T interruptible_wait_completion() {
    return interruptor::do_for_each(subops, [](auto&& kv) {
      return std::move(kv.second);
    });
  }

  T wait_completion() {
    return seastar::do_for_each(subops, [](auto&& kv) {
      return std::move(kv.second);
    });
  }

private:
  std::vector<id_done_t> subops;
};

} // namespace crimson::osd
