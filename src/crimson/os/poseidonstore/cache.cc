// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/common/log.h"
#include "crimson/os/poseidonstore/cache.h"
#include "crimson/os/poseidonstore/transaction.h"
#include <vector>

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::poseidonstore {

std::ostream &operator<<(std::ostream &out, const CachedExtent &ext) {
  return out << "CachedExtent(type=" << ext.get_type()
      << ", laddr=" << ext.get_laddr()
      << ", state=" << ext.state << ")";
}

std::ostream &operator<<(std::ostream &out, CachedExtent::ce_state_t state) {
  switch (state) {
    case CachedExtent::ce_state_t::WRITING:
      return out << "WRITING";
    case CachedExtent::ce_state_t::READING:
      return out << "READING";
    case CachedExtent::ce_state_t::NEW:
      return out << "NEW";
    case CachedExtent::ce_state_t::CLEAN:
      return out << "CLEAN";
    default:
      return out << "UNKNOWN";
  }
}

std::optional<RecordRef> Cache::try_construct_record(Transaction &t) 
{
  //record_t record;
  RecordRef record = new Record();
  for (auto &c : t.write_set) {
    logger().debug("try_construct_record: add writeset {}", *c);
    bufferlist bl;
    // add whole cache extent if modified range is empty
    if (c->modified_range.empty()) {
      bl.append(c->get_bptr());
      assert(bl.length() == c->get_length());
      record->to_write.push_back(
	  write_item{
	    std::move(bl),
	    c->get_type(),
	    c->get_laddr()
	  });
    } else {
      bl.append(c->get_bptr(), c->modified_range.range_start(), 
		    c->modified_range.range_end());
      record->to_write.push_back(
	  write_item{
	    std::move(bl),
	    c->get_type(),
	    c->get_laddr()
	  });
    }
  }
  return std::make_optional<RecordRef>(record);
}

void Cache::complete_commit(Transaction &t)  
{
  for (auto &p : t.read_set) {
    p->on_read();
    p->state = CachedExtent::ce_state_t::CLEAN;
  }
  for (auto &p : t.write_set) {
    p->on_write();
    if (p->prior_instance) {
      p->prior_instance = CachedExtentRef();
    }
    p->state = CachedExtent::ce_state_t::CLEAN;
    p->modified_range.clear();
  }
}

Cache::fill_cache_ret Cache::fill_cache(Transaction &t) 
{
  std::vector<CachedExtentRef> ret;
  for (auto &p : t.read_set) {
    if (p->is_new()) {
      ret.push_back(p);
    }
  }
  for (auto &p : t.write_set) {
    if (p->is_new()) {
      ret.push_back(p);
    }
  }
  /*
   * For function test, we directly use device_manger here.
   * This should be improved by using a warpper class such as DataPartition
   */
  return crimson::do_for_each(
      ret.begin(),
      ret.end(),
      [this](const auto &p) {
	return device_manager.read(
	  p->get_laddr(),    // for test, use laddr instead of paddr
	  p->get_length(),
	  p->get_bptr()).safe_then([this, iter=p]() {
	    iter->set_state(CachedExtent::ce_state_t::CLEAN);
	    return DeviceManager::read_ertr::now();
	  });
      }).safe_then([this]() {
	return fill_cache_ertr::now();
	},
	fill_cache_ertr::pass_further{},
	crimson::ct_error::discard_all{});
}

}
