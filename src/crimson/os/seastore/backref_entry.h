// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <memory>
#include <iostream>
#include <map>
#include <set>
#include <vector>

#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

#include <boost/intrusive/set.hpp>

#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore {

struct backref_entry_t {
  using ref_t = std::unique_ptr<backref_entry_t>;

  backref_entry_t(
    const paddr_t& paddr,
    const laddr_t& laddr,
    extent_len_t len,
    extent_types_t type)
    : paddr(paddr),
      laddr(laddr),
      len(len),
      type(type) {
    assert(len > 0);
  }
  paddr_t paddr = P_ADDR_NULL;
  laddr_t laddr = L_ADDR_NULL;
  extent_len_t len = 0;
  extent_types_t type = extent_types_t::NONE;
  friend bool operator< (
    const backref_entry_t &l,
    const backref_entry_t &r) {
    return l.paddr < r.paddr;
  }
  friend bool operator> (
    const backref_entry_t &l,
    const backref_entry_t &r) {
    return l.paddr > r.paddr;
  }
  friend bool operator== (
    const backref_entry_t &l,
    const backref_entry_t &r) {
    return l.paddr == r.paddr;
  }

  using set_hook_t =
    boost::intrusive::set_member_hook<
      boost::intrusive::link_mode<
	boost::intrusive::auto_unlink>>;
  set_hook_t backref_set_hook;
  using backref_set_member_options = boost::intrusive::member_hook<
    backref_entry_t,
    set_hook_t,
    &backref_entry_t::backref_set_hook>;
  using multiset_t = boost::intrusive::multiset<
    backref_entry_t,
    backref_set_member_options,
    boost::intrusive::constant_time_size<false>>;

  struct cmp_t {
    using is_transparent = paddr_t;
    bool operator()(
      const backref_entry_t &l,
      const backref_entry_t &r) const {
      return l.paddr < r.paddr;
    }
    bool operator()(const paddr_t l, const backref_entry_t &r) const {
      return l < r.paddr;
    }
    bool operator()(const backref_entry_t &l, const paddr_t r) const {
      return l.paddr < r;
    }
  };

  static ref_t create_alloc(
      const paddr_t& paddr,
      const laddr_t& laddr,
      extent_len_t len,
      extent_types_t type) {
    assert(is_backref_mapped_type(type));
    assert(laddr != L_ADDR_NULL);
    return std::make_unique<backref_entry_t>(
      paddr, laddr, len, type);
  }

  static ref_t create_retire(
      const paddr_t& paddr,
      extent_len_t len,
      extent_types_t type) {
    assert(is_backref_mapped_type(type) ||
	   is_retired_placeholder_type(type));
    return std::make_unique<backref_entry_t>(
      paddr, L_ADDR_NULL, len, type);
  }

  static ref_t create(const alloc_blk_t& delta) {
    return std::make_unique<backref_entry_t>(
      delta.paddr, delta.laddr, delta.len, delta.type);
  }
};

inline std::ostream &operator<<(std::ostream &out, const backref_entry_t &ent) {
  return out << "backref_entry_t{"
	     << ent.paddr << "~0x" << std::hex << ent.len << std::dec << ", "
	     << "laddr: " << ent.laddr << ", "
	     << "type: " << ent.type
	     << "}";
}

using backref_entry_ref = backref_entry_t::ref_t;
using backref_entry_mset_t = backref_entry_t::multiset_t;
using backref_entry_refs_t = std::vector<backref_entry_ref>;
using backref_entryrefs_by_seq_t = std::map<journal_seq_t, backref_entry_refs_t>;
using backref_entry_query_set_t = std::set<backref_entry_t, backref_entry_t::cmp_t>;

} // namespace crimson::os::seastore

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::backref_entry_t> : fmt::ostream_formatter {};
#endif
