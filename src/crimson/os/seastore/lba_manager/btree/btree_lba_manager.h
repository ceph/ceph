// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "include/buffer_fwd.h"
#include "include/interval_set.h"
#include "common/interval_map.h"
#include "crimson/osd/exceptions.h"

#include "crimson/os/seastore/btree/fixed_kv_btree.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/lba_manager.h"
#include "crimson/os/seastore/cache.h"

#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"
#include "crimson/os/seastore/btree/btree_range_pin.h"

namespace crimson::os::seastore::lba_manager::btree {

class BtreeLBAMapping : public BtreeNodeMapping<laddr_t, paddr_t> {
// To support cloning, there are two kinds of lba mappings:
// 	1. physical lba mapping: the pladdr in the value of which is the paddr of
// 	   the corresponding extent;
// 	2. indirect lba mapping: the pladdr in the value of which is an laddr pointing
// 	   to the physical lba mapping that's pointing to the actual paddr of the
// 	   extent being searched;
//
// Accordingly, BtreeLBAMapping may also work under two modes: indirect or direct
// 	1. BtreeLBAMappings that come from quering an indirect lba mapping in the lba tree
// 	   are indirect;
// 	2. BtreeLBAMappings that come from quering a physical lba mapping in the lba tree
// 	   are direct.
//
// For direct BtreeLBAMappings, there are two important fields:
//      1. key: the laddr of the lba mapping being queried;
//      2. paddr: the paddr recorded in the value of the lba mapping being queried.
// For indirect BtreeLBAMappings, BtreeLBAMapping has three important fields:
// 	1. key: the laddr key of the lba entry being queried;
// 	2. intermediate_key: the laddr within the scope of the physical lba mapping
// 	   that the current indirect lba mapping points to; although an indirect mapping
// 	   points to the start of the physical lba mapping, it may change to other
// 	   laddr after remap
// 	3. intermediate_base: the laddr key of the physical lba mapping, intermediate_key
// 	   and intermediate_base should be the same when doing cloning
// 	4. intermediate_offset: intermediate_key - intermediate_base
// 	5. intermediate_length: the length of the actual physical lba mapping
// 	6. paddr: the paddr recorded in the physical lba mapping pointed to by the
// 	   indirect lba mapping being queried;
//
// NOTE THAT, for direct BtreeLBAMappings, their intermediate_keys are the same as
// their keys.
public:
  BtreeLBAMapping(op_context_t<laddr_t> ctx)
    : BtreeNodeMapping(ctx) {}
  BtreeLBAMapping(
    op_context_t<laddr_t> c,
    CachedExtentRef parent,
    uint16_t pos,
    lba_map_val_t &val,
    lba_node_meta_t meta)
    : BtreeNodeMapping(
	c,
	parent,
	pos,
	val.pladdr.is_paddr() ? val.pladdr.get_paddr() : P_ADDR_NULL,
	val.len,
	meta),
      key(meta.begin),
      indirect(val.pladdr.is_laddr()),
      intermediate_key(indirect ? val.pladdr.get_laddr() : L_ADDR_NULL),
      intermediate_length(indirect ? val.len : 0),
      raw_val(val.pladdr),
      map_val(val)
  {}

  lba_map_val_t get_map_val() const {
    return map_val;
  }

  bool is_indirect() const final {
    return indirect;
  }

  void make_indirect(
    laddr_t new_key,
    extent_len_t length,
    laddr_t interkey = L_ADDR_NULL)
  {
    assert(!indirect);
    indirect = true;
    intermediate_base = key;
    intermediate_length = len;
    adjust_mutable_indirect_attrs(new_key, length, interkey);
  }

  laddr_t get_key() const final {
    return key;
  }

  pladdr_t get_raw_val() const {
    return raw_val;
  }

  laddr_t get_intermediate_key() const final {
    assert(is_indirect());
    assert(intermediate_key != L_ADDR_NULL);
    return intermediate_key;
  }

  laddr_t get_intermediate_base() const final {
    assert(is_indirect());
    assert(intermediate_base != L_ADDR_NULL);
    return intermediate_base;
  }

  extent_len_t get_intermediate_offset() const final {
    assert(intermediate_key >= intermediate_base);
    assert((intermediate_key == L_ADDR_NULL)
      == (intermediate_base == L_ADDR_NULL));
    return intermediate_key - intermediate_base;
  }

  extent_len_t get_intermediate_length() const final {
    assert(is_indirect());
    assert(intermediate_length);
    return intermediate_length;
  }

  bool is_clone() const final {
    return get_map_val().refcount > 1;
  }

  uint32_t get_checksum() const final {
    return get_map_val().checksum;
  }

  void adjust_mutable_indirect_attrs(
    laddr_t new_key,
    extent_len_t length,
    laddr_t interkey = L_ADDR_NULL)
  {
    assert(indirect);
    assert(value.is_paddr());
    intermediate_key = (interkey == L_ADDR_NULL ? key : interkey);
    key = new_key;
    len = length;
  }

protected:
  std::unique_ptr<BtreeNodeMapping<laddr_t, paddr_t>> _duplicate(
    op_context_t<laddr_t> ctx) const final {
    auto pin = std::unique_ptr<BtreeLBAMapping>(new BtreeLBAMapping(ctx));
    pin->key = key;
    pin->intermediate_base = intermediate_base;
    pin->intermediate_key = intermediate_key;
    pin->intermediate_length = intermediate_length;
    pin->indirect = indirect;
    pin->raw_val = raw_val;
    pin->map_val = map_val;
    return pin;
  }
private:
  laddr_t key = L_ADDR_NULL;
  bool indirect = false;
  laddr_t intermediate_key = L_ADDR_NULL;
  laddr_t intermediate_base = L_ADDR_NULL;
  extent_len_t intermediate_length = 0;
  pladdr_t raw_val;
  lba_map_val_t map_val;
};

using BtreeLBAMappingRef = std::unique_ptr<BtreeLBAMapping>;

using LBABtree = FixedKVBtree<
  laddr_t, lba_map_val_t, LBAInternalNode,
  LBALeafNode, BtreeLBAMapping, LBA_BLOCK_SIZE, true>;

/**
 * BtreeLBAManager
 *
 * Uses a wandering btree to track two things:
 * 1) lba state including laddr_t -> paddr_t mapping
 * 2) reverse paddr_t -> laddr_t mapping for gc (TODO)
 *
 * Generally, any transaction will involve
 * 1) deltas against lba tree nodes
 * 2) new lba tree nodes
 *    - Note, there must necessarily be a delta linking
 *      these new nodes into the tree -- might be a
 *      bootstrap_state_t delta if new root
 *
 * get_mappings, alloc_extent_*, etc populate a Transaction
 * which then gets submitted
 */
class BtreeLBAManager : public LBAManager {
public:
  BtreeLBAManager(Cache &cache)
    : cache(cache)
  {
    register_metrics();
  }

  mkfs_ret mkfs(
    Transaction &t) final;

  get_mappings_ret get_mappings(
    Transaction &t,
    laddr_t offset, extent_len_t length) final;

  get_mappings_ret get_mappings(
    Transaction &t,
    laddr_list_t &&list) final;

  get_mapping_ret get_mapping(
    Transaction &t,
    laddr_t offset) final;


  struct alloc_mapping_info_t {
    laddr_t key = L_ADDR_NULL; // once assigned, the allocation to
			       // key must be exact and successful
    extent_len_t len = 0;
    pladdr_t val;
    uint32_t checksum = 0;
    LogicalCachedExtent* extent = nullptr;

    static alloc_mapping_info_t create_zero(extent_len_t len) {
      return {L_ADDR_NULL, len, P_ADDR_ZERO, 0, nullptr};
    }
    static alloc_mapping_info_t create_indirect(
      laddr_t laddr,
      extent_len_t len,
      laddr_t intermediate_key) {
      return {
	laddr,
	len,
	intermediate_key,
	0,	// crc will only be used and checked with LBA direct mappings
		// also see pin_to_extent(_by_type)
	nullptr};
    }
    static alloc_mapping_info_t create_direct(
      laddr_t laddr,
      extent_len_t len,
      paddr_t paddr,
      uint32_t checksum,
      LogicalCachedExtent *extent) {
      return {laddr, len, paddr, checksum, extent};
    }
  };

  alloc_extent_ret reserve_region(
    Transaction &t,
    laddr_t hint,
    extent_len_t len) final
  {
    std::vector<alloc_mapping_info_t> alloc_infos = {
      alloc_mapping_info_t::create_zero(len)};
    return seastar::do_with(
      std::move(alloc_infos),
      [&t, hint, this](auto &alloc_infos) {
      return _alloc_extents(
	t,
	hint,
	alloc_infos,
	EXTENT_DEFAULT_REF_COUNT
      ).si_then([](auto mappings) {
	assert(mappings.size() == 1);
	auto mapping = std::move(mappings.front());
	return mapping;
      });
    });
  }

  alloc_extent_ret clone_mapping(
    Transaction &t,
    laddr_t laddr,
    extent_len_t len,
    laddr_t intermediate_key,
    laddr_t intermediate_base) final
  {
    std::vector<alloc_mapping_info_t> alloc_infos = {
      alloc_mapping_info_t::create_indirect(
	laddr, len, intermediate_key)};
    return alloc_cloned_mappings(
      t,
      laddr,
      std::move(alloc_infos)
    ).si_then([&t, this, intermediate_base](auto imappings) {
      assert(imappings.size() == 1);
      auto &imapping = imappings.front();
      return update_refcount(t, intermediate_base, 1, false
      ).si_then([imapping=std::move(imapping)](auto p) mutable {
	auto mapping = std::move(p.mapping);
	ceph_assert(mapping->is_stable());
	ceph_assert(imapping->is_indirect());
	mapping->make_indirect(
	  imapping->get_key(),
	  imapping->get_length(),
	  imapping->get_intermediate_key());
	return seastar::make_ready_future<
	  LBAMappingRef>(std::move(mapping));
      });
    }).handle_error_interruptible(
      crimson::ct_error::input_output_error::pass_further{},
      crimson::ct_error::assert_all{"unexpect enoent"}
    );
  }

  alloc_extent_ret alloc_extent(
    Transaction &t,
    laddr_t hint,
    LogicalCachedExtent &ext,
    extent_ref_count_t refcount = EXTENT_DEFAULT_REF_COUNT) final
  {
    // The real checksum will be updated upon transaction commit
    assert(ext.get_last_committed_crc() == 0);
    assert(!ext.has_laddr());
    std::vector<alloc_mapping_info_t> alloc_infos = {
      alloc_mapping_info_t::create_direct(
	L_ADDR_NULL,
	ext.get_length(),
	ext.get_paddr(),
	ext.get_last_committed_crc(),
	&ext)};
    return seastar::do_with(
      std::move(alloc_infos),
      [this, &t, hint, refcount](auto &alloc_infos) {
      return _alloc_extents(
	t,
	hint,
	alloc_infos,
	refcount
      ).si_then([](auto mappings) {
	assert(mappings.size() == 1);
	auto mapping = std::move(mappings.front());
	return mapping;
      });
    });
  }

  alloc_extents_ret alloc_extents(
    Transaction &t,
    laddr_t hint,
    std::vector<LogicalCachedExtentRef> extents,
    extent_ref_count_t refcount) final
  {
    std::vector<alloc_mapping_info_t> alloc_infos;
    for (auto &extent : extents) {
      alloc_infos.emplace_back(
	alloc_mapping_info_t::create_direct(
	  extent->has_laddr() ? extent->get_laddr() : L_ADDR_NULL,
	  extent->get_length(),
	  extent->get_paddr(),
	  extent->get_last_committed_crc(),
	  extent.get()));
    }
    return seastar::do_with(
      std::move(alloc_infos),
      [this, &t, hint, refcount](auto &alloc_infos) {
      return _alloc_extents(t, hint, alloc_infos, refcount);
    });
  }

  ref_ret decref_extent(
    Transaction &t,
    laddr_t addr) final {
    return update_refcount(t, addr, -1, true
    ).si_then([](auto res) {
      return std::move(res.ref_update_res);
    });
  }

  ref_ret incref_extent(
    Transaction &t,
    laddr_t addr) final {
    return update_refcount(t, addr, 1, false
    ).si_then([](auto res) {
      return std::move(res.ref_update_res);
    });
  }

  remap_ret remap_mappings(
    Transaction &t,
    LBAMappingRef orig_mapping,
    std::vector<remap_entry> remaps,
    std::vector<LogicalCachedExtentRef> extents) final {
    LOG_PREFIX(BtreeLBAManager::remap_mappings);
    assert((orig_mapping->is_indirect())
      == (remaps.size() != extents.size()));
    return seastar::do_with(
      lba_remap_ret_t{},
      std::move(remaps),
      std::move(extents),
      std::move(orig_mapping),
      [&t, FNAME, this](auto &ret, const auto &remaps,
			auto &extents, auto &orig_mapping) {
      return update_refcount(t, orig_mapping->get_key(), -1, false
      ).si_then([&ret, this, &extents, &remaps,
		&t, &orig_mapping, FNAME](auto r) {
	ret.ruret = std::move(r.ref_update_res);
	if (!orig_mapping->is_indirect()) {
	  ceph_assert(ret.ruret.refcount == 0 &&
	    ret.ruret.addr.is_paddr() &&
	    !ret.ruret.addr.get_paddr().is_zero());
	}
	auto fut = alloc_extent_iertr::make_ready_future<
	  std::vector<LBAMappingRef>>();
	laddr_t orig_laddr = orig_mapping->get_key();
	if (orig_mapping->is_indirect()) {
	  std::vector<alloc_mapping_info_t> alloc_infos;
	  for (auto &remap : remaps) {
	    extent_len_t orig_len = orig_mapping->get_length();
	    paddr_t orig_paddr = orig_mapping->get_val();
	    laddr_t intermediate_base = orig_mapping->is_indirect()
	      ? orig_mapping->get_intermediate_base()
	      : L_ADDR_NULL;
	    laddr_t intermediate_key = orig_mapping->is_indirect()
	      ? orig_mapping->get_intermediate_key()
	      : L_ADDR_NULL;
	    auto remap_offset = remap.offset;
	    auto remap_len = remap.len;
	    auto remap_laddr = orig_laddr + remap_offset;
	    ceph_assert(intermediate_base != L_ADDR_NULL);
	    ceph_assert(intermediate_key != L_ADDR_NULL);
	    ceph_assert(remap_len < orig_len);
	    ceph_assert(remap_offset + remap_len <= orig_len);
	    ceph_assert(remap_len != 0);
	    SUBDEBUGT(seastore_lba,
	      "remap laddr: {}, remap paddr: {}, remap length: {},"
	      " intermediate_base: {}, intermediate_key: {}", t,
	      remap_laddr, orig_paddr, remap_len,
	      intermediate_base, intermediate_key);
	    auto remapped_intermediate_key = intermediate_key + remap_offset;
	    alloc_infos.emplace_back(
	      alloc_mapping_info_t::create_indirect(
		remap_laddr,
		remap_len,
		remapped_intermediate_key));
	  }
	  fut = alloc_cloned_mappings(
	    t,
	    remaps.front().offset + orig_laddr,
	    std::move(alloc_infos)
	  ).si_then([&orig_mapping](auto imappings) mutable {
	    std::vector<LBAMappingRef> mappings;
	    for (auto &imapping : imappings) {
	      auto mapping = orig_mapping->duplicate();
	      auto bmapping = static_cast<BtreeLBAMapping*>(mapping.get());
	      bmapping->adjust_mutable_indirect_attrs(
		imapping->get_key(),
		imapping->get_length(),
		imapping->get_intermediate_key());
	      mappings.emplace_back(std::move(mapping));
	    }
	    return seastar::make_ready_future<std::vector<LBAMappingRef>>(
	      std::move(mappings));
	  });
	} else { // !orig_mapping->is_indirect()
	  fut = alloc_extents(
	    t,
	    remaps.front().offset + orig_laddr,
	    std::move(extents),
	    EXTENT_DEFAULT_REF_COUNT);
	}

	return fut.si_then([&ret, &remaps, &orig_mapping](auto &&refs) {
	  assert(refs.size() == remaps.size());
#ifndef NDEBUG
	  auto ref_it = refs.begin();
	  auto remap_it = remaps.begin();
	  for (;ref_it != refs.end(); ref_it++, remap_it++) {
	    auto &ref = *ref_it;
	    auto &remap = *remap_it;
	    assert(ref->get_key() == orig_mapping->get_key() + remap.offset);
	    assert(ref->get_length() == remap.len);
	  }
#endif
	  ret.remapped_mappings = std::move(refs);
	  return seastar::now();
	});
      }).si_then([&remaps, &t, &orig_mapping, this] {
	if (remaps.size() > 1 && orig_mapping->is_indirect()) {
	  auto intermediate_base = orig_mapping->get_intermediate_base();
	  return _incref_extent(t, intermediate_base, remaps.size() - 1
	  ).si_then([](auto) {
	    return seastar::now();
	  });
	}
	return ref_iertr::now();
      }).si_then([&ret, &remaps] {
	assert(ret.remapped_mappings.size() == remaps.size());
	return seastar::make_ready_future<lba_remap_ret_t>(std::move(ret));
      });
    });
  }

  /**
   * init_cached_extent
   *
   * Checks whether e is live (reachable from lba tree) and drops or initializes
   * accordingly.
   *
   * Returns if e is live.
   */
  init_cached_extent_ret init_cached_extent(
    Transaction &t,
    CachedExtentRef e) final;

  check_child_trackers_ret check_child_trackers(Transaction &t) final;

  scan_mappings_ret scan_mappings(
    Transaction &t,
    laddr_t begin,
    laddr_t end,
    scan_mappings_func_t &&f) final;

  rewrite_extent_ret rewrite_extent(
    Transaction &t,
    CachedExtentRef extent) final;

  update_mapping_ret update_mapping(
    Transaction& t,
    laddr_t laddr,
    extent_len_t prev_len,
    paddr_t prev_addr,
    extent_len_t len,
    paddr_t paddr,
    uint32_t checksum,
    LogicalCachedExtent*) final;

  get_physical_extent_if_live_ret get_physical_extent_if_live(
    Transaction &t,
    extent_types_t type,
    paddr_t addr,
    laddr_t laddr,
    extent_len_t len) final;
private:
  Cache &cache;


  struct {
    uint64_t num_alloc_extents = 0;
    uint64_t num_alloc_extents_iter_nexts = 0;
  } stats;

  op_context_t<laddr_t> get_context(Transaction &t) {
    return op_context_t<laddr_t>{cache, t};
  }

  seastar::metrics::metric_group metrics;
  void register_metrics();

  /**
   * update_refcount
   *
   * Updates refcount, returns resulting refcount
   */
  struct update_refcount_ret_bare_t {
    ref_update_result_t ref_update_res;
    BtreeLBAMappingRef mapping;
  };
  using update_refcount_iertr = ref_iertr;
  using update_refcount_ret = update_refcount_iertr::future<
    update_refcount_ret_bare_t>;
  update_refcount_ret update_refcount(
    Transaction &t,
    laddr_t addr,
    int delta,
    bool cascade_remove);

  /**
   * _update_mapping
   *
   * Updates mapping, removes if f returns nullopt
   */
  struct update_mapping_ret_bare_t {
    lba_map_val_t map_value;
    BtreeLBAMappingRef mapping;
  };
  using _update_mapping_iertr = ref_iertr;
  using _update_mapping_ret = ref_iertr::future<
    update_mapping_ret_bare_t>;
  using update_func_t = std::function<
    lba_map_val_t(const lba_map_val_t &v)
    >;
  _update_mapping_ret _update_mapping(
    Transaction &t,
    laddr_t addr,
    update_func_t &&f,
    LogicalCachedExtent*);

  alloc_extents_ret _alloc_extents(
    Transaction &t,
    laddr_t hint,
    std::vector<alloc_mapping_info_t> &alloc_infos,
    extent_ref_count_t refcount);

  ref_ret _incref_extent(
    Transaction &t,
    laddr_t addr,
    int delta) {
    ceph_assert(delta > 0);
    return update_refcount(t, addr, delta, false
    ).si_then([](auto res) {
      return std::move(res.ref_update_res);
    });
  }

  alloc_extent_iertr::future<std::vector<BtreeLBAMappingRef>> alloc_cloned_mappings(
    Transaction &t,
    laddr_t laddr,
    std::vector<alloc_mapping_info_t> alloc_infos)
  {
#ifndef NDEBUG
    for (auto &alloc_info : alloc_infos) {
      assert(alloc_info.val.get_laddr() != L_ADDR_NULL);
    }
#endif
    return seastar::do_with(
      std::move(alloc_infos),
      [this, &t, laddr](auto &alloc_infos) {
      return _alloc_extents(
	t,
	laddr,
	alloc_infos,
	EXTENT_DEFAULT_REF_COUNT
      ).si_then([&alloc_infos](auto mappings) {
	assert(alloc_infos.size() == mappings.size());
	std::vector<BtreeLBAMappingRef> rets;
	auto mit = mappings.begin();
	auto ait = alloc_infos.begin();
	for (; mit != mappings.end(); mit++, ait++) {
	  auto mapping = static_cast<BtreeLBAMapping*>(mit->release());
	  auto &alloc_info = *ait;
	  assert(mapping->get_key() == alloc_info.key);
	  assert(mapping->get_raw_val().get_laddr() ==
	    alloc_info.val.get_laddr());
	  assert(mapping->get_length() == alloc_info.len);
	  rets.emplace_back(mapping);
	}
	return rets;
      });
    });
  }

  using _get_mapping_ret = get_mapping_iertr::future<BtreeLBAMappingRef>;
  _get_mapping_ret _get_mapping(
    Transaction &t,
    laddr_t offset);

  using _get_original_mappings_ret = get_mappings_ret;
  _get_original_mappings_ret _get_original_mappings(
    op_context_t<laddr_t> c,
    std::list<BtreeLBAMappingRef> &pin_list);

  using _decref_intermediate_ret = ref_iertr::future<
    std::optional<ref_update_result_t>>;
  _decref_intermediate_ret _decref_intermediate(
    Transaction &t,
    laddr_t addr,
    extent_len_t len);
};
using BtreeLBAManagerRef = std::unique_ptr<BtreeLBAManager>;

}
