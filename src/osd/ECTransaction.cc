// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iostream>
#include <vector>
#include <sstream>

#include "ECTransaction.h"
#include "ECUtil.h"
#include "os/ObjectStore.h"
#include "common/inline_variant.h"

using std::less;
using std::make_pair;
using std::map;
using std::pair;
using std::set;
using std::string;
using std::vector;

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;
using ceph::ErasureCodeInterfaceRef;

void debug(hobject_t oid, const std::string &str, ECUtil::shard_extent_map_t &map, DoutPrefixProvider *dpp)
{
#if DEBUG_EC_BUFFERS
  ldpp_dout(dpp, 0)
    << "EC_DEBUG_BUFFERS: generate_transactions: "
    << "oid: " << oid
    << " " << str << " " << map.debug_string(2048, 8) << dendl;
#else
  ldpp_dout(dpp, 20)
    << "generate_transactions: "
    << "oid: " << oid
    << str << map << dendl;
#endif
}

static void encode_and_write(
  pg_t pgid,
  const hobject_t &oid,
  ErasureCodeInterfaceRef &ecimpl,
  ECTransaction::WritePlanObj &plan,
  ECUtil::shard_extent_map_t &shard_extent_map,
  uint32_t flags,
  map<shard_id_t, ObjectStore::Transaction> *transactions,
  DoutPrefixProvider *dpp)
{
  int r = shard_extent_map.encode(ecimpl, plan.hinfo, plan.orig_size);
  ceph_assert(r == 0);

  debug(oid, "parity", shard_extent_map, dpp);
  ldpp_dout(dpp, 20) << __func__ << ": " << oid
	             << " plan " << plan
	             << dendl;

  for (auto &&[shard_id, t]: *transactions) {
    if (plan.will_write.contains(shard_id)) {
      extent_map emap = shard_extent_map.get_extent_map(shard_id);
      extent_set to_write_eset = plan.will_write[shard_id];
      if (to_write_eset.begin().get_start() >= plan.orig_size) {
	t.set_alloc_hint(
	  coll_t(spg_t(pgid, shard_id)),
	  ghobject_t(oid, ghobject_t::NO_GEN, shard_id),
	  0, 0,
	  CEPH_OSD_ALLOC_HINT_FLAG_SEQUENTIAL_WRITE |
	  CEPH_OSD_ALLOC_HINT_FLAG_APPEND_ONLY);
      }

      for (auto &&[offset, len]: to_write_eset) {
	buffer::list bl;
        shard_extent_map.zero_pad(shard_id, offset, len);
	shard_extent_map.get_buffer(shard_id, offset, len, bl);
        t.write(coll_t(spg_t(pgid, shard_id)),
	  ghobject_t(oid, ghobject_t::NO_GEN, shard_id),
	  offset, bl.length(), bl, flags);
      }
    }
  }
}

ECTransaction::WritePlanObj::WritePlanObj(
  const PGTransaction::ObjectOperation &op,
  const ECUtil::stripe_info_t &sinfo,
  uint64_t orig_size,
  const std::optional<object_info_t> &oi,
  const std::optional<object_info_t> &soi,
  const ECUtil::HashInfoRef &&hinfo,
  const ECUtil::HashInfoRef &&shinfo) :
hinfo(hinfo),
shinfo(shinfo),
orig_size(orig_size) // On-disk object sizes are rounded up to the next page.
{
  extent_set ro_writes;

  projected_size = oi?oi->size:0;

  if (soi) {
    projected_size = soi->size;
  }

  /* If we are truncating, then we need to over-write the new end to
   * the end of that page with zeros. Everything after that will get
   * truncated to the shard objects. */
  if (op.truncate &&
      op.truncate->first < projected_size) {

    ro_writes.insert(ECUtil::align_page_prev(op.truncate->first),
      ECUtil::align_page_next(projected_size));
  }

  for (auto &&extent: op.buffer_updates) {
    using BufferUpdate = PGTransaction::ObjectOperation::BufferUpdate;
    ceph_assertf(!boost::get<BufferUpdate::CloneRange>(&(extent.get_val())),
      "CloneRange is not allowed, do_op should have returned ENOTSUPP");

    uint64_t start = extent.get_off();
    uint64_t end = start + extent.get_len();

    ro_writes.insert(start, end - start);
  }

  extent_set outter_extent_superset;

  std::optional<ECUtil::shard_extent_set_t> inner;
  for (const auto& [ro_off, ro_len] : ro_writes) {
    /* Here, we calculate the "inner" and "outer" extent sets. The inner
     * represents the complete pages read. The outer represents the rounded
     * up/down pages. Clearly if the IO is entirely aligned, then the inner
     * and outer sets are the same and we optimise this by avoiding
     * calculating the inner in this case.
     *
     * This is useful because partially written pages must be fully read
     * from the backend as part of the RMW.
     */
    uint64_t raw_end = ro_off + ro_len;
    uint64_t outter_off = ECUtil::align_page_prev(ro_off);
    uint64_t outter_len = ECUtil::align_page_next(raw_end) - outter_off;
    uint64_t inner_off = ECUtil::align_page_next(ro_off);
    uint64_t inner_len = std::max(inner_off, ECUtil::align_page_prev(raw_end)) - inner_off;

    if (inner || outter_off != inner_off || outter_len != inner_len) {
      if (!inner) inner = ECUtil::shard_extent_set_t(will_write);
      sinfo.ro_range_to_shard_extent_set(inner_off,inner_len, *inner);
    }

    // Will write is expanded to page offsets.
    sinfo.ro_range_to_shard_extent_set(outter_off,outter_len,
      will_write, outter_extent_superset);
  }

  /* Construct the to read on the stack, to avoid having to insert and
   * erase into maps */
  ECUtil::shard_extent_set_t reads;
  if (!sinfo.supports_partial_writes())
  {
    ECUtil::shard_extent_set_t read_mask;
    sinfo.ro_size_to_stripe_aligned_read_mask(orig_size, read_mask);

    /* We are not yet attempting to optimise this path and we are instead opting to maintain the old behaviour, where
     * a full read and write is performed for every stripe.
     */
    outter_extent_superset.align(sinfo.get_chunk_size());
    if (!outter_extent_superset.empty()) {
      for (int raw_shard = 0; raw_shard < sinfo.get_k_plus_m(); raw_shard++) {
        int shard = sinfo.get_shard(raw_shard);
        will_write[shard].insert(outter_extent_superset);
        if (read_mask.contains(shard)) {
          reads[shard].insert(outter_extent_superset);
          reads[shard].intersection_of(read_mask[shard]);
        }
      }
    }
  } else {
    ECUtil::shard_extent_set_t &small_set = inner?*inner:will_write;
    ECUtil::shard_extent_set_t partial_stripe;
    ECUtil::shard_extent_set_t zero;
    ECUtil::shard_extent_set_t orig;

    sinfo.ro_range_to_shard_extent_set(projected_size,
      sinfo.logical_to_next_stripe_offset(projected_size), partial_stripe);

    uint64_t aligned_orig_size = ECUtil::align_page_next(orig_size);

    sinfo.ro_range_to_shard_extent_set(0,orig_size, orig);

    /* The zero stripe is any area that gets zeroed if not written to. It is used
     * by appends (old size -> new size) and truncates if truncate.second >
     * truncate.first.
     */
    if (aligned_orig_size < projected_size) {
      sinfo.ro_range_to_shard_extent_set(aligned_orig_size,
        projected_size - aligned_orig_size, zero, outter_extent_superset);
    }
    if (op.truncate && op.truncate->first < op.truncate->second) {
      sinfo.ro_range_to_shard_extent_set(op.truncate->first,
        op.truncate->second - op.truncate->first, zero);
    }

    for (int raw_shard = 0; raw_shard< sinfo.get_k_plus_m(); raw_shard++) {
      int shard = sinfo.get_shard(raw_shard);
      extent_set _to_read;

      if (raw_shard < sinfo.get_k()) {

        if (zero.contains(shard)) {
          will_write[shard].insert(zero.at(shard));
        }

        if (!orig.contains(shard))
          continue;

        _to_read.intersection_of(outter_extent_superset, orig.at((shard)));
        _to_read.align(CEPH_PAGE_SIZE);

        if (small_set.contains(shard)) {
          _to_read.subtract(small_set.at(shard));
        }

        if (!_to_read.empty()) {
          reads.emplace(shard, std::move(_to_read));
        }
      } else {
        will_write[shard].insert(outter_extent_superset);
      }
    }
  }

  // Do not do a read if there is nothing to read!
  if (!reads.empty()) {
     to_read = reads;
  }

  /* validate post conditions:
   * to_read should have an entry for `obj` if it isn't empty
   * and if we are reading from `obj`, we can't be renaming or
   * cloning it */
  ceph_assert(!to_read || !soi);
}

void ECTransaction::generate_transactions(
  PGTransaction *_t,
  WritePlan &plans,
  ErasureCodeInterfaceRef &ecimpl,
  pg_t pgid,
  const ECUtil::stripe_info_t &sinfo,
  const map<hobject_t, ECUtil::shard_extent_map_t> &partial_extents,
  vector<pg_log_entry_t> &entries,
  map<hobject_t, ECUtil::shard_extent_map_t>* written_map,
  map<shard_id_t, ObjectStore::Transaction> *transactions,
  set<hobject_t> *temp_added,
  set<hobject_t> *temp_removed,
  DoutPrefixProvider *dpp,
  const ceph_release_t require_osd_release)
{
  ceph_assert(written_map);
  ceph_assert(transactions);
  ceph_assert(temp_added);
  ceph_assert(temp_removed);
  ceph_assert(_t);
  auto &t = *_t;

  map<hobject_t, pg_log_entry_t*> obj_to_log;
  for (auto &&i: entries) {
    obj_to_log.insert(make_pair(i.soid, &i));
  }

  map<hobject_t, extent_set> write_plan_validation;

  t.safe_create_traverse(
    [&](pair<const hobject_t, PGTransaction::ObjectOperation> &opair)
    {
      const hobject_t &oid = opair.first;
      auto &op = opair.second;
      auto &obc_map = t.obc_map;

      auto iter = obj_to_log.find(oid);
      pg_log_entry_t *entry = iter != obj_to_log.end() ? iter->second : nullptr;

      ObjectContextRef obc;
      auto obiter = t.obc_map.find(oid);
      if (obiter != t.obc_map.end()) {
        obc = obiter->second;
      }
      if (entry) {
        ceph_assert(obc);
      } else {
        ceph_assert(oid.is_temp());
      }

      write_plan_validation[oid];

      WritePlanObj &plan = plans.plans.at(oid);

      if (oid.is_temp()) {
        if (op.is_fresh_object()) {
          temp_added->insert(oid);
        } else if (op.is_delete()) {
          temp_removed->insert(oid);
        }
      }

      if (entry &&
          entry->is_modify() &&
          op.updated_snaps) {
        bufferlist bl(op.updated_snaps->second.size() * 8 + 8);
        encode(op.updated_snaps->second, bl);
        entry->snaps.swap(bl);
        entry->snaps.reassign_to_mempool(mempool::mempool_osd_pglog);
      }

      ldpp_dout(dpp, 20) << "generate_transactions: "
                         << opair.first
                         << ", current size is "
                         << plan.orig_size
                         << " buffers are "
                         << op.buffer_updates
                         << " fresh_object: " << op.is_fresh_object()
                         << dendl;
      if (op.truncate) {
        ldpp_dout(dpp, 20) << "generate_transactions: "
                           << " truncate is "
                           << *(op.truncate)
                           << dendl;
      }

      if (entry && op.updated_snaps) {
        entry->mod_desc.update_snaps(op.updated_snaps->first);
      }

      map<string, std::optional<bufferlist> > xattr_rollback;
      bufferlist old_hinfo;
      if (plan.hinfo) {
        encode(*(plan.hinfo), old_hinfo);
        xattr_rollback[ECUtil::get_hinfo_key()] = old_hinfo;
      }

      if (op.is_none() && op.truncate && op.truncate->first == 0) {
	ceph_assert(entry);
	ceph_assert(obc);

	if (op.truncate->first != op.truncate->second) {
	  op.truncate->first = op.truncate->second;
	} else {
	  op.truncate = std::nullopt;
	}

	op.delete_first = true;
	op.init_type = PGTransaction::ObjectOperation::Init::Create();

	if (obc) {
	  /* We need to reapply all of the cached xattrs.
	     * std::map insert fortunately only writes keys
	     * which don't already exist, so this should do
	     * the right thing. */
	  op.attr_updates.insert(
	    obc->attr_cache.begin(),
	    obc->attr_cache.end());
	}
      }

      if (op.delete_first) {
	/* We also want to remove the std::nullopt entries since
	   * the keys already won't exist */
	for (auto j = op.attr_updates.begin();
	     j != op.attr_updates.end();
	  ) {
	  if (j->second) {
	    ++j;
	  } else {
	    op.attr_updates.erase(j++);
	  }
	}
	/* Fill in all current entries for xattr rollback */
	if (obc) {
	  xattr_rollback.insert(
	    obc->attr_cache.begin(),
	    obc->attr_cache.end());
	  obc->attr_cache.clear();
	}
	if (entry) {
	  entry->mod_desc.rmobject(entry->version.version);
	  for (auto &&st: *transactions) {
	    st.second.collection_move_rename(
	      coll_t(spg_t(pgid, st.first)),
	      ghobject_t(oid, ghobject_t::NO_GEN, st.first),
	      coll_t(spg_t(pgid, st.first)),
	      ghobject_t(oid, entry->version.version, st.first));
	  }
	} else {
	  for (auto &&st: *transactions) {
	    st.second.remove(
	      coll_t(spg_t(pgid, st.first)),
	      ghobject_t(oid, ghobject_t::NO_GEN, st.first));
	  }
	}
        if (plan.hinfo)
	  plan.hinfo->clear();
      }

      if (op.is_fresh_object() && entry) {
	entry->mod_desc.create();
      }

      match(
	op.init_type,
	[&](const PGTransaction::ObjectOperation::Init::None &) {},
	[&](const PGTransaction::ObjectOperation::Init::Create &_) {
	  for (auto &&st: *transactions) {
	    if (require_osd_release >= ceph_release_t::octopus) {
	      st.second.create(
		coll_t(spg_t(pgid, st.first)),
		ghobject_t(oid, ghobject_t::NO_GEN, st.first));
	    } else {
	      st.second.touch(
		coll_t(spg_t(pgid, st.first)),
		ghobject_t(oid, ghobject_t::NO_GEN, st.first));
	    }
	  }
	},
	[&](const PGTransaction::ObjectOperation::Init::Clone &cop) {
	  for (auto &&st: *transactions) {
	    st.second.clone(
	      coll_t(spg_t(pgid, st.first)),
	      ghobject_t(cop.source, ghobject_t::NO_GEN, st.first),
	      ghobject_t(oid, ghobject_t::NO_GEN, st.first));
	  }

	  if(plan.hinfo && plan.shinfo)
	    plan.hinfo->update_to(*plan.shinfo);

	  if (obc) {
	    auto cobciter = obc_map.find(cop.source);
	    ceph_assert(cobciter != obc_map.end());
	    obc->attr_cache = cobciter->second->attr_cache;
	  }
	},
	[&](const PGTransaction::ObjectOperation::Init::Rename &rop) {
	  ceph_assert(rop.source.is_temp());
	  for (auto &&st: *transactions) {
	    st.second.collection_move_rename(
	      coll_t(spg_t(pgid, st.first)),
	      ghobject_t(rop.source, ghobject_t::NO_GEN, st.first),
	      coll_t(spg_t(pgid, st.first)),
	      ghobject_t(oid, ghobject_t::NO_GEN, st.first));
	  }
	  if(plan.hinfo && plan.shinfo)
	    plan.hinfo->update_to(*plan.shinfo);
	  if (obc) {
	    auto cobciter = obc_map.find(rop.source);
	    ceph_assert(cobciter == obc_map.end());
	    obc->attr_cache.clear();
	  }
	});

      // omap not supported (except 0, handled above)
      ceph_assert(!(op.clear_omap));
      ceph_assert(!(op.omap_header));
      ceph_assert(op.omap_updates.empty());

      if (!op.attr_updates.empty()) {
	map<string, bufferlist, less<>> to_set;
	for (auto &&[attr, update]: op.attr_updates) {
	  if (update) {
	    to_set[attr] = *(update);
	  } else {
	    for (auto &&st : *transactions) {
	      st.second.rmattr(
		coll_t(spg_t(pgid, st.first)),
		ghobject_t(oid, ghobject_t::NO_GEN, st.first),
		attr);
	    }
	  }
	  if (obc) {
	    auto citer = obc->attr_cache.find(attr);
	    if (entry) {
	      if (citer != obc->attr_cache.end()) {
		// won't overwrite anything we put in earlier
		xattr_rollback.insert(
		  make_pair(
		    attr,
		    std::optional<bufferlist>(citer->second)));
	      } else {
		// won't overwrite anything we put in earlier
		xattr_rollback.insert(
		  make_pair(
		    attr,
		    std::nullopt));
	      }
	    }
	    if (update) {
	      obc->attr_cache[attr] = *(update);
	    } else if (citer != obc->attr_cache.end()) {
	      obc->attr_cache.erase(citer);
	    }
	  } else {
	    ceph_assert(!entry);
	  }
	}
	for (auto &&st : *transactions) {
	  st.second.setattrs(
	    coll_t(spg_t(pgid, st.first)),
	    ghobject_t(oid, ghobject_t::NO_GEN, st.first),
	    to_set);
	}
	ceph_assert(!xattr_rollback.empty());
      }
      if (entry && !xattr_rollback.empty()) {
	entry->mod_desc.setattrs(xattr_rollback);
      }

      if (op.alloc_hint) {
	/* logical_to_next_chunk_offset() scales down both aligned and
	   * unaligned offsets
	   
	   * we don't bother to roll this back at this time for two reasons:
	   * 1) it's advisory
	   * 2) we don't track the old value */
	uint64_t object_size = sinfo.logical_to_next_chunk_offset(
	  op.alloc_hint->expected_object_size);
	uint64_t write_size = sinfo.logical_to_next_chunk_offset(
	  op.alloc_hint->expected_write_size);
	
	for (auto &&st : *transactions) {
	  st.second.set_alloc_hint(
	    coll_t(spg_t(pgid, st.first)),
	    ghobject_t(oid, ghobject_t::NO_GEN, st.first),
	    object_size,
	    write_size,
	    op.alloc_hint->flags);
	}
      }

      ECUtil::shard_extent_map_t to_write(&sinfo);
      auto pextiter = partial_extents.find(oid);
      if (pextiter != partial_extents.end()) {
	to_write = pextiter->second;
      }
      debug(oid, "to_write", to_write, dpp);

      vector<pair<uint64_t, uint64_t>> rollback_extents;

      if (op.truncate && op.truncate->first < plan.orig_size) {
	ceph_assert(!op.is_fresh_object());
	to_write.erase_after_ro_offset(plan.orig_size); // causes encode to invent zeros

        debug(oid, "truncate_erase", to_write, dpp);

	if (entry && !op.is_fresh_object()) {
	  uint64_t restore_from = sinfo.logical_to_prev_chunk_offset(
	    op.truncate->first);
	  uint64_t restore_len = sinfo.aligned_logical_offset_to_chunk_offset(
	    plan.orig_size -
	    sinfo.logical_to_prev_stripe_offset(op.truncate->first));
	  ceph_assert(rollback_extents.empty());

	  rollback_extents.emplace_back(
	    make_pair(restore_from, restore_len));
	  for (auto &&[shard, t]: *transactions) {
	    t.touch(
	      coll_t(spg_t(pgid, shard)),
	      ghobject_t(oid, entry->version.version, shard));
	    t.clone_range(
	      coll_t(spg_t(pgid, shard)),
	      ghobject_t(oid, ghobject_t::NO_GEN, shard),
	      ghobject_t(oid, entry->version.version, shard),
	      restore_from,
	      restore_len,
	      restore_from);
	  }
	}

	for (auto &&[shard, t] : *transactions) {
	  t.truncate(
	    coll_t(spg_t(pgid, shard)),
	    ghobject_t(oid, ghobject_t::NO_GEN, shard),
	    sinfo.ro_offset_to_shard_offset(plan.orig_size, shard));
	}
      }

      uint32_t fadvise_flags = 0;
      for (auto &&extent: op.buffer_updates) {
	using BufferUpdate = PGTransaction::ObjectOperation::BufferUpdate;
	bufferlist bl;
	match(
	  extent.get_val(),
	  [&](const BufferUpdate::Write &wop) {
	    bl = wop.buffer;
	    fadvise_flags |= wop.fadvise_flags;
	  },
	  [&](const BufferUpdate::Zero &) {
	    bl.append_zero(extent.get_len());
	  },
	  [&](const BufferUpdate::CloneRange &) {
	    ceph_abort_msg("CloneRange is not allowed, do_op should have returned ENOTSUPP");
	  });

	uint64_t off = extent.get_off();
	uint64_t len = extent.get_len();

        sinfo.ro_range_to_shard_extent_map(off, len, bl, to_write);
        debug(oid, "overlay_buffer", to_write, dpp);
      }

      extent_set clone_ranges;
      for (auto &&[shard, eset] : plan.will_write) {
        clone_ranges.insert(eset);
      }

      uint64_t clone_max = ECUtil::align_page_next(plan.orig_size);

      if (op.delete_first) {
        clone_max = 0;
      } else if (op.truncate && op.truncate->first < clone_max) {
        clone_max = ECUtil::align_page_next(op.truncate->first);
      }
      if (!sinfo.supports_ec_optimizations()) {
        clone_max = sinfo.logical_to_next_stripe_offset(clone_max);
      }
      uint64_t rollback_max = sinfo.logical_to_next_stripe_offset(clone_max);
      clone_ranges.erase(rollback_max, 0 - rollback_max - 1);
      for (auto &[start, len] : clone_ranges) {
        rollback_extents.emplace_back(start, len);
      }

      if (!to_write.empty()) {
        // Depending on the write, we may or may not have the parity buffers.
        // Here we invent some buffers.

        to_write.insert_parity_buffers();
        debug(oid, "overwrite insert parity", to_write, dpp);

        /* Generate the clone transactions for every shard. These are the same
         * for each shard and cover complete chunks.
         *
         * This could probably be more efficient...
         */
        auto clone_region = to_write.get_extent_superset();
        clone_region.align(sinfo.get_chunk_size());

        ECUtil::shard_extent_set_t cloneable_range;
        sinfo.ro_range_to_shard_extent_set_with_parity(0, clone_max, cloneable_range);

        for (auto &&[shard, eset] : cloneable_range) {
          eset.intersection_of(clone_ranges);
          shard_id_t shard_id(shard);

          auto &&st = (*transactions)[shard_id];

          if (!eset.empty()) {
            entry->written_shards.insert(shard);
            st.touch(
              coll_t(spg_t(pgid, shard_id)),
              ghobject_t(oid, entry->version.version, shard_id));
          }
          // First write to this shard
          for (auto &[start, len] : eset) {
            st.clone_range(
              coll_t(spg_t(pgid, shard_id)),
              ghobject_t(oid, ghobject_t::NO_GEN, shard_id),
              ghobject_t(oid, entry->version.version, shard_id),
              start,
              len,
              start);
          }
        }

	encode_and_write(pgid, oid, ecimpl, plan, to_write, fadvise_flags,
	  transactions, dpp);
      }

      written_map->emplace(oid, std::move(to_write));

      if (!rollback_extents.empty() && entry) {
	if (entry) {
	  entry->mod_desc.rollback_extents(entry->version.version, rollback_extents,
            ECUtil::align_page_next(plan.orig_size));
	}
        if (entry->written_shards.size() == ecimpl->get_chunk_count()) {
          // More efficient to encode an empty set to mean all shards
          entry->written_shards.clear();
        }
        if (plan.hinfo)
	  plan.hinfo->set_total_chunk_size_clear_hash(
	    sinfo.logical_to_next_stripe_offset(plan.projected_size));
      }

      if (entry && plan.orig_size < plan.projected_size) {
	ldpp_dout(dpp, 20) << "generate_transactions: marking append "
			   << plan.orig_size
			   << dendl;
	entry->mod_desc.append(plan.orig_size);
      }

      if (!op.is_delete()) {
        bufferlist hbuf;
        if (plan.hinfo) {
          encode(*plan.hinfo, hbuf);
          for (auto &&i : *transactions) {
            i.second.setattr(
              coll_t(spg_t(pgid, i.first)),
              ghobject_t(oid, ghobject_t::NO_GEN, i.first),
              ECUtil::get_hinfo_key(),
              hbuf);
          }
        }
      }
    });
}

std::ostream& ECTransaction::operator<<(std::ostream& lhs, const ECTransaction::WritePlan& rhs)
{
  return lhs << " { invalidate_caches : " << rhs.invalidates_cache
      << ", plans : " << rhs.plans
      << "}";
}

std::ostream& ECTransaction::operator<<(std::ostream& lhs, const ECTransaction::WritePlanObj& obj)
{
  return lhs
    << "to_read: " << obj.to_read
    << " will_write: " << obj.will_write
    << " hinfo: " << obj.hinfo
    << " shinfo: " << obj.shinfo
    << " orig_size: " << obj.orig_size
    << " projected_size: " << obj.projected_size;
}