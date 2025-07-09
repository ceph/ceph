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

void debug(const hobject_t &oid, const std::string &str,
           const ECUtil::shard_extent_map_t &map, DoutPrefixProvider *dpp) {
  ldpp_dout(dpp, 20)
    << " generate_transactions: " << "oid: " << oid << str << map << dendl;
  ldpp_dout(dpp, 30)
    << "EC_DEBUG_BUFFERS: " << map.debug_string(2048, 8) << dendl;
}

void ECTransaction::Generate::encode_and_write() {
  // For PDW, we already have necessary parity buffers.
  if (!plan.do_parity_delta_write) {
    to_write.insert_parity_buffers();
  }

  // If partial writes are not supported, pad out to_write to a full stripe.
  if (!sinfo.supports_partial_writes()) {
    for (auto &&[shard, eset]: plan.will_write) {
      if (sinfo.get_raw_shard(shard) >= sinfo.get_k()) {
        continue;
      }

      for (auto [off, len]: eset) {
        to_write.zero_pad(shard, off, len);
      }
    }
  }

  int r = 0;
  if (plan.do_parity_delta_write) {
    /* For parity delta writes, we remove any unwanted writes before calculating
     * the parity.
     */
    read_sem->zero_pad(plan.will_write);
    to_write.pad_with_other(plan.will_write, *read_sem);
    r = to_write.encode_parity_delta(ec_impl, *read_sem);
  } else {
    r = to_write.encode(ec_impl, plan.hinfo, plan.orig_size);
  }
  ceph_assert(r == 0);
  // Remove any unnecessary writes.
  //to_write = to_write.intersect(plan.will_write);

  debug(oid, "parity", to_write, dpp);
  ldpp_dout(dpp, 20) << __func__ << ": " << oid
	             << " plan " << plan
	             << dendl;

  for (auto &&[shard, to_write_eset]: plan.will_write) {
    /* Zero pad, even if we are not writing.  The extent cache requires that
     * all shards are fully populated with write data, even if the OSDs are
     * down. This is not a fundamental requirement of the cache, but dealing
     * with implied zeros due to incomplete writes is both difficult and
     * removes a level of protection against bugs.
     */
    for (auto &&[offset, len]: to_write_eset) {
      to_write.zero_pad(shard, offset, len);
    }

    if (transactions.contains(shard)) {
      auto &t = transactions.at(shard);
      if (to_write_eset.begin().get_start() >= plan.orig_size) {
        t.set_alloc_hint(
          coll_t(spg_t(pgid, shard)),
          ghobject_t(oid, ghobject_t::NO_GEN, shard),
          0, 0,
          CEPH_OSD_ALLOC_HINT_FLAG_SEQUENTIAL_WRITE |
          CEPH_OSD_ALLOC_HINT_FLAG_APPEND_ONLY);
      }

      for (auto &&[offset, len]: to_write_eset) {
        buffer::list bl;
        to_write.get_buffer(shard, offset, len, bl);
        t.write(coll_t(spg_t(pgid, shard)),
                ghobject_t(oid, ghobject_t::NO_GEN, shard),
                offset, bl.length(), bl, fadvise_flags);
      }
    }
  }
}

ECTransaction::WritePlanObj::WritePlanObj(
    const hobject_t &hoid,
    const PGTransaction::ObjectOperation &op,
    const ECUtil::stripe_info_t &sinfo,
    const shard_id_set readable_shards,
    const shard_id_set writable_shards,
    const bool object_in_cache,
    uint64_t orig_size,
    const std::optional<object_info_t> &oi,
    const std::optional<object_info_t> &soi,
    const ECUtil::HashInfoRef &&hinfo,
    const ECUtil::HashInfoRef &&shinfo,
    const unsigned pdw_write_mode
  ) :
  hoid(hoid),
  will_write(sinfo.get_k_plus_m()),
  hinfo(hinfo),
  shinfo(shinfo),
  orig_size(orig_size), // On-disk object sizes are rounded up to the next page.
  projected_size(soi?soi->size:(oi?oi->size:0))
{
  extent_set unaligned_ro_writes;
  hobject_t source;
  /* Certain transactions mean that the cache is invalid:
   * 1. Clone operations invalidate the *target*
   * 2. ALL delete operations (do NOT use is_delete() here!!!)
   * 3. Truncates that reduce size.
   */
  invalidates_cache = op.has_source(&source) || op.delete_first || projected_size < orig_size;

  op.buffer_updates.to_interval_set(unaligned_ro_writes);

  /* Calculate any non-aligned pages. These need to be read and written */
  extent_set aligned_ro_writes(unaligned_ro_writes);
  aligned_ro_writes.align(EC_ALIGN_SIZE);
  extent_set partial_page_ro_writes(aligned_ro_writes);
  partial_page_ro_writes.subtract(unaligned_ro_writes);
  partial_page_ro_writes.align(EC_ALIGN_SIZE);

  extent_set write_superset;
  for (auto &&[off, len] : unaligned_ro_writes) {
    sinfo.ro_range_to_shard_extent_set_with_superset(
      off, len, will_write, write_superset);
  }
  write_superset.align(EC_ALIGN_SIZE);

  shard_id_set writable_parity_shards = shard_id_set::intersection(sinfo.get_parity_shards(), writable_shards);
  if (write_superset.size() > 0) {
    for (auto shard : writable_parity_shards) {
      will_write[shard].insert(write_superset);
    }

    ECUtil::shard_extent_set_t reads(sinfo.get_k_plus_m());
    ECUtil::shard_extent_set_t read_mask(sinfo.get_k_plus_m());

    if (!sinfo.supports_partial_writes()) {
      for (shard_id_t shard; shard < sinfo.get_k_plus_m(); ++shard) {
        will_write[shard].insert(write_superset);
      }
      will_write.align(sinfo.get_chunk_size());
      reads = will_write;
      sinfo.ro_size_to_read_mask(sinfo.ro_offset_to_next_stripe_ro_offset(orig_size), read_mask);
      reads.intersection_of(read_mask);
      do_parity_delta_write = false;
    } else {
      will_write.align(EC_ALIGN_SIZE);
      ECUtil::shard_extent_set_t pdw_reads(will_write);

      sinfo.ro_size_to_read_mask(ECUtil::align_next(orig_size), read_mask);

      /* Next we need to add the reads required for a conventional write */
      for (auto shard : sinfo.get_data_shards()) {
        reads[shard].insert(write_superset);
        if (will_write.contains(shard)) {
          reads[shard].subtract(will_write.at(shard));
        }
        if (reads[shard].empty()) {
          reads.erase(shard);
        }
      }

      /* We now need to add in the partial page ro writes. This is not particularly
       * efficient as the are many divs in here, but non-4k aligned writes are
       * not very efficient anyway
       */
      for (auto &&[off, len] : partial_page_ro_writes) {
        sinfo.ro_range_to_shard_extent_set(
          off, len, reads);
      }

      reads.intersection_of(read_mask);

      /* Here we decide if we want to do a conventional write or a parity delta write. */
      if (sinfo.supports_parity_delta_writes() && !object_in_cache &&
          orig_size == projected_size && !reads.empty()) {

        shard_id_set read_shards = reads.get_shard_id_set();
        shard_id_set pdw_read_shards = pdw_reads.get_shard_id_set();

        if (pdw_write_mode != 0) {
          do_parity_delta_write = (pdw_write_mode == 2);
        } else if (!shard_id_set::difference(pdw_read_shards, readable_shards).empty()) {
          // Some kind of reconstruct would be needed for PDW, so don't bother.
          do_parity_delta_write = false;
        } else if (!shard_id_set::difference(read_shards, readable_shards).empty()) {
          // Some kind of reconstruct is needed for conventional, but NOT for PDW!
          do_parity_delta_write = true;
        } else {
          /* Everything we need for both is available, opt for which ever is less
           * reads.
           */
          do_parity_delta_write = pdw_read_shards.size() < read_shards.size();
        }

        if (do_parity_delta_write) {
          to_read = std::move(pdw_reads);
          reads.clear(); // So we don't stash it at the end.
        }
      }

      /* NOTE: We intentionally leave un-writable shards in the write plan.  As
       * it is actually less efficient to take them out:- PDWs still need to
       * compute the deltas and conventional writes still need to calculate the
       * parity. The transaction will be dropped by generate_transactions.
       */
    }

    /* If the read is not empty AND the object is not going tbe deleted as
     * part of the op, then record that we need to do the reads.  Potentially
     * some performance could be gained by not calculating reads at all if
     * delete_first is set, but this is not a performance-critical code path.
     */
    if (!reads.empty() && !op.delete_first) {
      to_read = std::move(reads);
    }
  }

  /* Plan for truncates. A truncate can reduce a stripe to a partial stripe.
   * This means we must update the parity buffers.  To do this, we must
   * read the existing data on the partial stripe.
   */
  if (op.truncate && op.truncate->first < orig_size) {
    ECUtil::shard_extent_set_t truncate_read(sinfo.get_k_plus_m());
    extent_set truncate_write;
    uint64_t prev_stripe = sinfo.ro_offset_to_prev_stripe_ro_offset(op.truncate->first);
    uint64_t next_align = ECUtil::align_next(op.truncate->first);
    sinfo.ro_range_to_shard_extent_set_with_superset(
      prev_stripe, next_align - prev_stripe,
      truncate_read, truncate_write);

    /* We must always update the entire parity chunk, even if we only read
     * a small amount of one shard.
     */
    truncate_write.align(sinfo.get_chunk_size());

    if (!truncate_read.empty()) {
      if (to_read) {
        to_read->insert(truncate_read);
      } else {
        to_read = std::move(truncate_read);
      }

      // We only need to update the parity buffer for the write
      for (auto && shard : sinfo.get_parity_shards()) {
        will_write[shard] = truncate_write;
      }
    }
  }

  /* validate post conditions:
   * to_read should have an entry for `obj` if it isn't empty
   * and if we are reading from `obj`, we can't be renaming or
   * cloning it */
  ceph_assert(!to_read || !soi);
}

void ECTransaction::Generate::all_shards_written() {
  ceph_assert(!written_shards_final);
  if (entry) {
    entry->written_shards.insert_range(shard_id_t(0), sinfo.get_k_plus_m());
  }
}

void ECTransaction::Generate::shard_written(const shard_id_t shard) {
  ceph_assert(!written_shards_final);
  if (entry) {
    entry->written_shards.insert(shard);
  }
}

void ECTransaction::Generate::shards_written(const shard_id_set &shards) {
  ceph_assert(!written_shards_final);
  if (entry) {
    entry->written_shards.insert(shards);
  }
}

void ECTransaction::Generate::zero_truncate_to_delete() {
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

void ECTransaction::Generate::delete_first() {
  /* We also want to remove the std::nullopt entries since
   * the keys already won't exist */
  for (auto j = op.attr_updates.begin();
       j != op.attr_updates.end();) {
    if (j->second) {
      ++j;
    } else {
      j = op.attr_updates.erase(j);
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
    all_shards_written();
    for (auto &&[shard, t]: transactions) {
      t.collection_move_rename(
        coll_t(spg_t(pgid, shard)),
        ghobject_t(oid, ghobject_t::NO_GEN, shard),
        coll_t(spg_t(pgid, shard)),
        ghobject_t(oid, entry->version.version, shard));
    }
  } else {
    for (auto &&[shard, t]: transactions) {
      t.remove(
        coll_t(spg_t(pgid, shard)),
        ghobject_t(oid, ghobject_t::NO_GEN, shard));
    }
  }
  if (plan.hinfo)
    plan.hinfo->clear();
}

void ECTransaction::Generate::process_init() {
  match(
    op.init_type,
    [&](const PGTransaction::ObjectOperation::Init::None &) {},
    [&](const PGTransaction::ObjectOperation::Init::Create &_) {
      all_shards_written();
      for (auto &&[shard, t]: transactions) {
        if (osdmap->require_osd_release >= ceph_release_t::octopus) {
          t.create(
            coll_t(spg_t(pgid, shard)),
            ghobject_t(oid, ghobject_t::NO_GEN, shard));
        } else {
          t.touch(
            coll_t(spg_t(pgid, shard)),
            ghobject_t(oid, ghobject_t::NO_GEN, shard));
        }
      }
    },
    [&](const PGTransaction::ObjectOperation::Init::Clone &cop) {
      all_shards_written();
      for (auto &&[shard, t]: transactions) {
        t.clone(
          coll_t(spg_t(pgid, shard)),
          ghobject_t(cop.source, ghobject_t::NO_GEN, shard),
          ghobject_t(oid, ghobject_t::NO_GEN, shard));
      }

      if (plan.hinfo && plan.shinfo) {
        plan.hinfo->update_to(*plan.shinfo);
      }

      if (obc) {
        auto cobciter = t.obc_map.find(cop.source);
        ceph_assert(cobciter != t.obc_map.end());
        obc->attr_cache = cobciter->second->attr_cache;
      }
    },
    [&](const PGTransaction::ObjectOperation::Init::Rename &rop) {
      ceph_assert(rop.source.is_temp());
      all_shards_written();
      for (auto &&[shard, t]: transactions) {
        t.collection_move_rename(
          coll_t(spg_t(pgid, shard)),
          ghobject_t(rop.source, ghobject_t::NO_GEN, shard),
          coll_t(spg_t(pgid, shard)),
          ghobject_t(oid, ghobject_t::NO_GEN, shard));
      }
      if (plan.hinfo && plan.shinfo) {
        plan.hinfo->update_to(*plan.shinfo);
      }
      if (obc) {
        auto cobciter = t.obc_map.find(rop.source);
        ceph_assert(cobciter == t.obc_map.end());
        obc->attr_cache.clear();
      }
    });
}

void alloc_hint(PGTransaction::ObjectOperation& op,
      shard_id_map<ObjectStore::Transaction> &transactions,
      pg_t &pgid,
      const hobject_t &oid,
      const ECUtil::stripe_info_t &sinfo) {
  /* ro_offset_to_next_chunk_offset() scales down both aligned and
   * unaligned offsets

   * we don't bother to roll this back at this time for two reasons:
   * 1) it's advisory
   * 2) we don't track the old value */
  uint64_t object_size = sinfo.ro_offset_to_next_chunk_offset(
    op.alloc_hint->expected_object_size);
  uint64_t write_size = sinfo.ro_offset_to_next_chunk_offset(
    op.alloc_hint->expected_write_size);

  for (auto &&[shard, t]: transactions) {
    t.set_alloc_hint(
      coll_t(spg_t(pgid, shard)),
      ghobject_t(oid, ghobject_t::NO_GEN, shard),
      object_size,
      write_size,
      op.alloc_hint->flags);
  }
}

ECTransaction::Generate::Generate(PGTransaction &t,
    ErasureCodeInterfaceRef &ec_impl,
    pg_t &pgid,
    const ECUtil::stripe_info_t &sinfo,
    const std::map<hobject_t, ECUtil::shard_extent_map_t> &partial_extents,
    std::map<hobject_t, ECUtil::shard_extent_map_t> *written_map,
    shard_id_map<ceph::os::Transaction> &transactions,
    const OSDMapRef &osdmap,
    const hobject_t &oid,
    PGTransaction::ObjectOperation &op,
    WritePlanObj &plan,
    DoutPrefixProvider *dpp,
    pg_log_entry_t *entry)
  : t(t),
    ec_impl(ec_impl),
    pgid(pgid),
    sinfo(sinfo),
    transactions(transactions),
    dpp(dpp),
    osdmap(osdmap),
    entry(entry),
    oid(oid),
    op(op),
    plan(plan),
    read_sem(&sinfo),
    to_write(&sinfo) {

  vector<unsigned> old_transaction_counts(sinfo.get_k_plus_m());

  for (auto &&[shard, t] : transactions) {
    old_transaction_counts[int(shard)] = t.get_num_ops();
  }

  auto obiter = t.obc_map.find(oid);
  if (obiter != t.obc_map.end()) {
    obc = obiter->second;
  }

  if (entry) {
    ceph_assert(obc);
  } else {
    ceph_assert(oid.is_temp());
  }

  if (entry && entry->is_modify() && op.updated_snaps) {
    bufferlist bl(op.updated_snaps->second.size() * 8 + 8);
    encode(op.updated_snaps->second, bl);
    entry->snaps.swap(bl);
    entry->snaps.reassign_to_mempool(mempool::mempool_osd_pglog);
  }

  ldpp_dout(dpp, 20) << __func__ << ": " << oid << plan
                     << " fresh_object: " << op.is_fresh_object()
                     << dendl;
  if (op.truncate) {
    ldpp_dout(dpp, 20) << __func__ << ": truncate is " << *(op.truncate) << dendl;
  }

  if (entry && op.updated_snaps) {
    entry->mod_desc.update_snaps(op.updated_snaps->first);
  }

  bufferlist old_hinfo;
  if (plan.hinfo) {
    encode(*(plan.hinfo), old_hinfo);
    xattr_rollback[ECUtil::get_hinfo_key()] = old_hinfo;
  }

  if (op.is_none() && op.truncate && op.truncate->first == 0) {
    zero_truncate_to_delete();
  }

  if (op.delete_first) {
    delete_first();
  }

  if (op.is_fresh_object() && entry) {
    entry->mod_desc.create();
  }

  process_init();

  // omap not supported (except 0, handled above)
  ceph_assert(!(op.clear_omap) && !(op.omap_header) && op.omap_updates.empty());

  if (op.alloc_hint) {
    all_shards_written();
    alloc_hint(op, transactions, pgid, oid, sinfo);
  }

  auto pextiter = partial_extents.find(oid);
  if (pextiter != partial_extents.end()) {
    if (plan.do_parity_delta_write) {
      read_sem = pextiter->second;
    } else {
      to_write = pextiter->second;
    }
  }
  debug(oid, "to_write", to_write, dpp);
  ldpp_dout(dpp, 20) << " generate_transactions: plan: " << plan << dendl;

  if (op.truncate && op.truncate->first < plan.orig_size) {
    truncate();
  }

  overlay_writes();
  appends_and_clone_ranges();

  if (!to_write.empty()) {
    encode_and_write();
  }

  written_map->emplace(oid, std::move(to_write));

  if (entry && plan.hinfo) {
    plan.hinfo->set_total_chunk_size_clear_hash(
      sinfo.ro_offset_to_next_stripe_ro_offset(plan.projected_size));
  }

  if (entry && plan.orig_size < plan.projected_size) {
    entry->mod_desc.append(ECUtil::align_next(plan.orig_size));
  }

  if (op.is_delete()) {
    handle_deletes();
  }

  // On a size change, we want to update OI on all shards
  if (plan.orig_size != plan.projected_size) {
    all_shards_written();
  } else {
    // All primary shards must always be written, regardless of the write plan.
    shards_written(sinfo.get_parity_shards());
    shard_written(shard_id_t(0));
  }

  written_and_present_shards();

  if (!op.attr_updates.empty()) {
    attr_updates();
  }

  if (!entry) {
    return;
  }

  if (!xattr_rollback.empty()) {
    entry->mod_desc.setattrs(xattr_rollback);
  }

  /* It is essential for rollback that every shard with a non-empty transaction
   * is recorded in written_shards. In fact written shards contains every
   * shard that would have a transaction if it were present. This is why we do
   * not simply construct written shards here.
   */
  for (auto &&[shard, t] : transactions) {
    if (t.get_num_ops() > old_transaction_counts[int(shard)] &&
        !entry->is_written_shard(shard)) {
      ldpp_dout(dpp, 20) << __func__ << " Transaction for shard " << shard << ": ";
      Formatter *f = Formatter::create("json");
      f->open_object_section("t");
      t.dump(f);
      f->close_section();
      f->flush(*_dout);
      delete f;
      *_dout << dendl;
      ceph_abort_msg("Written shard not set, but messages present. ");
    }
  }
}

void ECTransaction::Generate::truncate() {
  ceph_assert(!op.is_fresh_object());
  /* We always read aligned. If the new size is not aligned, there will be
   * some data in the read buffer that needs to be zeroed before the parity
   * is calculate.  By simply removing the buffer, the parity encode functions
   * will assume zeros.
   */
  to_write.erase_after_ro_offset(op.truncate->first);
  all_shards_written();

  debug(oid, "truncate_erase", to_write, dpp);

  if (entry && !op.is_fresh_object()) {
    // Truncate each shard to match the new *actual* size
    ECUtil::shard_extent_set_t truncate_eset(sinfo.get_k_plus_m());
    ECUtil::shard_extent_set_t new_size_eset(sinfo.get_k_plus_m());
    sinfo.ro_size_to_read_mask(plan.orig_size, truncate_eset);
    sinfo.ro_range_to_shard_extent_set_with_parity(0, op.truncate->first,
                                         new_size_eset);
    truncate_eset.subtract(new_size_eset);

    uint64_t clone_start = std::numeric_limits<uint64_t>::max();
    uint64_t clone_end = 0;

    shard_id_set clone_shards; // intentionally left blank!

    for (auto &&[shard, eset]: truncate_eset) {
      clone_shards.insert(shard);
      if (!transactions.contains(shard)) {
        continue;
      }

      auto &t = transactions.at(shard);
      uint64_t start = eset.range_start();
      uint64_t start_align_prev = ECUtil::align_prev(start);
      uint64_t start_align_next = ECUtil::align_next(start);
      uint64_t end = eset.range_end();
      t.touch(
        coll_t(spg_t(pgid, shard)),
        ghobject_t(oid, entry->version.version, shard));
      t.clone_range(
        coll_t(spg_t(pgid, shard)),
        ghobject_t(oid, ghobject_t::NO_GEN, shard),
        ghobject_t(oid, entry->version.version, shard),
        start_align_prev,
        end - start_align_prev,
        start_align_prev);

      // First truncate to exactly the right size.
      t.truncate(
        coll_t(spg_t(pgid, shard)),
        ghobject_t(oid, ghobject_t::NO_GEN, shard),
        start);

      /* We have truncated to the correct size, but we guarantee aligned
       * shard sizes. So here, we truncate back up to an aligned size if needed.
       */
      if (start != start_align_next) {
        t.truncate(
          coll_t(spg_t(pgid, shard)),
          ghobject_t(oid, ghobject_t::NO_GEN, shard),
          start_align_next);
      }

      if (clone_start > start_align_prev) {
        clone_start = start_align_prev;
      }
      if (clone_end < end) {
        clone_end = end;
      }
    }
    shards_written(clone_shards);
    rollback_extents.emplace_back(make_pair(clone_start, clone_end));
    rollback_shards.emplace_back(clone_shards);
  }
}

void ECTransaction::Generate::overlay_writes() {
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
        ceph_abort_msg(
          "CloneRange is not allowed, do_op should have returned ENOTSUPP");
      });

    uint64_t off = extent.get_off();
    uint64_t len = extent.get_len();

    sinfo.ro_range_to_shard_extent_map(off, len, bl, to_write);
    debug(oid, "overlay_buffer", to_write, dpp);
  }
}

void ECTransaction::Generate::appends_and_clone_ranges() {

  extent_set clone_ranges = plan.will_write.get_extent_superset();
  uint64_t clone_max = ECUtil::align_next(plan.orig_size);

  if (op.delete_first) {
    clone_max = 0;
  } else if (op.truncate && op.truncate->first < clone_max) {
    clone_max = ECUtil::align_next(op.truncate->first);
  }
  ECUtil::shard_extent_set_t cloneable_range(sinfo.get_k_plus_m());
  sinfo.ro_size_to_read_mask(clone_max, cloneable_range);

  if (plan.orig_size < plan.projected_size) {
    ECUtil::shard_extent_set_t projected_cloneable_range(sinfo.get_k_plus_m());
    sinfo.ro_size_to_read_mask(plan.projected_size,projected_cloneable_range);

    for (auto &&[shard, eset]: projected_cloneable_range) {
      uint64_t old_shard_size = 0;
      if (cloneable_range.contains(shard)) {
        old_shard_size = cloneable_range.at(shard).range_end();
      }
      uint64_t new_shard_size = eset.range_end();

      if (new_shard_size == old_shard_size) {
        continue;
      }

      uint64_t write_end = 0;
      if (plan.will_write.contains(shard)) {
        write_end = plan.will_write.at(shard).range_end();
      }

      if (write_end == new_shard_size) {
        continue;
      }

      /* If code is executing here, it means that the written part of the
       * shard does not reflect the size that EC believes the shard to be.
       * This is not a problem for reads (they will be truncated), but it
       * is a problem for writes, where future writes may attempt a clone
       * off the end of the object.
       * To solve this, we use an interesting quirk of "truncate" where we
       * can actually truncate to a size larger than the object!
       */
      if (transactions.contains(shard)) {
        auto &t = transactions.at(shard);
        t.truncate(
          coll_t(spg_t(pgid, shard)),
          ghobject_t(oid, ghobject_t::NO_GEN, shard),
          new_shard_size);
      }
      // Update written_shards because this must complete to consider
      // the write as complete
      shard_written(shard);
    }
  }

  shard_id_set touched;

  for (auto &[start, len]: clone_ranges) {
    shard_id_set to_clone_shards;
    uint64_t clone_end = 0;

    for (auto &&[shard, eset]: plan.will_write) {
      shard_written(shard);

      // If no clonable range here, then ignore.
      if (!cloneable_range.contains(shard)) continue;

      // Do not clone off the end of the old range
      uint64_t shard_clone_max = cloneable_range.at(shard).range_end();
      uint64_t shard_end = start + len;
      if (shard_end > shard_clone_max) shard_end = shard_clone_max;

      // clone_end needs to be the biggest shard_end.
      if (shard_end > clone_end) clone_end = shard_end;

      // Ignore pure appends on this shard.
      if (shard_end <= start) continue;

      // Ignore clones that do not intersect with the write.
      if (!eset.intersects(start, len)) continue;

      // We need a clone...
      if (transactions.contains(shard)) {
        auto &t = transactions.at(shard);

        // Only touch once.
        if (!touched.contains(shard)) {
          t.touch(
            coll_t(spg_t(pgid, shard)),
            ghobject_t(oid, entry->version.version, shard));
          touched.insert(shard_id_t(shard));
        }
        t.clone_range(
          coll_t(spg_t(pgid, shard)),
          ghobject_t(oid, ghobject_t::NO_GEN, shard),
          ghobject_t(oid, entry->version.version, shard),
          start,
          shard_end - start,
          start);

        // We have done a clone, so tell the rollback.
        to_clone_shards.insert(shard);
      }
    }

    if (!to_clone_shards.empty()) {
      // It is more efficent to store an empty set to represent the common
      // all shards case.
      if (to_clone_shards.size() == sinfo.get_k_plus_m()) {
        to_clone_shards.clear();
      }
      if (clone_end > start) {
        rollback_extents.emplace_back(make_pair(start, clone_end - start));
        rollback_shards.emplace_back(to_clone_shards);
      }
    }
  }
}

void ECTransaction::Generate::written_and_present_shards() {
  if (entry) {
    if (!rollback_extents.empty()) {
      entry->mod_desc.rollback_extents(
        entry->version.version,
        rollback_extents,
        ECUtil::align_next(plan.orig_size),
        rollback_shards);
    }
    if (entry->written_shards.size() == sinfo.get_k_plus_m()) {
      // More efficient to encode an empty set for all shards
      entry->written_shards.clear();
      written_shards_final = true;
    }
    // Calculate set of present shards
    for (auto &&[shard, t]: transactions) {
      entry->present_shards.insert(shard);
    }
    if (entry->present_shards.size() == sinfo.get_k_plus_m()) {
      // More efficient to encode an empty set for all shards
      entry->present_shards.clear();
    }

    // Update shard_versions in object_info to record which shards are being
    // written
    if (op.attr_updates.contains(OI_ATTR)) {
      object_info_t oi(*(op.attr_updates[OI_ATTR]));
      // The majority of the updates to OI are made before a transaction is
      // submitted to ECBackend, these are cached by OBC and are encoded into
      // the OI attr update for the transaction. By the time the transaction
      // gets here the OBC cached copy may have been advanced by further in
      // flight writes that will be committed after this one.
      //
      // The exception is oi.shard_versions which is updated here. We therefore
      // need to update the OI attr update with the latest version from the
      // cache here (to account for any modifications made by previous in flight
      // transactions), make any required modifications to shard_versions and
      // then update the OBC cached copy and the encoded OI attr.
      bool update = oi.shard_versions != obc->obs.oi.shard_versions;
      oi.shard_versions = obc->obs.oi.shard_versions;

      if (entry->written_shards.empty()) {
        if (!oi.shard_versions.empty()) {
          oi.shard_versions.clear();
          update = true;
        }
      } else {
        for (shard_id_t shard; shard < sinfo.get_k_plus_m(); ++shard) {
          if (sinfo.is_nonprimary_shard(shard)) {
            if (entry->is_written_shard(shard) || plan.orig_size != plan.
                projected_size) {
              // Written - erase per shard version
              if (oi.shard_versions.erase(shard)) {
                update = true;
              }
            } else if (!oi.shard_versions.count(shard)) {
              // Unwritten shard, previously up to date
              oi.shard_versions[shard] = oi.prior_version;
              update = true;
            } else {
              // Unwritten shard, already out of date
            }
          } else {
            // Primary shards are always written and use oi.version
          }
        }
      }
      if (update) {
        bufferlist bl;
        oi.encode(bl, osdmap->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
        op.attr_updates[OI_ATTR] = bl;
        // Update cached OI
        obc->obs.oi.shard_versions = oi.shard_versions;
      }
      ldpp_dout(dpp, 20) << __func__ << " shard_info: oid=" << oid
                         << " version=" << entry->version
                         << " present=" << entry->present_shards
                         << " written=" << entry->written_shards
                         << " shard_versions=" << oi.shard_versions << dendl;
    }
  }
}

void ECTransaction::Generate::attr_updates() {
  map<string, bufferlist, less<>> to_set;
  for (auto &&[attr, update]: op.attr_updates) {
    if (update) {
      to_set[attr] = *(update);
    } else {
      for (auto &&[shard, t]: transactions) {
        if (!sinfo.is_nonprimary_shard(shard)) {
          t.rmattr(
            coll_t(spg_t(pgid, shard)),
            ghobject_t(oid, ghobject_t::NO_GEN, shard),
            attr);
        }
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
  for (auto &&[shard, t]: transactions) {
    if (!sinfo.is_nonprimary_shard(shard)) {
      // Primary shard - Update all attributes
      t.setattrs(
        coll_t(spg_t(pgid, shard)),
        ghobject_t(oid, ghobject_t::NO_GEN, shard),
        to_set);
    } else if (entry->is_written_shard(shard)) {
      // Written shard - Only update object_info attribute
      t.setattr(
        coll_t(spg_t(pgid, shard)),
        ghobject_t(oid, ghobject_t::NO_GEN, shard),
        OI_ATTR,
        to_set[OI_ATTR]);
    } // Else: Unwritten shard - Don't update any attributes
  }
  ceph_assert(!xattr_rollback.empty());
}

void ECTransaction::Generate::handle_deletes() {
  bufferlist hbuf;
  if (plan.hinfo) {
    encode(*plan.hinfo, hbuf);
    for (auto &&[shard, t]: transactions) {
      if (!sinfo.is_nonprimary_shard(shard)) {
        shard_written(shard);
        t.setattr(
          coll_t(spg_t(pgid, shard)),
          ghobject_t(oid, ghobject_t::NO_GEN, shard),
          ECUtil::get_hinfo_key(),
          hbuf);
      }
    }
  }
}

void ECTransaction::generate_transactions(
    PGTransaction *_t,
    WritePlan &plans,
    ErasureCodeInterfaceRef &ec_impl,
    pg_t pgid,
    const ECUtil::stripe_info_t &sinfo,
    const map<hobject_t, ECUtil::shard_extent_map_t> &partial_extents,
    vector<pg_log_entry_t> &entries,
    map<hobject_t, ECUtil::shard_extent_map_t> *written_map,
    shard_id_map<ObjectStore::Transaction> *transactions,
    set<hobject_t> *temp_added,
    set<hobject_t> *temp_removed,
    DoutPrefixProvider *dpp,
    const OSDMapRef &osdmap) {
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

  t.safe_create_traverse(
    [&](pair<const hobject_t, PGTransaction::ObjectOperation> &opair) {
      auto oid = opair.first;
      PGTransaction::ObjectOperation& op = opair.second;
      auto iter = obj_to_log.find(oid);
      pg_log_entry_t *entry = iter != obj_to_log.end() ? iter->second : nullptr;
      if (oid.is_temp()) {
        if (op.is_fresh_object()) {
          temp_added->insert(oid);
        } else if (op.is_delete()) {
          temp_removed->insert(oid);
        }
      }

      // Transactions must be submitted in the same order that they were planned in.
      ceph_assert(!plans.plans.empty());
      ECTransaction::WritePlanObj &plan = plans.plans.front();
      ceph_assert(plan.hoid == oid);

      Generate generate(t, ec_impl, pgid, sinfo, partial_extents, written_map,
        *transactions, osdmap, oid, op, plan, dpp, entry);

      plans.plans.pop_front();
  });
}
