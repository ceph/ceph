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

#ifndef ECTRANSACTION_H
#define ECTRANSACTION_H

#include "OSD.h"
#include "PGBackend.h"
#include "ECUtil.h"
#include "erasure-code/ErasureCodeInterface.h"
#include "PGTransaction.h"
#include "ExtentCache.h"

namespace ECTransaction {
  struct WritePlan {
    PGTransactionUPtr t;
    bool inplace = false; // Yes, both are possible
    bool rollforward = false;
    hobject_t::bitwisemap<extent_set> to_read;
    hobject_t::bitwisemap<extent_set> will_write; // superset of to_read

    hobject_t::bitwisemap<ECUtil::HashInfoRef> hash_infos;
  };

  bool requires_rollforward(
    uint64_t prev_size,
    const PGTransaction::ObjectOperation &op);

  bool requires_inplace(
    uint64_t prev_size,
    const PGTransaction::ObjectOperation &op);

  template <typename F>
  WritePlan get_write_plan(
    const ECUtil::stripe_info_t &sinfo,
    PGTransactionUPtr &&t,
    F &&get_hinfo,
    DoutPrefixProvider *dpp) {
    WritePlan plan;
    t->safe_create_traverse(
      [&](pair<const hobject_t, PGTransaction::ObjectOperation> &i) {
	ECUtil::HashInfoRef hinfo = get_hinfo(i.first);
	plan.hash_infos[i.first] = hinfo;

	uint64_t projected_size =
	  hinfo->get_projected_total_logical_size(sinfo);

	if (requires_rollforward(projected_size, i.second)) {
	  plan.rollforward = true;
	} else if (requires_inplace(projected_size, i.second)) {
	  assert(!requires_rollforward(projected_size, i.second));
	  plan.inplace = true;
	}

	if (i.second.is_delete()) {
	  projected_size = 0;
	}

	hobject_t source;
	if (i.second.has_source(&source)) {
	  ECUtil::HashInfoRef shinfo = get_hinfo(source);
	  projected_size = shinfo->get_projected_total_logical_size(sinfo);
	  plan.hash_infos[source] = shinfo;
	}

	auto &will_write = plan.will_write[i.first];
	if (i.second.truncate &&
	    i.second.truncate->first < projected_size) {
	  if (!(sinfo.logical_offset_is_stripe_aligned(
		  i.second.truncate->first))) {
	    plan.to_read[i.first].insert(
	      sinfo.logical_to_prev_stripe_offset(i.second.truncate->first),
	      sinfo.get_stripe_width());

	    ldpp_dout(dpp, 20) << __func__ << ": unaligned truncate" << dendl;

	    will_write.insert(
	      sinfo.logical_to_prev_stripe_offset(i.second.truncate->first),
	      sinfo.get_stripe_width());
	  }
	  projected_size = sinfo.logical_to_next_stripe_offset(
	    i.second.truncate->first);
	}

	for (auto &&extent: i.second.buffer_updates) {
	  using BufferUpdate = PGTransaction::ObjectOperation::BufferUpdate;
	  if (boost::get<BufferUpdate::CloneRange>(&(extent.get_val()))) {
	    assert(
	      0 ==
	      "CloneRange is not allowed, do_op should have returned ENOTSUPP");
	  }

	  uint64_t head_start =
	    sinfo.logical_to_prev_stripe_offset(extent.get_off());
	  uint64_t head_finish =
	    sinfo.logical_to_next_stripe_offset(extent.get_off());
	  if (head_start > projected_size) {
	    head_start = projected_size;
	  }
	  if (head_start != head_finish &&
	      head_start < projected_size) {
	    assert(head_finish <= projected_size);
	    assert(head_finish - head_start == sinfo.get_stripe_width());
	    plan.to_read[i.first].insert(
	      head_start, sinfo.get_stripe_width());
	  }

	  uint64_t tail_start =
	    sinfo.logical_to_prev_stripe_offset(
	      extent.get_off() + extent.get_len());
	  uint64_t tail_finish =
	    sinfo.logical_to_next_stripe_offset(
	      extent.get_off() + extent.get_len());
	  if (tail_start != tail_finish &&
	      tail_start != head_start &&
	      tail_start < projected_size) {
	    assert(tail_finish <= projected_size);
	    assert(tail_finish - tail_start == sinfo.get_stripe_width());
	    plan.to_read[i.first].insert(
	      tail_start, sinfo.get_stripe_width());
	  }

	  if (head_start != tail_finish) {
	    assert(
	      sinfo.logical_offset_is_stripe_aligned(
		tail_finish - head_start)
	      );
	    will_write.insert(
	      head_start, tail_finish - head_start);
	    if (tail_finish > projected_size)
	      projected_size = tail_finish;
	  } else {
	    assert(tail_finish <= projected_size);
	  }
	}

	if (i.second.truncate &&
	    i.second.truncate->second > projected_size) {
	  uint64_t truncating_to =
	    sinfo.logical_to_next_stripe_offset(i.second.truncate->second);
	  ldpp_dout(dpp, 20) << __func__ << ": truncating out to "
			     <<  truncating_to
			     << dendl;
	  will_write.insert(projected_size, truncating_to - projected_size);
	  projected_size = truncating_to;
	}

	ldpp_dout(dpp, 20) << __func__ << ": " << i.first
			   << " projected size "
			   << projected_size
			   << dendl;
	hinfo->set_projected_total_logical_size(
	  sinfo,
	  projected_size);
      });
    plan.t = std::move(t);
    return plan;
  }

  void generate_transactions(
    WritePlan &plan,
    ErasureCodeInterfaceRef &ecimpl,
    pg_t pgid,
    bool legacy_log_entries,
    const ECUtil::stripe_info_t &sinfo,
    const hobject_t::bitwisemap<extent_map> &partial_extents,
    vector<pg_log_entry_t> &entries,
    hobject_t::bitwisemap<extent_map> *written,
    map<shard_id_t, ObjectStore::Transaction> *transactions,
    set<hobject_t, hobject_t::BitwiseComparator> *temp_added,
    set<hobject_t, hobject_t::BitwiseComparator> *temp_removed,
    DoutPrefixProvider *dpp);


  void generate_rollback(
    pg_t pgid,
    const hobject_t &oid,
    PGTransaction::ObjectOperation &op,
    const ECUtil::stripe_info_t &sinfo,
    ErasureCodeInterfaceRef &ecimpl,
    ECUtil::HashInfoRef hinfo,
    hobject_t::bitwisemap<ECUtil::HashInfoRef> &hash_infos,
    hobject_t::bitwisemap<ObjectContextRef> &obc_map,
    ObjectContextRef obc,
    bool legacy_log_entries,
    pg_log_entry_t *log_entry, // optional
    extent_map &written,
    map<shard_id_t, ObjectStore::Transaction> *transactions,
    DoutPrefixProvider *dpp);

  void generate_rollforward(
    pg_t pgid,
    const hobject_t &oid,
    const map<string, boost::optional<bufferlist> > &attrs,
    const boost::optional<pair<uint64_t, uint64_t> > &truncate,
    const extent_map &partial_extents,
    const PGTransaction::ObjectOperation::buffer_update_type &buffer_updates,
    const ECUtil::stripe_info_t &sinfo,
    ErasureCodeInterfaceRef &ecimpl,
    ECUtil::HashInfoRef hinfo,
    ObjectContextRef obc,
    pg_log_entry_t &log_entry,
    extent_map &written,
    map<shard_id_t, ObjectStore::Transaction> *transactions,
    DoutPrefixProvider *dpp);

};

#endif
