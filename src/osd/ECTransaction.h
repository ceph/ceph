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

#include "ECUtil.h"
#include "ExtentCache.h"
#include "common/ceph_releases.h"
#include "erasure-code/ErasureCodeInterface.h"
#include "os/Transaction.h"
#include "PGTransaction.h"

namespace ECTransaction {
  struct WritePlan {
    bool invalidates_cache = false; // Yes, both are possible
    std::map<hobject_t,extent_set> to_read;
    std::map<hobject_t,extent_set> will_write; // superset of to_read

    std::map<hobject_t,ECUtil::HashInfoRef> hash_infos;
  };

  template <typename F>
  WritePlan get_write_plan(
    const ECUtil::stripe_info_t &sinfo,
    PGTransaction& t,
    F &&get_hinfo,
    DoutPrefixProvider *dpp) {
    WritePlan plan;
    t.safe_create_traverse(
      [&](std::pair<const hobject_t, PGTransaction::ObjectOperation> &i) {
        const auto& [obj, op] = i;
	ECUtil::HashInfoRef hinfo = get_hinfo(obj);
	plan.hash_infos[obj] = hinfo;

	uint64_t projected_size =
	  hinfo->get_projected_total_logical_size(sinfo);

	if (op.deletes_first()) {
	  ldpp_dout(dpp, 20) << __func__ << ": delete, setting projected size"
			     << " to 0" << dendl;
	  projected_size = 0;
	}

	hobject_t source;
	if (op.has_source(&source)) {
	  // typically clone or mv
	  plan.invalidates_cache = true;

	  ECUtil::HashInfoRef shinfo = get_hinfo(source);
	  projected_size = shinfo->get_projected_total_logical_size(sinfo);
	  plan.hash_infos[source] = shinfo;
	}

	auto &will_write = plan.will_write[obj];
	if (op.truncate &&
	    op.truncate->first < projected_size) {
	  if (!(sinfo.logical_offset_is_stripe_aligned(
		  op.truncate->first))) {
	    plan.to_read[obj].union_insert(
	      sinfo.logical_to_prev_stripe_offset(op.truncate->first),
	      sinfo.get_stripe_width());

	    ldpp_dout(dpp, 20) << __func__ << ": unaligned truncate" << dendl;

	    will_write.union_insert(
	      sinfo.logical_to_prev_stripe_offset(op.truncate->first),
	      sinfo.get_stripe_width());
	  }
	  projected_size = sinfo.logical_to_next_stripe_offset(
	    op.truncate->first);
	}

	extent_set raw_write_set;
	for (auto &&extent: op.buffer_updates) {
	  using BufferUpdate = PGTransaction::ObjectOperation::BufferUpdate;
	  if (boost::get<BufferUpdate::CloneRange>(&(extent.get_val()))) {
	    ceph_assert(
	      0 ==
	      "CloneRange is not allowed, do_op should have returned ENOTSUPP");
	  }
	  raw_write_set.insert(extent.get_off(), extent.get_len());
	}

	auto orig_size = projected_size;
	for (auto extent = raw_write_set.begin();
	     extent != raw_write_set.end();
	     ++extent) {
	  uint64_t head_start =
	    sinfo.logical_to_prev_stripe_offset(extent.get_start());
	  uint64_t head_finish =
	    sinfo.logical_to_next_stripe_offset(extent.get_start());
	  if (head_start > projected_size) {
	    head_start = projected_size;
	  }
	  if (head_start != head_finish &&
	      head_start < orig_size) {
	    ceph_assert(head_finish <= orig_size);
	    ceph_assert(head_finish - head_start == sinfo.get_stripe_width());
	    ldpp_dout(dpp, 20) << __func__ << ": reading partial head stripe "
			       << head_start << "~" << sinfo.get_stripe_width()
			       << dendl;
	    plan.to_read[obj].union_insert(
	      head_start, sinfo.get_stripe_width());
	  }

	  uint64_t tail_start =
	    sinfo.logical_to_prev_stripe_offset(
	      extent.get_start() + extent.get_len());
	  uint64_t tail_finish =
	    sinfo.logical_to_next_stripe_offset(
	      extent.get_start() + extent.get_len());
	  if (tail_start != tail_finish &&
	      (head_start == head_finish || tail_start != head_start) &&
	      tail_start < orig_size) {
	    ceph_assert(tail_finish <= orig_size);
	    ceph_assert(tail_finish - tail_start == sinfo.get_stripe_width());
	    ldpp_dout(dpp, 20) << __func__ << ": reading partial tail stripe "
			       << tail_start << "~" << sinfo.get_stripe_width()
			       << dendl;
	    plan.to_read[obj].union_insert(
	      tail_start, sinfo.get_stripe_width());
	  }

	  if (head_start != tail_finish) {
	    ceph_assert(
	      sinfo.logical_offset_is_stripe_aligned(
		tail_finish - head_start)
	      );
	    will_write.union_insert(
	      head_start, tail_finish - head_start);
	    if (tail_finish > projected_size)
	      projected_size = tail_finish;
	  } else {
	    ceph_assert(tail_finish <= projected_size);
	  }
	}

	if (op.truncate && op.truncate->second > projected_size) {
	  uint64_t truncating_to =
	    sinfo.logical_to_next_stripe_offset(op.truncate->second);
	  ldpp_dout(dpp, 20) << __func__ << ": truncating out to "
			     <<  truncating_to
			     << dendl;
	  will_write.union_insert(projected_size,
				  truncating_to - projected_size);
	  projected_size = truncating_to;
	}

	ldpp_dout(dpp, 20) << __func__ << ": " << obj
			   << " projected size "
			   << projected_size
			   << dendl;
	hinfo->set_projected_total_logical_size(
	  sinfo,
	  projected_size);

	/* validate post conditions:
	 * to_read should have an entry for `obj` if it isn't empty
	 * and if we are reading from `obj`, we can't be renaming or
	 * cloning it */
	ceph_assert(plan.to_read.count(obj) == 0 ||
	       (!plan.to_read.at(obj).empty() &&
		!i.second.has_source()));
      });
    return plan;
  }

  void generate_transactions(
    PGTransaction* _t,
    WritePlan &plan,
    ceph::ErasureCodeInterfaceRef &ecimpl,
    pg_t pgid,
    const ECUtil::stripe_info_t &sinfo,
    const std::map<hobject_t,extent_map> &partial_extents,
    std::vector<pg_log_entry_t> &entries,
    std::map<hobject_t,extent_map> *written,
    std::map<shard_id_t, ceph::os::Transaction> *transactions,
    std::set<hobject_t> *temp_added,
    std::set<hobject_t> *temp_removed,
    DoutPrefixProvider *dpp,
    const ceph_release_t require_osd_release = ceph_release_t::unknown);
};

#endif
