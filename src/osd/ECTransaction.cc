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
#include <vector>
#include <sstream>

#include "ECTransaction.h"
#include "ECUtil.h"
#include "os/ObjectStore.h"
#include "common/inline_variant.h"


void append(
  pg_t pgid,
  const hobject_t &oid,
  const ECUtil::stripe_info_t &sinfo,
  ErasureCodeInterfaceRef &ecimpl,
  const set<int> &want,
  uint64_t offset,
  bufferlist &bl,
  uint32_t flags,
  ECUtil::HashInfoRef hinfo,
  extent_map &written,
  map<shard_id_t, ObjectStore::Transaction> *transactions,
  DoutPrefixProvider *dpp) {

  uint64_t old_size = hinfo->get_total_logical_size(sinfo);
  assert(offset >= old_size);
  if (old_size != offset) {
    bl.prepend_zero(offset - old_size);
    offset = old_size;
  }

  // align
  if (bl.length() % sinfo.get_stripe_width()) {
    bl.append_zero(
      sinfo.get_stripe_width() -
      ((offset + bl.length()) % sinfo.get_stripe_width()));
  }

  map<int, bufferlist> buffers;
  int r = ECUtil::encode(
    sinfo, ecimpl, bl, want, &buffers);
  assert(r == 0);

  written.insert(offset, bl.length(), bl);

  ldpp_dout(dpp, 20) << __func__ << ": " << oid
		     << " new_size "
		     << offset + bl.length()
		     << dendl;

  hinfo->append(
    sinfo.aligned_logical_offset_to_chunk_offset(offset),
    buffers);

  for (auto &&i : *transactions) {
    assert(buffers.count(i.first));
    bufferlist &enc_bl = buffers[i.first];
    i.second.set_alloc_hint(
      coll_t(spg_t(pgid, i.first)),
      ghobject_t(oid, ghobject_t::NO_GEN, i.first),
      0, 0,
      CEPH_OSD_ALLOC_HINT_FLAG_SEQUENTIAL_WRITE |
      CEPH_OSD_ALLOC_HINT_FLAG_APPEND_ONLY);
    i.second.write(
      coll_t(spg_t(pgid, i.first)),
      ghobject_t(oid, ghobject_t::NO_GEN, i.first),
      sinfo.logical_to_prev_chunk_offset(
	offset),
      enc_bl.length(),
      enc_bl,
      flags);
  }
}

bool ECTransaction::requires_rollforward(
  uint64_t prev_size,
  const PGTransaction::ObjectOperation &op) {
  // special handling for truncates to 0
  if (op.truncate && op.truncate->first == 0)
    return false;
  return op.is_none() &&
    ((!op.buffer_updates.empty() &&
      (op.buffer_updates.begin().get_off() < prev_size)) ||
     (op.truncate &&
      (op.truncate->first < prev_size)));
}

bool ECTransaction::requires_inplace(
  uint64_t prev_size,
  const PGTransaction::ObjectOperation &op) {
  // can be more complicated, append goes either way
  return !requires_rollforward(prev_size, op);
}

void ECTransaction::generate_rollforward(
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
  pg_log_entry_t &entry,
  extent_map &written,
  map<shard_id_t, ObjectStore::Transaction> *transactions,
  DoutPrefixProvider *dpp)
{
  TransactionInfo::LocalRollForward lrf;
  lrf.new_attrs = attrs;
  lrf.version = entry.version.version;

  assert(obc);
  for (auto &&attr: lrf.new_attrs) {
    if (attr.second) {
      obc->attr_cache[attr.first] = *(attr.second);
    } else {
      obc->attr_cache.erase(attr.first);
    }
  }

  written.insert(partial_extents);

  uint64_t new_size = hinfo->get_total_logical_size(sinfo);
  ldpp_dout(dpp, 20) << __func__ << ": new_size start " << new_size << dendl;
  if (truncate && truncate->first < new_size) {
    new_size = sinfo.logical_to_next_stripe_offset(
      truncate->first);
    ldpp_dout(dpp, 20) << __func__ << ": new_size truncate down "
		       << new_size << dendl;
    lrf.truncate = sinfo.aligned_logical_offset_to_chunk_offset(new_size);
    if (new_size != truncate->first) { // 0 the unaligned part
      bufferlist bl;
      bl.append_zero(new_size - truncate->first);
      written.insert(
	truncate->first,
	bl.length(),
	bl);
    }
    written.erase(new_size, std::numeric_limits<uint64_t>::max() - new_size);
  }

  auto iter = buffer_updates.end();
  if (iter != buffer_updates.begin()) {
    --iter;
    uint64_t end = MAX(
      sinfo.logical_to_next_stripe_offset(
	iter.get_off() + iter.get_len()),
      truncate ?
        sinfo.logical_to_next_stripe_offset(truncate->second) :
        0);
    if (end > new_size) {
      bufferlist bl;
      bl.append_zero(end - new_size);
      written.insert(
	new_size,
	bl.length(),
	bl);
      new_size = end;
      ldpp_dout(dpp, 20) << __func__ << ": new_size written out to "
			 << new_size << dendl;
    }
  }

  for (auto &&extent: buffer_updates) {
    using BufferUpdate = PGTransaction::ObjectOperation::BufferUpdate;
    match(
      extent.get_val(),
      [&](const BufferUpdate::Write &op) {
	written.insert(
	  extent.get_off(),
	  extent.get_len(),
	  op.buffer);
      },
      [&](const BufferUpdate::Zero &) {
	bufferlist bl;
	bl.append_zero(extent.get_len());
	written.insert(
	  extent.get_off(),
	  extent.get_len(),
	  bl);
      },
      [&](const BufferUpdate::CloneRange &) {
	assert(
	  0 ==
	  "CloneRange is not allowed, do_op should have returned ENOTSUPP");
      });
  }

  if (!written.empty()) {
    for (auto &&i: *transactions) {
      i.second.touch(
	coll_t(spg_t(pgid, i.first)),
	ghobject_t(oid, lrf.version, i.first));
    }
    for (auto &&extent: written) {
      uint64_t offset = extent.get_off();
      bufferlist bl = extent.get_val();

      assert(offset % sinfo.get_stripe_width() == 0);
      assert(bl.length() % sinfo.get_stripe_width() == 0);

      set<int> want;
      for (unsigned i = 0; i < ecimpl->get_chunk_count(); ++i) {
	want.insert(i);
      }
      map<int, bufferlist> buffers;
      int r = ECUtil::encode(
	sinfo, ecimpl, bl, want, &buffers);

      uint64_t chunk_offset =
	sinfo.aligned_logical_offset_to_chunk_offset(offset);
      uint64_t chunk_length =
	sinfo.aligned_logical_offset_to_chunk_offset(bl.length());

      assert(r == 0);
      for (auto &&i : *transactions) {
	assert(buffers.count(i.first));
	bufferlist &enc_bl = buffers[i.first];
	assert(enc_bl.length() == chunk_length);
	i.second.write(
	  coll_t(spg_t(pgid, i.first)),
	  ghobject_t(oid, lrf.version, i.first),
	  chunk_offset,
	  chunk_length,
	  enc_bl);
      }
      lrf.extents.emplace_back(
	make_pair(
	  chunk_offset,
	  chunk_length));
    }
  }
  ldpp_dout(dpp, 20) << __func__ << ": " << oid
		     << " new_size "
		     << new_size
		     << dendl;
  hinfo->set_total_chunk_size_clear_hash(
    sinfo.aligned_logical_offset_to_chunk_offset(new_size));
  bufferlist hbuf;
  ::encode(*hinfo, hbuf);
  lrf.new_attrs[ECUtil::get_hinfo_key()] = hbuf;
  entry.mark_local_rollforward(lrf);
}

void ECTransaction::generate_rollback(
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
  pg_log_entry_t *entry, // optional
  extent_map &written,
  map<shard_id_t, ObjectStore::Transaction> *transactions,
  DoutPrefixProvider *dpp)
{
  TransactionInfo::LocalRollBack lrb;
  if (entry && op.updated_snaps) {
    lrb.update_snaps(op.updated_snaps->first);
  }

  map<string, boost::optional<bufferlist> > xattr_rollback;
  assert(hinfo);
  bufferlist old_hinfo;
  ::encode(*hinfo, old_hinfo);
  xattr_rollback[ECUtil::get_hinfo_key()] = old_hinfo;

  if (op.is_none() && op.truncate && op.truncate->first == 0) {
    assert(op.truncate->first == 0);
    assert(op.truncate->first ==
	   op.truncate->second);
    assert(entry);
    assert(obc);

    if (op.truncate->first != op.truncate->second) {
      op.truncate->first = op.truncate->second;
    } else {
      op.truncate = boost::none;
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
    /* We also want to remove the boost::none entries since
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
      lrb.rmobject(entry->version.version);
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
    hinfo->clear();
  }

  /* We'll fill in the actual truncate below, but assert here that the
   * truncate extends the size of the object. (requires_rollforward
   * should have caused us to take the rollforward path otherwise) */
  assert(
    op.is_fresh_object() ||
    !op.truncate ||
    (op.truncate->first >=
     hinfo->get_total_logical_size(sinfo)));

  if (op.is_fresh_object() && entry) {
    lrb.create();
  }

  match(
    op.init_type,
    [&](const PGTransaction::ObjectOperation::Init::None &) {},
    [&](const PGTransaction::ObjectOperation::Init::Create &op) {
      for (auto &&st: *transactions) {
	st.second.touch(
	  coll_t(spg_t(pgid, st.first)),
	  ghobject_t(oid, ghobject_t::NO_GEN, st.first));
      }
    },
    [&](const PGTransaction::ObjectOperation::Init::Clone &op) {
      for (auto &&st: *transactions) {
	st.second.clone(
	  coll_t(spg_t(pgid, st.first)),
	  ghobject_t(op.source, ghobject_t::NO_GEN, st.first),
	  ghobject_t(oid, ghobject_t::NO_GEN, st.first));
      }

      auto siter = hash_infos.find(op.source);
      assert(siter != hash_infos.end());
      hinfo->update_to(*(siter->second));

      if (obc) {
	auto cobciter = obc_map.find(op.source);
	assert(cobciter != obc_map.end());
	obc->attr_cache = cobciter->second->attr_cache;
      }
    },
    [&](const PGTransaction::ObjectOperation::Init::Rename &op) {
      assert(op.source.is_temp());
      for (auto &&st: *transactions) {
	st.second.collection_move_rename(
	  coll_t(spg_t(pgid, st.first)),
	  ghobject_t(op.source, ghobject_t::NO_GEN, st.first),
	  coll_t(spg_t(pgid, st.first)),
	  ghobject_t(oid, ghobject_t::NO_GEN, st.first));
      }
      auto siter = hash_infos.find(op.source);
      assert(siter != hash_infos.end());
      hinfo->update_to(*(siter->second));
      if (obc) {
	auto cobciter = obc_map.find(op.source);
	assert(cobciter == obc_map.end());
	obc->attr_cache.clear();
      }
    });

  // omap not supported (except 0, handled above)
  assert(!(op.clear_omap));
  assert(!(op.omap_header));
  assert(op.omap_updates.empty());

  if (!op.attr_updates.empty()) {
    map<string, bufferlist> to_set;
    for (auto &&j: op.attr_updates) {
      if (j.second) {
	to_set[j.first] = *(j.second);
      } else {
	for (auto &&st : *transactions) {
	  st.second.rmattr(
	    coll_t(spg_t(pgid, st.first)),
	    ghobject_t(oid, ghobject_t::NO_GEN, st.first),
	    j.first);
	}
      }
      if (obc) {
	auto citer = obc->attr_cache.find(j.first);
	if (entry) {
	  if (citer != obc->attr_cache.end()) {
	    // won't overwrite anything we put in earlier
	    xattr_rollback.insert(
	      make_pair(
		j.first,
		boost::optional<bufferlist>(citer->second)));
	  } else {
	    // won't overwrite anything we put in earlier
	    xattr_rollback.insert(
	      make_pair(
		j.first,
		boost::none));
	  }
	}
	if (j.second) {
	  obc->attr_cache[j.first] = *(j.second);
	} else if (citer != obc->attr_cache.end()) {
	  obc->attr_cache.erase(citer);
	}
      } else {
	assert(!entry);
      }
    }
    for (auto &&st : *transactions) {
      st.second.setattrs(
	coll_t(spg_t(pgid, st.first)),
	ghobject_t(oid, ghobject_t::NO_GEN, st.first),
	to_set);
    }
    assert(!xattr_rollback.empty());
  }
  if (entry && !xattr_rollback.empty()) {
    lrb.setattrs(xattr_rollback);
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

  bool did_append = false;
  uint64_t old_size = hinfo->get_total_logical_size(sinfo);
  set<int> want;
  for (unsigned i = 0; i < ecimpl->get_chunk_count(); ++i) {
    want.insert(i);
  }
  for (auto &&extent: op.buffer_updates) {
    using BufferUpdate = PGTransaction::ObjectOperation::BufferUpdate;
    match(
      extent.get_val(),
      [&](const BufferUpdate::Write &op) {
	assert(op.buffer.length() == extent.get_len());
	bufferlist bl = op.buffer;
	append(
	  pgid,
	  oid,
	  sinfo,
	  ecimpl,
	  want,
	  extent.get_off(),
	  bl,
	  op.fadvise_flags,
	  hinfo,
	  written,
	  transactions,
	  dpp);
	did_append = true;
      },
      [&](const BufferUpdate::Zero &) {
	bufferlist bl;
	bl.append_zero(extent.get_len());
	append(
	  pgid,
	  oid,
	  sinfo,
	  ecimpl,
	  want,
	  extent.get_off(),
	  bl,
	  0,
	  hinfo,
	  written,
	  transactions,
	  dpp);
	did_append = true;
      },
      [&](const BufferUpdate::CloneRange &) {
	assert(
	  0 ==
	  "CloneRange is not allowed, do_op should have returned ENOTSUPP");
      });
  }
  if (op.truncate &&
      op.truncate->second > hinfo->get_total_logical_size(sinfo)) {
    /* We asserted above that we can only be extending the object size, but
     * we might already have written past the truncate position making it
     * a noop */
    did_append = true;
    bufferlist bl;
    append(
      pgid,
      oid,
      sinfo,
      ecimpl,
      want,
      op.truncate->second,
      bl,
      0,
      hinfo,
      written,
      transactions,
      dpp);
  }

  bufferlist hbuf;
  ::encode(*hinfo, hbuf);
  for (auto &&i : *transactions) {
    i.second.setattr(
      coll_t(spg_t(pgid, i.first)),
      ghobject_t(oid, ghobject_t::NO_GEN, i.first),
      ECUtil::get_hinfo_key(),
      hbuf);
  }
  if (entry) {
    if (did_append) {
      lrb.append(old_size);
    }
    entry->mark_local_rollback(
      lrb,
      legacy_log_entries);
  }
}

void ECTransaction::generate_transactions(
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
  DoutPrefixProvider *dpp)
{
  assert(written);
  assert(transactions);
  assert(temp_added);
  assert(temp_removed);
  assert(plan.t);
  auto &t = *(plan.t);

  auto &hash_infos = plan.hash_infos;

  assert(transactions);
  assert(temp_added);
  assert(temp_removed);

  map<hobject_t, pg_log_entry_t*, hobject_t::BitwiseComparator> obj_to_log;
  for (auto &&i: entries) {
    obj_to_log.insert(make_pair(i.soid, &i));
  }

  t.safe_create_traverse(
    [&](pair<const hobject_t, PGTransaction::ObjectOperation> &opair) {
      const hobject_t &oid = opair.first;

      auto iter = obj_to_log.find(oid);
      pg_log_entry_t *entry = iter != obj_to_log.end() ? iter->second : nullptr;

      ObjectContextRef obc;
      auto obiter = t.obc_map.find(oid);
      if (obiter != t.obc_map.end()) {
	obc = obiter->second;
      }
      if (entry) {
	assert(obc);
      } else {
	assert(oid.is_temp());
      }

      ECUtil::HashInfoRef hinfo;
      {
	auto iter = hash_infos.find(oid);
	assert(iter != hash_infos.end());
	hinfo = iter->second;
      }

      if (oid.is_temp()) {
	if (opair.second.is_fresh_object()) {
	  temp_added->insert(oid);
	} else if (opair.second.is_delete()) {
	  temp_removed->insert(oid);
	}
      }

      if (entry &&
	  entry->is_modify() &&
	  opair.second.updated_snaps) {
	vector<snapid_t> snaps(
	  opair.second.updated_snaps->second.begin(),
	  opair.second.updated_snaps->second.end());
	::encode(snaps, entry->snaps);
      }

      ldpp_dout(dpp, 20) << "generate_transactions: "
			 << opair.first
			 << ", current size is "
			 << hinfo->get_total_logical_size(sinfo)
			 << " buffers are "
			 << opair.second.buffer_updates
			 << dendl;
      if (opair.second.truncate) {
	ldpp_dout(dpp, 20) << "generate_transactions: "
			   << " truncate is "
			   << *(opair.second.truncate)
			   << dendl;
      }

      if (requires_rollforward(
	    hinfo->get_total_logical_size(sinfo), opair.second)) {
	auto pextents = partial_extents.find(oid);
	ldpp_dout(dpp, 20) << __func__ << ": rollforward" << dendl;
	assert(!opair.second.updated_snaps);
	generate_rollforward(
	  pgid,
	  oid,
	  opair.second.attr_updates,
	  opair.second.truncate,
	  pextents != partial_extents.end() ? pextents->second : extent_map(),
	  opair.second.buffer_updates,
	  sinfo,
	  ecimpl,
	  hinfo,
	  obc,
	  *entry,
	  (*written)[oid],
	  transactions,
	  dpp);
      } else {
	ldpp_dout(dpp, 20) << __func__ << ": rollback" << dendl;
	generate_rollback(
	  pgid,
	  oid,
	  opair.second,
	  sinfo,
	  ecimpl,
	  hinfo,
	  hash_infos,
	  t.obc_map,
	  obc,
	  legacy_log_entries,
	  entry,
	  (*written)[oid],
	  transactions,
	  dpp);
      }
    });
}
