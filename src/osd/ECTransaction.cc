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
#include <map>
#include <sstream>
#include <vector>

#include "ECTransaction.h"
#include "ECUtil.h"
#include "os/ObjectStore.h"
#include "common/inline_variant.h"

#ifndef WITH_SEASTAR
#include "osd/osd_internal_types.h"
#else
#include "crimson/osd/object_context.h"
#endif

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

static void encode_and_write(
  pg_t pgid,
  const hobject_t &oid,
  const ECUtil::stripe_info_t &sinfo,
  ErasureCodeInterfaceRef &ecimpl,
  const set<int> &want,
  uint64_t offset,
  bufferlist bl,
  uint32_t flags,
  ECUtil::HashInfoRef hinfo,
  extent_map &written,
  map<shard_id_t, ObjectStore::Transaction> *transactions,
  DoutPrefixProvider *dpp)
{
  const uint64_t before_size = hinfo->get_total_logical_size(sinfo);
  ceph_assert(sinfo.logical_offset_is_stripe_aligned(offset));
  ceph_assert(sinfo.logical_offset_is_stripe_aligned(bl.length()));
  ceph_assert(bl.length());

  map<int, bufferlist> buffers;
  int r = ECUtil::encode(
    sinfo, ecimpl, bl, want, &buffers);
  ceph_assert(r == 0);

  written.insert(offset, bl.length(), bl);

  ldpp_dout(dpp, 20) << __func__ << ": " << oid
		     << " new_size "
		     << offset + bl.length()
		     << dendl;

  if (offset >= before_size) {
    ceph_assert(offset == before_size);
    hinfo->append(
      sinfo.aligned_logical_offset_to_chunk_offset(offset),
      buffers);
  }

  for (auto &&i : *transactions) {
    ceph_assert(buffers.count(i.first));
    bufferlist &enc_bl = buffers[i.first];
    if (offset >= before_size) {
      i.second.set_alloc_hint(
	coll_t(spg_t(pgid, i.first)),
	ghobject_t(oid, ghobject_t::NO_GEN, i.first),
	0, 0,
	CEPH_OSD_ALLOC_HINT_FLAG_SEQUENTIAL_WRITE |
	CEPH_OSD_ALLOC_HINT_FLAG_APPEND_ONLY);
    }
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

void ECTransaction::generate_transactions(
  PGTransaction* _t,
  WritePlan &plan,
  ErasureCodeInterfaceRef &ecimpl,
  pg_t pgid,
  const ECUtil::stripe_info_t &sinfo,
  const map<hobject_t,extent_map> &partial_extents,
  vector<pg_log_entry_t> &entries,
  map<hobject_t,extent_map> *written_map,
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

  auto &hash_infos = plan.hash_infos;

  map<hobject_t, pg_log_entry_t*> obj_to_log;
  for (auto &&i: entries) {
    obj_to_log.insert(make_pair(i.soid, &i));
  }

  t.safe_create_traverse(
    [&](pair<const hobject_t, PGTransaction::ObjectOperation> &opair) {
      const hobject_t &oid = opair.first;
      auto &op = opair.second;
      auto &obc_map = t.obc_map;
      auto &written = (*written_map)[oid];

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

      ECUtil::HashInfoRef hinfo;
      {
	auto iter = hash_infos.find(oid);
	ceph_assert(iter != hash_infos.end());
	hinfo = iter->second;
      }

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
			 << hinfo->get_total_logical_size(sinfo)
			 << " buffers are "
			 << op.buffer_updates
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
      ceph_assert(hinfo);
      bufferlist old_hinfo;
      encode(*hinfo, old_hinfo);
      xattr_rollback[ECUtil::get_hinfo_key()] = old_hinfo;

      if (op.is_none() && op.truncate && op.truncate->first == 0) {
	ceph_assert(op.truncate->first == 0);
	ceph_assert(op.truncate->first ==
	       op.truncate->second);
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
	hinfo->clear();
      }

      if (op.is_fresh_object() && entry) {
	entry->mod_desc.create();
      }

      match(
	op.init_type,
	[&](const PGTransaction::ObjectOperation::Init::None &) {},
	[&](const PGTransaction::ObjectOperation::Init::Create &op) {
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
	[&](const PGTransaction::ObjectOperation::Init::Clone &op) {
	  for (auto &&st: *transactions) {
	    st.second.clone(
	      coll_t(spg_t(pgid, st.first)),
	      ghobject_t(op.source, ghobject_t::NO_GEN, st.first),
	      ghobject_t(oid, ghobject_t::NO_GEN, st.first));
	  }

	  auto siter = hash_infos.find(op.source);
	  ceph_assert(siter != hash_infos.end());
	  hinfo->update_to(*(siter->second));

	  if (obc) {
	    auto cobciter = obc_map.find(op.source);
	    ceph_assert(cobciter != obc_map.end());
	    obc->attr_cache = cobciter->second->attr_cache;
	  }
	},
	[&](const PGTransaction::ObjectOperation::Init::Rename &op) {
	  ceph_assert(op.source.is_temp());
	  for (auto &&st: *transactions) {
	    st.second.collection_move_rename(
	      coll_t(spg_t(pgid, st.first)),
	      ghobject_t(op.source, ghobject_t::NO_GEN, st.first),
	      coll_t(spg_t(pgid, st.first)),
	      ghobject_t(oid, ghobject_t::NO_GEN, st.first));
	  }
	  auto siter = hash_infos.find(op.source);
	  ceph_assert(siter != hash_infos.end());
	  hinfo->update_to(*(siter->second));
	  if (obc) {
	    auto cobciter = obc_map.find(op.source);
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
		    std::optional<bufferlist>(citer->second)));
	      } else {
		// won't overwrite anything we put in earlier
		xattr_rollback.insert(
		  make_pair(
		    j.first,
		    std::nullopt));
	      }
	    }
	    if (j.second) {
	      obc->attr_cache[j.first] = *(j.second);
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

      extent_map to_write;
      auto pextiter = partial_extents.find(oid);
      if (pextiter != partial_extents.end()) {
	to_write = pextiter->second;
      }

      vector<pair<uint64_t, uint64_t> > rollback_extents;
      const uint64_t orig_size = hinfo->get_total_logical_size(sinfo);

      uint64_t new_size = orig_size;
      uint64_t append_after = new_size;
      ldpp_dout(dpp, 20) << "generate_transactions: new_size start "
        << new_size << dendl;
      if (op.truncate && op.truncate->first < new_size) {
	ceph_assert(!op.is_fresh_object());
	new_size = sinfo.logical_to_next_stripe_offset(
	  op.truncate->first);
	ldpp_dout(dpp, 20) << "generate_transactions: new_size truncate down "
			   << new_size << dendl;
	if (new_size != op.truncate->first) { // 0 the unaligned part
	  bufferlist bl;
	  bl.append_zero(new_size - op.truncate->first);
	  to_write.insert(
	    op.truncate->first,
	    bl.length(),
	    bl);
	  append_after = sinfo.logical_to_prev_stripe_offset(
	    op.truncate->first);
	} else {
	  append_after = new_size;
	}
	to_write.erase(
	  new_size,
	  std::numeric_limits<uint64_t>::max() - new_size);

	if (entry && !op.is_fresh_object()) {
	  uint64_t restore_from = sinfo.logical_to_prev_chunk_offset(
	    op.truncate->first);
	  uint64_t restore_len = sinfo.aligned_logical_offset_to_chunk_offset(
	    orig_size -
	    sinfo.logical_to_prev_stripe_offset(op.truncate->first));
	  ceph_assert(rollback_extents.empty());

	  ldpp_dout(dpp, 20) << "generate_transactions: saving extent "
			     << make_pair(restore_from, restore_len)
			     << dendl;
	  ldpp_dout(dpp, 20) << "generate_transactions: truncating to "
			     << new_size
			     << dendl;
	  rollback_extents.emplace_back(
	    make_pair(restore_from, restore_len));
	  for (auto &&st : *transactions) {
	    st.second.touch(
	      coll_t(spg_t(pgid, st.first)),
	      ghobject_t(oid, entry->version.version, st.first));
	    st.second.clone_range(
	      coll_t(spg_t(pgid, st.first)),
	      ghobject_t(oid, ghobject_t::NO_GEN, st.first),
	      ghobject_t(oid, entry->version.version, st.first),
	      restore_from,
	      restore_len,
	      restore_from);
	    
	  }
	} else {
	  ldpp_dout(dpp, 20) << "generate_transactions: not saving extents"
                                ", fresh object" << dendl;
	}
	for (auto &&st : *transactions) {
	  st.second.truncate(
	    coll_t(spg_t(pgid, st.first)),
	    ghobject_t(oid, ghobject_t::NO_GEN, st.first),
	    sinfo.aligned_logical_offset_to_chunk_offset(new_size));
	}
      }

      uint32_t fadvise_flags = 0;
      for (auto &&extent: op.buffer_updates) {
	using BufferUpdate = PGTransaction::ObjectOperation::BufferUpdate;
	bufferlist bl;
	match(
	  extent.get_val(),
	  [&](const BufferUpdate::Write &op) {
	    bl = op.buffer;
	    fadvise_flags |= op.fadvise_flags;
	  },
	  [&](const BufferUpdate::Zero &) {
	    bl.append_zero(extent.get_len());
	  },
	  [&](const BufferUpdate::CloneRange &) {
	    ceph_assert(
	      0 ==
	      "CloneRange is not allowed, do_op should have returned ENOTSUPP");
	  });

	uint64_t off = extent.get_off();
	uint64_t len = extent.get_len();
	uint64_t end = off + len;
	ldpp_dout(dpp, 20) << "generate_transactions: adding buffer_update "
			   << make_pair(off, len)
			   << dendl;
	ceph_assert(len > 0);
	if (off > new_size) {
	  ceph_assert(off > append_after);
	  bl.prepend_zero(off - new_size);
	  len += off - new_size;
	  ldpp_dout(dpp, 20) << "generate_transactions: prepending zeroes to align "
			     << off << "->" << new_size
			     << dendl;
	  off = new_size;
	}
	if (!sinfo.logical_offset_is_stripe_aligned(end) && (end > append_after)) {
	  uint64_t aligned_end = sinfo.logical_to_next_stripe_offset(
	    end);
	  uint64_t tail = aligned_end - end;
	  bl.append_zero(tail);
	  ldpp_dout(dpp, 20) << "generate_transactions: appending zeroes to align end "
			     << end << "->" << end+tail
			     << ", len: " << len << "->" << len+tail
			     << dendl;
	  end += tail;
	  len += tail;
	}

	to_write.insert(off, len, bl);
	if (end > new_size)
	  new_size = end;
      }

      if (op.truncate &&
	  op.truncate->second > new_size) {
	ceph_assert(op.truncate->second > append_after);
	uint64_t truncate_to =
	  sinfo.logical_to_next_stripe_offset(
	    op.truncate->second);
	uint64_t zeroes = truncate_to - new_size;
	bufferlist bl;
	bl.append_zero(zeroes);
	to_write.insert(
	  new_size,
	  zeroes,
	  bl);
	new_size = truncate_to;
	ldpp_dout(dpp, 20) << "generate_transactions: truncating out to "
			   << truncate_to
			   << dendl;
      }

      set<int> want;
      for (unsigned i = 0; i < ecimpl->get_chunk_count(); ++i) {
	want.insert(i);
      }
      auto to_overwrite = to_write.intersect(0, append_after);
      ldpp_dout(dpp, 20) << "generate_transactions: to_overwrite: "
			 << to_overwrite
			 << dendl;
      for (auto &&extent: to_overwrite) {
	ceph_assert(extent.get_off() + extent.get_len() <= append_after);
	ceph_assert(sinfo.logical_offset_is_stripe_aligned(extent.get_off()));
	ceph_assert(sinfo.logical_offset_is_stripe_aligned(extent.get_len()));
	if (entry) {
	  uint64_t restore_from = sinfo.aligned_logical_offset_to_chunk_offset(
	    extent.get_off());
	  uint64_t restore_len = sinfo.aligned_logical_offset_to_chunk_offset(
	    extent.get_len());
	  ldpp_dout(dpp, 20) << "generate_transactions: overwriting "
			     << restore_from << "~" << restore_len
			     << dendl;
	  if (rollback_extents.empty()) {
	    for (auto &&st : *transactions) {
	      st.second.touch(
		coll_t(spg_t(pgid, st.first)),
		ghobject_t(oid, entry->version.version, st.first));
	    }
	  }
	  rollback_extents.emplace_back(make_pair(restore_from, restore_len));
	  for (auto &&st : *transactions) {
	    st.second.clone_range(
	      coll_t(spg_t(pgid, st.first)),
	      ghobject_t(oid, ghobject_t::NO_GEN, st.first),
	      ghobject_t(oid, entry->version.version, st.first),
	      restore_from,
	      restore_len,
	      restore_from);
	  }
	}
	encode_and_write(
	  pgid,
	  oid,
	  sinfo,
	  ecimpl,
	  want,
	  extent.get_off(),
	  extent.get_val(),
	  fadvise_flags,
	  hinfo,
	  written,
	  transactions,
	  dpp);
      }

      auto to_append = to_write.intersect(
	append_after,
	std::numeric_limits<uint64_t>::max() - append_after);
      ldpp_dout(dpp, 20) << "generate_transactions: to_append: "
			 << to_append
			 << dendl;
      for (auto &&extent: to_append) {
	ceph_assert(sinfo.logical_offset_is_stripe_aligned(extent.get_off()));
	ceph_assert(sinfo.logical_offset_is_stripe_aligned(extent.get_len()));
	ldpp_dout(dpp, 20) << "generate_transactions: appending "
			   << extent.get_off() << "~" << extent.get_len()
			   << dendl;
	encode_and_write(
	  pgid,
	  oid,
	  sinfo,
	  ecimpl,
	  want,
	  extent.get_off(),
	  extent.get_val(),
	  fadvise_flags,
	  hinfo,
	  written,
	  transactions,
	  dpp);
      }

      ldpp_dout(dpp, 20) << "generate_transactions: " << oid
			 << " resetting hinfo to logical size "
			 << new_size
			 << dendl;
      if (!rollback_extents.empty() && entry) {
	if (entry) {
	  ldpp_dout(dpp, 20) << "generate_transactions: " << oid
			     << " marking rollback extents "
			     << rollback_extents
			     << dendl;
	  entry->mod_desc.rollback_extents(
	    entry->version.version, rollback_extents);
	}
	hinfo->set_total_chunk_size_clear_hash(
	  sinfo.aligned_logical_offset_to_chunk_offset(new_size));
      } else {
	ceph_assert(hinfo->get_total_logical_size(sinfo) == new_size);
      }

      if (entry && !to_append.empty()) {
	ldpp_dout(dpp, 20) << "generate_transactions: marking append "
			   << append_after
			   << dendl;
	entry->mod_desc.append(append_after);
      }

      if (!op.is_delete()) {
	bufferlist hbuf;
	encode(*hinfo, hbuf);
	for (auto &&i : *transactions) {
	  i.second.setattr(
	    coll_t(spg_t(pgid, i.first)),
	    ghobject_t(oid, ghobject_t::NO_GEN, i.first),
	    ECUtil::get_hinfo_key(),
	    hbuf);
	}
      }
    });
}
