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

void ECTransaction::get_append_objects(
  const PGTransaction &t,
  set<hobject_t, hobject_t::BitwiseComparator> *out)
{
  for (auto &&i: t.op_map) {
    out->insert(i.first);
    hobject_t source;
    if (i.second.has_source(&source))
      out->insert(source);
  }
}

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
  map<shard_id_t, ObjectStore::Transaction> *transactions) {

  assert(bl.length());
  assert(offset % sinfo.get_stripe_width() == 0);
  assert(
    sinfo.aligned_logical_offset_to_chunk_offset(offset) ==
    hinfo->get_total_chunk_size());
  map<int, bufferlist> buffers;

  // align
  if (bl.length() % sinfo.get_stripe_width())
    bl.append_zero(
      sinfo.get_stripe_width() -
      ((offset + bl.length()) % sinfo.get_stripe_width()));
  int r = ECUtil::encode(
    sinfo, ecimpl, bl, want, &buffers);

  hinfo->append(
    sinfo.aligned_logical_offset_to_chunk_offset(offset),
    buffers);
  bufferlist hbuf;
  ::encode(*hinfo, hbuf);

  assert(r == 0);
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
    i.second.setattr(
      coll_t(spg_t(pgid, i.first)),
      ghobject_t(oid, ghobject_t::NO_GEN, i.first),
      ECUtil::get_hinfo_key(),
      hbuf);
  }
}

void ECTransaction::generate_transactions(
  PGTransaction &t,
  map<
    hobject_t, ECUtil::HashInfoRef, hobject_t::BitwiseComparator
    > &hash_infos,
  ErasureCodeInterfaceRef &ecimpl,
  pg_t pgid,
  bool legacy_log_entries,
  const ECUtil::stripe_info_t &sinfo,
  vector<pg_log_entry_t> &entries,
  map<shard_id_t, ObjectStore::Transaction> *transactions,
  set<hobject_t, hobject_t::BitwiseComparator> *temp_added,
  set<hobject_t, hobject_t::BitwiseComparator> *temp_removed,
  stringstream *out)
{
  assert(transactions);
  assert(temp_added);
  assert(temp_removed);

  map<hobject_t, pg_log_entry_t*, hobject_t::BitwiseComparator> obj_to_log;
  for (auto &&i: entries) {
    obj_to_log.insert(make_pair(i.soid, &i));
  }

  TransactionInfo::LocalRollBack lrb;
  t.safe_create_traverse(
    [&](pair<const hobject_t, PGTransaction::ObjectOperation> &opair) {
    const hobject_t &oid = opair.first;

    auto iter = obj_to_log.find(oid);
    pg_log_entry_t *entry = iter != obj_to_log.end() ? iter->second : nullptr;

    if (entry && opair.second.updated_snaps) {
      lrb.update_snaps(opair.second.updated_snaps->first);
      vector<snapid_t> snaps(
	opair.second.updated_snaps->second.begin(),
	opair.second.updated_snaps->second.end());
      ::encode(snaps, entry->snaps);
    }

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

    map<string, boost::optional<bufferlist> > xattr_rollback;
    ECUtil::HashInfoRef hinfo;
    {
      auto iter = hash_infos.find(oid);
      assert(iter != hash_infos.end());
      hinfo = iter->second;
      bufferlist old_hinfo;
      ::encode(*hinfo, old_hinfo);
      xattr_rollback[ECUtil::get_hinfo_key()] = old_hinfo;
    }

    if (opair.second.is_none() && opair.second.truncate) {
      assert(opair.second.truncate->first == 0);
      assert(opair.second.truncate->first ==
	     opair.second.truncate->second);
      assert(entry);
      assert(obc);

      opair.second.truncate = boost::none;
      opair.second.delete_first = true;
      opair.second.init_type = PGTransaction::ObjectOperation::Init::Create();

      if (obc) {
	/* We need to reapply all of the cached xattrs.
	 * std::map insert fortunately only writes keys
	 * which don't already exist, so this should do
	 * the right thing. */
	opair.second.attr_updates.insert(
	  obc->attr_cache.begin(),
	  obc->attr_cache.end());
      }
    }

    if (oid.is_temp()) {
      if (opair.second.is_fresh_object()) {
	temp_added->insert(oid);
      } else if (opair.second.is_delete()) {
	temp_removed->insert(oid);
      }
    }

    if (opair.second.delete_first) {
      /* We also want to remove the boost::none entries since
       * the keys already won't exist */
      for (auto j = opair.second.attr_updates.begin();
	   j != opair.second.attr_updates.end();
	) {
	if (j->second) {
	  ++j;
	} else {
	  opair.second.attr_updates.erase(j++);
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

    if (opair.second.is_fresh_object() && entry) {
      lrb.create();
    }

    match(
      opair.second.init_type,
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
	*hinfo = *(siter->second);

	if (obc) {
	  auto cobciter = t.obc_map.find(op.source);
	  assert(cobciter != t.obc_map.end());
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
	*hinfo = *(siter->second);
	assert(obc->attr_cache.empty());
      });

    // omap, truncate not supported (except 0, handled above)
    assert(!(opair.second.clear_omap));
    assert(!(opair.second.truncate));
    assert(!(opair.second.omap_header));
    assert(opair.second.omap_updates.empty());

    if (!opair.second.attr_updates.empty()) {
      map<string, bufferlist> to_set;
      for (auto &&j: opair.second.attr_updates) {
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

    if (opair.second.alloc_hint) {
      /* logical_to_next_chunk_offset() scales down both aligned and
       * unaligned offsets

       * we don't bother to roll this back at this time for two reasons:
       * 1) it's advisory
       * 2) we don't track the old value */
      uint64_t object_size = sinfo.logical_to_next_chunk_offset(
	opair.second.alloc_hint->expected_object_size);
      uint64_t write_size = sinfo.logical_to_next_chunk_offset(
	opair.second.alloc_hint->expected_write_size);

      for (auto &&st : *transactions) {
	st.second.set_alloc_hint(
	  coll_t(spg_t(pgid, st.first)),
	  ghobject_t(oid, ghobject_t::NO_GEN, st.first),
	  object_size,
	  write_size,
	  opair.second.alloc_hint->flags);
      }
    }


    if (!opair.second.buffer_updates.empty()) {
      set<int> want;
      for (unsigned i = 0; i < ecimpl->get_chunk_count(); ++i) {
	want.insert(i);
      }
      if (entry) {
	lrb.append(
	  sinfo.aligned_chunk_offset_to_logical_offset(
	    hinfo->get_total_chunk_size()
	    ));
      }
      for (auto &&extent: opair.second.buffer_updates) {
	using BufferUpdate = PGTransaction::ObjectOperation::BufferUpdate;
	match(
	  extent.get_val(),
	  [&](const BufferUpdate::Write &op) {
	    if (extent.get_len()) {
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
		transactions);
	    }
	  },
	  [&](const BufferUpdate::Zero &) {
	    assert(
	      0 ==
	      "Zero is not allowed, do_op should have returned ENOTSUPP");
	  },
	  [&](const BufferUpdate::CloneRange &) {
	    assert(
	      0 ==
	      "CloneRange is not allowed, do_op should have returned ENOTSUPP");
	  });
      }
    }
    entry->mark_local_rollback(
      lrb,
      legacy_log_entries);
  });
}
