// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "acconfig.h"
#include "mds/CInode.h"
#include "mds/CDir.h"
#include "mds/MDSRank.h"
#include "mds/MDCache.h"
#include "osdc/Objecter.h"
#include "OpenFileTable.h"

#include "common/config.h"
#include "common/errno.h"

enum {
  l_oft_first = 1000000,
  l_oft_omap_total_objs,
  l_oft_omap_total_kv_pairs,
  l_oft_omap_total_updates,
  l_oft_omap_total_removes,
  l_oft_last
};

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix _prefix(_dout, mds)
static ostream& _prefix(std::ostream *_dout, MDSRank *mds) {
  return *_dout << "mds." << mds->get_nodeid() << ".openfiles ";
}

OpenFileTable::OpenFileTable(MDSRank *m) : mds(m) {
  PerfCountersBuilder b(mds->cct, "oft", l_oft_first, l_oft_last);

  b.add_u64(l_oft_omap_total_objs, "omap_total_objs");
  b.add_u64(l_oft_omap_total_kv_pairs, "omap_total_kv_pairs");
  b.add_u64(l_oft_omap_total_updates, "omap_total_updates");
  b.add_u64(l_oft_omap_total_removes, "omap_total_removes");
  logger.reset(b.create_perf_counters());
  mds->cct->get_perfcounters_collection()->add(logger.get());
  logger->set(l_oft_omap_total_objs, 0);
  logger->set(l_oft_omap_total_kv_pairs, 0);
  logger->set(l_oft_omap_total_updates, 0);
  logger->set(l_oft_omap_total_removes, 0);
}

OpenFileTable::~OpenFileTable() {
  if (logger) {
    mds->cct->get_perfcounters_collection()->remove(logger.get());
  }
}

void OpenFileTable::get_ref(CInode *in, frag_t fg)
{
  do {
    auto p = anchor_map.find(in->ino());
    if (!in->is_dir()) {
      ceph_assert(fg == -1U);
      ceph_assert(p == anchor_map.end());
    }

    if (p != anchor_map.end()) {
      ceph_assert(in->state_test(CInode::STATE_TRACKEDBYOFT));
      ceph_assert(p->second.nref > 0);
      p->second.nref++;

      if (fg != -1U) {
	auto ret = p->second.frags.insert(fg);
	ceph_assert(ret.second);
	dirty_items.emplace(in->ino(), (int)DIRTY_UNDEF);
      }
      break;
    }

    CDentry *dn = in->get_parent_dn();
    CInode *pin = dn ? dn->get_dir()->get_inode() : nullptr;

    auto ret = anchor_map.emplace(std::piecewise_construct, std::forward_as_tuple(in->ino()),
				  std::forward_as_tuple(in->ino(), (pin ? pin->ino() : inodeno_t(0)),
				  (dn ? dn->get_name() : string()), in->d_type(), 1));
    ceph_assert(ret.second == true);
    in->state_set(CInode::STATE_TRACKEDBYOFT);

    if (fg != -1U)
      ret.first->second.frags.insert(fg);

    auto ret1 = dirty_items.emplace(in->ino(), (int)DIRTY_NEW);
    if (!ret1.second) {
      int omap_idx = ret1.first->second;
      ceph_assert(omap_idx >= 0);
      ret.first->second.omap_idx = omap_idx;
    }

    in = pin;
    fg = -1U;
  } while (in);
}

void OpenFileTable::put_ref(CInode *in, frag_t fg)
{
  do {
    ceph_assert(in->state_test(CInode::STATE_TRACKEDBYOFT));
    auto p = anchor_map.find(in->ino());
    ceph_assert(p != anchor_map.end());
    ceph_assert(p->second.nref > 0);

    if (!in->is_dir()) {
      ceph_assert(fg == -1U);
      ceph_assert(p->second.nref == 1);
    }

    if (p->second.nref > 1) {
      p->second.nref--;
      if (fg != -1U) {
	auto ret = p->second.frags.erase(fg);
	ceph_assert(ret);
	dirty_items.emplace(in->ino(), (int)DIRTY_UNDEF);
      }
      break;
    }

    CDentry *dn = in->get_parent_dn();
    CInode *pin = dn ? dn->get_dir()->get_inode() : nullptr;
    if (dn) {
      ceph_assert(p->second.dirino == pin->ino());
      ceph_assert(p->second.d_name == dn->get_name());
    } else {
      ceph_assert(p->second.dirino == inodeno_t(0));
      ceph_assert(p->second.d_name == "");
    }

    if (fg != -1U) {
      ceph_assert(p->second.frags.size() == 1);
      ceph_assert(*p->second.frags.begin() == fg);
    }

    int omap_idx = p->second.omap_idx;
    anchor_map.erase(p);
    in->state_clear(CInode::STATE_TRACKEDBYOFT);

    auto ret = dirty_items.emplace(in->ino(), omap_idx);
    if (!ret.second) {
      if (ret.first->second == DIRTY_NEW) {
	ceph_assert(omap_idx < 0);
	dirty_items.erase(ret.first);
      } else {
	ceph_assert(omap_idx >= 0);
	ret.first->second = omap_idx;
      }
    }

    in = pin;
    fg = -1U;
  } while (in);
}

void OpenFileTable::add_inode(CInode *in)
{
  dout(10) << __func__ << " " << *in << dendl;
  get_ref(in);
}

void OpenFileTable::remove_inode(CInode *in)
{
  dout(10) << __func__ << " " << *in << dendl;
  put_ref(in);
}

void OpenFileTable::add_dirfrag(CDir *dir)
{
  dout(10) << __func__ << " " << *dir << dendl;
  ceph_assert(!dir->state_test(CDir::STATE_TRACKEDBYOFT));
  dir->state_set(CDir::STATE_TRACKEDBYOFT);
  get_ref(dir->get_inode(), dir->get_frag());
}

void OpenFileTable::remove_dirfrag(CDir *dir)
{
  dout(10) << __func__ << " " << *dir << dendl;
  ceph_assert(dir->state_test(CDir::STATE_TRACKEDBYOFT));
  dir->state_clear(CDir::STATE_TRACKEDBYOFT);
  put_ref(dir->get_inode(), dir->get_frag());
}

void OpenFileTable::notify_link(CInode *in)
{
  dout(10) << __func__ << " " << *in << dendl;
  auto p = anchor_map.find(in->ino());
  ceph_assert(p != anchor_map.end());
  ceph_assert(p->second.nref > 0);
  ceph_assert(p->second.dirino == inodeno_t(0));
  ceph_assert(p->second.d_name == "");

  CDentry *dn = in->get_parent_dn();
  CInode *pin = dn->get_dir()->get_inode();

  p->second.dirino = pin->ino();
  p->second.d_name = dn->get_name();
  dirty_items.emplace(in->ino(), (int)DIRTY_UNDEF);

  get_ref(pin);
}

void OpenFileTable::notify_unlink(CInode *in)
{
  dout(10) << __func__ << " " << *in << dendl;
  auto p = anchor_map.find(in->ino());
  ceph_assert(p != anchor_map.end());
  ceph_assert(p->second.nref > 0);

  CDentry *dn = in->get_parent_dn();
  CInode *pin = dn->get_dir()->get_inode();
  ceph_assert(p->second.dirino == pin->ino());
  ceph_assert(p->second.d_name == dn->get_name());

  p->second.dirino = inodeno_t(0);
  p->second.d_name = "";
  dirty_items.emplace(in->ino(), (int)DIRTY_UNDEF);

  put_ref(pin);
}

object_t OpenFileTable::get_object_name(unsigned idx) const
{
  char s[30];
  snprintf(s, sizeof(s), "mds%d_openfiles.%x", int(mds->get_nodeid()), idx);
  return object_t(s);
}

void OpenFileTable::_encode_header(bufferlist &bl, version_t& _omap_version,
                                   unsigned _omap_num_objs, int j_state)
{
  std::string_view magic = CEPH_FS_ONDISK_MAGIC;
  encode(magic, bl);
  ENCODE_START(1, 1, bl);
  encode(_omap_version, bl);
  encode(_omap_num_objs, bl);
  encode((__u8)j_state, bl);
  ENCODE_FINISH(bl);
}

void OpenFileTable::_encode_header(bufferlist &bl, int j_state)
{
  _encode_header(bl, omap_version, omap_num_objs, j_state);
}

class C_IO_OFT_Save : public MDSIOContextBase {
protected:
  OpenFileTable *oft;
  uint64_t log_seq;
  MDSContext *fin;
  MDSRank *get_mds() override { return oft->mds; }
public:
  C_IO_OFT_Save(OpenFileTable *t, uint64_t s, MDSContext *c) :
    oft(t), log_seq(s), fin(c) {}
  void finish(int r) {
    oft->_commit_finish(r, log_seq, fin);
  }
  void print(ostream& out) const override {
    out << "openfiles_save";
  }
};

void OpenFileTable::_commit_finish(int r, uint64_t log_seq, MDSContext *fin)
{
  dout(10) << __func__ << " log_seq " << log_seq << " r " << r << dendl;
  if (r < 0) {
    mds->handle_write_error(r);
    return;
  }

  ceph_assert(log_seq <= committing_log_seq);
  ceph_assert(log_seq >= committed_log_seq);
  committed_log_seq = log_seq;
  num_pending_commit--;

  if (fin)
    fin->complete(r);

  journal_state = JOURNAL_NONE;
}

void OpenFileTable::_journal_commit(int r, uint64_t log_seq, MDSContext *c,
				    int op_prio, version_t& _omap_version,
                                    unsigned _old_num_objs, unsigned _omap_num_objs,
                                    std::vector<struct omap_update_ctl>& omap_updates)
{
  dout(10) << __func__ << " log_seq " << log_seq << " omap_version "
           << omap_version << " r " << r << dendl;
  if (r < 0) {
    mds->handle_write_error_with_lock(r);
    return;
  }

  C_GatherBuilder gather(g_ceph_context,
                         new C_OnFinisher(new C_IO_OFT_Save(this, log_seq, c),
                         mds->finisher));

  std::map<unsigned, std::vector<ObjectOperation>> ops_map;

  auto create_journal_ops_func = [&](unsigned idx) {
    auto& ctl = omap_updates.at(idx);
    auto& op_vec = ops_map[idx];
    unsigned op_idx = op_vec.size();
    op_vec.resize(op_vec.size() + ctl.metas.size());
    bool first = true;

    unsigned journal_idx = 0;
    for (auto& meta : ctl.metas) {
      ObjectOperation& op = op_vec.at(op_idx++);
      op.priority = op_prio;

      if (ctl.clear) {
        ctl.clear = false;
        op.omap_clear();
        op.set_last_op_flags(CEPH_OSD_OP_FLAG_FAILOK);
      }

      if (first) {
        bufferlist header;
        _encode_header(header, _omap_version, _omap_num_objs, JOURNAL_START);
        op.omap_set_header(header);
      }

      bufferlist bl;
      encode(_omap_version, bl);
      encode(meta.to_update, bl);
      encode(meta.to_remove, bl);

      char key[32];
      snprintf(key, sizeof(key), "_journal.%x", journal_idx++);
      std::map<string, bufferlist> tmp_map;
      tmp_map[key].swap(bl);
      op.omap_set(tmp_map);
    }

    return ctl.metas.size();
  };

  auto create_ops_func = [&](unsigned idx, bool journaled=false) {
    auto& ctl = omap_updates.at(idx);
    auto& op_vec = ops_map[idx];
    unsigned op_idx = op_vec.size();

    op_vec.resize(op_vec.size() + ctl.metas.size());

    bool first = true;
    for (auto& meta : ctl.metas) {
      ObjectOperation& op = op_vec.at(op_idx++);
      op.priority = op_prio;

      if (ctl.clear) {
        ctl.clear = false;
        op.omap_clear();
        op.set_last_op_flags(CEPH_OSD_OP_FLAG_FAILOK);
      }

      if (first) {
        bufferlist header;
        int j_state = journaled ? JOURNAL_FINISH : JOURNAL_NONE;
        _encode_header(header, _omap_version, _omap_num_objs, j_state);
        op.omap_set_header(header);
      }

      if (!meta.to_update.empty()) {
        op.omap_set(meta.to_update);
      }
      if (!meta.to_remove.empty()) {
        op.omap_rm_keys(meta.to_remove);
      }
    }

    // create the object with updating the header
    if (ctl.clear) {
      op_vec.resize(op_vec.size() + 1);
      ObjectOperation& op = op_vec.back();
      op.priority = op_prio;

      ctl.clear = false;
      op.omap_clear();
      op.set_last_op_flags(CEPH_OSD_OP_FLAG_FAILOK);

      bufferlist header;
      _encode_header(header, _omap_version, _omap_num_objs, JOURNAL_NONE);
      op.omap_set_header(header);

      return;
    }

    if (!journaled)
      return;

    // remove the journals
    op_vec.resize(op_vec.size() + 1);
    ObjectOperation& op = op_vec.back();
    op.priority = op_prio;
    std::set<string> to_remove;

    for (unsigned i = 0; i < ctl.metas.size(); ++i) {
      char key[32];
      snprintf(key, sizeof(key), "_journal.%x", i);
      to_remove.emplace(key);
    }

    if (!to_remove.empty()) {
      op.omap_rm_keys(to_remove);
    }
  };

  // skip journal if only one osd request is required and
  // object count does not change.
  unsigned total_ops = 0;
  unsigned omap_idx = 0;
  for (unsigned idx = 0; idx < omap_updates.size(); idx++) {
    auto& ctl = omap_updates.at(idx);
    if (ctl.metas.size()) {
      total_ops += ctl.metas.size();
      omap_idx = idx;
    }
  }

  // skip journal if only one osd request is required and object count
  // does not change.
  if (total_ops == 1 && _old_num_objs != _omap_num_objs) {
    create_ops_func(omap_idx, false);
  } else {
    // commit journal
    unsigned total_journal_ops = 0;
    for (unsigned idx = 0; idx < omap_updates.size(); idx++) {
      total_journal_ops += create_journal_ops_func(idx);
    }

    // commit openfiles table
    for (unsigned idx = 0; idx < omap_updates.size(); idx++) {
      create_ops_func(idx, !!total_journal_ops);
    }
  }

  SnapContext snapc;
  object_locator_t oloc(mds->mdsmap->get_metadata_pool());
  for (auto& it : ops_map) {
    object_t oid = get_object_name(it.first);
    for (auto& op : it.second) {
      mds->objecter->mutate(oid, oloc, op, snapc, ceph::real_clock::now(),
			    0, gather.new_sub());
    }
  }
  gather.activate();
}

class C_IO_OFT_Journal : public Context {
protected:
  OpenFileTable *oft;
  uint64_t log_seq;
  MDSContext *fin;
  int op_prio;
  version_t omap_version;
  unsigned old_num_objs;
  unsigned omap_num_objs;
  std::vector<OpenFileTable::omap_update_ctl> omap_updates;
public:
  C_IO_OFT_Journal(OpenFileTable *t, uint64_t s, MDSContext *c, int prio,
		   version_t& v, unsigned old, unsigned _new,
                   std::vector<OpenFileTable::omap_update_ctl>&& updates) :
    oft(t), log_seq(s), fin(c), op_prio(prio), omap_version(v),
    old_num_objs(old), omap_num_objs(_new) {
    omap_updates.swap(updates);
  }
  void finish(int r) override {
    oft->_journal_commit(r, log_seq, fin, op_prio, omap_version, old_num_objs,
                         omap_num_objs, omap_updates);
  }
};

void OpenFileTable::commit(MDSContext *c, uint64_t log_seq, int op_prio)
{
  dout(10) << __func__ << " log_seq " << log_seq << dendl;

  uint64_t total_updates = 0;
  uint64_t total_removes = 0;
  ceph_assert(num_pending_commit == 0);
  num_pending_commit++;
  ceph_assert(log_seq >= committing_log_seq);
  committing_log_seq = log_seq;

  omap_version++;

  SnapContext snapc;
  object_locator_t oloc(mds->mdsmap->get_metadata_pool());

  const unsigned max_write_size = mds->mdcache->max_dir_commit_size;

  std::vector<struct omap_update_ctl> omap_updates(omap_num_objs);

  auto ctl_update_func = [&](struct omap_update_ctl& ctl, char* key, bufferlist& bl) {
    if (ctl.meta_idx == -1) {
      ctl.meta_idx = 0;
      ctl.metas.resize(1);
    }
    ctl.metas[ctl.meta_idx].to_update[key].swap(bl);
  };

  auto ctl_remove_func = [&](struct omap_update_ctl& ctl, char* key) {
    if (ctl.meta_idx == -1) {
      ctl.meta_idx = 0;
      ctl.metas.resize(1);
    }
    ctl.metas[ctl.meta_idx].to_remove.emplace(key);
  };

  auto ctl_inc_metas_func = [&](struct omap_update_ctl& ctl) {
    ctl.meta_idx++;
    ctl.metas.resize(ctl.meta_idx);
    ctl.write_size = 0;
  };

  bool first_commit = !loaded_anchor_map.empty();

  unsigned first_free_idx = 0;
  unsigned old_num_objs = omap_num_objs;
  if (omap_num_objs == 0) {
    omap_num_objs = 1;
    omap_num_items.resize(omap_num_objs);
    omap_updates.resize(omap_num_objs);
    omap_updates.back().clear = true;
  }

  auto submit_func = [&]() {
    ceph_assert(!omap_updates.empty());
    auto _c = new C_IO_OFT_Journal(this, log_seq, c, op_prio, omap_version,
                                   old_num_objs, omap_num_objs, std::move(omap_updates));
    mds->finisher->queue(_c);
  };

  for (auto& [ino, state] : dirty_items) {
    auto p = anchor_map.find(ino);

    if (first_commit) {
      auto q = loaded_anchor_map.find(ino);
      if (q != loaded_anchor_map.end()) {
	ceph_assert(p != anchor_map.end());
	p->second.omap_idx = q->second.omap_idx;
	bool same = (p->second == q->second);
	loaded_anchor_map.erase(q);
	if (same)
	  continue;
      }
    }

    char key[32];
    int len = snprintf(key, sizeof(key), "%llx", (unsigned long long)ino.val);

    int omap_idx;
    if (p != anchor_map.end()) {
      omap_idx = p->second.omap_idx;
      if (omap_idx < 0) {
	ceph_assert(state == DIRTY_NEW);
	// find omap object to store the key
	for (unsigned i = first_free_idx; i < omap_num_objs; i++) {
	  if (omap_num_items[i] < MAX_ITEMS_PER_OBJ) {
	    omap_idx = i;
	    break;
	  }
	}
	if (omap_idx < 0) {
	  ++omap_num_objs;
	  ceph_assert(omap_num_objs <= MAX_OBJECTS);
	  omap_num_items.resize(omap_num_objs);
	  omap_updates.resize(omap_num_objs);
	  omap_updates.back().clear = true;
	  omap_idx = omap_num_objs - 1;
	}
	first_free_idx = omap_idx;

	p->second.omap_idx = omap_idx;
	++omap_num_items[omap_idx];
      }
    } else {
      omap_idx = state;
      unsigned& count = omap_num_items.at(omap_idx);
      ceph_assert(count > 0);
      --count;
      if ((unsigned)omap_idx < first_free_idx && count < MAX_ITEMS_PER_OBJ)
	first_free_idx = omap_idx;
    }
    auto& ctl = omap_updates.at(omap_idx);
    if (ctl.write_size >= max_write_size)
      ctl_inc_metas_func(ctl);

    if (p != anchor_map.end()) {
      bufferlist bl;
      using ceph::encode;
      encode(p->second, bl);
      encode((__u32)0, bl); // frags set was encoded here

      ctl.write_size += bl.length() + len + 2 * sizeof(__u32);
      ctl_update_func(ctl, key, bl);
      total_updates++;
    } else {
      ctl.write_size += len + sizeof(__u32);
      ctl_remove_func(ctl, key);
      total_removes++;
    }
  }

  dirty_items.clear();

  if (first_commit) {
    for (auto& [ino, anchor] : loaded_anchor_map) {
      char key[32];
      int len = snprintf(key, sizeof(key), "%llx", (unsigned long long)ino.val);

      int omap_idx = anchor.omap_idx;
      unsigned& count = omap_num_items.at(omap_idx);
      ceph_assert(count > 0);
      --count;

      auto& ctl = omap_updates.at(omap_idx);
      if (ctl.write_size >= max_write_size)
        ctl_inc_metas_func(ctl);

      ctl.write_size += len + sizeof(__u32);
      ctl_remove_func(ctl, key);
      total_removes++;
    }
    loaded_anchor_map.clear();
  }

  size_t total_items = 0;
  {
    unsigned used_objs = 1;
    std::vector<unsigned> objs_to_write;
    for (unsigned i = 0; i < omap_num_objs; i++) {
      total_items += omap_num_items[i];
      if (omap_num_items[i] > 0)
	used_objs = i + 1;
    }
    ceph_assert(total_items == anchor_map.size());
    // adjust omap object count
    if (used_objs < omap_num_objs) {
      omap_num_objs = used_objs;
      omap_num_items.resize(omap_num_objs);
    }
  }

  ceph_assert(journal_state == JOURNAL_NONE);
  journal_state = JOURNAL_FINISH;

  submit_func();

  logger->set(l_oft_omap_total_objs, omap_num_objs);
  logger->set(l_oft_omap_total_kv_pairs, total_items);
  logger->inc(l_oft_omap_total_updates, total_updates);
  logger->inc(l_oft_omap_total_removes, total_removes);
}

class C_IO_OFT_Load : public MDSIOContextBase {
protected:
  OpenFileTable *oft;
  MDSRank *get_mds() override { return oft->mds; }

public:
  int header_r = 0;  //< Return value from OMAP header read
  int values_r = 0;  //< Return value from OMAP value read
  bufferlist header_bl;
  std::map<std::string, bufferlist> values;
  unsigned index;
  bool first;
  bool more = false;

  C_IO_OFT_Load(OpenFileTable *t, unsigned i, bool f) :
    oft(t), index(i), first(f) {}
  void finish(int r) override {
    oft->_load_finish(r, header_r, values_r, index, first, more, header_bl, values);
  }
  void print(ostream& out) const override {
    out << "openfiles_load";
  }
};

class C_IO_OFT_Recover : public MDSIOContextBase {
protected:
  OpenFileTable *oft;
  MDSRank *get_mds() override { return oft->mds; }
public:
  C_IO_OFT_Recover(OpenFileTable *t) : oft(t) {}
  void finish(int r) override {
    oft->_recover_finish(r);
  }
  void print(ostream& out) const override {
    out << "openfiles_recover";
  }
};

void OpenFileTable::_recover_finish(int r)
{
  if (r < 0) {
    derr << __func__ << " got " << cpp_strerror(r) << dendl;
    _reset_states();
  } else {
    dout(10) << __func__ << ": load complete" << dendl;
  }

  journal_state = JOURNAL_NONE;
  load_done = true;
  finish_contexts(g_ceph_context, waiting_for_load);
  waiting_for_load.clear();
}

void OpenFileTable::_read_omap_values(const std::string& key, unsigned idx,
                                      bool first)
{
    object_t oid = get_object_name(idx);
    dout(10) << __func__ << ": load from '" << oid << ":" << key << "'" << dendl;
    object_locator_t oloc(mds->mdsmap->get_metadata_pool());
    C_IO_OFT_Load *c = new C_IO_OFT_Load(this, idx, first);
    ObjectOperation op;
    if (first)
      op.omap_get_header(&c->header_bl, &c->header_r);
    op.omap_get_vals(key, "", uint64_t(-1),
		     &c->values, &c->more, &c->values_r);
    mds->objecter->read(oid, oloc, op, CEPH_NOSNAP, nullptr, 0,
			new C_OnFinisher(c, mds->finisher));
}

void OpenFileTable::_load_finish(int op_r, int header_r, int values_r,
				 unsigned idx, bool first, bool more,
				 bufferlist &header_bl,
				 std::map<std::string, bufferlist> &values)
{
  using ceph::decode;
  int err = -EINVAL;

  auto decode_func = [this](unsigned idx, inodeno_t ino, bufferlist &bl) {
    auto p = bl.cbegin();

    size_t count = loaded_anchor_map.size();
    auto it = loaded_anchor_map.emplace_hint(loaded_anchor_map.end(),
					    std::piecewise_construct,
					    std::make_tuple(ino),
					    std::make_tuple());
    RecoveredAnchor& anchor = it->second;
    decode(anchor, p);
    frag_vec_t frags; // unused
    decode(frags, p);
    ceph_assert(ino == anchor.ino);
    anchor.omap_idx = idx;
    anchor.auth = MDS_RANK_NONE;


    if (loaded_anchor_map.size() > count)
      ++omap_num_items[idx];
  };

  if (op_r < 0) {
    derr << __func__ << " got " << cpp_strerror(op_r) << dendl;
    err = op_r;
    goto out;
  }

  try {
    if (first) {
      auto p = header_bl.cbegin();

      string magic;
      version_t version;
      unsigned num_objs;
      __u8 jstate;

      if (header_bl.length() == 13) {
	// obsolete format.
	decode(version, p);
	decode(num_objs, p);
	decode(jstate, p);
      } else {
	decode(magic, p);
	if (magic != CEPH_FS_ONDISK_MAGIC) {
	  CachedStackStringStream css;
	  *css << "invalid magic '" << magic << "'";
	  throw buffer::malformed_input(css->str());
	}

	DECODE_START(1, p);
	decode(version, p);
	decode(num_objs, p);
	decode(jstate, p);
	DECODE_FINISH(p);
      }

      if (num_objs > MAX_OBJECTS) {
	  CachedStackStringStream css;
	  *css << "invalid object count '" << num_objs << "'";
	  throw buffer::malformed_input(css->str());
      }
      if (jstate > JOURNAL_FINISH) {
	  CachedStackStringStream css;
	  *css << "invalid journal state '" << jstate << "'";
	  throw buffer::malformed_input(css->str());
      }

      if (version > omap_version) {
	omap_version = version;
	omap_num_objs = num_objs;
	omap_num_items.resize(omap_num_objs);
	journal_state = jstate;
      } else if (version == omap_version) {
	ceph_assert(omap_num_objs == num_objs);
	if (jstate > journal_state)
	  journal_state = jstate;
      }
    }

    for (auto& it : values) {
      if (it.first.compare(0, 9, "_journal.") == 0) {
	if (idx >= loaded_journals.size())
	  loaded_journals.resize(idx + 1);

	if (journal_state == JOURNAL_FINISH) {
	  loaded_journals[idx][it.first].swap(it.second);
	} else { // incomplete journal
	  loaded_journals[idx][it.first].length();
	}
	continue;
      }

      inodeno_t ino;
      sscanf(it.first.c_str(), "%llx", (unsigned long long*)&ino.val);
      decode_func(idx, ino, it.second);
    }
  } catch (buffer::error &e) {
    derr << __func__ << ": corrupted header/values: " << e.what() << dendl;
    goto out;
  }

  if (more || idx + 1 < omap_num_objs) {
    // Issue another read if we're not at the end of the omap
    std::string last_key;
    if (more)
      last_key = values.rbegin()->first;
    else
      idx++;

    _read_omap_values(last_key, idx, !more);
    return;
  }

  // replay journal
  if (loaded_journals.size() > 0) {
    dout(10) << __func__ << ": recover journal" << dendl;

    C_GatherBuilder gather(g_ceph_context,
			   new C_OnFinisher(new C_IO_OFT_Recover(this),
					    mds->finisher));
    object_locator_t oloc(mds->mdsmap->get_metadata_pool());
    SnapContext snapc;

    for (unsigned omap_idx = 0; omap_idx < loaded_journals.size(); omap_idx++) {
      auto& loaded_journal = loaded_journals[omap_idx];

      std::vector<ObjectOperation> op_vec;
      try {
	for (auto& it : loaded_journal) {
	  if (journal_state != JOURNAL_FINISH)
	    continue;
	  auto p = it.second.cbegin();
	  version_t version;
	  std::map<string, bufferlist> to_update;
	  std::set<string> to_remove;
	  decode(version, p);
	  if (version != omap_version)
	    continue;
	  decode(to_update, p);
	  decode(to_remove, p);
	  it.second.clear();

	  for (auto& q : to_update) {
	    inodeno_t ino;
	    sscanf(q.first.c_str(), "%llx", (unsigned long long*)&ino.val);
	    decode_func(omap_idx, ino, q.second);
	  }
	  for (auto& q : to_remove) {
	    inodeno_t ino;
	    sscanf(q.c_str(), "%llx",(unsigned long long*)&ino.val);
	    ceph_assert(ino.val > 0);
	    if (loaded_anchor_map.erase(ino)) {
	      unsigned& count = omap_num_items[omap_idx];
	      ceph_assert(count > 0);
	      --count;
	    }
	  }

	  op_vec.resize(op_vec.size() + 1);
	  ObjectOperation& op = op_vec.back();
	  op.priority = CEPH_MSG_PRIO_HIGH;
	  if (!to_update.empty())
	    op.omap_set(to_update);
	  if (!to_remove.empty())
	    op.omap_rm_keys(to_remove);
	}
      } catch (buffer::error &e) {
	derr << __func__ << ": corrupted journal: " << e.what() << dendl;
	goto out;
      }

      op_vec.resize(op_vec.size() + 1);
      ObjectOperation& op = op_vec.back();
      {
	bufferlist header;
	if (journal_state == JOURNAL_FINISH)
	  _encode_header(header, JOURNAL_FINISH);
	else
	  _encode_header(header, JOURNAL_NONE);
	op.omap_set_header(header);
      }
      {
	// remove journal
	std::set<string> to_remove;
	for (auto &it : loaded_journal)
	  to_remove.emplace(it.first);
	op.omap_rm_keys(to_remove);
      }
      loaded_journal.clear();

      object_t oid = get_object_name(omap_idx);
      for (auto& op : op_vec) {
	mds->objecter->mutate(oid, oloc, op, snapc, ceph::real_clock::now(),
			      0, gather.new_sub());
      }
    }
    gather.activate();
    return;
  }

  journal_state = JOURNAL_NONE;
  err = 0;
  dout(10) << __func__ << ": load complete" << dendl;
out:

  if (err < 0)
    _reset_states();

  load_done = true;
  finish_contexts(g_ceph_context, waiting_for_load);
  waiting_for_load.clear();
}

void OpenFileTable::load(MDSContext *onload)
{
  dout(10) << __func__ << dendl;
  ceph_assert(!load_done);
  if (onload)
    waiting_for_load.push_back(onload);

  _read_omap_values("", 0, true);
}

void OpenFileTable::_get_ancestors(const Anchor& parent,
				   vector<inode_backpointer_t>& ancestors,
				   mds_rank_t& auth_hint)
{
  inodeno_t dirino = parent.dirino;
  std::string_view d_name = parent.d_name;

  bool first = true;
  ancestors.clear();
  while (true) {
    ancestors.push_back(inode_backpointer_t(dirino, string{d_name}, 0));

    auto p = loaded_anchor_map.find(dirino);
    if (p == loaded_anchor_map.end())
      break;

    if (first)
      auth_hint = p->second.auth;

    dirino = p->second.dirino;
    d_name = p->second.d_name;
    if (dirino == inodeno_t(0))
      break;

    first = false;
  }
}

class C_OFT_OpenInoFinish: public MDSContext {
  OpenFileTable *oft;
  inodeno_t ino;
  MDSRank *get_mds() override { return oft->mds; }
public:
  C_OFT_OpenInoFinish(OpenFileTable *t, inodeno_t i) : oft(t), ino(i) {}
  void finish(int r) override {
    oft->_open_ino_finish(ino, r);
  }
};

void OpenFileTable::_open_ino_finish(inodeno_t ino, int r)
{
  if (prefetch_state == DIR_INODES && r >= 0 && ino != inodeno_t(0)) {
    auto p = loaded_anchor_map.find(ino);
    ceph_assert(p != loaded_anchor_map.end());
    p->second.auth = mds_rank_t(r);
  }

  if (r != mds->get_nodeid())
    mds->mdcache->rejoin_prefetch_ino_finish(ino, r);

  num_opening_inodes--;
  if (num_opening_inodes == 0) {
    if (prefetch_state == DIR_INODES)  {
      if (g_conf().get_val<bool>("mds_oft_prefetch_dirfrags")) {
	prefetch_state = DIRFRAGS;
	_prefetch_dirfrags();
      } else {
	prefetch_state = FILE_INODES;
	_prefetch_inodes();
      }
    } else if (prefetch_state == FILE_INODES) {
      prefetch_state = DONE;
      logseg_destroyed_inos.clear();
      destroyed_inos_set.clear();
      finish_contexts(g_ceph_context, waiting_for_prefetch);
      waiting_for_prefetch.clear();
    } else {
      ceph_abort();
    }
  }
}

void OpenFileTable::_prefetch_dirfrags()
{
  dout(10) << __func__ << dendl;
  ceph_assert(prefetch_state == DIRFRAGS);

  MDCache *mdcache = mds->mdcache;
  std::vector<CDir*> fetch_queue;

  for (auto& [ino, anchor] : loaded_anchor_map) {
    if (anchor.frags.empty())
      continue;
    CInode *diri = mdcache->get_inode(ino);
    if (!diri)
      continue;
    if (diri->state_test(CInode::STATE_REJOINUNDEF))
      continue;

    for (auto& fg: anchor.frags) {
      CDir *dir = diri->get_dirfrag(fg);
      if (dir) {
	if (dir->is_auth() && !dir->is_complete())
	  fetch_queue.push_back(dir);
      } else {
	frag_vec_t leaves;
	diri->dirfragtree.get_leaves_under(fg, leaves);
	for (auto& leaf : leaves) {
	  if (diri->is_auth()) {
	    dir = diri->get_or_open_dirfrag(mdcache, leaf);
	  } else {
	    dir = diri->get_dirfrag(leaf);
	  }
	  if (dir && dir->is_auth() && !dir->is_complete())
	    fetch_queue.push_back(dir);
	}
      }
    }
  }

  MDSGatherBuilder gather(g_ceph_context);
  int num_opening_dirfrags = 0;
  for (const auto& dir : fetch_queue) {
    if (dir->state_test(CDir::STATE_REJOINUNDEF))
      ceph_assert(dir->get_inode()->dirfragtree.is_leaf(dir->get_frag()));
    dir->fetch(gather.new_sub());

    if (!(++num_opening_dirfrags % 1000))
      mds->heartbeat_reset();
  }

  auto finish_func = [this](int r) {
    prefetch_state = FILE_INODES;
    _prefetch_inodes();
  };
  if (gather.has_subs()) {
    gather.set_finisher(
	new MDSInternalContextWrapper(mds,
	  new LambdaContext(std::move(finish_func))));
    gather.activate();
  } else {
    finish_func(0);
  }
}

void OpenFileTable::_prefetch_inodes()
{
  dout(10) << __func__ << " state " << prefetch_state << dendl;
  ceph_assert(!num_opening_inodes);
  num_opening_inodes = 1;

  int64_t pool;
  if (prefetch_state == DIR_INODES)
    pool = mds->mdsmap->get_metadata_pool();
  else if (prefetch_state == FILE_INODES)
    pool = mds->mdsmap->get_first_data_pool();
  else
    ceph_abort();

  MDCache *mdcache = mds->mdcache;

  if (destroyed_inos_set.empty()) {
    for (auto& it : logseg_destroyed_inos)
      destroyed_inos_set.insert(it.second.begin(), it.second.end());
  }

  for (auto& [ino, anchor] : loaded_anchor_map) {
    if (destroyed_inos_set.count(ino))
	continue;
    if (anchor.d_type == DT_DIR) {
      if (prefetch_state != DIR_INODES)
	continue;
      if (MDS_INO_IS_MDSDIR(ino)) {
	anchor.auth = MDS_INO_MDSDIR_OWNER(ino);
	continue;
      }
      if (MDS_INO_IS_STRAY(ino)) {
	anchor.auth = MDS_INO_STRAY_OWNER(ino);
	continue;
      }
    } else {
      if (prefetch_state != FILE_INODES)
	continue;
      // load all file inodes for MDCache::identify_files_to_recover()
    }
    CInode *in = mdcache->get_inode(ino);
    if (in)
      continue;

    num_opening_inodes++;

    auto fin = new C_OFT_OpenInoFinish(this, ino);
    if (anchor.dirino != inodeno_t(0)) {
      vector<inode_backpointer_t> ancestors;
      mds_rank_t auth_hint = MDS_RANK_NONE;
      _get_ancestors(anchor, ancestors, auth_hint);
      mdcache->open_ino(ino, pool, fin, false, false, &ancestors, auth_hint);
    } else {
      mdcache->open_ino(ino, pool, fin, false);
    }

    if (!(num_opening_inodes % 1000))
      mds->heartbeat_reset();
  }

  _open_ino_finish(inodeno_t(0), 0);
}

bool OpenFileTable::prefetch_inodes()
{
  dout(10) << __func__ << dendl;
  ceph_assert(!prefetch_state);
  prefetch_state = DIR_INODES;

  if (!load_done) {
    wait_for_load(
	new MDSInternalContextWrapper(mds,
	  new LambdaContext([this](int r) {
	    _prefetch_inodes();
	    })
	  )
	);
    return true;
  }

  _prefetch_inodes();
  return !is_prefetched();
}

bool OpenFileTable::should_log_open(CInode *in)
{
  if (in->state_test(CInode::STATE_TRACKEDBYOFT)) {
    // inode just journaled
    if (in->last_journaled >= committing_log_seq)
      return false;
    // item not dirty. it means the item has already been saved
    auto p = dirty_items.find(in->ino());
    if (p == dirty_items.end())
      return false;
  }
  return true;
}

void OpenFileTable::note_destroyed_inos(uint64_t seq, const vector<inodeno_t>& inos)
{
   auto& vec = logseg_destroyed_inos[seq];
   vec.insert(vec.end(), inos.begin(), inos.end());
}

void OpenFileTable::trim_destroyed_inos(uint64_t seq)
{
  auto p = logseg_destroyed_inos.begin();
  while (p != logseg_destroyed_inos.end()) {
    if (p->first >= seq)
      break;
    logseg_destroyed_inos.erase(p++);
  }
}
