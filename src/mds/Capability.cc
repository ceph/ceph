// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "Capability.h"
#include "CInode.h"
#include "SessionMap.h"

#include "common/Formatter.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "Capability "


/*
 * Capability::Export
 */

void Capability::Export::encode(ceph::buffer::list &bl) const
{
  ENCODE_START(3, 2, bl);
  encode(cap_id, bl);
  encode(wanted, bl);
  encode(issued, bl);
  encode(pending, bl);
  encode(client_follows, bl);
  encode(seq, bl);
  encode(mseq, bl);
  encode(last_issue_stamp, bl);
  encode(state, bl);
  ENCODE_FINISH(bl);
}

void Capability::Export::decode(ceph::buffer::list::const_iterator &p)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, p);
  decode(cap_id, p);
  decode(wanted, p);
  decode(issued, p);
  decode(pending, p);
  decode(client_follows, p);
  decode(seq, p);
  decode(mseq, p);
  decode(last_issue_stamp, p);
  if (struct_v >= 3)
    decode(state, p);
  DECODE_FINISH(p);
}

void Capability::Export::dump(ceph::Formatter *f) const
{
  f->dump_unsigned("cap_id", cap_id);
  f->dump_stream("wanted") << ccap_string(wanted);
  f->dump_stream("issued") << ccap_string(issued);
  f->dump_stream("pending") << ccap_string(pending);
  f->dump_unsigned("client_follows", client_follows);
  f->dump_unsigned("seq", seq);
  f->dump_unsigned("migrate_seq", mseq);
  f->dump_stream("last_issue_stamp") << last_issue_stamp;
}

void Capability::Export::generate_test_instances(std::list<Capability::Export*>& ls)
{
  ls.push_back(new Export);
  ls.push_back(new Export);
  ls.back()->wanted = 1;
  ls.back()->issued = 2;
  ls.back()->pending = 3;
  ls.back()->client_follows = 4;
  ls.back()->mseq = 5;
  ls.back()->last_issue_stamp = utime_t(6, 7);
}

void Capability::Import::encode(ceph::buffer::list &bl) const
{
  ENCODE_START(1, 1, bl);
  encode(cap_id, bl);
  encode(issue_seq, bl);
  encode(mseq, bl);
  ENCODE_FINISH(bl);
}

void Capability::Import::decode(ceph::buffer::list::const_iterator &bl)
{
  DECODE_START(1, bl);
  decode(cap_id, bl);
  decode(issue_seq, bl);
  decode(mseq, bl);
  DECODE_FINISH(bl);
}

void Capability::Import::dump(ceph::Formatter *f) const
{
  f->dump_unsigned("cap_id", cap_id);
  f->dump_unsigned("issue_seq", issue_seq);
  f->dump_unsigned("migrate_seq", mseq);
}

/*
 * Capability::revoke_info
 */

void Capability::revoke_info::encode(ceph::buffer::list& bl) const
{
  ENCODE_START(2, 2, bl)
  encode(before, bl);
  encode(seq, bl);
  encode(last_issue, bl);
  ENCODE_FINISH(bl);
}

void Capability::revoke_info::decode(ceph::buffer::list::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  decode(before, bl);
  decode(seq, bl);
  decode(last_issue, bl);
  DECODE_FINISH(bl);
}

void Capability::revoke_info::dump(ceph::Formatter *f) const
{
  f->dump_unsigned("before", before);
  f->dump_unsigned("seq", seq);
  f->dump_unsigned("last_issue", last_issue);
}

void Capability::revoke_info::generate_test_instances(std::list<Capability::revoke_info*>& ls)
{
  ls.push_back(new revoke_info);
  ls.push_back(new revoke_info);
  ls.back()->before = 1;
  ls.back()->seq = 2;
  ls.back()->last_issue = 3;
}


/*
 * Capability
 */
Capability::Capability(CInode *i, Session *s, uint64_t id) :
  item_session_caps(this), item_snaprealm_caps(this),
  item_revoking_caps(this), item_client_revoking_caps(this),
  lock_caches(member_offset(MDLockCache, item_cap_lock_cache)),
  inode(i), session(s), cap_id(id)
{
  if (session) {
    session->touch_cap_bottom(this);
    cap_gen = session->get_cap_gen();
    if (session->is_stale())
      --cap_gen; // not valid

    auto& conn = session->get_connection();
    if (conn) {
      if (!conn->has_feature(CEPH_FEATURE_MDS_INLINE_DATA))
	state |= STATE_NOINLINE;
      if (!conn->has_feature(CEPH_FEATURE_FS_FILE_LAYOUT_V2))
	state |= STATE_NOPOOLNS;
      if (!conn->has_feature(CEPH_FEATURE_MDS_QUOTA))
	state |= STATE_NOQUOTA;
    }
  } else {
    cap_gen = 0;
  }
}

client_t Capability::get_client() const
{
  return session ? session->get_client() : client_t(-1);
}

int Capability::confirm_receipt(ceph_seq_t seq, unsigned caps) {
  int was_revoking = (_issued & ~_pending);
  if (seq == last_sent) {
    _revokes.clear();
    _issued = caps;
    // don't add bits
    _pending &= caps;

    // if the revoking is not totally finished just add the
    // new revoking caps back.
    if (was_revoking && revoking()) {
      CInode *in = get_inode();
      dout(10) << "revocation is not totally finished yet on " << *in
               << ", the session " << *session << dendl;
      _revokes.emplace_back(_pending, last_sent, last_issue);
      if (!is_notable())
        mark_notable();
    }
  } else {
    // can i forget any revocations?
    while (!_revokes.empty() && _revokes.front().seq < seq)
      _revokes.pop_front();
    if (!_revokes.empty()) {
      if (_revokes.front().seq == seq)
        _revokes.begin()->before = caps;
      calc_issued();
    } else {
      // seq < last_sent
      _issued = caps | _pending;
    }
  }

  if (was_revoking && _issued == _pending) {
    item_revoking_caps.remove_myself();
    item_client_revoking_caps.remove_myself();
    maybe_clear_notable();
  }
  return was_revoking & ~_issued; // return revoked
}

bool Capability::is_stale() const
{
  return session ? session->is_stale() : false;
}

bool Capability::is_valid() const
{
  return !session || session->get_cap_gen() == cap_gen;
}

void Capability::revalidate()
{
  if (!is_valid())
    cap_gen = session->get_cap_gen();
}

void Capability::mark_notable()
{
  state |= STATE_NOTABLE;
  session->touch_cap(this);
}

void Capability::maybe_clear_notable()
{
  if ((_issued == _pending) &&
      !is_clientwriteable() &&
      !is_wanted_notable(_wanted)) {
    ceph_assert(is_notable());
    state &= ~STATE_NOTABLE;
    session->touch_cap_bottom(this);
  }
}

void Capability::set_wanted(int w) {
  CInode *in = get_inode();
  if (in) {
    if (!is_wanted_notable(_wanted) && is_wanted_notable(w)) {
      in->adjust_num_caps_notable(1);
      if (!is_notable())
	mark_notable();
    } else if (is_wanted_notable(_wanted) && !is_wanted_notable(w)) {
      in->adjust_num_caps_notable(-1);
      maybe_clear_notable();
    }
  }
  _wanted = w;
}

void Capability::encode(ceph::buffer::list& bl) const
{
  ENCODE_START(2, 2, bl)
  encode(last_sent, bl);
  encode(last_issue_stamp, bl);

  encode(_wanted, bl);
  encode(_pending, bl);
  encode(_revokes, bl);
  ENCODE_FINISH(bl);
}

void Capability::decode(ceph::buffer::list::const_iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl)
  decode(last_sent, bl);
  decode(last_issue_stamp, bl);

  __u32 tmp_wanted;
  decode(tmp_wanted, bl);
  set_wanted(tmp_wanted);
  decode(_pending, bl);
  decode(_revokes, bl);
  DECODE_FINISH(bl);
  
  calc_issued();
}

void Capability::dump(ceph::Formatter *f) const
{
  if (inode)
    f->dump_stream("ino") << inode->ino();
  f->dump_unsigned("last_sent", last_sent);
  f->dump_stream("last_issue_stamp") << last_issue_stamp;
  f->dump_stream("wanted") << ccap_string(_wanted);
  f->dump_stream("pending") << ccap_string(_pending);

  f->open_array_section("revokes");
  for (const auto &r : _revokes) {
    f->open_object_section("revoke");
    r.dump(f);
    f->close_section();
  }
  f->close_section();
}

void Capability::generate_test_instances(std::list<Capability*>& ls)
{
  ls.push_back(new Capability);
  ls.push_back(new Capability);
  ls.back()->last_sent = 11;
  ls.back()->last_issue_stamp = utime_t(12, 13);
  ls.back()->set_wanted(14);
  ls.back()->_pending = 15;
  {
    auto &r = ls.back()->_revokes.emplace_back();
    r.before = 16;
    r.seq = 17;
    r.last_issue = 18;
  }
  {
    auto &r = ls.back()->_revokes.emplace_back();
    r.before = 19;
    r.seq = 20;
    r.last_issue = 21;
  }
}

MEMPOOL_DEFINE_OBJECT_FACTORY(Capability, co_cap, mds_co);
