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


/*
 * Capability::Export
 */

void Capability::Export::encode(bufferlist &bl) const
{
  ENCODE_START(2, 2, bl);
  encode(cap_id, bl);
  encode(wanted, bl);
  encode(issued, bl);
  encode(pending, bl);
  encode(client_follows, bl);
  encode(seq, bl);
  encode(mseq, bl);
  encode(last_issue_stamp, bl);
  ENCODE_FINISH(bl);
}

void Capability::Export::decode(bufferlist::const_iterator &p)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, p);
  decode(cap_id, p);
  decode(wanted, p);
  decode(issued, p);
  decode(pending, p);
  decode(client_follows, p);
  decode(seq, p);
  decode(mseq, p);
  decode(last_issue_stamp, p);
  DECODE_FINISH(p);
}

void Capability::Export::dump(Formatter *f) const
{
  f->dump_unsigned("cap_id", cap_id);
  f->dump_unsigned("wanted", wanted);
  f->dump_unsigned("issued", issued);
  f->dump_unsigned("pending", pending);
  f->dump_unsigned("client_follows", client_follows);
  f->dump_unsigned("seq", seq);
  f->dump_unsigned("migrate_seq", mseq);
  f->dump_stream("last_issue_stamp") << last_issue_stamp;
}

void Capability::Export::generate_test_instances(list<Capability::Export*>& ls)
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

void Capability::Import::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  encode(cap_id, bl);
  encode(issue_seq, bl);
  encode(mseq, bl);
  ENCODE_FINISH(bl);
}

void Capability::Import::decode(bufferlist::const_iterator &bl)
{
  DECODE_START(1, bl);
  decode(cap_id, bl);
  decode(issue_seq, bl);
  decode(mseq, bl);
  DECODE_FINISH(bl);
}

void Capability::Import::dump(Formatter *f) const
{
  f->dump_unsigned("cap_id", cap_id);
  f->dump_unsigned("issue_seq", issue_seq);
  f->dump_unsigned("migrate_seq", mseq);
}

/*
 * Capability::revoke_info
 */

void Capability::revoke_info::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl)
  encode(before, bl);
  encode(seq, bl);
  encode(last_issue, bl);
  ENCODE_FINISH(bl);
}

void Capability::revoke_info::decode(bufferlist::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  decode(before, bl);
  decode(seq, bl);
  decode(last_issue, bl);
  DECODE_FINISH(bl);
}

void Capability::revoke_info::dump(Formatter *f) const
{
  f->dump_unsigned("before", before);
  f->dump_unsigned("seq", seq);
  f->dump_unsigned("last_issue", last_issue);
}

void Capability::revoke_info::generate_test_instances(list<Capability::revoke_info*>& ls)
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
  client_follows(0),
  client_xattr_version(0), client_inline_version(0),
  last_rbytes(0), last_rsize(0),
  item_session_caps(this), item_snaprealm_caps(this),
  item_revoking_caps(this), item_client_revoking_caps(this),
  inode(i), session(s),
  cap_id(id), _wanted(0), num_revoke_warnings(0),
  _pending(0), _issued(0), last_sent(0), last_issue(0), mseq(0),
  suppress(0), state(0)
{
  if (session) {
    session->touch_cap_bottom(this);
    cap_gen = session->get_cap_gen();
  }
}

client_t Capability::get_client() const
{
  return session ? session->get_client() : client_t(-1);
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
  if (is_valid())
    return;

  if (_pending & ~CEPH_CAP_PIN)
    inc_last_seq();

  bool was_revoking = _issued & ~_pending;
  _pending = _issued = CEPH_CAP_PIN;
  _revokes.clear();

  cap_gen = session->get_cap_gen();

  if (was_revoking)
    maybe_clear_notable();
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
    if (!_wanted && w) {
      in->adjust_num_caps_wanted(1);
    } else if (_wanted && !w) {
      in->adjust_num_caps_wanted(-1);
    }
    if (!is_wanted_notable(_wanted) && is_wanted_notable(w)) {
      if (!is_notable())
	mark_notable();
    } else if (is_wanted_notable(_wanted) && !is_wanted_notable(w)) {
      maybe_clear_notable();
    }
  }
  _wanted = w;
}

void Capability::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl)
  encode(last_sent, bl);
  encode(last_issue_stamp, bl);

  encode(_wanted, bl);
  encode(_pending, bl);
  encode(_revokes, bl);
  ENCODE_FINISH(bl);
}

void Capability::decode(bufferlist::const_iterator &bl)
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

void Capability::dump(Formatter *f) const
{
  f->dump_unsigned("last_sent", last_sent);
  f->dump_unsigned("last_issue_stamp", last_issue_stamp);
  f->dump_unsigned("wanted", _wanted);
  f->dump_unsigned("pending", _pending);

  f->open_array_section("revokes");
  for (const auto &r : _revokes) {
    f->open_object_section("revoke");
    r.dump(f);
    f->close_section();
  }
  f->close_section();
}

void Capability::generate_test_instances(list<Capability*>& ls)
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
