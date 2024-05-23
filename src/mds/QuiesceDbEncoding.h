/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM, Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once
#include "QuiesceDb.h"
#include "include/encoding.h"
#include <stdint.h>

void encode(QuiesceDbVersion const& v, bufferlist& bl, uint64_t features = 0)
{
  ENCODE_START(1, 1, bl);
  encode(v.epoch, bl, features);
  encode(v.set_version, bl, features);
  ENCODE_FINISH(bl);
}

void decode(QuiesceDbVersion& v, bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  decode(v.epoch, p);
  decode(v.set_version, p);
  DECODE_FINISH(p);
}

void encode(QuiesceState const & state, bufferlist& bl, uint64_t features=0)
{
  static_assert(QuiesceState::QS__MAX <= UINT8_MAX);
  uint8_t v = (uint8_t)state;
  encode(v, bl, features);
}

void decode(QuiesceState & state, bufferlist::const_iterator& p)
{
  uint8_t v = 0;
  decode(v, p);
  state = (QuiesceState)v;
}

void encode(QuiesceTimeInterval const & interval, bufferlist& bl, uint64_t features=0)
{
  encode(interval.count(), bl, features);
}

void decode(QuiesceTimeInterval & interval, bufferlist::const_iterator& p)
{
  QuiesceClock::rep count;
  decode(count, p);
  interval = QuiesceTimeInterval { count };
}

void encode(RecordedQuiesceState const& rstate, bufferlist& bl, uint64_t features = 0)
{
  ENCODE_START(1, 1, bl);
  encode(rstate.state, bl, features);
  encode(rstate.at_age.count(), bl, features);
  ENCODE_FINISH(bl);
}

void decode(RecordedQuiesceState& rstate, bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  decode(rstate.state, p);
  decode(rstate.at_age, p);
  DECODE_FINISH(p);
}

void encode(QuiesceSet::MemberInfo const& member, bufferlist& bl, uint64_t features = 0)
{
  ENCODE_START(1, 1, bl);
  encode(member.rstate, bl, features);
  encode(member.excluded, bl, features);
  ENCODE_FINISH(bl);
}

void decode(QuiesceSet::MemberInfo& member, bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  decode(member.rstate, p);
  decode(member.excluded, p);
  DECODE_FINISH(p);
}

void encode(QuiesceSet const& set, bufferlist& bl, uint64_t features = 0)
{
  ENCODE_START(1, 1, bl);
  encode(set.version, bl, features);
  encode(set.rstate, bl, features);
  ::encode(set.timeout, bl, features);
  ::encode(set.expiration, bl, features);
  encode(set.members, bl, features);
  ENCODE_FINISH(bl);
}

void decode(QuiesceSet& set, bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  decode(set.version, p);
  decode(set.rstate, p);
  ::decode(set.timeout, p);
  ::decode(set.expiration, p);
  decode(set.members, p);
  DECODE_FINISH(p);
}

void encode(QuiesceDbRequest const& req, bufferlist& bl, uint64_t features = 0)
{
  ENCODE_START(1, 1, bl);
  encode(req.control.raw, bl, features);
  encode(req.set_id, bl);
  encode(req.if_version, bl);
  encode(req.timeout, bl);
  encode(req.expiration, bl);
  encode(req.await, bl);
  encode(req.roots, bl);
  ENCODE_FINISH(bl);
}

void decode(QuiesceDbRequest& req, bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  decode(req.control.raw, p);
  decode(req.set_id, p);
  decode(req.if_version, p);
  decode(req.timeout, p);
  decode(req.expiration, p);
  decode(req.await, p);
  decode(req.roots, p);
  DECODE_FINISH(p);
}

void encode(QuiesceDbListing const& listing, bufferlist& bl, uint64_t features = 0)
{
  ENCODE_START(1, 1, bl);
  encode(listing.db_version, bl, features);
  ::encode(listing.db_age, bl, features);
  encode(listing.sets, bl, features);
  ENCODE_FINISH(bl);
}

void decode(QuiesceDbListing& listing, bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  decode(listing.db_version, p);
  ::decode(listing.db_age, p);
  decode(listing.sets, p);
  DECODE_FINISH(p);
}

void encode(QuiesceDbPeerListing const& listing, bufferlist& bl, uint64_t features = 0)
{
  ENCODE_START(1, 1, bl);
  encode(listing.origin, bl, features);
  encode(listing.db, bl, features);
  ENCODE_FINISH(bl);
}

void decode(QuiesceDbPeerListing& listing, bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  decode(listing.origin, p);
  decode(listing.db, p);
  DECODE_FINISH(p);
}

void encode(QuiesceMap::RootInfo const& root, bufferlist& bl, uint64_t features = 0)
{
  ENCODE_START(1, 1, bl);
  encode(root.state, bl, features);
  ::encode(root.ttl, bl, features);
  ENCODE_FINISH(bl);
}

void decode(QuiesceMap::RootInfo& root, bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  decode(root.state, p);
  ::decode(root.ttl, p);
  DECODE_FINISH(p);
}

void encode(QuiesceMap const& map, bufferlist& bl, uint64_t features = 0)
{
  ENCODE_START(1, 1, bl);
  encode(map.db_version, bl, features);
  encode(map.roots, bl, features);
  ENCODE_FINISH(bl);
}

void decode(QuiesceMap& map, bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  decode(map.db_version, p);
  decode(map.roots, p);
  DECODE_FINISH(p);
}

void encode(QuiesceDbPeerAck const& ack, bufferlist& bl, uint64_t features = 0)
{
  ENCODE_START(1, 1, bl);
  encode(ack.origin, bl, features);
  encode(ack.diff_map, bl, features);
  ENCODE_FINISH(bl);
}

void decode(QuiesceDbPeerAck& ack, bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  decode(ack.origin, p);
  decode(ack.diff_map, p);
  DECODE_FINISH(p);
}
