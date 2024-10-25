// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "Auth.h"
#include "common/ceph_json.h"
#include "common/Formatter.h"

void EntityAuth::encode(ceph::buffer::list& bl) const {
  __u8 struct_v = 3;
  using ceph::encode;
  encode(struct_v, bl);
  encode((uint64_t)CEPH_AUTH_UID_DEFAULT, bl);
  encode(key, bl);
  encode(caps, bl);
  encode(pending_key, bl);
}

void EntityAuth::decode(ceph::buffer::list::const_iterator& bl) {
  using ceph::decode;
  __u8 struct_v;
  decode(struct_v, bl);
  if (struct_v >= 2) {
    uint64_t old_auid;
    decode(old_auid, bl);
  }
  decode(key, bl);
  decode(caps, bl);
  if (struct_v >= 3) {
    decode(pending_key, bl);
  }
}

void EntityAuth::dump(ceph::Formatter *f) const {
  f->dump_object("key", key);
  encode_json("caps", caps, f);
  f->dump_object("pending_key", pending_key);
}

std::list<EntityAuth> EntityAuth::generate_test_instances() {
  std::list<EntityAuth> ls;
  ls.emplace_back();
  return ls;
}

std::ostream& operator<<(std::ostream& out, const EntityAuth& a)
{
  out << "auth(key=" << a.key;
  if (!a.pending_key.empty()) {
    out << " pending_key=" << a.pending_key;
  }
  out << ")";
  return out;
}

void AuthCapsInfo::encode(ceph::buffer::list& bl) const {
  using ceph::encode;
  __u8 struct_v = 1;
  encode(struct_v, bl);
  __u8 a = (__u8)allow_all;
  encode(a, bl);
  encode(caps, bl);
}

void AuthCapsInfo::decode(ceph::buffer::list::const_iterator& bl) {
  using ceph::decode;
  __u8 struct_v;
  decode(struct_v, bl);
  __u8 a;
  decode(a, bl);
  allow_all = (bool)a;
  decode(caps, bl);
}

void AuthCapsInfo::dump(ceph::Formatter *f) const {
  f->dump_bool("allow_all", allow_all);
  encode_json("caps", caps, f);
  f->dump_unsigned("caps_len", caps.length());
}

std::list<AuthCapsInfo> AuthCapsInfo::generate_test_instances() {
  std::list<AuthCapsInfo> ls;
  ls.emplace_back();
  ls.emplace_back();
  ls.back().allow_all = true;
  ls.emplace_back();
  ls.back().caps.append("foo");
  ls.back().caps.append("bar");
  return ls;
}

void AuthTicket::encode(ceph::buffer::list& bl) const {
  using ceph::encode;
  __u8 struct_v = 2;
  encode(struct_v, bl);
  encode(name, bl);
  encode(global_id, bl);
  encode((uint64_t)CEPH_AUTH_UID_DEFAULT, bl);
  encode(created, bl);
  encode(expires, bl);
  encode(caps, bl);
  encode(flags, bl);
}

void AuthTicket::decode(ceph::buffer::list::const_iterator& bl) {
  using ceph::decode;
  __u8 struct_v;
  decode(struct_v, bl);
  decode(name, bl);
  decode(global_id, bl);
  if (struct_v >= 2) {
    uint64_t old_auid;
    decode(old_auid, bl);
  }
  decode(created, bl);
  decode(expires, bl);
  decode(caps, bl);
  decode(flags, bl);
}

void AuthTicket::dump(ceph::Formatter *f) const {
  f->dump_object("name", name);
  f->dump_unsigned("global_id", global_id);
  f->dump_stream("created") << created;
  f->dump_stream("expires") << expires;
  f->dump_object("caps", caps);
  f->dump_unsigned("flags", flags);
}

std::list<AuthTicket> AuthTicket::generate_test_instances() {
  std::list<AuthTicket> ls;
  ls.emplace_back();
  ls.emplace_back();
  ls.back().name.set_id("client.123");
  ls.back().global_id = 123;
  ls.back().init_timestamps(utime_t(123, 456), 7);
  ls.back().caps.caps.append("foo");
  ls.back().caps.caps.append("bar");
  ls.back().flags = 0x12345678;
  return ls;
}

void ExpiringCryptoKey::dump(ceph::Formatter *f) const {
  f->dump_object("key", key);
  f->dump_stream("expiration") << expiration;
}

std::list<ExpiringCryptoKey> ExpiringCryptoKey::generate_test_instances() {
  std::list<ExpiringCryptoKey> ls;
  ls.emplace_back();
  ls.emplace_back();
  ls.back().key.set_secret(
    CEPH_CRYPTO_AES, bufferptr("1234567890123456", 16), utime_t(123, 456));
  return ls;
}

std::ostream& operator<<(std::ostream& out, const ExpiringCryptoKey& c)
{
  return out << c.key << " expires " << c.expiration;
}

void RotatingSecrets::encode(ceph::buffer::list& bl) const {
  using ceph::encode;
  __u8 struct_v = 1;
  encode(struct_v, bl);
  encode(secrets, bl);
  encode(max_ver, bl);
}

void RotatingSecrets::decode(ceph::buffer::list::const_iterator& bl) {
  using ceph::decode;
  __u8 struct_v;
  decode(struct_v, bl);
  decode(secrets, bl);
  decode(max_ver, bl);
}
  
void RotatingSecrets::dump(ceph::Formatter *f) const {
  encode_json("secrets", secrets, f);
}

std::list<RotatingSecrets> RotatingSecrets::generate_test_instances() {
  std::list<RotatingSecrets> ls;
  ls.emplace_back();
  ls.emplace_back();
  ExpiringCryptoKey eck{};
  ls.back().add(eck);
  return ls;
}
