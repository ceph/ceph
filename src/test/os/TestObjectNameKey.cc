// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: xie xingguo <xie.xingguo@zte.com.cn>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library Public License for more details.
 *
 */

#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "include/compat.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/hobject.h"
#include "osd/osd_types.h"
#include "include/byteorder.h"
#include <gtest/gtest.h>


class TestObjectNameKey : public ::testing::Test {
public:
  TestObjectNameKey() {}
  virtual ~TestObjectNameKey() {}
};

/*
 * Wrappers for fast string to WORD32/64 conversion or vice versa.
 */ 
static void _key_encode_u32(uint32_t u, std::string *key)
{
  uint32_t bu;
#ifdef CEPH_BIG_ENDIAN
  bu = u;
#elif defined(CEPH_LITTLE_ENDIAN)
  bu = swab32(u);
#else
# error wtf
#endif
  key->append((char*)&bu, 4);
}

static const char *_key_decode_u32(const char *key, uint32_t *pu)
{
  uint32_t bu;
  memcpy(&bu, key, 4);
#ifdef CEPH_BIG_ENDIAN
  *pu = bu;
#elif defined(CEPH_LITTLE_ENDIAN)
  *pu = swab32(bu);
#else
# error wtf
#endif
  return key + 4;
}

static void _key_encode_u64(uint64_t u, std::string *key)
{
  uint64_t bu;
#ifdef CEPH_BIG_ENDIAN
  bu = u;
#elif defined(CEPH_LITTLE_ENDIAN)
  bu = swab64(u);
#else
# error wtf
#endif
  key->append((char*)&bu, 8);
}

static const char *_key_decode_u64(const char *key, uint64_t *pu)
{
  uint64_t bu;
  memcpy(&bu, key, 8);
#ifdef CEPH_BIG_ENDIAN
  *pu = bu;
#elif defined(CEPH_LITTLE_ENDIAN)
  *pu = swab64(bu);
#else
# error wtf
#endif
  return key + 8;
}

/*
 * object name key structure
 *
 * 2 chars: shard (-- for none, or hex digit, so that we sort properly)
 * encoded u64: poolid + 2^63 (so that it sorts properly)
 * encoded u32: hash (bit reversed)
 *
 * 1 char: '.'
 *
 * escaped string: namespace
 *
 * 1 char: '<', '=', or '>'.  if =, then object key == object name, and
 *         we are followed just by the key.  otherwise, we are followed 
 *         by the key and then the object name.
 * escaped string: key
 * escaped string: object name (unless '=' above)
 *
 * encoded u64: snap
 * encoded u64: generation
 */

/*
 * string encoding in the key
 *
 * The key string needs to lexicographically sort the same way that
 * ghobject_t does.  We do this by escaping anything <= to '#' with #
 * plus a 2 digit hex string, and anything >= '~' with ~ plus the two
 * hex digits.
 *
 * We use ! as a terminator for strings; this works because it is < #
 * and will get escaped if it is present in the string.
 *
 */

static void append_escaped(const string &in, string *out)
{
  char hexbyte[8];
  for (string::const_iterator i = in.begin(); i != in.end(); ++i) {
    if (*i <= '#') {
      snprintf(hexbyte, sizeof(hexbyte), "#%02x", (unsigned)*i);
      out->append(hexbyte);
    } else if (*i >= '~') {
      snprintf(hexbyte, sizeof(hexbyte), "~%02x", (unsigned)*i);
      out->append(hexbyte);
    } else {
      out->push_back(*i);
    }
  }
  out->push_back('!');
}

static int decode_escaped(const char *p, string *out)
{
  const char *orig_p = p;
  while (*p && *p != '!') {
    if (*p == '#' || *p == '~') {
      unsigned hex;
      int r = sscanf(++p, "%2x", &hex);
      if (r < 1)
        return -EINVAL;
      out->push_back((char)hex);
      p += 2;
    } else {
      out->push_back(*p++);
    }
  }
  return p - orig_p;
}

// some things we encode in binary (as le32 or le64); print the
// resulting key strings nicely
static string pretty_binary_string(const string& in)
{
  char buf[10];
  string out;
  out.reserve(in.length() * 3);
  enum { NONE, HEX, STRING } mode = NONE;
  unsigned from = 0, i;
  for (i=0; i < in.length(); ++i) {
    if ((in[i] < 32 || (unsigned char)in[i] > 126) ||
     (mode == HEX && in.length() - i >= 4 &&
     ((in[i] < 32 || (unsigned char)in[i] > 126) ||
      (in[i+1] < 32 || (unsigned char)in[i+1] > 126) ||
      (in[i+2] < 32 || (unsigned char)in[i+2] > 126) ||
      (in[i+3] < 32 || (unsigned char)in[i+3] > 126)))) {
      if (mode == STRING) {
        out.append(in.substr(from, i - from));
        out.push_back('\'');
      }
      if (mode != HEX) {
        out.append("0x");
        mode = HEX;
      }
      if (in.length() - i >= 4) {
        // print a whole u32 at once
        snprintf(buf, sizeof(buf), "%08x",
          (uint32_t)(((unsigned char)in[i] << 24) |
          ((unsigned char)in[i+1] << 16) |
          ((unsigned char)in[i+2] << 8) |
          ((unsigned char)in[i+3] << 0)));
        i += 3;
      } else {
        snprintf(buf, sizeof(buf), "%02x", (int)(unsigned char)in[i]);
      }
      out.append(buf);
    } else {
      if (mode != STRING) {
        out.push_back('\'');
        mode = STRING;
        from = i;
      }
    }
  }
  if (mode == STRING) {
    out.append(in.substr(from, i - from));
    out.push_back('\'');
  }
  return out;
}

static void _key_encode_shard(shard_id_t shard, string *key)
{
  // make field ordering match with ghobject_t compare operations
  if (shard == shard_id_t::NO_SHARD) {
    // otherwise ff will sort *after* 0, not before.
    key->append("--");
  } else {
    char buf[32];
    snprintf(buf, sizeof(buf), "%02x", (int)shard);
    key->append(buf);
  }
}
static const char *_key_decode_shard(const char *key, shard_id_t *pshard)
{
  if (key[0] == '-') {
    *pshard = shard_id_t::NO_SHARD;
  } else {
    unsigned shard;
    int r = sscanf(key, "%x", &shard);
    if (r < 1)
      return NULL;
    *pshard = shard_id_t(shard);
  }
  return key + 2;
}

static int get_key_object(const string& key, ghobject_t *oid);

static void get_object_key(const ghobject_t& oid, string *key)
{
  key->clear();

  _key_encode_shard(oid.shard_id, key);
  _key_encode_u64(oid.hobj.pool + 0x8000000000000000ull, key);
  _key_encode_u32(oid.hobj.get_bitwise_key_u32(), key);
  key->append(".");

  append_escaped(oid.hobj.nspace, key);

  if (oid.hobj.get_key().length()) {
    // is a key... could be < = or >.
    // (ASCII chars < = and > sort in that order, yay)
    if (oid.hobj.get_key() < oid.hobj.oid.name) {
      key->append("<");
      append_escaped(oid.hobj.get_key(), key);
      append_escaped(oid.hobj.oid.name, key);
    } else if (oid.hobj.get_key() > oid.hobj.oid.name) {
      key->append(">");
      append_escaped(oid.hobj.get_key(), key);
      append_escaped(oid.hobj.oid.name, key);
    } else {
      // same as no key
      key->append("=");
      append_escaped(oid.hobj.oid.name, key);
    }
  } else {
    // no key
    key->append("=");
    append_escaped(oid.hobj.oid.name, key);
  }

  _key_encode_u64(oid.hobj.snap, key);
  _key_encode_u64(oid.generation, key);
  
  // sanity check
  if (true) {
    ghobject_t t;
    int r = get_key_object(*key, &t);
    if (r || t != oid) {
      std::cout << "  r " << r << std::endl;
      std::cout << "key " << pretty_binary_string(*key) << std::endl;
      std::cout << "oid " << oid << std::endl;
      std::cout << "  t " << t << std::endl;
      assert(t == oid);
    }
  }
}

static int get_key_object(const string& key, ghobject_t *oid)
{
  int r;
  const char *p = key.c_str();

  // Note: DO NOT check against null terminator in any intermediate
  // position of key string. That may happen as we directly store
  // the integer fields of gobject into the key string without 
  // performing the necessary visual conversion in advance and 
  // rocksdb has no problem with null characters in the key string.
  p = _key_decode_shard(p, &oid->shard_id);

  uint64_t pool;
  p = _key_decode_u64(p, &pool);
  oid->hobj.pool = pool - 0x8000000000000000ull;

  unsigned hash;
  p = _key_decode_u32(p, &hash);
  oid->hobj.set_bitwise_key_u32(hash);
  if (*p != '.')
    return -1;
  ++p;

  r = decode_escaped(p, &oid->hobj.nspace);
  if (r < 0)
    return -2;
  p += r + 1;

  if (*p == '=') {
    // no key
    ++p;
    r = decode_escaped(p, &oid->hobj.oid.name);
    if (r < 0)
      return -3;
    p += r + 1;
  } else if (*p == '<' || *p == '>') {
    // key + name
    ++p;
    string okey;
    r = decode_escaped(p, &okey);
    if (r < 0)
      return -4;
    p += r + 1;
    r = decode_escaped(p, &oid->hobj.oid.name);
    if (r < 0)
      return -5;
    p += r + 1;
    oid->hobj.set_key(okey);
  } else {
    // malformed
    return -6;
  }

  p = _key_decode_u64(p, &oid->hobj.snap.val);
  p = _key_decode_u64(p, &oid->generation);
  if (*p) {
    // there shall be no more contents, for now.
    return -7;
  }  

  return 0;
}

TEST_F(TestObjectNameKey, SanityCheck) {

  // basic
  {
    ghobject_t hoid(hobject_t(sobject_t("ABC", CEPH_NOSNAP)));
    std::string key;
    get_object_key(hoid, &key);
    ghobject_t temp;
    ASSERT_EQ(0, get_key_object(key, &temp));
  } 

  // no key 
  {
    uint64_t hash = 0x342908F;
    uint64_t pool = 1;
    int64_t gen = 0xefefefefef;
    shard_id_t shard_id(255);
    std::string name("ABC");
    ghobject_t hoid(hobject_t(object_t(name), "", 1, hash, pool, ""));
    hoid.hobj.nspace = "NSPACE";
    hoid.generation = gen;
    std::string key;
    get_object_key(hoid, &key);
    ghobject_t temp;
    ASSERT_EQ(0, get_key_object(key, &temp));
  } 
  
  // no pool
  {
    uint64_t hash = -1;
    uint64_t pool = 0;
    int64_t gen = 0xefefefefef;
    shard_id_t shard_id(0xb);
    std::string name("123");
    ghobject_t hoid(hobject_t(object_t(name), "", 1, hash, pool, ""));
    hoid.hobj.nspace = "NSPACE";
    hoid.generation = gen;
    std::string key;
    get_object_key(hoid, &key);
    ghobject_t temp;
    ASSERT_EQ(0, get_key_object(key, &temp));
  } 

  const std::string key("KEY");
  
  // no shard
  {
    uint64_t hash = 0x123400AB;
    uint64_t pool = 0xAB;
    int64_t gen = 0;
    shard_id_t shard_id(shard_id_t::NO_SHARD);
    std::string name("123");
    ghobject_t hoid(hobject_t(object_t(name), key, 1, hash, pool, ""));
    hoid.hobj.nspace = "NSPACE";
    hoid.generation = gen;
    std::string key;
    get_object_key(hoid, &key);
    ghobject_t temp;
    ASSERT_EQ(0, get_key_object(key, &temp));
  }
  
  // no hash 
  {
    uint64_t hash = 0;
    uint64_t pool = 0xCDCDCDCD;
    int64_t gen = 0xefefefefef;
    shard_id_t shard_id(100);
    std::string name("123");
    ghobject_t hoid(hobject_t(object_t(name), key, 1, hash, pool, ""));
    hoid.hobj.nspace = "AX32%#";
    hoid.generation = gen;
    std::string key;
    get_object_key(hoid, &key);
    ghobject_t temp;
    ASSERT_EQ(0, get_key_object(key, &temp));
  } 

  // no snap 
  {
    uint64_t hash = 0xAB;
    uint64_t pool = -1;
    int64_t gen = 0xefefefefef;
    shard_id_t shard_id(0xb);
    std::string name(".XA/B_\\C.D");
    ghobject_t hoid(hobject_t(object_t(name), key, CEPH_NOSNAP, hash, pool, ""));
    hoid.hobj.nspace = "NSPACE";
    hoid.generation = gen;
    std::string key;
    get_object_key(hoid, &key);
    ghobject_t temp;
    ASSERT_EQ(0, get_key_object(key, &temp));
  }  

   // no gen 
  {
    uint64_t hash = 0x12345678;
    uint64_t pool = 0xAB;
    int64_t gen = 0;
    shard_id_t shard_id(123);
    std::string name(1024, 'A'); // long name
    ghobject_t hoid(hobject_t(object_t(name), key, 1, hash, pool, ""));
    hoid.hobj.nspace = "NSPACE";
    hoid.generation = gen;
    std::string key;
    get_object_key(hoid, &key);
    ghobject_t temp;
    ASSERT_EQ(0, get_key_object(key, &temp));
  } 
}

/* 
 * Local Variables:
 * compile-command: "cd ../.. ; 
 *   make unittest_object_name_key && 
 *   valgrind --tool=memcheck ./unittest_object_name_key \
 *   # --gtest_filter=TestObjectNameKey.* --log-to-stderr=true --debug-filestore=20"
 * End:
 */
