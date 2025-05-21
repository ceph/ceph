// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Casey Bodley <cbodley@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "os/ObjectStore.h"
#include <gtest/gtest.h>
#include "common/Clock.h"
#include "include/utime.h"
#include <boost/tuple/tuple.hpp>

using namespace std;

TEST(Transaction, MoveConstruct)
{
  auto a = ObjectStore::Transaction{};
  a.nop();
  ASSERT_FALSE(a.empty());

  // move-construct in b
  auto b = std::move(a);
  ASSERT_TRUE(a.empty());
  ASSERT_FALSE(b.empty());
}

TEST(Transaction, MoveAssign)
{
  auto a = ObjectStore::Transaction{};
  a.nop();
  ASSERT_FALSE(a.empty());

  auto b = ObjectStore::Transaction{};
  b = std::move(a); // move-assign to b
  ASSERT_TRUE(a.empty());
  ASSERT_FALSE(b.empty());
}

TEST(Transaction, CopyConstruct)
{
  auto a = ObjectStore::Transaction{};
  a.nop();
  ASSERT_FALSE(a.empty());

  auto b = a; // copy-construct in b
  ASSERT_FALSE(a.empty());
  ASSERT_FALSE(b.empty());
}

TEST(Transaction, CopyAssign)
{
  auto a = ObjectStore::Transaction{};
  a.nop();
  ASSERT_FALSE(a.empty());

  auto b = ObjectStore::Transaction{};
  b = a; // copy-assign to b
  ASSERT_FALSE(a.empty());
  ASSERT_FALSE(b.empty());
}

TEST(Transaction, Swap)
{
  auto a = ObjectStore::Transaction{};
  a.nop();
  ASSERT_FALSE(a.empty());

  auto b = ObjectStore::Transaction{};
  std::swap(a, b); // swap a and b
  ASSERT_TRUE(a.empty());
  ASSERT_FALSE(b.empty());
}

ObjectStore::Transaction generate_transaction()
{
  auto a = ObjectStore::Transaction{};
  a.nop();

  coll_t cid;
  object_t obj("test_name");
  snapid_t snap(0);
  hobject_t hoid(obj, "key", snap, 0, 0, "nspace");
  ghobject_t oid(hoid);

  coll_t acid;
  object_t aobj("another_test_name");
  hobject_t ahoid(obj, "another_key", snap, 0, 0, "another_nspace");
  ghobject_t aoid(hoid);
  std::set<string> keys;
  keys.insert("any_1");
  keys.insert("any_2");
  keys.insert("any_3");

  bufferlist bl;
  bl.append_zero(4096);

  a.write(cid, oid, 1, 4096, bl, 0);

  a.omap_setkeys(acid, aoid, bl);

  a.omap_rmkeys(cid, aoid, keys);

  a.touch(acid, oid);

  return a;
}

TEST(Transaction, MoveRangesDelSrcObj)
{
  auto t = ObjectStore::Transaction{};
  t.nop();

  coll_t c(spg_t(pg_t(1,2), shard_id_t::NO_SHARD));

  ghobject_t o1(hobject_t("obj", "", 123, 456, -1, ""));
  ghobject_t o2(hobject_t("obj2", "", 123, 456, -1, ""));
  vector<std::pair<uint64_t, uint64_t>> move_info = {
    make_pair(1, 5),
    make_pair(10, 5)
  };

  t.touch(c, o1);
  bufferlist bl;
  bl.append("some data");
  t.write(c, o1, 1, bl.length(), bl);
  t.write(c, o1, 10, bl.length(), bl);

  t.clone(c, o1, o2);
  bl.append("some other data");
  t.write(c, o2, 1, bl.length(), bl);
}

TEST(Transaction, GetNumBytes)
{
  auto a = ObjectStore::Transaction{};
  a.nop();
  ASSERT_TRUE(a.get_encoded_bytes() == a.get_encoded_bytes_test());

  coll_t cid;
  object_t obj("test_name");
  snapid_t snap(0);
  hobject_t hoid(obj, "key", snap, 0, 0, "nspace");
  ghobject_t oid(hoid);

  coll_t acid;
  object_t aobj("another_test_name");
  hobject_t ahoid(obj, "another_key", snap, 0, 0, "another_nspace");
  ghobject_t aoid(hoid);
  std::set<string> keys;
  keys.insert("any_1");
  keys.insert("any_2");
  keys.insert("any_3");

  bufferlist bl;
  bl.append_zero(4096);

  a.write(cid, oid, 1, 4096, bl, 0);
  ASSERT_TRUE(a.get_encoded_bytes() == a.get_encoded_bytes_test());

  a.omap_setkeys(acid, aoid, bl);
  ASSERT_TRUE(a.get_encoded_bytes() == a.get_encoded_bytes_test());

  a.omap_rmkeys(cid, aoid, keys);
  ASSERT_TRUE(a.get_encoded_bytes() == a.get_encoded_bytes_test());

  a.touch(acid, oid);
  ASSERT_TRUE(a.get_encoded_bytes() == a.get_encoded_bytes_test());
}

void bench_num_bytes(bool legacy)
{
  const int max = 2500000;
  auto a = generate_transaction();

  if (legacy) {
    cout << "get_encoded_bytes_test: ";
  } else {
    cout << "get_encoded_bytes: ";
  }

  utime_t start = ceph_clock_now();
  if (legacy) {
    for (int i = 0; i < max; ++i) {
      a.get_encoded_bytes_test();
    }
  } else {
    for (int i = 0; i < max; ++i) {
      a.get_encoded_bytes();
    }
  }

  utime_t end = ceph_clock_now();
  cout << max << " encodes in " << (end - start) << std::endl;

}

TEST(Transaction, GetNumBytesBenchLegacy)
{
   bench_num_bytes(true);
}

TEST(Transaction, GetNumBytesBenchCurrent)
{
   bench_num_bytes(false);
}

/**
 * create_pattern
 *
 * Fill bufferlist generating data from a seed value
 */
void create_pattern(bufferlist& bl, unsigned int seed, unsigned int length)
{
  ASSERT_TRUE(length % sizeof(int) == 0);
  for (unsigned int i = 0; i < length / sizeof(int); i++) {
    encode(seed, bl);
    seed = (seed << 1) ^ seed;
  }
}

/**
 * check_pattern
 *
 * Validate bufferlist contents matches seed value
 */
void check_pattern(bufferlist& bl, unsigned int seed, unsigned int length)
{
  ceph::buffer::list::const_iterator p;
  ASSERT_TRUE(length % sizeof(int) == 0);
  p = bl.cbegin();
  for (unsigned int i = 0; i < length / sizeof(int); i++) {
    unsigned int j;
    decode(j, p);
    ASSERT_TRUE(j == seed);
    seed = (seed << 1) ^ seed;
  }
}
void check_pattern(bufferptr& bptr, unsigned int seed, unsigned int length)
{
  bufferlist bl;
  bl.append(bptr);
  check_pattern(bl, seed, length);
}

/**
 * create_check_transaction1
 *
 * Construct/validate an instance of every type of Op in an ObjectStore:Transaction
 */
void create_check_transaction1(ObjectStore::Transaction& t_in, bool create, bool append)
{
  coll_t c1 = coll_t(spg_t(pg_t(0,111), shard_id_t::NO_SHARD));
  coll_t c2 = coll_t(spg_t(pg_t(0,111), shard_id_t::NO_SHARD));
  ghobject_t o1 = ghobject_t(hobject_t(sobject_t("testobject1", CEPH_NOSNAP)));
  ghobject_t o2 = ghobject_t(hobject_t(sobject_t("testobject2", CEPH_NOSNAP)));
  bufferlist bl1;
  bufferlist bl2;

  const unsigned int bl1_seed = 1234;
  const unsigned int bl1_len = 1024;
  const unsigned int bl2_seed = 6666;
  const unsigned int bl2_len = 128;

  create_pattern(bl1, bl1_seed, bl1_len);
  create_pattern(bl2, bl2_seed, bl2_len);

  ObjectStore::Transaction::iterator i = t_in.begin();
  ObjectStore::Transaction::Op *op = nullptr;

  bool done = false;
  for (int pos = 0; !done ; ++pos) {
    ObjectStore::Transaction t_append;
    ObjectStore::Transaction& t = append ? t_append : t_in;
    if (!create) {
      ASSERT_TRUE(i.have_op());
      op = i.decode_op();
      cout << " Checking pos " << pos << " op " << op->op << std::endl;
    }
    switch (pos) {
    case 0:
      // NOP
      if (create) {
        t.nop();
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_NOP);
      }
      break;
    case 1:
      // CREATE
      if (create) {
        t.create(c1, o1);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_CREATE);
        ASSERT_TRUE(c1 == i.get_cid(op->cid));
	ASSERT_TRUE(o1 == i.get_oid(op->oid));
      }
      break;
    case 2:
      // TOUCH
      if (create) {
        t.touch(c2, o2);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_TOUCH);
        ASSERT_TRUE(c2 == i.get_cid(op->cid));
	ASSERT_TRUE(o2 == i.get_oid(op->oid));
      }
      break;
    case 3:
      // WRITE
      if (create) {
        t.write(c1, o1, 0, bl1_len, bl1);
      }else{
        bufferlist bl;
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_WRITE);
        ASSERT_TRUE(c1 == i.get_cid(op->cid));
	ASSERT_TRUE(o1 == i.get_oid(op->oid));
        ASSERT_TRUE(op->off == 0);
        ASSERT_TRUE(op->len == 1024);
        i.decode_bl(bl);
	ASSERT_TRUE(bl.length() == op->len);
        check_pattern(bl, bl1_seed, bl1_len);
      }
      break;
    case 4:
      // ZERO
      if (create) {
        t.zero(c2, o2, 1111, 2222);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_ZERO);
        ASSERT_TRUE(c2 == i.get_cid(op->cid));
	ASSERT_TRUE(o2 == i.get_oid(op->oid));
        ASSERT_TRUE(op->off == 1111);
        ASSERT_TRUE(op->len == 2222);
      }
      break;
    case 5:
      // TRUNCATE
      if (create) {
        t.truncate(c1, o1, 3333);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_TRUNCATE);
        ASSERT_TRUE(c1 == i.get_cid(op->cid));
	ASSERT_TRUE(o1 == i.get_oid(op->oid));
        ASSERT_TRUE(op->off = 3333);
      }
      break;
    case 6:
      // REMOVE
      if (create) {
        t.remove(c2, o2);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_REMOVE);
        ASSERT_TRUE(c2 == i.get_cid(op->cid));
	ASSERT_TRUE(o2 == i.get_oid(op->oid));
      }
      break;
    case 7:
      // SETATTR (1)
      if (create) {
        t.setattr(c1, o1, "attr1", bl2);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_SETATTR);
        ASSERT_TRUE(c1 == i.get_cid(op->cid));
	ASSERT_TRUE(o1 == i.get_oid(op->oid));
        string name = i.decode_string();
        bufferlist bl;
        i.decode_bl(bl);
        ASSERT_TRUE(name == "attr1");
	check_pattern(bl, bl2_seed, bl2_len);
      }
      break;
    case 8:
      // SETATTR (2)
      if (create) {
        t.setattr(c2, o2, std::string("attr2"), bl2);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_SETATTR);
        ASSERT_TRUE(c2 == i.get_cid(op->cid));
	ASSERT_TRUE(o2 == i.get_oid(op->oid));
        string name = i.decode_string();
        bufferlist bl;
        i.decode_bl(bl);
        ASSERT_TRUE(name == "attr2");
	check_pattern(bl, bl2_seed, bl2_len);
      }
      break;
    case 9:
      // SETATTRS (1)
      if (create) {
        map<string,bufferptr,less<>> m;
        m["a"] = buffer::copy("this", 4);
        m["b"] = buffer::copy("that", 4);
        t.setattrs(c1, o1, m);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_SETATTRS);
        ASSERT_TRUE(c1 == i.get_cid(op->cid));
	ASSERT_TRUE(o1 == i.get_oid(op->oid));
        map<string, bufferptr> aset;
        i.decode_attrset(aset);
	ASSERT_TRUE(aset.size()==2);
	auto a = aset.find("a");
	ASSERT_TRUE(a != aset.end());
	ASSERT_TRUE(!a->second.cmp(buffer::copy("this",4)));
	auto b = aset.find("b");
	ASSERT_TRUE(b != aset.end());
	ASSERT_TRUE(!b->second.cmp(buffer::copy("that",4)));
      }
      break;
    case 10:
      // SETATTRS (2)
      if (create) {
        map<string,bufferlist,less<>> m;
        bufferlist bflip, bflop;
        bflip.append(buffer::copy("flip",4));
        bflop.append(buffer::copy("flop",4));
        m["a"] = bflip;
        m["b"] = bflop;
        t.setattrs(c2, o2, m);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_SETATTRS);
        ASSERT_TRUE(c2 == i.get_cid(op->cid));
	ASSERT_TRUE(o2 == i.get_oid(op->oid));
        map<string, bufferptr> aset;
        i.decode_attrset(aset);
	ASSERT_TRUE(aset.size()==2);
	auto a = aset.find("a");
	ASSERT_TRUE(a != aset.end());
	ASSERT_TRUE(!a->second.cmp(buffer::copy("flip",4)));
	auto b = aset.find("b");
	ASSERT_TRUE(b != aset.end());
	ASSERT_TRUE(!b->second.cmp(buffer::copy("flop",4)));
      }
      break;
    case 11:
      // RMATTR (1)
      if (create) {
        t.rmattr(c1, o1, "attr3");
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_RMATTR);
        ASSERT_TRUE(c1 == i.get_cid(op->cid));
	ASSERT_TRUE(o1 == i.get_oid(op->oid));
        string name = i.decode_string();
        ASSERT_TRUE(name == "attr3");
      }
      break;
    case 12:
      // RMATTR (2)
      if (create) {
        t.rmattr(c2, o2, std::string("attr4"));
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_RMATTR);
        ASSERT_TRUE(c2 == i.get_cid(op->cid));
	ASSERT_TRUE(o2 == i.get_oid(op->oid));
        string name = i.decode_string();
        ASSERT_TRUE(name == "attr4");
      }
      break;
    case 13:
      // RMATTRS
      if (create) {
        t.rmattrs(c1, o1);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_RMATTRS);
        ASSERT_TRUE(c1 == i.get_cid(op->cid));
	ASSERT_TRUE(o1 == i.get_oid(op->oid));
      }
      break;
    case 14:
      // CLONE
      if (create) {
        t.clone(c2, o2, o1);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_CLONE);
        ASSERT_TRUE(c2 == i.get_cid(op->cid));
	ASSERT_TRUE(o2 == i.get_oid(op->oid));
	ASSERT_TRUE(o1 == i.get_oid(op->dest_oid));
      }
      break;
    case 15:
      // CLONERANGE2
      if (create) {
        t.clone_range(c1, o2, o1, 4444, 5555, 6666);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_CLONERANGE2);
        ASSERT_TRUE(c1 == i.get_cid(op->cid));
	ASSERT_TRUE(o2 == i.get_oid(op->oid));
	ASSERT_TRUE(o1 == i.get_oid(op->dest_oid));
        ASSERT_TRUE(op->off == 4444);
        ASSERT_TRUE(op->len == 5555);
        ASSERT_TRUE(op->dest_off == 6666);
      }
      break;
    case 16:
      // MKCOLL
      if (create) {
        t.create_collection(c2, 7777);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_MKCOLL);
        ASSERT_TRUE(c2 == i.get_cid(op->cid));
        ASSERT_TRUE(op->split_bits == 7777);
      }
      break;
    case 17:
      // COLL_HINT
      if (create) {
        t.collection_hint(c1, 8888, bl2);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_COLL_HINT);
        ASSERT_TRUE(c1 == i.get_cid(op->cid));
        ASSERT_TRUE(op->hint == 8888);
        bufferlist bl;
        i.decode_bl(bl);
	check_pattern(bl, bl2_seed, bl2_len);
      }
      break;
    case 18:
      // RMCOLL
      if (create) {
        t.remove_collection(c1);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_RMCOLL);
        ASSERT_TRUE(c1 == i.get_cid(op->cid));
      }
      break;
    case 19:
      // COLL_MOVE_RENAME
      if (create) {
        t.collection_move_rename(c2, o2, c1, o1);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_COLL_MOVE_RENAME);
        ASSERT_TRUE(c2 == i.get_cid(op->cid));
	ASSERT_TRUE(o2 == i.get_oid(op->oid));
        ASSERT_TRUE(c1 == i.get_cid(op->dest_cid));
	ASSERT_TRUE(o1 == i.get_oid(op->dest_oid));
      }
      break;
    case 20:
      // TRY_RENAME
      if (create) {
        t.try_rename(c2, o2, o1);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_TRY_RENAME);
        ASSERT_TRUE(c2 == i.get_cid(op->cid));
	ASSERT_TRUE(o2 == i.get_oid(op->oid));
	ASSERT_TRUE(o1 == i.get_oid(op->dest_oid));
      }
      break;
    case 21:
      // OMAP_CLEAR
      if (create) {
        t.omap_clear(c1, o1);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_OMAP_CLEAR);
        ASSERT_TRUE(c1 == i.get_cid(op->cid));
	ASSERT_TRUE(o1 == i.get_oid(op->oid));
      }
      break;
    case 22:
      // OMAP_SETKEYS (1)
      if (create) {
        map<string,bufferlist> m;
        bufferlist bthis, bthat;
        bthis.append(buffer::copy("this",4));
        bthat.append(buffer::copy("that",4));
        m["a"] = bthis;
        m["b"] = bthat;
        t.omap_setkeys(c2, o2, m);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_OMAP_SETKEYS);
        ASSERT_TRUE(c2 == i.get_cid(op->cid));
	ASSERT_TRUE(o2 == i.get_oid(op->oid));
	map<string,bufferlist> aset;
        i.decode_attrset(aset);
	ASSERT_TRUE(aset.size()==2);
	auto a = aset.find("a");
	ASSERT_TRUE(a != aset.end());
	ASSERT_TRUE(a->second.contents_equal("this",4));
	auto b = aset.find("b");
	ASSERT_TRUE(b != aset.end());
	ASSERT_TRUE(b->second.contents_equal("that",4));
      }
      break;
    case 23:
      // OMAP_SETKEYS (2)
      if (create) {
        map<string,bufferlist> m;
        bufferlist bthis, bthat;
        bthis.append(buffer::copy("this",4));
        bthat.append(buffer::copy("that",4));
        m["a"] = bthis;
        m["b"] = bthat;
        bufferlist bkeys;
        encode(m,bkeys);
        t.omap_setkeys(c1, o1, bkeys);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_OMAP_SETKEYS);
        ASSERT_TRUE(c1 == i.get_cid(op->cid));
	ASSERT_TRUE(o1 == i.get_oid(op->oid));
	map<string,bufferlist> aset;
        i.decode_attrset(aset);
	ASSERT_TRUE(aset.size()==2);
	auto a = aset.find("a");
	ASSERT_TRUE(a != aset.end());
	ASSERT_TRUE(a->second.contents_equal("this",4));
	auto b = aset.find("b");
	ASSERT_TRUE(b != aset.end());
	ASSERT_TRUE(b->second.contents_equal("that",4));
      }
      break;
    case 24:
      // OMAP_RMKEYS (1)
      if (create) {
        std::set<std::string> keys;
        keys.insert(std::string("attr7"));
        t.omap_rmkeys(c2, o2, keys);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_OMAP_RMKEYS);
        ASSERT_TRUE(c2 == i.get_cid(op->cid));
	ASSERT_TRUE(o2 == i.get_oid(op->oid));
	bufferlist keys_bl;
	i.decode_keyset_bl(&keys_bl);
        std::set<std::string> keys;
	decode(keys,keys_bl);
	ASSERT_TRUE(keys.size() == 1);
	for(auto& str: keys) {
	  ASSERT_TRUE(str == "attr7");
	}
      }
      break;
    case 25:
      // OMAP_RMKEYS (2)
      if (create) {
        t.omap_rmkey(c1, o1, std::string("attr8"));
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_OMAP_RMKEYS);
        ASSERT_TRUE(c1 == i.get_cid(op->cid));
	ASSERT_TRUE(o1 == i.get_oid(op->oid));
	bufferlist keys_bl;
	i.decode_keyset_bl(&keys_bl);
        std::set<std::string> keys;
	decode(keys,keys_bl);
	ASSERT_TRUE(keys.size() == 1);
	for(auto& str: keys) {
	  ASSERT_TRUE(str == "attr8");
	}
      }
      break;
    case 26:
      // OMAP_RMKEYS (3)
      if (create) {
        std::set<std::string> keys;
        keys.insert(std::string("attr9"));
        bufferlist bkeys;
        encode(keys,bkeys);
        t.omap_rmkeys(c2, o2, bkeys);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_OMAP_RMKEYS);
        ASSERT_TRUE(c2 == i.get_cid(op->cid));
	ASSERT_TRUE(o2 == i.get_oid(op->oid));
	bufferlist keys_bl;
	i.decode_keyset_bl(&keys_bl);
        std::set<std::string> keys;
	decode(keys,keys_bl);
	ASSERT_TRUE(keys.size() == 1);
	for(auto& str: keys) {
	  ASSERT_TRUE(str == "attr9");
	}
      }
      break;
    case 27:
      // OMAP_RMKEYRANGE (1)
      if (create) {
        t.omap_rmkeyrange(c1, o1, std::string("attr1"), std::string("attr2"));
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_OMAP_RMKEYRANGE);
        ASSERT_TRUE(c1 == i.get_cid(op->cid));
	ASSERT_TRUE(o1 == i.get_oid(op->oid));
        string first, last;
        first = i.decode_string();
        last = i.decode_string();
        ASSERT_TRUE(first == "attr1");
        ASSERT_TRUE(last == "attr2");
      }
      break;
    case 28:
      // OMAP_RMKEYRANGE (2)
      if (create) {
        bufferlist brange;
        encode(std::string("attr3"),brange);
        encode(std::string("attr4"),brange);
        t.omap_rmkeyrange(c2, o2, brange);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_OMAP_RMKEYRANGE);
        ASSERT_TRUE(c2 == i.get_cid(op->cid));
	ASSERT_TRUE(o2 == i.get_oid(op->oid));
        string first, last;
        first = i.decode_string();
        last = i.decode_string();
        ASSERT_TRUE(first == "attr3");
        ASSERT_TRUE(last == "attr4");
      }
      break;
    case 29:
      // OMAP_SETHEADER
      if (create) {
        t.omap_setheader(c1, o1, bl1);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_OMAP_SETHEADER);
        ASSERT_TRUE(c1 == i.get_cid(op->cid));
	ASSERT_TRUE(o1 == i.get_oid(op->oid));
      }
      break;
    case 30:
      // SPLIT_COLLECTION
      if (create) {
        t.split_collection(c2, 9999, 1000, c1);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_SPLIT_COLLECTION2);
        ASSERT_TRUE(c2 == i.get_cid(op->cid));
        ASSERT_TRUE(c1 == i.get_cid(op->dest_cid));
        ASSERT_TRUE(op->split_bits == 9999);
        ASSERT_TRUE(op->split_rem == 1000);
      }
      break;
    case 31:
      // MERGE_COLLECTION
      if (create) {
        t.merge_collection(c2, c1, 1100);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_MERGE_COLLECTION);
        ASSERT_TRUE(c2 == i.get_cid(op->cid));
        ASSERT_TRUE(c1 == i.get_cid(op->dest_cid));
        ASSERT_TRUE(op->split_bits == 1100);
      }
      break;
    case 32:
      // SET_BITS
      if (create) {
        t.collection_set_bits(c2, 1200);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_COLL_SET_BITS);
        ASSERT_TRUE(c2 == i.get_cid(op->cid));
        ASSERT_TRUE(op->split_bits == 1200);
      }
      break;
    case 33:
      // SET_ALLOCHINT
      if (create) {
        t.set_alloc_hint(c1, o1, 1300, 1400, 1500);
      }else{
        ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_SETALLOCHINT);
        ASSERT_TRUE(c1 == i.get_cid(op->cid));
	ASSERT_TRUE(o1 == i.get_oid(op->oid));
        ASSERT_TRUE(op->expected_object_size == 1300);
        ASSERT_TRUE(op->expected_write_size == 1400);
        ASSERT_TRUE(op->hint == 1500);
      }
      // End of cases
      done = true;
      break;
    }
    if (append) {
      t_in.append(t_append);
    }
  }
  if (!create) {
    ASSERT_TRUE(!i.have_op());
  }
}

void create_transaction1(ObjectStore::Transaction& t)
{
  create_check_transaction1(t, true, false);
}

void create_transaction1_using_append(ObjectStore::Transaction& t)
{
  create_check_transaction1(t, true, true);
}

void check_content_transaction1(ObjectStore::Transaction& t)
{
  create_check_transaction1(t, false, false);
}

TEST(Transaction, EncodeDecode)
{
  ObjectStore::Transaction source;
  bufferlist encoded1;
  bufferlist encoded2p;
  bufferlist encoded2d;
  ObjectStore::Transaction decode1;
  ObjectStore::Transaction decode2;
  ceph::buffer::list::const_iterator p;
  ceph::buffer::list::const_iterator d;

  cout << "Creating transaction1" << std::endl;
  create_transaction1(source);
  cout << "Checking transaction1" << std::endl;
  check_content_transaction1(source);

  encode(source, encoded1);
  p = encoded1.cbegin();
  decode(decode1, p);
  cout << "Checking encoded/decoded with 1 buffer transaction1" << std::endl;
  check_content_transaction1(decode1);
  source.encode(encoded2p, encoded2d, CEPH_FEATUREMASK_SERVER_TENTACLE);
  p = encoded2p.cbegin();
  d = encoded2d.cbegin();
  decode2.decode(p, d);
  cout << "Checking encoded/decoded with 2 buffers transaction1" << std::endl;
  check_content_transaction1(decode2);
}

/**
 * create_check_transaction2
 *
 * Construct/validate write Ops with different lengths and alignements
 * in an ObejctStore::Transaction
 */
void create_check_transaction2(ObjectStore::Transaction& t,bool create)
{
  coll_t c = coll_t();
  ghobject_t o1 = ghobject_t(hobject_t(sobject_t("testobject", CEPH_NOSNAP)));

  bufferlist blshort;
  bufferlist blpage;
  bufferlist blfewpages;
  bufferlist bllong;

  // Less than 1 page
  const unsigned int blshort_seed = 4567;
  const unsigned int blshort_len = CEPH_PAGE_SIZE/4;
  // 1 page
  const unsigned int blpage_seed = 7654;
  const unsigned int blpage_len = CEPH_PAGE_SIZE;
  // Whole number of pages
  const unsigned int blfewpages_seed = 1357;
  const unsigned int blfewpages_len = CEPH_PAGE_SIZE*3;
  // 1.5 pages
  const unsigned int bllong_seed = 9876;
  const unsigned int bllong_len = (CEPH_PAGE_SIZE*3)/2;

  create_pattern(blshort,blshort_seed,blshort_len);
  create_pattern(blpage,blpage_seed,blpage_len);
  create_pattern(blfewpages,blfewpages_seed,blfewpages_len);
  create_pattern(bllong,bllong_seed,bllong_len);

  ObjectStore::Transaction::iterator i = t.begin();
  ObjectStore::Transaction::Op *op = nullptr;

  bool done = false;
  for (int pos = 0; !done ; ++pos) {
    bufferlist bl;
    if (!create) {
      ASSERT_TRUE(i.have_op());
      op = i.decode_op();
      cout << " Checking pos " << pos << " off " << op->off << " len " << op->len << std::endl;
      ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_WRITE);
      i.decode_bl(bl);
      ASSERT_TRUE(bl.length() == op->len);
    }
    switch (pos) {
    case 0:
      // Short buffer, PAGE aligned offset
      if (create) {
        t.write(c, o1, 0, blshort_len, blshort);
      }else{
        ASSERT_TRUE(op->off == 0);
        ASSERT_TRUE(op->len == blshort_len);
        check_pattern(bl, blshort_seed, blshort_len);
      }
      break;
    case 1:
      // Short buffer, straddle PAGE offset
      if (create) {
        t.write(c, o1, CEPH_PAGE_SIZE-10, blshort_len, blshort);
      }else{
        ASSERT_TRUE(op->off == CEPH_PAGE_SIZE-10);
        ASSERT_TRUE(op->len == blshort_len);
        check_pattern(bl, blshort_seed, blshort_len);
      }
      break;
    case 2:
      // Short buffer, end of buffer PAGE aligned
      if (create) {
        t.write(c, o1, CEPH_PAGE_SIZE-blshort_len, blshort_len, blshort);
      }else{
        ASSERT_TRUE(op->off == CEPH_PAGE_SIZE-blshort_len);
        ASSERT_TRUE(op->len == blshort_len);
        check_pattern(bl, blshort_seed, blshort_len);
      }
      break;
    case 3:
      // Page buffer, PAGE aligned offset
      if (create) {
        t.write(c, o1, CEPH_PAGE_SIZE*3, blpage_len, blpage);
      }else{
        ASSERT_TRUE(op->off == CEPH_PAGE_SIZE*3);
        ASSERT_TRUE(op->len == blpage_len);
        check_pattern(bl, blpage_seed, blpage_len);
      }
      break;
    case 4:
      // Page buffer, misaligned offset
      if (create) {
        t.write(c, o1, CEPH_PAGE_SIZE-10, blpage_len, blpage);
      }else{
        ASSERT_TRUE(op->off == CEPH_PAGE_SIZE-10);
        ASSERT_TRUE(op->len == blpage_len);
        check_pattern(bl, blpage_seed, blpage_len);
      }
      break;
    case 5:
      // Multiple page buffer, PAGE aligned offset
      if (create) {
        t.write(c, o1, CEPH_PAGE_SIZE*7, blfewpages_len, blfewpages);
      }else{
        ASSERT_TRUE(op->off == CEPH_PAGE_SIZE*7);
        ASSERT_TRUE(op->len == blfewpages_len);
        check_pattern(bl, blfewpages_seed, blfewpages_len);
      }
      break;
    case 6:
      // Multiple page buffer, misaligned offset
      if (create) {
        t.write(c, o1, CEPH_PAGE_SIZE-10, blfewpages_len, blfewpages);
      }else{
        ASSERT_TRUE(op->off == CEPH_PAGE_SIZE-10);
        ASSERT_TRUE(op->len == blfewpages_len);
        check_pattern(bl, blfewpages_seed, blfewpages_len);
      }
      break;
    case 7:
      // Long buffer, PAGE aligned offset
      if (create) {
        t.write(c, o1, 0, bllong_len, bllong);
      }else{
        ASSERT_TRUE(op->off == 0);
        ASSERT_TRUE(op->len == bllong_len);
        check_pattern(bl, bllong_seed, bllong_len);
      }
      break;
    case 8:
      // Long buffer, misaligned offset
      if (create) {
        t.write(c, o1, CEPH_PAGE_SIZE-10, bllong_len, bllong);
      }else{
        ASSERT_TRUE(op->off == CEPH_PAGE_SIZE-10);
        ASSERT_TRUE(op->len == bllong_len);
        check_pattern(bl, bllong_seed, bllong_len);
      }
      break;
    case 9:
      // Long buffer, end of buffer PAGE aligned
      if (create) {
        t.write(c, o1, 100*CEPH_PAGE_SIZE-bllong_len, bllong_len, bllong);
      }else{
        ASSERT_TRUE(op->off == 100*CEPH_PAGE_SIZE-bllong_len);
        ASSERT_TRUE(op->len == bllong_len);
        check_pattern(bl, bllong_seed, bllong_len);
      }
      // Last op
      done = true;
      break;
    }
  }
  if (!create) {
    ASSERT_TRUE(!i.have_op());
  }
}

void create_transaction2(ObjectStore::Transaction& t)
{
  create_check_transaction2(t, true);
}

void check_content_transaction2(ObjectStore::Transaction& t)
{
  create_check_transaction2(t, false);
}

/**
 * check_alignment_transaction2
 *
 * Validate alignment of write Op data buffers in an ObjectStore::Transaction
 */
void check_alignment_transaction2(ObjectStore::Transaction& t)
{
  ObjectStore::Transaction::iterator i = t.begin();
  unsigned int longest_aligned = 0;
  unsigned int longest_misaligned = 0;
  unsigned int quantity_aligned = 0;
  unsigned int quantity_misaligned = 0;
  while (i.have_op()) {
    ObjectStore::Transaction::Op *op = i.decode_op();
    ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_WRITE);
    bufferlist bl;
    i.decode_bl(bl);
    auto off = op->off;
    auto len = bl.length();
    for (const auto& bptr : bl.buffers()) {
      cout << " Checking off " << off << " len " << bptr.length() << " ptr " << (void*)bptr.c_str() << std::endl;
      if (off % CEPH_PAGE_SIZE) {
        unsigned int align = (0-off) & ~CEPH_PAGE_MASK;
        if (bptr.length() >= CEPH_PAGE_SIZE+align) {
	  cout << "  Should have been split" << std::endl;
	  if (longest_misaligned < bptr.length()-align) {
	    longest_misaligned = bptr.length()-align;
	  }
	  ASSERT_TRUE(false);
	}else{
	  cout << "  OK - Sub PAGE misaligned offset" << std::endl;
	}
	quantity_misaligned += bptr.length();
      }else{
	if (bptr.length() >= CEPH_PAGE_SIZE) {
	  if (!bptr.is_aligned(CEPH_PAGE_SIZE)) {
	    cout << "  Should have been aligned" << std::endl;
            if (longest_misaligned < bptr.length()) {
              longest_misaligned = bptr.length();
	    }
            quantity_misaligned += bptr.length();
            ASSERT_TRUE(false);
	  }else{
	    cout << "  Good alignment" << std::endl;
            if (longest_aligned < bptr.length()) {
              longest_aligned = bptr.length();
	    }
            quantity_aligned += bptr.length();
	  }
	}else{
	  cout << "  OK - Sub PAGE aligned offset" << std::endl;
          quantity_misaligned += bptr.length();
	}
      }
      off = off + bptr.length();
      len = len - bptr.length();
    }
  }
  ASSERT_TRUE(longest_aligned>=longest_misaligned);
  cout << " Longest segment is aligned" << std::endl;
  cout << " Bytes aligned "<< quantity_aligned << " Bytes misaligned " << quantity_misaligned << std::endl;
}

TEST(Transaction, WriteBufferAlignment)
{
  ObjectStore::Transaction source{CEPH_FEATURES_ALL};
  bufferlist encoded1;
  bufferlist encoded2p;
  bufferlist encoded2d;
  ObjectStore::Transaction decode1;
  ObjectStore::Transaction decode2;
  ObjectStore::Transaction decode3;
  ceph::buffer::list::const_iterator p;
  ceph::buffer::list::const_iterator d;

  cout << "Creating transaction2" << std::endl;
  create_transaction2(source);
  cout << "Checking transaction2" << std::endl;
  check_content_transaction2(source);
#if 0
  encode(source, encoded1);
  p = encoded1.cbegin();
  decode(decode1, p);
  cout << "Checking encoded/decoded with 1 buffer transaction2" << std::endl;
  check_content_transaction2(decode1);
#endif

  source.encode(encoded2p, encoded2d, CEPH_FEATUREMASK_SERVER_TENTACLE);
  p = encoded2p.cbegin();
  d = encoded2d.cbegin();
  decode2.decode(p, d);
  cout << "Checking encoded/decoded with 2 buffers transaction2" << std::endl;
  check_content_transaction2(decode2);

  ceph::buffer::list alignedbuf;
  ceph::bufferptr ptr(ceph::buffer::create_aligned(encoded2d.length(), CEPH_PAGE_SIZE));

  alignedbuf.push_back(ptr);
  encoded2d.begin().copy(encoded2d.length(), ptr.c_str());
  p = encoded2p.cbegin();
  d = alignedbuf.cbegin();
  decode3.decode(p, d);
  cout << "Checking alignment of transaction2" << std::endl;
  check_alignment_transaction2(decode3);
}

/**
 * create_check_transaction3
 *
 * Construct/validate an ObjectStore:Transaction for appends
 */
void create_check_transaction3(ObjectStore::Transaction& t, bool create, bool part1)
{
  coll_t c1 = coll_t(spg_t(pg_t(0,111), shard_id_t::NO_SHARD));
  ghobject_t o1 = ghobject_t(hobject_t(sobject_t("testobject1", CEPH_NOSNAP)));
  bufferlist bllong;

  // 1.5 pages
  const unsigned int bllong_seed = 9876;
  const unsigned int bllong_len = (CEPH_PAGE_SIZE*3)/2;
  create_pattern(bllong,bllong_seed,bllong_len);

  if (create) {
    if (part1) {
      //Part 1
      t.create(c1, o1);
    }else{
      //Part 2
      t.zero(c1, o1, 1111, 2222);
      t.write(c1, o1, 0, bllong_len, bllong);
    }
  }else{
    ObjectStore::Transaction::iterator i = t.begin();
    ObjectStore::Transaction::Op *op;

    //Part 1
    ASSERT_TRUE(i.have_op());
    op = i.decode_op();
    ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_CREATE);
    ASSERT_TRUE(c1 == i.get_cid(op->cid));
    ASSERT_TRUE(o1 == i.get_oid(op->oid));
    //Part 2
    ASSERT_TRUE(i.have_op());
    op = i.decode_op();
    ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_ZERO);
    ASSERT_TRUE(c1 == i.get_cid(op->cid));
    ASSERT_TRUE(o1 == i.get_oid(op->oid));
    ASSERT_TRUE(op->off == 1111);
    ASSERT_TRUE(op->len == 2222);
    op = i.decode_op();
    ASSERT_TRUE(op->op == ObjectStore::Transaction::OP_WRITE);
    ASSERT_TRUE(c1 == i.get_cid(op->cid));
    ASSERT_TRUE(o1 == i.get_oid(op->oid));
    ASSERT_TRUE(op->off == 0);
    ASSERT_TRUE(op->len == bllong_len);
    bufferlist bl;
    i.decode_bl(bl);
    ASSERT_TRUE(bl.length() == bllong_len);
    check_pattern(bl, bllong_seed, bllong_len);
    ASSERT_FALSE(i.have_op());
  }
}

void create_transaction3_part1(ObjectStore::Transaction& t)
{
  create_check_transaction3(t, true, true);
}

void create_transaction3_part2(ObjectStore::Transaction& t)
{
  create_check_transaction3(t, true, false);
}

void check_content_transaction3(ObjectStore::Transaction& t)
{
  create_check_transaction3(t, false, false);
}

void transaction_to_old_format(ObjectStore::Transaction& source,ObjectStore::Transaction& dest)
{
  bufferlist encoded;
  ceph::buffer::list::const_iterator p;
  encode(source, encoded);
  p = encoded.cbegin();
  decode(dest, p);
}

void transaction_to_new_format(ObjectStore::Transaction& source,ObjectStore::Transaction& dest)
{
  bufferlist encodedp;
  bufferlist encodedd;
  ceph::buffer::list::const_iterator p;
  ceph::buffer::list::const_iterator d;
  source.encode(encodedp, encodedd, CEPH_FEATUREMASK_SERVER_TENTACLE);
  p = encodedp.cbegin();
  d = encodedd.cbegin();
  dest.decode(p, d);
}

TEST(Transaction, AppendSimple) //FormatDualDual
{
  ObjectStore::Transaction t1;
  ObjectStore::Transaction t2;

  create_transaction3_part1(t1);
  create_transaction3_part2(t2);
  t1.append(t2);
  check_content_transaction3(t1);
}

TEST(Transaction, AppendFormatDualNew)
{
  ObjectStore::Transaction t1_dual;
  ObjectStore::Transaction t2_dual;;
  ObjectStore::Transaction t2_new;

  create_transaction3_part1(t1_dual);
  create_transaction3_part2(t2_dual);
  transaction_to_new_format(t2_dual,t2_new);
  t1_dual.append(t2_new);
  check_content_transaction3(t1_dual);
}

TEST(Transaction, AppendFormatDualOld)
{
  ObjectStore::Transaction t1_dual;
  ObjectStore::Transaction t2_dual;;
  ObjectStore::Transaction t2_old;

  create_transaction3_part1(t1_dual);
  create_transaction3_part2(t2_dual);
  transaction_to_old_format(t2_dual,t2_old);
  t1_dual.append(t2_old);
  check_content_transaction3(t1_dual);
}

TEST(Transaction, AppendFormatNewDual)
{
  ObjectStore::Transaction t1_dual;
  ObjectStore::Transaction t2_dual;;
  ObjectStore::Transaction t1_new;

  create_transaction3_part1(t1_dual);
  create_transaction3_part2(t2_dual);
  transaction_to_new_format(t1_dual,t1_new);
  t1_new.append(t2_dual);
  check_content_transaction3(t1_new);
}

TEST(Transaction, AppendFormatNewNew)
{
  ObjectStore::Transaction t1_dual;
  ObjectStore::Transaction t2_dual;;
  ObjectStore::Transaction t1_new;
  ObjectStore::Transaction t2_new;

  create_transaction3_part1(t1_dual);
  create_transaction3_part2(t2_dual);
  transaction_to_new_format(t1_dual,t1_new);
  transaction_to_new_format(t2_dual,t2_new);
  t1_new.append(t2_new);
  check_content_transaction3(t1_new);
}

TEST(Transaction, AppendFormatOldDual)
{
  ObjectStore::Transaction t1_dual;
  ObjectStore::Transaction t2_dual;;
  ObjectStore::Transaction t1_old;

  create_transaction3_part1(t1_dual);
  create_transaction3_part2(t2_dual);
  transaction_to_old_format(t1_dual,t1_old);
  t1_old.append(t2_dual);
  check_content_transaction3(t1_old);
}

TEST(Transaction, AppendFormatOldOld)
{
  ObjectStore::Transaction t1_dual;
  ObjectStore::Transaction t2_dual;;
  ObjectStore::Transaction t1_old;
  ObjectStore::Transaction t2_old;

  create_transaction3_part1(t1_dual);
  create_transaction3_part2(t2_dual);
  transaction_to_old_format(t1_dual,t1_old);
  transaction_to_old_format(t2_dual,t2_old);
  t1_old.append(t2_old);
  check_content_transaction3(t1_old);
}

TEST(Transaction, AppendOpTypes)
{
  ObjectStore::Transaction source;
  bufferlist encoded1;
  bufferlist encoded2p;
  bufferlist encoded2d;
  ObjectStore::Transaction decode1;
  ObjectStore::Transaction decode2;
  ceph::buffer::list::const_iterator p;
  ceph::buffer::list::const_iterator d;

  cout << "Creating transaction1 using append" << std::endl;
  create_transaction1_using_append(source);
  cout << "Checking transaction1" << std::endl;
  check_content_transaction1(source);

  encode(source, encoded1);
  p = encoded1.cbegin();
  decode(decode1, p);
  cout << "Checking encoded/decoded with 1 buffer transaction1" << std::endl;
  check_content_transaction1(decode1);
  source.encode(encoded2p, encoded2d, CEPH_FEATUREMASK_SERVER_TENTACLE);
  p = encoded2p.cbegin();
  d = encoded2d.cbegin();
  decode2.decode(p, d);
  cout << "Checking encoded/decoded with 2 buffers transaction1" << std::endl;
  check_content_transaction1(decode2);
}
