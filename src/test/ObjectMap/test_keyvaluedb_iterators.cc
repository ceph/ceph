// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2012 Inktank, Inc.
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/
#include <tr1/memory>
#include <map>
#include <set>
#include <deque>
#include <boost/scoped_ptr.hpp>

#include "test/ObjectMap/KeyValueDBMemory.h"
#include "os/KeyValueDB.h"
#include "os/LevelDBStore.h"
#include <sys/types.h>
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "gtest/gtest.h"

using namespace std;

string store_path;

class IteratorTest : public ::testing::Test
{
public:
  boost::scoped_ptr<KeyValueDB> db;
  boost::scoped_ptr<KeyValueDBMemory> mock;

  virtual void SetUp() {
    assert(!store_path.empty());

    LevelDBStore *db_ptr = new LevelDBStore(g_ceph_context, store_path);
    assert(!db_ptr->create_and_open(std::cerr));
    db.reset(db_ptr);
    mock.reset(new KeyValueDBMemory());
  }

  virtual void TearDown() { }

  ::testing::AssertionResult validate_db_clear(KeyValueDB *store) {
    KeyValueDB::WholeSpaceIterator it = store->get_iterator();
    it->seek_to_first();
    while (it->valid()) {
      pair<string,string> k = it->raw_key();
      if (mock->db.count(k)) {
	return ::testing::AssertionFailure()
		<< __func__
		<< " mock store count " << mock->db.count(k)
		<< " key(" << k.first << "," << k.second << ")";
      }
      it->next();
    }
    return ::testing::AssertionSuccess();
  }

  ::testing::AssertionResult validate_db_match() {
    KeyValueDB::WholeSpaceIterator it = db->get_iterator();
    it->seek_to_first();
    while (it->valid()) {
      pair<string, string> k = it->raw_key();
      if (!mock->db.count(k)) {
	return ::testing::AssertionFailure()
		<< __func__
		<< " mock db.count() " << mock->db.count(k)
		<< " key(" << k.first << "," << k.second << ")";
      }

      bufferlist it_bl = it->value();
      bufferlist mock_bl = mock->db[k];

      string it_val = _bl_to_str(it_bl);
      string mock_val = _bl_to_str(mock_bl);

      if (it_val != mock_val) {
	return ::testing::AssertionFailure()
		<< __func__
		<< " key(" << k.first << "," << k.second << ")"
		<< " mismatch db value(" << it_val << ")"
		<< " mock value(" << mock_val << ")";
      }
      it->next();
    }
    return ::testing::AssertionSuccess();
  }

  ::testing::AssertionResult validate_iterator(
				KeyValueDB::WholeSpaceIterator it,
				string expected_prefix,
				string expected_key,
				string expected_value) {
    if (!it->valid()) {
      return ::testing::AssertionFailure()
	      << __func__
	      << " iterator not valid";
    }
    pair<string,string> key = it->raw_key();

    if (expected_prefix != key.first) {
      return ::testing::AssertionFailure()
	      << __func__
	      << " expected prefix '" << expected_prefix << "'"
	      << " got prefix '" << key.first << "'";
    }

    if (expected_key != it->key()) {
      return ::testing::AssertionFailure()
	      << __func__
	      << " expected key '" << expected_key << "'"
	      << " got key '" << it->key() << "'";
    }

    if (it->key() != key.second) {
      return ::testing::AssertionFailure()
	      << __func__
	      << " key '" << it->key() << "'"
	      << " does not match"
	      << " pair key '" << key.second << "'";
    }

    if (_bl_to_str(it->value()) != expected_value) {
      return ::testing::AssertionFailure()
	<< __func__
	<< " key '(" << key.first << "," << key.second << ")''"
	<< " expected value '" << expected_value << "'"
	<< " got value '" << _bl_to_str(it->value()) << "'";
    }

    return ::testing::AssertionSuccess();
  }

  /**
   * Checks if each key in the queue can be forward sequentially read from
   * the iterator iter. All keys must be present and be prefixed with prefix,
   * otherwise the validation will fail.
   *
   * Assumes that each key value must be based on the key name and generated
   * by _gen_val().
   */
  void validate_prefix(KeyValueDB::WholeSpaceIterator iter,
      string &prefix, deque<string> &keys) {

    while (!keys.empty()) {
      ASSERT_TRUE(iter->valid());
      string expected_key = keys.front();
      keys.pop_front();
      string expected_value = _gen_val_str(expected_key);

      ASSERT_TRUE(validate_iterator(iter, prefix,
		  expected_key, expected_value));

      iter->next();
    }
  }
  /**
   * Checks if each key in the queue can be backward sequentially read from
   * the iterator iter. All keys must be present and be prefixed with prefix,
   * otherwise the validation will fail.
   *
   * Assumes that each key value must be based on the key name and generated
   * by _gen_val().
   */
  void validate_prefix_backwards(KeyValueDB::WholeSpaceIterator iter,
      string &prefix, deque<string> &keys) {

    while (!keys.empty()) {
      ASSERT_TRUE(iter->valid());
      string expected_key = keys.front();
      keys.pop_front();
      string expected_value = _gen_val_str(expected_key);

      ASSERT_TRUE(validate_iterator(iter, prefix,
		  expected_key, expected_value));

      iter->prev();
    }
  }

  void clear(KeyValueDB *store) {
    KeyValueDB::WholeSpaceIterator it = store->get_snapshot_iterator();
    it->seek_to_first();
    KeyValueDB::Transaction t = store->get_transaction();
    while (it->valid()) {
      pair<string,string> k = it->raw_key();
      t->rmkey(k.first, k.second);
      it->next();
    }
    store->submit_transaction_sync(t);
  }

  string _bl_to_str(bufferlist val) {
    string str(val.c_str(), val.length());
    return str;
  }

  string _gen_val_str(string key) {
    ostringstream ss;
    ss << "##value##" << key << "##";
    return ss.str();
 }

  bufferlist _gen_val(string key) {
    bufferlist bl;
    bl.append(_gen_val_str(key));
    return bl;
  }

  void print_iterator(KeyValueDB::WholeSpaceIterator iter) {
    if (!iter->valid()) {
      std::cerr << __func__ << " iterator is not valid; stop." << std::endl;
      return;
    }

    int i = 0;
    while (iter->valid()) {
      pair<string,string> k = iter->raw_key();
      std::cerr << __func__
		<< " pos " << (++i)
		<< " key (" << k.first << "," << k.second << ")"
		<< " value(" << _bl_to_str(iter->value()) << ")" << std::endl;
      iter->next();
    }
  }

  void print_db(KeyValueDB *store) {
    KeyValueDB::WholeSpaceIterator it = store->get_iterator();
    it->seek_to_first();
    print_iterator(it);
  }
};

// ------- Remove Keys / Remove Keys By Prefix -------
class RmKeysTest : public IteratorTest
{
public:
  string prefix1;
  string prefix2;
  string prefix3;

  void init(KeyValueDB *db) {
    KeyValueDB::Transaction tx = db->get_transaction();

    tx->set(prefix1, "11", _gen_val("11"));
    tx->set(prefix1, "12", _gen_val("12"));
    tx->set(prefix1, "13", _gen_val("13"));
    tx->set(prefix2, "21", _gen_val("21"));
    tx->set(prefix2, "22", _gen_val("22"));
    tx->set(prefix2, "23", _gen_val("23"));
    tx->set(prefix3, "31", _gen_val("31"));
    tx->set(prefix3, "32", _gen_val("32"));
    tx->set(prefix3, "33", _gen_val("33"));

    db->submit_transaction_sync(tx);
  }

  virtual void SetUp() {
    IteratorTest::SetUp();

    prefix1 = "_PREFIX_1_";
    prefix2 = "_PREFIX_2_";
    prefix3 = "_PREFIX_3_";

    clear(db.get());
    ASSERT_TRUE(validate_db_clear(db.get()));
    clear(mock.get());
    ASSERT_TRUE(validate_db_match());

    init(db.get());
    init(mock.get());

    ASSERT_TRUE(validate_db_match());
  }

  virtual void TearDown() {
    IteratorTest::TearDown();
  }


  /**
   * Test the transaction's rmkeys behavior when we remove a given prefix
   * from the beginning of the key space, or from the end of the key space,
   * or even simply in the middle.
   */
  void RmKeysByPrefix(KeyValueDB *store) {
    // remove prefix2 ; check if prefix1 remains, and then prefix3
    KeyValueDB::Transaction tx = store->get_transaction();
    // remove the prefix in the middle of the key space
    tx->rmkeys_by_prefix(prefix2);
    store->submit_transaction_sync(tx);

    deque<string> key_deque;
    KeyValueDB::WholeSpaceIterator iter = store->get_iterator();
    iter->seek_to_first();

    // check for prefix1
    key_deque.push_back("11");
    key_deque.push_back("12");
    key_deque.push_back("13");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());

    // check for prefix3
    ASSERT_TRUE(iter->valid());
    key_deque.clear();
    key_deque.push_back("31");
    key_deque.push_back("32");
    key_deque.push_back("33");
    validate_prefix(iter, prefix3, key_deque);
    ASSERT_FALSE(HasFatalFailure());

    ASSERT_FALSE(iter->valid());

    clear(store);
    ASSERT_TRUE(validate_db_clear(store));
    init(store);

    // remove prefix1 ; check if prefix2 and then prefix3 remain
    tx = store->get_transaction();
    // remove the prefix at the beginning of the key space
    tx->rmkeys_by_prefix(prefix1);
    store->submit_transaction_sync(tx);

    iter = store->get_iterator();
    iter->seek_to_first();

    // check for prefix2
    key_deque.clear();
    key_deque.push_back("21");
    key_deque.push_back("22");
    key_deque.push_back("23");
    validate_prefix(iter, prefix2, key_deque);
    ASSERT_FALSE(HasFatalFailure());

    // check for prefix3
    ASSERT_TRUE(iter->valid());
    key_deque.clear();
    key_deque.push_back("31");
    key_deque.push_back("32");
    key_deque.push_back("33");
    validate_prefix(iter, prefix3, key_deque);
    ASSERT_FALSE(HasFatalFailure());

    ASSERT_FALSE(iter->valid());

    clear(store);
    ASSERT_TRUE(validate_db_clear(store));
    init(store);

    // remove prefix3 ; check if prefix1 and then prefix2 remain
    tx = store->get_transaction();
    // remove the prefix at the end of the key space
    tx->rmkeys_by_prefix(prefix3);
    store->submit_transaction_sync(tx);

    iter = store->get_iterator();
    iter->seek_to_first();

    // check for prefix1
    key_deque.clear();
    key_deque.push_back("11");
    key_deque.push_back("12");
    key_deque.push_back("13");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());

    // check for prefix2
    ASSERT_TRUE(iter->valid());
    key_deque.clear();
    key_deque.push_back("21");
    key_deque.push_back("22");
    key_deque.push_back("23");
    validate_prefix(iter, prefix2, key_deque);
    ASSERT_FALSE(HasFatalFailure());

    ASSERT_FALSE(iter->valid());
  }

  /**
   * Test how the leveldb's whole-space iterator behaves when we remove
   * keys from the store while iterating over them.
   */
  void RmKeysWhileIteratingSnapshot(KeyValueDB *store,
				    KeyValueDB::WholeSpaceIterator iter) {

    SCOPED_TRACE("RmKeysWhileIteratingSnapshot");

    iter->seek_to_first();
    ASSERT_TRUE(iter->valid());

    KeyValueDB::Transaction t = store->get_transaction();
    t->rmkey(prefix1, "11");
    t->rmkey(prefix1, "12");
    t->rmkey(prefix2, "23");
    t->rmkey(prefix3, "33");
    store->submit_transaction_sync(t);

    deque<string> key_deque;

    // check for prefix1
    key_deque.push_back("11");
    key_deque.push_back("12");
    key_deque.push_back("13");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());

    // check for prefix2
    key_deque.clear();
    key_deque.push_back("21");
    key_deque.push_back("22");
    key_deque.push_back("23");
    validate_prefix(iter, prefix2, key_deque);
    ASSERT_FALSE(HasFatalFailure());

    // check for prefix3
    key_deque.clear();
    key_deque.push_back("31");
    key_deque.push_back("32");
    key_deque.push_back("33");
    validate_prefix(iter, prefix3, key_deque);
    ASSERT_FALSE(HasFatalFailure());

    iter->next();
    ASSERT_FALSE(iter->valid());

    // make sure those keys were removed from the store
    KeyValueDB::WholeSpaceIterator tmp_it = store->get_iterator();
    tmp_it->seek_to_first();
    ASSERT_TRUE(tmp_it->valid());

    key_deque.clear();
    key_deque.push_back("13");
    validate_prefix(tmp_it, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());

    ASSERT_TRUE(tmp_it->valid());
    key_deque.clear();
    key_deque.push_back("21");
    key_deque.push_back("22");
    validate_prefix(tmp_it, prefix2, key_deque);
    ASSERT_FALSE(HasFatalFailure());

    ASSERT_TRUE(tmp_it->valid());
    key_deque.clear();
    key_deque.push_back("31");
    key_deque.push_back("32");
    validate_prefix(tmp_it, prefix3, key_deque);
    ASSERT_FALSE(HasFatalFailure());

    ASSERT_FALSE(tmp_it->valid());
  }
};

TEST_F(RmKeysTest, RmKeysByPrefixLevelDB)
{
  SCOPED_TRACE("LevelDB");
  RmKeysByPrefix(db.get());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(RmKeysTest, RmKeysByPrefixMockDB)
{
  SCOPED_TRACE("Mock DB");
  RmKeysByPrefix(mock.get());
  ASSERT_FALSE(HasFatalFailure());
}

/**
 * If you refer to function RmKeysTest::RmKeysWhileIteratingSnapshot(),
 * you will notice that we seek the iterator to the first key, and then
 * we go on to remove several keys from the underlying store, including
 * the first couple keys.
 *
 * We would expect that during this test, as soon as we removed the keys
 * from the store, the iterator would get invalid, or cause some sort of
 * unexpected mess.
 *
 * Instead, the current version of leveldb handles it perfectly, by making
 * the iterator to use a snapshot instead of the store's real state. This
 * way, LevelDBStore's whole-space iterator will behave much like its own
 * whole-space snapshot iterator.
 *
 * However, this particular behavior of the iterator hasn't been documented
 * on leveldb, and we should assume that it can be changed at any point in
 * time.
 *
 * Therefore, we keep this test, being exactly the same as the one for the
 * whole-space snapshot iterator, as we currently assume they should behave
 * identically. If this test fails, at some point, and the whole-space
 * snapshot iterator passes, then it probably means that leveldb changed
 * how its iterator behaves.
 */
TEST_F(RmKeysTest, RmKeysWhileIteratingLevelDB)
{
  SCOPED_TRACE("LevelDB -- WholeSpaceIterator");
  RmKeysWhileIteratingSnapshot(db.get(), db->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(RmKeysTest, RmKeysWhileIteratingMockDB)
{
  std::cout << "There is no safe way to test key removal while iterating\n"
	    << "over the mock store without using snapshots" << std::endl;
}

TEST_F(RmKeysTest, RmKeysWhileIteratingSnapshotLevelDB)
{
  SCOPED_TRACE("LevelDB -- WholeSpaceSnapshotIterator");
  RmKeysWhileIteratingSnapshot(db.get(), db->get_snapshot_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(RmKeysTest, RmKeysWhileIteratingSnapshotMockDB)
{
  SCOPED_TRACE("Mock DB -- WholeSpaceSnapshotIterator");
  RmKeysWhileIteratingSnapshot(mock.get(), mock->get_snapshot_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

// ------- Set Keys / Update Values -------
class SetKeysTest : public IteratorTest
{
public:
  string prefix1;
  string prefix2;

  void init(KeyValueDB *db) {
    KeyValueDB::Transaction tx = db->get_transaction();

    tx->set(prefix1, "aaa", _gen_val("aaa"));
    tx->set(prefix1, "ccc", _gen_val("ccc"));
    tx->set(prefix1, "eee", _gen_val("eee"));
    tx->set(prefix2, "vvv", _gen_val("vvv"));
    tx->set(prefix2, "xxx", _gen_val("xxx"));
    tx->set(prefix2, "zzz", _gen_val("zzz"));

    db->submit_transaction_sync(tx);
  }

  virtual void SetUp() {
    IteratorTest::SetUp();

    prefix1 = "_PREFIX_1_";
    prefix2 = "_PREFIX_2_";

    clear(db.get());
    ASSERT_TRUE(validate_db_clear(db.get()));
    clear(mock.get());
    ASSERT_TRUE(validate_db_match());

    init(db.get());
    init(mock.get());

    ASSERT_TRUE(validate_db_match());
  }

  virtual void TearDown() {
    IteratorTest::TearDown();
  }

  /**
   * Make sure that the iterator picks on new keys added if it hasn't yet
   * iterated away from that position.
   * 
   * This should only happen for the whole-space iterator when not using
   * the snapshot version.
   *
   * We don't need to test the validity of all elements, but we do test
   * inserting while moving from the first element to the last, using next()
   * to move forward, and then we test the same behavior while iterating
   * from the last element to the first, using prev() to move backwards.
   */
  void SetKeysWhileIterating(KeyValueDB *store,
			     KeyValueDB::WholeSpaceIterator iter) {
    iter->seek_to_first();
    ASSERT_TRUE(iter->valid());
    ASSERT_TRUE(validate_iterator(iter, prefix1, "aaa",
	  	_gen_val_str("aaa")));
    iter->next();
    ASSERT_TRUE(iter->valid());
    ASSERT_TRUE(validate_iterator(iter, prefix1, "ccc",
		_bl_to_str(_gen_val("ccc"))));

    // insert new key 'ddd' after 'ccc' and before 'eee'
    KeyValueDB::Transaction tx = store->get_transaction();
    tx->set(prefix1, "ddd", _gen_val("ddd"));
    store->submit_transaction_sync(tx);

    iter->next();
    ASSERT_TRUE(iter->valid());
    ASSERT_TRUE(validate_iterator(iter, prefix1, "ddd",
		_gen_val_str("ddd")));

    iter->seek_to_last();
    ASSERT_TRUE(iter->valid());
    tx = store->get_transaction();
    tx->set(prefix2, "yyy", _gen_val("yyy"));
    store->submit_transaction_sync(tx);

    iter->prev();
    ASSERT_TRUE(iter->valid());
    ASSERT_TRUE(validate_iterator(iter, prefix2,
				  "yyy", _gen_val_str("yyy")));
  }

  /**
   * Make sure that the whole-space snapshot iterator does not pick on new keys
   * added to the store since we created the iterator, thus guaranteeing
   * read-consistency.
   *
   * We don't need to test the validity of all elements, but we do test
   * inserting while moving from the first element to the last, using next()
   * to move forward, and then we test the same behavior while iterating
   * from the last element to the first, using prev() to move backwards.
   */
  void SetKeysWhileIteratingSnapshot(KeyValueDB *store,
				     KeyValueDB::WholeSpaceIterator iter) {
    iter->seek_to_first();
    ASSERT_TRUE(iter->valid());
    ASSERT_TRUE(validate_iterator(iter, prefix1, "aaa",
	  	_gen_val_str("aaa")));
    iter->next();
    ASSERT_TRUE(iter->valid());
    ASSERT_TRUE(validate_iterator(iter, prefix1, "ccc",
		_bl_to_str(_gen_val("ccc"))));

    // insert new key 'ddd' after 'ccc' and before 'eee'
    KeyValueDB::Transaction tx = store->get_transaction();
    tx->set(prefix1, "ddd", _gen_val("ddd"));
    store->submit_transaction_sync(tx);

    iter->next();
    ASSERT_TRUE(iter->valid());
    ASSERT_TRUE(validate_iterator(iter, prefix1, "eee",
		_gen_val_str("eee")));

    iter->seek_to_last();
    ASSERT_TRUE(iter->valid());
    tx = store->get_transaction();
    tx->set(prefix2, "yyy", _gen_val("yyy"));
    store->submit_transaction_sync(tx);

    iter->prev();
    ASSERT_TRUE(iter->valid());
    ASSERT_TRUE(validate_iterator(iter, prefix2,
				  "xxx", _gen_val_str("xxx")));
  }

  /**
   * Make sure that the whole-space iterator is able to read values changed on
   * the store, even after we moved to the updated position.
   *
   * This should only be possible when not using the whole-space snapshot
   * version of the iterator.
   */
  void UpdateValuesWhileIterating(KeyValueDB *store,
				  KeyValueDB::WholeSpaceIterator iter) {
    iter->seek_to_first();
    ASSERT_TRUE(iter->valid());
    ASSERT_TRUE(validate_iterator(iter, prefix1,
				  "aaa", _gen_val_str("aaa")));

    KeyValueDB::Transaction tx = store->get_transaction();
    tx->set(prefix1, "aaa", _gen_val("aaa_1"));
    store->submit_transaction_sync(tx);

    ASSERT_TRUE(validate_iterator(iter, prefix1,
				  "aaa", _gen_val_str("aaa_1")));

    iter->seek_to_last();
    ASSERT_TRUE(iter->valid());
    ASSERT_TRUE(validate_iterator(iter, prefix2,
				  "zzz", _gen_val_str("zzz")));

    tx = store->get_transaction();
    tx->set(prefix2, "zzz", _gen_val("zzz_1"));
    store->submit_transaction_sync(tx);

    ASSERT_TRUE(validate_iterator(iter, prefix2,
				  "zzz", _gen_val_str("zzz_1")));
  }

  /**
   * Make sure that the whole-space iterator is able to read values changed on
   * the store, even after we moved to the updated position.
   *
   * This should only be possible when not using the whole-space snapshot
   * version of the iterator.
   */
  void UpdateValuesWhileIteratingSnapshot(
				  KeyValueDB *store,
				  KeyValueDB::WholeSpaceIterator iter) {
    iter->seek_to_first();
    ASSERT_TRUE(iter->valid());
    ASSERT_TRUE(validate_iterator(iter, prefix1,
				  "aaa", _gen_val_str("aaa")));

    KeyValueDB::Transaction tx = store->get_transaction();
    tx->set(prefix1, "aaa", _gen_val("aaa_1"));
    store->submit_transaction_sync(tx);

    ASSERT_TRUE(validate_iterator(iter, prefix1,
				  "aaa", _gen_val_str("aaa")));

    iter->seek_to_last();
    ASSERT_TRUE(iter->valid());
    ASSERT_TRUE(validate_iterator(iter, prefix2,
				  "zzz", _gen_val_str("zzz")));

    tx = store->get_transaction();
    tx->set(prefix2, "zzz", _gen_val("zzz_1"));
    store->submit_transaction_sync(tx);

    ASSERT_TRUE(validate_iterator(iter, prefix2,
				  "zzz", _gen_val_str("zzz")));

    // check those values were really changed in the store
    KeyValueDB::WholeSpaceIterator tmp_iter = store->get_iterator();
    tmp_iter->seek_to_first();
    ASSERT_TRUE(tmp_iter->valid());
    ASSERT_TRUE(validate_iterator(tmp_iter, prefix1,
				  "aaa", _gen_val_str("aaa_1")));
    tmp_iter->seek_to_last();
    ASSERT_TRUE(tmp_iter->valid());
    ASSERT_TRUE(validate_iterator(tmp_iter, prefix2,
				  "zzz", _gen_val_str("zzz_1")));
  }


};

TEST_F(SetKeysTest, DISABLED_SetKeysWhileIteratingLevelDB)
{
  SCOPED_TRACE("LevelDB: SetKeysWhileIteratingLevelDB");
  SetKeysWhileIterating(db.get(), db->get_iterator());
  ASSERT_TRUE(HasFatalFailure());
}

TEST_F(SetKeysTest, SetKeysWhileIteratingMockDB)
{
  SCOPED_TRACE("Mock DB: SetKeysWhileIteratingMockDB");
  SetKeysWhileIterating(mock.get(), mock->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(SetKeysTest, SetKeysWhileIteratingSnapshotLevelDB)
{
  SCOPED_TRACE("LevelDB: SetKeysWhileIteratingSnapshotLevelDB");
  SetKeysWhileIteratingSnapshot(db.get(), db->get_snapshot_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(SetKeysTest, SetKeysWhileIteratingSnapshotMockDB)
{
  SCOPED_TRACE("MockDB: SetKeysWhileIteratingSnapshotMockDB");
  SetKeysWhileIteratingSnapshot(mock.get(), mock->get_snapshot_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(SetKeysTest, DISABLED_UpdateValuesWhileIteratingLevelDB)
{
  SCOPED_TRACE("LevelDB: UpdateValuesWhileIteratingLevelDB");
  UpdateValuesWhileIterating(db.get(), db->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(SetKeysTest, UpdateValuesWhileIteratingMockDB)
{
  SCOPED_TRACE("MockDB: UpdateValuesWhileIteratingMockDB");
  UpdateValuesWhileIterating(mock.get(), mock->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(SetKeysTest, UpdateValuesWhileIteratingSnapshotLevelDB)
{
  SCOPED_TRACE("LevelDB: UpdateValuesWhileIteratingSnapshotLevelDB");
  UpdateValuesWhileIteratingSnapshot(db.get(), db->get_snapshot_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(SetKeysTest, UpdateValuesWhileIteratingSnapshotMockDB)
{
  SCOPED_TRACE("MockDB: UpdateValuesWhileIteratingSnapshotMockDB");
  UpdateValuesWhileIteratingSnapshot(mock.get(), mock->get_snapshot_iterator());
  ASSERT_FALSE(HasFatalFailure());
}


class BoundsTest : public IteratorTest
{
public:
  string prefix1;
  string prefix2;
  string prefix3;

  void init(KeyValueDB *store) {
    KeyValueDB::Transaction tx = store->get_transaction();

    tx->set(prefix1, "aaa", _gen_val("aaa"));
    tx->set(prefix1, "ccc", _gen_val("ccc"));
    tx->set(prefix1, "eee", _gen_val("eee"));
    tx->set(prefix2, "vvv", _gen_val("vvv"));
    tx->set(prefix2, "xxx", _gen_val("xxx"));
    tx->set(prefix2, "zzz", _gen_val("zzz"));
    tx->set(prefix3, "aaa", _gen_val("aaa"));
    tx->set(prefix3, "mmm", _gen_val("mmm"));
    tx->set(prefix3, "yyy", _gen_val("yyy"));

    store->submit_transaction_sync(tx);
  }

  virtual void SetUp() {
    IteratorTest::SetUp();

    prefix1 = "_PREFIX_1_";
    prefix2 = "_PREFIX_2_";
    prefix3 = "_PREFIX_4_";

    clear(db.get());
    ASSERT_TRUE(validate_db_clear(db.get()));
    clear(mock.get());
    ASSERT_TRUE(validate_db_match());

    init(db.get());
    init(mock.get());

    ASSERT_TRUE(validate_db_match());
  }

  virtual void TearDown() {
    IteratorTest::TearDown();
  }

  void LowerBoundWithEmptyKeyOnWholeSpaceIterator(
			    KeyValueDB::WholeSpaceIterator iter) {
    deque<string> key_deque;
    // see what happens when we have an empty key and try to get to the
    // first available prefix
    iter->lower_bound(prefix1, "");
    ASSERT_TRUE(iter->valid());

    key_deque.push_back("aaa");
    key_deque.push_back("ccc");
    key_deque.push_back("eee");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());
    // if we got here without problems, then it is safe to assume the
    // remaining prefixes are intact.

    // see what happens when we have an empty key and try to get to the
    // middle of the key-space
    iter->lower_bound(prefix2, "");
    ASSERT_TRUE(iter->valid());
    key_deque.clear();

    key_deque.push_back("vvv");
    key_deque.push_back("xxx");
    key_deque.push_back("zzz");
    validate_prefix(iter, prefix2, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());
    // if we got here without problems, then it is safe to assume the
    // remaining prefixes are intact.

    // see what happens when we have an empty key and try to get to the
    // last prefix on the key-space
    iter->lower_bound(prefix3, "");
    ASSERT_TRUE(iter->valid());
    key_deque.clear();

    key_deque.push_back("aaa");
    key_deque.push_back("mmm");
    key_deque.push_back("yyy");
    validate_prefix(iter, prefix3, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_FALSE(iter->valid());
    // we reached the end of the key_space, so the iterator should no longer
    // be valid
    
    // see what happens when we look for an inexistent prefix, that will
    // compare higher than the existing prefixes, with an empty key
    // expected: reach the store's end; iterator becomes invalid
    iter->lower_bound("_PREFIX_9_", "");
    ASSERT_FALSE(iter->valid());

    // see what happens when we look for an inexistent prefix, that will
    // compare lower than the existing prefixes, with an empty key
    // expected: find the first prefix; iterator is valid
    iter->lower_bound("_PREFIX_0_", "");
    ASSERT_TRUE(iter->valid());
    key_deque.clear();
    key_deque.push_back("aaa");
    key_deque.push_back("ccc");
    key_deque.push_back("eee");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());

    // see what happens when we look for an empty prefix (that should compare
    // lower than any existing prefixes)
    // expected: find the first prefix; iterator is valid
    iter->lower_bound("", "");
    ASSERT_TRUE(iter->valid());
    key_deque.push_back("aaa");
    key_deque.push_back("ccc");
    key_deque.push_back("eee");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());
  }

  void LowerBoundWithEmptyPrefixOnWholeSpaceIterator(
			  KeyValueDB::WholeSpaceIterator iter) {
    deque<string> key_deque;
    // check for an empty prefix, with key 'aaa'. Since this key is shared
    // among two different prefixes, it is relevant to check which will be
    // found first.
    // expected: find key (prefix1, aaa); iterator is valid
    iter->lower_bound("", "aaa");
    ASSERT_TRUE(iter->valid());

    key_deque.push_back("aaa");
    key_deque.push_back("ccc");
    key_deque.push_back("eee");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());
    // since we found prefix1, it is safe to assume that the remaining
    // prefixes (prefix2 and prefix3) will follow

    // any lower_bound operation with an empty prefix should always put the
    // iterator in the first key in the key-space, despite what key is
    // specified. This means that looking for ("","AAAAAAAAAA") should
    // also position the iterator on (prefix1, aaa).
    // expected: find key (prefix1, aaa); iterator is valid
    iter->lower_bound("", "AAAAAAAAAA");
    ASSERT_TRUE(iter->valid());
    key_deque.clear();
    key_deque.push_back("aaa");
    key_deque.push_back("ccc");
    key_deque.push_back("eee");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());

    // note: this test is a duplicate of the one in the function above. Why?
    // Well, because it also fits here (being its prefix empty), and one could
    // very well run solely this test (instead of the whole battery) and would
    // certainly expect this case to be tested.

    // see what happens when we look for an empty prefix (that should compare
    // lower than any existing prefixes)
    // expected: find the first prefix; iterator is valid
    iter->lower_bound("", "");
    ASSERT_TRUE(iter->valid());
    key_deque.push_back("aaa");
    key_deque.push_back("ccc");
    key_deque.push_back("eee");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());
  }

  void LowerBoundOnWholeSpaceIterator(
			  KeyValueDB::WholeSpaceIterator iter) {
    deque<string> key_deque;
    // check that we find the first key in the store
    // expected: find (prefix1, aaa); iterator is valid
    iter->lower_bound(prefix1, "aaa");
    ASSERT_TRUE(iter->valid());
    key_deque.push_back("aaa");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());

    // check that we find the last key in the store
    // expected: find (prefix3, yyy); iterator is valid
    iter->lower_bound(prefix3, "yyy");
    ASSERT_TRUE(iter->valid());
    key_deque.clear();
    key_deque.push_back("yyy");
    validate_prefix(iter, prefix3, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_FALSE(iter->valid());

    // check that looking for non-existent prefix '_PREFIX_0_' will
    // always result in the first value of prefix1 (prefix1,"aaa")
    // expected: find (prefix1, aaa); iterator is valid
    iter->lower_bound("_PREFIX_0_", "AAAAA");
    ASSERT_TRUE(iter->valid());
    key_deque.clear();
    key_deque.push_back("aaa");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());

    // check that looking for non-existent prefix '_PREFIX_3_' will
    // always result in the first value of prefix3 (prefix4,"aaa")
    // expected: find (prefix3, aaa); iterator is valid
    iter->lower_bound("_PREFIX_3_", "AAAAA");
    ASSERT_TRUE(iter->valid());
    key_deque.clear();
    key_deque.push_back("aaa");
    validate_prefix(iter, prefix3, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());

    // check that looking for non-existent prefix '_PREFIX_9_' will
    // always result in an invalid iterator.
    // expected: iterator is invalid
    iter->lower_bound("_PREFIX_9_", "AAAAA");
    ASSERT_FALSE(iter->valid());
  }

  void UpperBoundWithEmptyKeyOnWholeSpaceIterator(
			  KeyValueDB::WholeSpaceIterator iter) {
    deque<string> key_deque;
    // check that looking for (prefix1, "") will result in finding
    // the first key in prefix1 (prefix1, "aaa")
    // expected: find (prefix1, aaa); iterator is valid
    iter->upper_bound(prefix1, "");
    key_deque.push_back("aaa");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());

    // check that looking for (prefix2, "") will result in finding
    // the first key in prefix2 (prefix2, vvv)
    // expected: find (prefix2, aaa); iterator is valid
    iter->upper_bound(prefix2, "");
    key_deque.push_back("vvv");
    validate_prefix(iter, prefix2, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());


    // check that looking for (prefix3, "") will result in finding
    // the first key in prefix3 (prefix3, aaa)
    // expected: find (prefix3, aaa); iterator is valid
    iter->upper_bound(prefix3, "");
    key_deque.push_back("aaa");
    validate_prefix(iter, prefix3, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());

    // see what happens when we look for an inexistent prefix, that will
    // compare higher than the existing prefixes, with an empty key
    // expected: reach the store's end; iterator becomes invalid
    iter->upper_bound("_PREFIX_9_", "");
    ASSERT_FALSE(iter->valid());

    // see what happens when we look for an inexistent prefix, that will
    // compare lower than the existing prefixes, with an empty key
    // expected: find the first prefix; iterator is valid
    iter->upper_bound("_PREFIX_0_", "");
    ASSERT_TRUE(iter->valid());
    key_deque.clear();
    key_deque.push_back("aaa");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());

    // see what happens when we look for an empty prefix (that should compare
    // lower than any existing prefixes)
    // expected: find the first prefix; iterator is valid
    iter->upper_bound("", "");
    ASSERT_TRUE(iter->valid());
    key_deque.push_back("aaa");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());
  }

  void UpperBoundWithEmptyPrefixOnWholeSpaceIterator(
			  KeyValueDB::WholeSpaceIterator iter) {
    deque<string> key_deque;
    // check for an empty prefix, with key 'aaa'. Since this key is shared
    // among two different prefixes, it is relevant to check which will be
    // found first.
    // expected: find key (prefix1, aaa); iterator is valid
    iter->upper_bound("", "aaa");
    ASSERT_TRUE(iter->valid());
    key_deque.push_back("aaa");
    key_deque.push_back("ccc");
    key_deque.push_back("eee");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());

    // any upper_bound operation with an empty prefix should always put the
    // iterator in the first key whose prefix compares greater, despite the
    // key that is specified. This means that looking for ("","AAAAAAAAAA")
    // should position the iterator on (prefix1, aaa).
    // expected: find key (prefix1, aaa); iterator is valid
    iter->upper_bound("", "AAAAAAAAAA");
    ASSERT_TRUE(iter->valid());
    key_deque.clear();
    key_deque.push_back("aaa");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());

    // note: this test is a duplicate of the one in the function above. Why?
    // Well, because it also fits here (being its prefix empty), and one could
    // very well run solely this test (instead of the whole battery) and would
    // certainly expect this case to be tested.

    // see what happens when we look for an empty prefix (that should compare
    // lower than any existing prefixes)
    // expected: find the first prefix; iterator is valid
    iter->upper_bound("", "");
    ASSERT_TRUE(iter->valid());
    key_deque.push_back("aaa");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());
  }

  void UpperBoundOnWholeSpaceIterator(
			  KeyValueDB::WholeSpaceIterator iter) {
    deque<string> key_deque;
    // check that we find the second key in the store
    // expected: find (prefix1, ccc); iterator is valid
    iter->upper_bound(prefix1, "bbb");
    ASSERT_TRUE(iter->valid());
    key_deque.push_back("ccc");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());

    // check that we find the last key in the store
    // expected: find (prefix3, yyy); iterator is valid
    iter->upper_bound(prefix3, "xxx");
    ASSERT_TRUE(iter->valid());
    key_deque.clear();
    key_deque.push_back("yyy");
    validate_prefix(iter, prefix3, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_FALSE(iter->valid());

    // check that looking for non-existent prefix '_PREFIX_0_' will
    // always result in the first value of prefix1 (prefix1,"aaa")
    // expected: find (prefix1, aaa); iterator is valid
    iter->upper_bound("_PREFIX_0_", "AAAAA");
    ASSERT_TRUE(iter->valid());
    key_deque.clear();
    key_deque.push_back("aaa");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());

    // check that looking for non-existent prefix '_PREFIX_3_' will
    // always result in the first value of prefix3 (prefix3,"aaa")
    // expected: find (prefix3, aaa); iterator is valid
    iter->upper_bound("_PREFIX_3_", "AAAAA");
    ASSERT_TRUE(iter->valid());
    key_deque.clear();
    key_deque.push_back("aaa");
    validate_prefix(iter, prefix3, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());

    // check that looking for non-existent prefix '_PREFIX_9_' will
    // always result in an invalid iterator.
    // expected: iterator is invalid
    iter->upper_bound("_PREFIX_9_", "AAAAA");
    ASSERT_FALSE(iter->valid());
  }
};

TEST_F(BoundsTest, LowerBoundWithEmptyKeyOnWholeSpaceIteratorLevelDB)
{
  SCOPED_TRACE("LevelDB: Lower Bound, Empty Key, Whole-Space Iterator");
  LowerBoundWithEmptyKeyOnWholeSpaceIterator(db->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(BoundsTest, LowerBoundWithEmptyKeyOnWholeSpaceIteratorMockDB)
{
  SCOPED_TRACE("MockDB: Lower Bound, Empty Key, Whole-Space Iterator");
  LowerBoundWithEmptyKeyOnWholeSpaceIterator(mock->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(BoundsTest, LowerBoundWithEmptyPrefixOnWholeSpaceIteratorLevelDB)
{
  SCOPED_TRACE("LevelDB: Lower Bound, Empty Prefix, Whole-Space Iterator");
  LowerBoundWithEmptyPrefixOnWholeSpaceIterator(db->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(BoundsTest, LowerBoundWithEmptyPrefixOnWholeSpaceIteratorMockDB)
{
  SCOPED_TRACE("MockDB: Lower Bound, Empty Prefix, Whole-Space Iterator");
  LowerBoundWithEmptyPrefixOnWholeSpaceIterator(mock->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(BoundsTest, LowerBoundOnWholeSpaceIteratorLevelDB)
{
  SCOPED_TRACE("LevelDB: Lower Bound, Whole-Space Iterator");
  LowerBoundOnWholeSpaceIterator(db->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(BoundsTest, LowerBoundOnWholeSpaceIteratorMockDB)
{
  SCOPED_TRACE("MockDB: Lower Bound, Whole-Space Iterator");
  LowerBoundOnWholeSpaceIterator(mock->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(BoundsTest, UpperBoundWithEmptyKeyOnWholeSpaceIteratorLevelDB)
{
  SCOPED_TRACE("LevelDB: Upper Bound, Empty Key, Whole-Space Iterator");
  UpperBoundWithEmptyKeyOnWholeSpaceIterator(db->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(BoundsTest, UpperBoundWithEmptyKeyOnWholeSpaceIteratorMockDB)
{
  SCOPED_TRACE("MockDB: Upper Bound, Empty Key, Whole-Space Iterator");
  UpperBoundWithEmptyKeyOnWholeSpaceIterator(mock->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(BoundsTest, UpperBoundWithEmptyPrefixOnWholeSpaceIteratorLevelDB)
{
  SCOPED_TRACE("LevelDB: Upper Bound, Empty Prefix, Whole-Space Iterator");
  UpperBoundWithEmptyPrefixOnWholeSpaceIterator(db->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(BoundsTest, UpperBoundWithEmptyPrefixOnWholeSpaceIteratorMockDB)
{
  SCOPED_TRACE("MockDB: Upper Bound, Empty Prefix, Whole-Space Iterator");
  UpperBoundWithEmptyPrefixOnWholeSpaceIterator(mock->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(BoundsTest, UpperBoundOnWholeSpaceIteratorLevelDB)
{
  SCOPED_TRACE("LevelDB: Upper Bound, Whole-Space Iterator");
  UpperBoundOnWholeSpaceIterator(db->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(BoundsTest, UpperBoundOnWholeSpaceIteratorMockDB)
{
  SCOPED_TRACE("MockDB: Upper Bound, Whole-Space Iterator");
  UpperBoundOnWholeSpaceIterator(mock->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}


class SeeksTest : public IteratorTest
{
public:
  string prefix0;
  string prefix1;
  string prefix2;
  string prefix3;
  string prefix4;
  string prefix5;

  void init(KeyValueDB *store) {
    KeyValueDB::Transaction tx = store->get_transaction();

    tx->set(prefix1, "aaa", _gen_val("aaa"));
    tx->set(prefix1, "ccc", _gen_val("ccc"));
    tx->set(prefix1, "eee", _gen_val("eee"));
    tx->set(prefix2, "vvv", _gen_val("vvv"));
    tx->set(prefix2, "xxx", _gen_val("xxx"));
    tx->set(prefix2, "zzz", _gen_val("zzz"));
    tx->set(prefix4, "aaa", _gen_val("aaa"));
    tx->set(prefix4, "mmm", _gen_val("mmm"));
    tx->set(prefix4, "yyy", _gen_val("yyy"));

    store->submit_transaction_sync(tx);
  }

  virtual void SetUp() {
    IteratorTest::SetUp();

    prefix0 = "_PREFIX_0_";
    prefix1 = "_PREFIX_1_";
    prefix2 = "_PREFIX_2_";
    prefix3 = "_PREFIX_3_";
    prefix4 = "_PREFIX_4_";
    prefix5 = "_PREFIX_5_";

    clear(db.get());
    ASSERT_TRUE(validate_db_clear(db.get()));
    clear(mock.get());
    ASSERT_TRUE(validate_db_match());

    init(db.get());
    init(mock.get());

    ASSERT_TRUE(validate_db_match());
  }

  virtual void TearDown() {
    IteratorTest::TearDown();
  }


  void SeekToFirstOnWholeSpaceIterator(
			  KeyValueDB::WholeSpaceIterator iter) {
    iter->seek_to_first();
    ASSERT_TRUE(iter->valid());
    deque<string> key_deque;
    key_deque.push_back("aaa");
    key_deque.push_back("ccc");
    key_deque.push_back("eee");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());
  }

  void SeekToFirstWithPrefixOnWholeSpaceIterator(
			  KeyValueDB::WholeSpaceIterator iter) {
    deque<string> key_deque;

    // if the prefix is empty, we must end up seeking to the first key.
    // expected: seek to (prefix1, aaa); iterator is valid
    iter->seek_to_first("");
    ASSERT_TRUE(iter->valid());
    key_deque.push_back("aaa");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());

    // try seeking to non-existent prefix that compares lower than the
    // first available prefix
    // expected: seek to (prefix1, aaa); iterator is valid
    iter->seek_to_first(prefix0);
    ASSERT_TRUE(iter->valid());
    key_deque.clear();
    key_deque.push_back("aaa");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());

    // try seeking to non-existent prefix
    // expected: seek to (prefix4, aaa); iterator is valid
    iter->seek_to_first(prefix3);
    ASSERT_TRUE(iter->valid());
    key_deque.clear();
    key_deque.push_back("aaa");
    validate_prefix(iter, prefix4, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());

    // try seeking to non-existent prefix that compares greater than the
    // last available prefix
    // expected: iterator is invalid
    iter->seek_to_first(prefix5);
    ASSERT_FALSE(iter->valid());

    // try seeking to the first prefix and make sure we end up in its first
    // position
    // expected: seek to (prefix1,aaa); iterator is valid
    iter->seek_to_first(prefix1);
    ASSERT_TRUE(iter->valid());
    key_deque.clear();
    key_deque.push_back("aaa");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());

    // try seeking to the second prefix and make sure we end up in its
    // first position
    // expected: seek to (prefix2,vvv); iterator is valid
    iter->seek_to_first(prefix2);
    ASSERT_TRUE(iter->valid());
    key_deque.clear();
    key_deque.push_back("vvv");
    validate_prefix(iter, prefix2, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());

    // try seeking to the last prefix and make sure we end up in its
    // first position
    // expected: seek to (prefix4,aaa); iterator is valid
    iter->seek_to_first(prefix4);
    ASSERT_TRUE(iter->valid());
    key_deque.clear();
    key_deque.push_back("aaa");
    validate_prefix(iter, prefix4, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());
  }

  void SeekToLastOnWholeSpaceIterator(
			  KeyValueDB::WholeSpaceIterator iter) {
    deque<string> key_deque;
    iter->seek_to_last();
    key_deque.push_back("yyy");
    validate_prefix(iter, prefix4, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_FALSE(iter->valid());
  }

  void SeekToLastWithPrefixOnWholeSpaceIterator(
			  KeyValueDB::WholeSpaceIterator iter) {
    deque<string> key_deque;

    // if the prefix is empty, we must end up seeking to last position
    // that has an empty prefix, or to the previous position to the first
    // position whose prefix compares higher than empty.
    // expected: iterator is invalid (because (prefix1,aaa) is the first
    //		 position that compared higher than an empty prefix)
    iter->seek_to_last("");
    ASSERT_FALSE(iter->valid());

    // try seeking to non-existent prefix that compares lower than the
    // first available prefix
    // expected: iterator is invalid (because (prefix1,aaa) is the first
    //		 position that compared higher than prefix0)
    iter->seek_to_last(prefix0);
    ASSERT_FALSE(iter->valid());

    // try seeking to non-existent prefix
    // expected: seek to (prefix2, zzz); iterator is valid
    iter->seek_to_last(prefix3);
    ASSERT_TRUE(iter->valid());
    key_deque.clear();
    key_deque.push_back("zzz");
    validate_prefix(iter, prefix2, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());

    // try seeking to non-existent prefix that compares greater than the
    // last available prefix
    // expected: iterator is in the last position of the store;
    //		 i.e., (prefix4,yyy)
    iter->seek_to_last(prefix5);
    ASSERT_TRUE(iter->valid());
    key_deque.clear();
    key_deque.push_back("yyy");
    validate_prefix(iter, prefix4, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_FALSE(iter->valid());

    // try seeking to the first prefix and make sure we end up in its last
    // position
    // expected: seek to (prefix1,eee); iterator is valid
    iter->seek_to_last(prefix1);
    ASSERT_TRUE(iter->valid());
    key_deque.clear();
    key_deque.push_back("eee");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());

    // try seeking to the second prefix and make sure we end up in its
    // last position
    // expected: seek to (prefix2,vvv); iterator is valid
    iter->seek_to_last(prefix2);
    ASSERT_TRUE(iter->valid());
    key_deque.clear();
    key_deque.push_back("zzz");
    validate_prefix(iter, prefix2, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(iter->valid());

    // try seeking to the last prefix and make sure we end up in its
    // last position
    // expected: seek to (prefix4,aaa); iterator is valid
    iter->seek_to_last(prefix4);
    ASSERT_TRUE(iter->valid());
    key_deque.clear();
    key_deque.push_back("yyy");
    validate_prefix(iter, prefix4, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_FALSE(iter->valid());
  }
};

TEST_F(SeeksTest, SeekToFirstOnWholeSpaceIteratorLevelDB) {
  SCOPED_TRACE("LevelDB: Seek To First, Whole Space Iterator");
  SeekToFirstOnWholeSpaceIterator(db->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(SeeksTest, SeekToFirstOnWholeSpaceIteratorMockDB) {
  SCOPED_TRACE("MockDB: Seek To First, Whole Space Iterator");
  SeekToFirstOnWholeSpaceIterator(mock->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(SeeksTest, SeekToFirstWithPrefixOnWholeSpaceIteratorLevelDB) {
  SCOPED_TRACE("LevelDB: Seek To First, With Prefix, Whole Space Iterator");
  SeekToFirstWithPrefixOnWholeSpaceIterator(db->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(SeeksTest, SeekToFirstWithPrefixOnWholeSpaceIteratorMockDB) {
  SCOPED_TRACE("MockDB: Seek To First, With Prefix, Whole Space Iterator");
  SeekToFirstWithPrefixOnWholeSpaceIterator(mock->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(SeeksTest, SeekToLastOnWholeSpaceIteratorLevelDB) {
  SCOPED_TRACE("LevelDB: Seek To Last, Whole Space Iterator");
  SeekToLastOnWholeSpaceIterator(db->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(SeeksTest, SeekToLastOnWholeSpaceIteratorMockDB) {
  SCOPED_TRACE("MockDB: Seek To Last, Whole Space Iterator");
  SeekToLastOnWholeSpaceIterator(mock->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(SeeksTest, SeekToLastWithPrefixOnWholeSpaceIteratorLevelDB) {
  SCOPED_TRACE("LevelDB: Seek To Last, With Prefix, Whole Space Iterator");
  SeekToLastWithPrefixOnWholeSpaceIterator(db->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(SeeksTest, SeekToLastWithPrefixOnWholeSpaceIteratorMockDB) {
  SCOPED_TRACE("MockDB: Seek To Last, With Prefix, Whole Space Iterator");
  SeekToLastWithPrefixOnWholeSpaceIterator(mock->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

class KeySpaceIteration : public IteratorTest
{
public:
  string prefix1;

  void init(KeyValueDB *store) {
    KeyValueDB::Transaction tx = store->get_transaction();

    tx->set(prefix1, "aaa", _gen_val("aaa"));
    tx->set(prefix1, "vvv", _gen_val("vvv"));
    tx->set(prefix1, "zzz", _gen_val("zzz"));

    store->submit_transaction_sync(tx);
  }

  virtual void SetUp() {
    IteratorTest::SetUp();

    prefix1 = "_PREFIX_1_";

    clear(db.get());
    ASSERT_TRUE(validate_db_clear(db.get()));
    clear(mock.get());
    ASSERT_TRUE(validate_db_match());

    init(db.get());
    init(mock.get());

    ASSERT_TRUE(validate_db_match());
  }

  virtual void TearDown() {
    IteratorTest::TearDown();
  }

  void ForwardIteration(KeyValueDB::WholeSpaceIterator iter) {
    deque<string> key_deque;
    iter->seek_to_first();
    key_deque.push_back("aaa");
    key_deque.push_back("vvv");
    key_deque.push_back("zzz");
    validate_prefix(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_FALSE(iter->valid());
  }

  void BackwardIteration(KeyValueDB::WholeSpaceIterator iter) {
    deque<string> key_deque;
    iter->seek_to_last();
    key_deque.push_back("zzz");
    key_deque.push_back("vvv");
    key_deque.push_back("aaa");
    validate_prefix_backwards(iter, prefix1, key_deque);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_FALSE(iter->valid());
  }
};

TEST_F(KeySpaceIteration, ForwardIterationLevelDB)
{
  SCOPED_TRACE("LevelDB: Forward Iteration, Whole Space Iterator");
  ForwardIteration(db->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(KeySpaceIteration, ForwardIterationMockDB) {
  SCOPED_TRACE("MockDB: Forward Iteration, Whole Space Iterator");
  ForwardIteration(mock->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(KeySpaceIteration, BackwardIterationLevelDB)
{
  SCOPED_TRACE("LevelDB: Backward Iteration, Whole Space Iterator");
  BackwardIteration(db->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(KeySpaceIteration, BackwardIterationMockDB) {
  SCOPED_TRACE("MockDB: Backward Iteration, Whole Space Iterator");
  BackwardIteration(mock->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

class EmptyStore : public IteratorTest
{
public:
  virtual void SetUp() {
    IteratorTest::SetUp();

    clear(db.get());
    ASSERT_TRUE(validate_db_clear(db.get()));
    clear(mock.get());
    ASSERT_TRUE(validate_db_match());
  }

  void SeekToFirst(KeyValueDB::WholeSpaceIterator iter) {
    // expected: iterator is invalid
    iter->seek_to_first();
    ASSERT_FALSE(iter->valid());
  }

  void SeekToFirstWithPrefix(KeyValueDB::WholeSpaceIterator iter) {
    // expected: iterator is invalid
    iter->seek_to_first("prefix");
    ASSERT_FALSE(iter->valid());
  }

  void SeekToLast(KeyValueDB::WholeSpaceIterator iter) {
    // expected: iterator is invalid
    iter->seek_to_last();
    ASSERT_FALSE(iter->valid());
  }

  void SeekToLastWithPrefix(KeyValueDB::WholeSpaceIterator iter) {
    // expected: iterator is invalid
    iter->seek_to_last("prefix");
    ASSERT_FALSE(iter->valid());
  }

  void LowerBound(KeyValueDB::WholeSpaceIterator iter) {
    // expected: iterator is invalid
    iter->lower_bound("prefix", "");
    ASSERT_FALSE(iter->valid());

    // expected: iterator is invalid
    iter->lower_bound("", "key");
    ASSERT_FALSE(iter->valid());

    // expected: iterator is invalid
    iter->lower_bound("prefix", "key");
    ASSERT_FALSE(iter->valid());
  }

  void UpperBound(KeyValueDB::WholeSpaceIterator iter) {
    // expected: iterator is invalid
    iter->upper_bound("prefix", "");
    ASSERT_FALSE(iter->valid());

    // expected: iterator is invalid
    iter->upper_bound("", "key");
    ASSERT_FALSE(iter->valid());

    // expected: iterator is invalid
    iter->upper_bound("prefix", "key");
    ASSERT_FALSE(iter->valid());
  }
};

TEST_F(EmptyStore, SeekToFirstLevelDB)
{
  SCOPED_TRACE("LevelDB: Empty Store, Seek To First");
  SeekToFirst(db->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(EmptyStore, SeekToFirstMockDB)
{
  SCOPED_TRACE("MockDB: Empty Store, Seek To First");
  SeekToFirst(mock->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(EmptyStore, SeekToFirstWithPrefixLevelDB)
{
  SCOPED_TRACE("LevelDB: Empty Store, Seek To First With Prefix");
  SeekToFirstWithPrefix(db->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(EmptyStore, SeekToFirstWithPrefixMockDB)
{
  SCOPED_TRACE("MockDB: Empty Store, Seek To First With Prefix");
  SeekToFirstWithPrefix(mock->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(EmptyStore, SeekToLastLevelDB)
{
  SCOPED_TRACE("LevelDB: Empty Store, Seek To Last");
  SeekToLast(db->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(EmptyStore, SeekToLastMockDB)
{
  SCOPED_TRACE("MockDB: Empty Store, Seek To Last");
  SeekToLast(mock->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(EmptyStore, SeekToLastWithPrefixLevelDB)
{
  SCOPED_TRACE("LevelDB: Empty Store, Seek To Last With Prefix");
  SeekToLastWithPrefix(db->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(EmptyStore, SeekToLastWithPrefixMockDB)
{
  SCOPED_TRACE("MockDB: Empty Store, Seek To Last With Prefix");
  SeekToLastWithPrefix(mock->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(EmptyStore, LowerBoundLevelDB)
{
  SCOPED_TRACE("LevelDB: Empty Store, Lower Bound");
  LowerBound(db->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(EmptyStore, LowerBoundMockDB)
{
  SCOPED_TRACE("MockDB: Empty Store, Lower Bound");
  LowerBound(mock->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(EmptyStore, UpperBoundLevelDB)
{
  SCOPED_TRACE("LevelDB: Empty Store, Upper Bound");
  UpperBound(db->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(EmptyStore, UpperBoundMockDB)
{
  SCOPED_TRACE("MockDB: Empty Store, Upper Bound");
  UpperBound(mock->get_iterator());
  ASSERT_FALSE(HasFatalFailure());
}


int main(int argc, char *argv[])
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **) argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);

  if (argc < 2) {
    std::cerr << "Usage: " << argv[0]
	      << "[ceph_options] [gtest_options] <store_path>" << std::endl;
    return 1;
  }
  store_path = string(argv[1]);

  return RUN_ALL_TESTS();
}
