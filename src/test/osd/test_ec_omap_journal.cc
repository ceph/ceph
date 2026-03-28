// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <gtest/gtest.h>
#include "test/unit.cc"

#include "osd/ECOmapJournal.h"

TEST(ecomapjournalentry, equality)
{
  eversion_t version(1, 1);

  ECOmapJournalEntry entry1(version, false, std::nullopt, {});
  ECOmapJournalEntry entry2(version, false, std::nullopt, {});

  // Two journal entries with the same version value are equal
  ASSERT_TRUE(entry1 == entry2);
}

TEST(ecomapjournalentry, inequality)
{
  
  eversion_t version1(1, 1);
  eversion_t version2(1, 2);
  eversion_t version3(1, 3);

  ECOmapJournalEntry entry1(version1, false, std::nullopt, {});
  ECOmapJournalEntry entry2(version2, false, std::nullopt, {});
  ECOmapJournalEntry entry3(version3, false, std::nullopt, {});

  // Journal entries should be unequal if their versions differ
  ASSERT_TRUE(entry1 != entry2);
  ASSERT_TRUE(entry2 != entry3);
  ASSERT_TRUE(entry1 != entry3);
}

TEST(ecomapjournalentry, attributes_retained)
{
  eversion_t version(1, 1);
  bool clear_omap = true;
  ceph::buffer::list omap_val_bl, omap_header_bl, omap_map_bl, keys_to_remove_bl;
  const std::string omap_header = "this is the header";
  encode(omap_header, omap_header_bl);
  const std::string omap_key_1 = "omap_key_1_palomino";
  const std::string omap_key_2 = "omap_key_2_chestnut";
  const std::string omap_key_3 = "omap_key_3_bay";
  const std::string omap_value = "omap_value_1_horse";
  encode(omap_value, omap_val_bl);
  std::map<std::string, bufferlist> omap_map = {
    {omap_key_1.c_str(), omap_val_bl},
    {omap_key_2.c_str(), omap_val_bl},
    {omap_key_3.c_str(), omap_val_bl}
  };
  encode(omap_map, omap_map_bl);
  std::set<std::string> keys_to_remove = {omap_key_2};
  encode(keys_to_remove, keys_to_remove_bl);
  std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> omap_updates = {
    {OmapUpdateType::Insert, omap_map_bl},
    {OmapUpdateType::Remove, keys_to_remove_bl}
  };

  ECOmapJournalEntry entry(version, clear_omap, omap_header_bl, omap_updates);

  // Attributes should be retained correctly
  ASSERT_TRUE(entry.version == version);
  ASSERT_TRUE(entry.clear_omap == clear_omap);
  ASSERT_TRUE(*entry.omap_header == omap_header_bl);
  ASSERT_TRUE(entry.omap_updates == omap_updates);
}

TEST(ecomapjournal, new_journal_starts_empty)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_key", CEPH_NOSNAP, 1, 0, "test_namespace");

  // A new journal should start empty
  ASSERT_EQ(0u, journal.entries_size(test_hoid));
}

TEST(ecomapjournal, add_entry)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_key2", CEPH_NOSNAP, 1, 0, "test_namespace");

  ECOmapJournalEntry entry1(eversion_t(1, 1), false, std::nullopt, {});
  journal.add_entry(test_hoid, entry1);

  // The journal should contain the added entry
  ASSERT_EQ(1u, journal.entries_size(test_hoid));
  ASSERT_TRUE(journal.begin_entries(test_hoid)->version == entry1.version);
}

TEST(ecomapjournal, remove_entry)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_key3", CEPH_NOSNAP, 1, 0, "test_namespace");

  ECOmapJournalEntry entry1(eversion_t(1, 1), false, std::nullopt, {});
  ECOmapJournalEntry entry2(eversion_t(1, 2), false, std::nullopt, {});
  ECOmapJournalEntry entry3(eversion_t(1, 3), false, std::nullopt, {});
  journal.add_entry(test_hoid, entry1);
  journal.add_entry(test_hoid, entry2);
  journal.add_entry(test_hoid, entry3);
  bool res = journal.remove_entry(test_hoid, entry1);
  ASSERT_TRUE(res);

  // The journal should have 2 entries in it after removal
  ASSERT_EQ(2u, journal.entries_size(test_hoid));
  ASSERT_TRUE(journal.begin_entries(test_hoid)->version == entry2.version);
  ASSERT_TRUE((++journal.begin_entries(test_hoid))->version == entry3.version);
}

TEST(ecomapjournal, remove_entry_by_version)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_key4", CEPH_NOSNAP, 1, 0, "test_namespace");

  ECOmapJournalEntry entry1(eversion_t(1, 1), false, std::nullopt, {});
  ECOmapJournalEntry entry2(eversion_t(1, 2), false, std::nullopt, {});
  ECOmapJournalEntry entry3(eversion_t(1, 3), false, std::nullopt, {});
  journal.add_entry(test_hoid, entry1);
  journal.add_entry(test_hoid, entry2);
  journal.add_entry(test_hoid, entry3);
  bool res = journal.remove_entry_by_version(test_hoid, entry2.version);
  ASSERT_TRUE(res);

  // The journal should have 2 entries in it after removal
  ASSERT_EQ(2u, journal.entries_size(test_hoid));
  ASSERT_TRUE(journal.begin_entries(test_hoid)->version == entry1.version);
  ASSERT_TRUE((++journal.begin_entries(test_hoid))->version == entry3.version);
}

TEST(ecomapjournal, clear_one_journal)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_key5", CEPH_NOSNAP, 1, 0, "test_namespace");

  ECOmapJournalEntry entry1(eversion_t(1, 1), false, std::nullopt, {});
  ECOmapJournalEntry entry2(eversion_t(1, 2), false, std::nullopt, {});
  journal.add_entry(test_hoid, entry1);
  journal.add_entry(test_hoid, entry2);
  journal.clear(test_hoid);

  // The journal should be empty after clearing
  ASSERT_EQ(0u, journal.entries_size(test_hoid));
}

TEST(ecomapjournal, clear_all_journals)
{
  ECOmapJournal journal;
  const hobject_t test_hoid1("test_key6", CEPH_NOSNAP, 1, 0, "test_namespace");
  const hobject_t test_hoid2("test_key7", CEPH_NOSNAP, 1, 0, "test_namespace");

  ECOmapJournalEntry entry1(eversion_t(1, 1), false, std::nullopt, {});
  ECOmapJournalEntry entry2(eversion_t(1, 2), false, std::nullopt, {});
  journal.add_entry(test_hoid1, entry1);
  journal.add_entry(test_hoid2, entry2);
  journal.clear_all();

  // Both journals should be empty after clearing all
  ASSERT_EQ(0u, journal.entries_size(test_hoid1));
  ASSERT_EQ(0u, journal.entries_size(test_hoid2));
}

TEST(ecomapjournal, remove_bad_entry)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_key6", CEPH_NOSNAP, 1, 0, "test_namespace");

  ECOmapJournalEntry entry1(eversion_t(1, 1), false, std::nullopt, {});
  ECOmapJournalEntry entry2(eversion_t(1, 2), false, std::nullopt, {});
  journal.add_entry(test_hoid, entry1);

  // Attempting to remove an entry not in the journal should fail
  bool res = journal.remove_entry(test_hoid, entry2);
  ASSERT_FALSE(res);

  // The journal should still have 1 entry in it after failed removal
  ASSERT_EQ(1u, journal.entries_size(test_hoid));
  ASSERT_TRUE(journal.begin_entries(test_hoid)->version == entry1.version);
}

TEST(ecomapjournal, remove_bad_entry_by_version)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_key7", CEPH_NOSNAP, 1, 0, "test_namespace");

  ECOmapJournalEntry entry1(eversion_t(1, 1), false, std::nullopt, {});
  ECOmapJournalEntry entry2(eversion_t(1, 2), false, std::nullopt, {});
  journal.add_entry(test_hoid, entry1);

  // Attempting to remove an entry not in the journal should fail
  bool res = journal.remove_entry_by_version(test_hoid, entry2.version);
  ASSERT_FALSE(res);

  // The journal should still have 1 entry in it after failed removal
  ASSERT_EQ(1u, journal.entries_size(test_hoid));
  ASSERT_TRUE(journal.begin_entries(test_hoid)->version == entry1.version);
}

TEST(ecomapjournal, get_value_updates_no_updates)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_key7", CEPH_NOSNAP, 1, 0, "test_namespace");

  // The journal should return empty updates for an object with no entries
  auto [update_map, removed_ranges] = journal.get_value_updates(test_hoid);
  ASSERT_TRUE(update_map.empty());
  ASSERT_TRUE(removed_ranges.empty());
}

TEST(ecomapjournal, get_value_updates_multiple_updates)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_key7", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  std::map<std::string, ceph::buffer::list> key_value;
  ceph::buffer::list val_bl;
  encode("my_value", val_bl);
  key_value["key_1"] = val_bl;
  ceph::buffer::list map_bl;
  encode(key_value, map_bl);

  std::string range_begin = "key_1";
  std::string range_end = "key_2";
  
  ceph::buffer::list range_bl;
  encode(range_begin, range_bl);
  encode(range_end, range_bl);
  
  ECOmapJournalEntry entry1(
    eversion_t(1, 1), false, std::nullopt,
    {std::pair(OmapUpdateType::RemoveRange, range_bl)}
  );
  ECOmapJournalEntry entry2(
    eversion_t(1, 2), true, std::nullopt, 
    {std::pair(OmapUpdateType::Insert, map_bl)}
  );
  journal.add_entry(test_hoid, entry1);
  journal.add_entry(test_hoid, entry2);

  // The journal should return the combined updates for the object
  auto [update_map, removed_ranges] = journal.get_value_updates(test_hoid);

  ASSERT_TRUE(!update_map.empty());
  ASSERT_TRUE(!removed_ranges.empty());

  // Removed ranges should contain the clear from entry2
  ASSERT_TRUE(removed_ranges.contains(""));
  ASSERT_TRUE(!removed_ranges[""].has_value());

  // Update map should contain key1 inserted in entry2
  auto it = update_map.find("key_1");
  ASSERT_TRUE(it != update_map.end());
  ASSERT_TRUE(it->second.value.has_value());
}

TEST(ecomapjournal, get_updated_header_no_entries)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_key7", CEPH_NOSNAP, 1, 0, "test_namespace");

  // The journal should return no updated header for an object with no entries
  std::optional<ceph::buffer::list> header = journal.get_updated_header(test_hoid);
  ASSERT_TRUE(!header.has_value());
}

TEST(ecomapjournal, get_updated_header_single_entry)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_key7", CEPH_NOSNAP, 1, 0, "test_namespace");
  const std::string omap_header_str = "this is the header";
  ceph::buffer::list omap_header_bl;
  encode(omap_header_str, omap_header_bl);

  ECOmapJournalEntry entry1(eversion_t(1, 1), false, omap_header_bl, {});
  journal.add_entry(test_hoid, entry1);

  // The journal should return the updated header for the object
  std::optional<ceph::buffer::list> header = journal.get_updated_header(test_hoid);
  ASSERT_TRUE(header.has_value());
  ASSERT_TRUE(*header == omap_header_bl);
}

TEST(ecomapjournal, get_updated_header_multiple_entries)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_key7", CEPH_NOSNAP, 1, 0, "test_namespace");
  const std::string omap_header_str_1 = "this is the header";
  const std::string omap_header_str_2 = "this is the new header";
  ceph::buffer::list omap_header_bl_1, omap_header_bl_2;
  encode(omap_header_str_1, omap_header_bl_1);
  encode(omap_header_str_2, omap_header_bl_2);

  ECOmapJournalEntry entry1(eversion_t(1, 1), false, omap_header_bl_1, {});
  ECOmapJournalEntry entry2(eversion_t(1, 2), false, omap_header_bl_2, {});
  journal.add_entry(test_hoid, entry1);
  journal.add_entry(test_hoid, entry2);

  // The journal should return the second updated header for the object
  std::optional<ceph::buffer::list> header = journal.get_updated_header(test_hoid);
  ASSERT_TRUE(header.has_value());
  ASSERT_TRUE(*header == omap_header_bl_2);
}

std::string make_key(int i) {
  std::stringstream ss;
  ss << "key_" << std::setw(3) << std::setfill('0') << i;
  return ss.str();
}

// Encodes a map of keys for OmapUpdateType::Insert
ceph::buffer::list encode_map(const std::map<std::string, ceph::buffer::list>& keys) {
  ceph::buffer::list bl;
  encode(keys, bl);
  return bl;
}

// Encodes start/end strings for OmapUpdateType::RemoveRange
ceph::buffer::list encode_range(const std::string& start, const std::string& end) {
  ceph::buffer::list bl;
  encode(start, bl);
  encode(end, bl);
  return bl;
}

// Create an insert entry
ECOmapJournalEntry create_insert_entry(const eversion_t v, const int start_key, const int end_key) {
  std::map<std::string, ceph::buffer::list> key_map;
  for (int i = start_key; i <= end_key; ++i) {
    ceph::buffer::list val_bl;
    val_bl.append("val");
    key_map[make_key(i)] = val_bl;
  }

  std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> updates;
  updates.push_back({OmapUpdateType::Insert, encode_map(key_map)});

  return ECOmapJournalEntry(v, false, std::nullopt, updates);
}

TEST(ecomapjournal, RemoveOneRange) {
  ECOmapJournal journal;
  const hobject_t hoid("obj_rm_range", CEPH_NOSNAP, 1, 0, "nspace");

  journal.add_entry(hoid, create_insert_entry(eversion_t(1, 1), 1, 100));

  std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> rm_updates;
  rm_updates.push_back({
    OmapUpdateType::RemoveRange,
    encode_range(make_key(20), make_key(41))
  });
  journal.add_entry(hoid, ECOmapJournalEntry(eversion_t(1, 2), false, std::nullopt, rm_updates));

  auto [updates, ranges] = journal.get_value_updates(hoid);
  ASSERT_EQ(1u, ranges.size());
  auto it = ranges.begin();
  EXPECT_EQ(make_key(20), it->first);
  ASSERT_TRUE(it->second.has_value());
  EXPECT_EQ(make_key(41), *it->second);
}

TEST(ecomapjournal, RemoveTwoRanges) {
  ECOmapJournal journal;
  const hobject_t hoid("obj_rm_two_ranges", CEPH_NOSNAP, 1, 0, "nspace");

  journal.add_entry(hoid, create_insert_entry(eversion_t(1, 1), 1, 100));

  std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> rm_updates;
  rm_updates.push_back({OmapUpdateType::RemoveRange, encode_range(make_key(20), make_key(41))});
  rm_updates.push_back({OmapUpdateType::RemoveRange, encode_range(make_key(60), make_key(81))});
  journal.add_entry(hoid, ECOmapJournalEntry(eversion_t(1, 2), false, std::nullopt, rm_updates));

  auto [updates, ranges] = journal.get_value_updates(hoid);
  ASSERT_EQ(2u, ranges.size());
  auto it = ranges.begin();
  EXPECT_EQ(make_key(20), it->first);
  EXPECT_EQ(make_key(41), *it->second);
  ++it;
  EXPECT_EQ(make_key(60), it->first);
  EXPECT_EQ(make_key(81), *it->second);
}

TEST(ecomapjournal, OverlappingRanges) {
  ECOmapJournal journal;
  const hobject_t hoid("obj_overlap", CEPH_NOSNAP, 1, 0, "nspace");

  journal.add_entry(hoid, create_insert_entry(eversion_t(1, 1), 1, 100));

  std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> rm_updates;
  rm_updates.push_back({OmapUpdateType::RemoveRange, encode_range(make_key(10), make_key(21))});
  rm_updates.push_back({OmapUpdateType::RemoveRange, encode_range(make_key(15), make_key(31))});
  journal.add_entry(hoid, ECOmapJournalEntry(eversion_t(1, 2), false, std::nullopt, rm_updates));

  auto [updates, ranges] = journal.get_value_updates(hoid);
  ASSERT_EQ(1u, ranges.size());
  EXPECT_EQ(make_key(10), ranges.begin()->first);
  EXPECT_EQ(make_key(31), *ranges.begin()->second);
}

TEST(ecomapjournal, RemoveWithNullStart) {
  ECOmapJournal journal;
  const hobject_t hoid("obj_null_start", CEPH_NOSNAP, 1, 0, "nspace");

  journal.add_entry(hoid, create_insert_entry(eversion_t(1, 1), 1, 100));

  std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> rm_updates;
  rm_updates.push_back({OmapUpdateType::RemoveRange, encode_range("", make_key(21))});
  journal.add_entry(hoid, ECOmapJournalEntry(eversion_t(1, 2), false, std::nullopt, rm_updates));

  auto [updates, ranges] = journal.get_value_updates(hoid);
  ASSERT_EQ(1u, ranges.size());
  EXPECT_EQ("", ranges.begin()->first);
  EXPECT_EQ(make_key(21), *ranges.begin()->second);
}

TEST(ecomapjournal, RemoveRangeThenInsert) {
  ECOmapJournal journal;
  const hobject_t hoid("obj_rm_ins", CEPH_NOSNAP, 1, 0, "nspace");

  journal.add_entry(hoid, create_insert_entry(eversion_t(1, 1), 1, 100));

  std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> ops;
  ops.push_back({OmapUpdateType::RemoveRange, encode_range(make_key(30), make_key(61))});
  journal.add_entry(hoid, ECOmapJournalEntry(eversion_t(1, 2), false, std::nullopt, ops));

  journal.add_entry(hoid, create_insert_entry(eversion_t(1, 3), 45, 45));

  auto [updates, ranges] = journal.get_value_updates(hoid);
  ASSERT_EQ(1u, ranges.size());
  EXPECT_EQ(make_key(30), ranges.begin()->first);
  EXPECT_EQ(make_key(61), *ranges.begin()->second);
  ASSERT_TRUE(updates.count(make_key(45)));
  EXPECT_TRUE(updates.at(make_key(45)).value.has_value());
}

TEST(ecomapjournal, RemoveInsertRemove) {
  ECOmapJournal journal;
  const hobject_t hoid("obj_rm_ins_rm", CEPH_NOSNAP, 1, 0, "nspace");

  journal.add_entry(hoid, create_insert_entry(eversion_t(1, 1), 1, 100));

  std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> ops1;
  ops1.push_back({OmapUpdateType::RemoveRange, encode_range(make_key(30), make_key(61))});
  journal.add_entry(hoid, ECOmapJournalEntry(eversion_t(1, 2), false, std::nullopt, ops1));

  journal.add_entry(hoid, create_insert_entry(eversion_t(1, 3), 45, 45));

  std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> ops2;
  ops2.push_back({OmapUpdateType::RemoveRange, encode_range(make_key(30), make_key(61))});
  journal.add_entry(hoid, ECOmapJournalEntry(eversion_t(1, 4), false, std::nullopt, ops2));

  auto [updates, ranges] = journal.get_value_updates(hoid);
  ASSERT_EQ(1u, ranges.size());
  EXPECT_EQ(make_key(30), ranges.begin()->first);
  EXPECT_EQ(make_key(61), *ranges.begin()->second);
  ASSERT_TRUE(updates.count(make_key(45)));
  EXPECT_FALSE(updates.at(make_key(45)).value.has_value());
}

TEST(ecomapjournal, AdjacentRanges) {
  ECOmapJournal journal;
  const hobject_t hoid("obj_adjacent", CEPH_NOSNAP, 1, 0, "nspace");

  journal.add_entry(hoid, create_insert_entry(eversion_t(1, 1), 1, 100));

  std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> rm_updates;
  rm_updates.push_back({OmapUpdateType::RemoveRange, encode_range(make_key(10), make_key(20))});
  rm_updates.push_back({OmapUpdateType::RemoveRange, encode_range(make_key(20), make_key(30))});
  journal.add_entry(hoid, ECOmapJournalEntry(eversion_t(1, 2), false, std::nullopt, rm_updates));

  auto [updates, ranges] = journal.get_value_updates(hoid);
  EXPECT_EQ(make_key(10), ranges.begin()->first);
  EXPECT_EQ(make_key(30), *ranges.begin()->second);
}

TEST(ecomapjournal, ClearOmap) {
  ECOmapJournal journal;
  const hobject_t hoid("obj_clear", CEPH_NOSNAP, 1, 0, "nspace");

  ceph::buffer::list header_bl;
  header_bl.append("header_v1");
  auto entry_ins = create_insert_entry(eversion_t(1, 1), 1, 100);
  entry_ins.omap_header = header_bl;
  journal.add_entry(hoid, entry_ins);

  ECOmapJournalEntry entry_clear(eversion_t(1, 2), true, std::nullopt, {});
  journal.add_entry(hoid, entry_clear);

  journal.add_entry(hoid, create_insert_entry(eversion_t(1, 3), 50, 50));

  auto [updates, ranges] = journal.get_value_updates(hoid);
  auto header = journal.get_updated_header(hoid);
  ASSERT_TRUE(updates.contains(make_key(50)));
  EXPECT_TRUE(updates.at(make_key(50)).value.has_value());
  ASSERT_TRUE(header.has_value());
  EXPECT_EQ(0u, header->length());
  ASSERT_EQ(1u, ranges.size());
  EXPECT_TRUE(ranges.contains(""));
  EXPECT_TRUE(ranges.at("") == std::nullopt);
}

TEST(ecomapjournal, append_delete_clears_maps)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_delete", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  // Add some journal entries
  ECOmapJournalEntry entry1(eversion_t(1, 1), false, std::nullopt, {});
  ECOmapJournalEntry entry2(eversion_t(1, 2), false, std::nullopt, {});
  journal.add_entry(test_hoid, entry1);
  journal.add_entry(test_hoid, entry2);
  
  ASSERT_EQ(2u, journal.entries_size(test_hoid));
  
  // Delete the object
  journal.append_delete(test_hoid, 5, false);
  
  // All entries should be cleared
  ASSERT_EQ(0u, journal.entries_size(test_hoid));
}

TEST(ecomapjournal, append_delete_adds_to_object_state_map)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_delete2", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  // Delete the object at version 5
  journal.append_delete(test_hoid, 5, false);
  
  // Generation should be the delete version
  auto [gen, lost] = journal.get_generation(test_hoid);
  ASSERT_EQ(5u, gen);
  ASSERT_FALSE(lost);
}

TEST(ecomapjournal, append_delete_multiple_versions)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_delete3", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  // Delete the object at multiple versions
  journal.append_delete(test_hoid, 5, false);
  journal.append_delete(test_hoid, 8, false);
  journal.append_delete(test_hoid, 3, false);
  
  // Generation should be the lowest delete version
  auto [gen, lost] = journal.get_generation(test_hoid);
  ASSERT_EQ(3u, gen);
  ASSERT_FALSE(lost);
}

TEST(ecomapjournal, append_create_clears_entries)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_create", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  // Add some journal entries
  ECOmapJournalEntry entry1(eversion_t(1, 1), false, std::nullopt, {});
  ECOmapJournalEntry entry2(eversion_t(1, 2), false, std::nullopt, {});
  journal.add_entry(test_hoid, entry1);
  journal.add_entry(test_hoid, entry2);
  
  ASSERT_EQ(2u, journal.entries_size(test_hoid));
  
  // Create the object
  journal.append_create(test_hoid);
  
  // All entries should be cleared
  ASSERT_EQ(0u, journal.entries_size(test_hoid));
}

TEST(ecomapjournal, trim_delete_removes_version)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_trim_delete", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  // Delete the object at multiple versions
  journal.append_delete(test_hoid, 5, false);
  journal.append_delete(test_hoid, 8, false);
  journal.append_delete(test_hoid, 3, false);
  
  auto [gen1, lost1] = journal.get_generation(test_hoid);
  ASSERT_EQ(3u, gen1);
  ASSERT_FALSE(lost1);
  
  // Trim the lowest version
  journal.trim_delete(test_hoid, 3);
  
  // Generation should now be the next lowest version
  auto [gen2, lost2] = journal.get_generation(test_hoid);
  ASSERT_EQ(5u, gen2);
  ASSERT_FALSE(lost2);
}

TEST(ecomapjournal, trim_delete_last_version)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_trim_last", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  // Delete the object at one version
  journal.append_delete(test_hoid, 5, false);
  
  // Trim the only version
  journal.trim_delete(test_hoid, 5);
  
  // Generation should be NO_GEN
  auto [gen, lost] = journal.get_generation(test_hoid);
  ASSERT_EQ(UINT64_MAX, gen);
  ASSERT_FALSE(lost);
}

TEST(ecomapjournal, get_generation_no_deletes)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_no_delete", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  // Generation should be NO_GEN
  auto [gen, lost] = journal.get_generation(test_hoid);
  ASSERT_EQ(UINT64_MAX, gen);
  ASSERT_FALSE(lost);
}

TEST(ecomapjournal, append_delete_with_lost_flag)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_lost", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  // Delete with lost_delete = true
  journal.append_delete(test_hoid, 5, true);
  auto [gen1, lost1] = journal.get_generation(test_hoid);
  ASSERT_EQ(5u, gen1);
  ASSERT_TRUE(lost1);
  
  // Delete with lost_delete = false
  journal.append_delete(test_hoid, 8, false);
  auto [gen2, lost2] = journal.get_generation(test_hoid);
  ASSERT_EQ(5u, gen2);
  ASSERT_TRUE(lost2);
}

TEST(ecomapjournal, delete_then_create_workflow)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_delete_create", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  // Add entries
  journal.add_entry(test_hoid, create_insert_entry(eversion_t(1, 1), 1, 10));
  ASSERT_EQ(1u, journal.entries_size(test_hoid));
  
  // Delete object
  journal.append_delete(test_hoid, 5, false);
  ASSERT_EQ(0u, journal.entries_size(test_hoid));
  
  // Add more entries after delete
  journal.add_entry(test_hoid, create_insert_entry(eversion_t(1, 6), 20, 30));
  ASSERT_EQ(1u, journal.entries_size(test_hoid));
  
  // Create object (simulating recreation)
  journal.append_create(test_hoid);
  ASSERT_EQ(0u, journal.entries_size(test_hoid));
  
  // Object should still have the delete version tracked
  auto [gen, lost] = journal.get_generation(test_hoid);
  ASSERT_EQ(5u, gen);
  ASSERT_FALSE(lost);
}

TEST(ecomapjournal, multiple_objects_state_tracking)
{
  ECOmapJournal journal;
  const hobject_t hoid1("obj1", CEPH_NOSNAP, 1, 0, "ns");
  const hobject_t hoid2("obj2", CEPH_NOSNAP, 1, 0, "ns");
  const hobject_t hoid3("obj3", CEPH_NOSNAP, 1, 0, "ns");
  
  // Delete different objects at different versions
  journal.append_delete(hoid1, 10, false);
  journal.append_delete(hoid2, 5, false);
  journal.append_delete(hoid3, 15, false);
  
  auto [gen1, lost1] = journal.get_generation(hoid1);
  ASSERT_EQ(10u, gen1);
  ASSERT_FALSE(lost1);
  auto [gen2, lost2] = journal.get_generation(hoid2);
  ASSERT_EQ(5u, gen2);
  ASSERT_FALSE(lost2);
  auto [gen3, lost3] = journal.get_generation(hoid3);
  ASSERT_EQ(15u, gen3);
  ASSERT_FALSE(lost3);
  
  // Trim one object's delete
  journal.trim_delete(hoid2, 5);
  
  // Only hoid2 should have NO_GEN
  auto [gen1b, lost1b] = journal.get_generation(hoid1);
  ASSERT_EQ(10u, gen1b);
  ASSERT_FALSE(lost1b);
  auto [gen2b, lost2b] = journal.get_generation(hoid2);
  ASSERT_EQ(UINT64_MAX, gen2b);
  ASSERT_FALSE(lost2b);
  auto [gen3b, lost3b] = journal.get_generation(hoid3);
  ASSERT_EQ(15u, gen3b);
  ASSERT_FALSE(lost3b);
}

TEST(ecomapjournal, append_delete_clears_processed_entries)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_processed", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  // Add entries and process them
  journal.add_entry(test_hoid, create_insert_entry(eversion_t(1, 1), 1, 10));
  auto [updates, ranges] = journal.get_value_updates(test_hoid);
  
  // Verify entries were processed (entries_size should be 0 after processing)
  ASSERT_EQ(0u, journal.entries_size(test_hoid));
  ASSERT_FALSE(updates.empty());
  
  // Delete the object - should clear processed entries too
  journal.append_delete(test_hoid, 5, false);
  
  // Get updates again - should be empty
  auto [updates2, ranges2] = journal.get_value_updates(test_hoid);
  ASSERT_TRUE(updates2.empty());
  ASSERT_TRUE(ranges2.empty());
}

TEST(ecomapjournal, generation_with_whiteout)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_whiteout", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  // Simulate whiteout by deleting
  journal.append_delete(test_hoid, 10, false);
  
  auto [gen1, lost1] = journal.get_generation(test_hoid);
  ASSERT_EQ(10u, gen1);
  ASSERT_FALSE(lost1);
  
  // Add another delete (could represent another whiteout)
  journal.append_delete(test_hoid, 12, false);
  
  // Generation should still be the lowest
  auto [gen2, lost2] = journal.get_generation(test_hoid);
  ASSERT_EQ(10u, gen2);
  ASSERT_FALSE(lost2);
}

TEST(ecomapjournal, append_whiteout_clears_entries)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_whiteout_entries", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  // Add some journal entries
  ECOmapJournalEntry entry1(eversion_t(1, 1), false, std::nullopt, {});
  ECOmapJournalEntry entry2(eversion_t(1, 2), false, std::nullopt, {});
  journal.add_entry(test_hoid, entry1);
  journal.add_entry(test_hoid, entry2);
  
  ASSERT_EQ(2u, journal.entries_size(test_hoid));
  
  // Whiteout the object
  journal.append_whiteout(test_hoid);
  
  // All entries should be cleared
  ASSERT_EQ(0u, journal.entries_size(test_hoid));
}

TEST(ecomapjournal, append_whiteout_clears_processed_entries)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_whiteout_processed", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  // Add entries and process them
  journal.add_entry(test_hoid, create_insert_entry(eversion_t(1, 1), 1, 10));
  auto [updates, ranges] = journal.get_value_updates(test_hoid);
  
  // Verify entries were processed
  ASSERT_EQ(0u, journal.entries_size(test_hoid));
  ASSERT_FALSE(updates.empty());
  
  // Whiteout the object - should clear processed entries too
  journal.append_whiteout(test_hoid);
  
  // Get updates again - should be empty
  auto [updates2, ranges2] = journal.get_value_updates(test_hoid);
  ASSERT_TRUE(updates2.empty());
  ASSERT_TRUE(ranges2.empty());
}

TEST(ecomapjournal, append_whiteout_clears_header)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_whiteout_header", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  const std::string omap_header_str = "test header";
  ceph::buffer::list omap_header_bl;
  encode(omap_header_str, omap_header_bl);
  
  ECOmapJournalEntry entry1(eversion_t(1, 1), false, omap_header_bl, {});
  journal.add_entry(test_hoid, entry1);
  
  // Verify header exists
  std::optional<ceph::buffer::list> header = journal.get_updated_header(test_hoid);
  ASSERT_TRUE(header.has_value());
  
  // Whiteout the object
  journal.append_whiteout(test_hoid);
  
  // Header should be cleared
  std::optional<ceph::buffer::list> header2 = journal.get_updated_header(test_hoid);
  ASSERT_FALSE(header2.has_value());
}

TEST(ecomapjournal, append_whiteout_on_nonexistent_object)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_whiteout_nonexist", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  // Whiteout on non-existent object should not crash
  journal.append_whiteout(test_hoid);
  
  // Should still be empty
  ASSERT_EQ(0u, journal.entries_size(test_hoid));
}

TEST(ecomapjournal, append_whiteout_followed_by_new_entries)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_whiteout_new", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  // Add entries
  journal.add_entry(test_hoid, create_insert_entry(eversion_t(1, 1), 1, 10));
  ASSERT_EQ(1u, journal.entries_size(test_hoid));
  
  // Whiteout
  journal.append_whiteout(test_hoid);
  ASSERT_EQ(0u, journal.entries_size(test_hoid));
  
  // Add new entries after whiteout
  journal.add_entry(test_hoid, create_insert_entry(eversion_t(1, 2), 20, 30));
  ASSERT_EQ(1u, journal.entries_size(test_hoid));
  
  // New entries should be present
  auto [updates, ranges] = journal.get_value_updates(test_hoid);
  ASSERT_FALSE(updates.empty());
  ASSERT_TRUE(updates.contains(make_key(20)));
}

TEST(ecomapjournal, append_whiteout_does_not_affect_generation)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_whiteout_gen", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  // Delete object to set generation
  journal.append_delete(test_hoid, 5, false);
  auto [gen1, lost1] = journal.get_generation(test_hoid);
  ASSERT_EQ(5u, gen1);
  
  // Add entries
  journal.add_entry(test_hoid, create_insert_entry(eversion_t(1, 6), 1, 10));
  
  // Whiteout should clear entries but not generation
  journal.append_whiteout(test_hoid);
  
  auto [gen2, lost2] = journal.get_generation(test_hoid);
  ASSERT_EQ(5u, gen2);
  ASSERT_FALSE(lost2);
}

TEST(ecomapjournal, whiteout_create_delete_workflow)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_whiteout_workflow", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  // Add entries
  journal.add_entry(test_hoid, create_insert_entry(eversion_t(1, 1), 1, 10));
  ASSERT_EQ(1u, journal.entries_size(test_hoid));
  
  // Whiteout
  journal.append_whiteout(test_hoid);
  ASSERT_EQ(0u, journal.entries_size(test_hoid));
  
  // Create
  journal.append_create(test_hoid);
  ASSERT_EQ(0u, journal.entries_size(test_hoid));
  
  // Add more entries
  journal.add_entry(test_hoid, create_insert_entry(eversion_t(1, 2), 20, 30));
  ASSERT_EQ(1u, journal.entries_size(test_hoid));
  
  // Delete
  journal.append_delete(test_hoid, 10, false);
  ASSERT_EQ(0u, journal.entries_size(test_hoid));
  
  auto [gen, lost] = journal.get_generation(test_hoid);
  ASSERT_EQ(10u, gen);
}

TEST(ecomapvalue, update_value_with_data)
{
  eversion_t v1(1, 1);
  ceph::buffer::list bl1;
  bl1.append("value1");
  
  ECOmapValue value(v1, bl1);
  ASSERT_EQ(v1, value.version);
  ASSERT_TRUE(value.value.has_value());
  
  eversion_t v2(1, 2);
  ceph::buffer::list bl2;
  bl2.append("value2");
  
  value.update_value(v2, bl2);
  ASSERT_EQ(v2, value.version);
  ASSERT_TRUE(value.value.has_value());
  ASSERT_EQ(bl2, *value.value);
}

TEST(ecomapvalue, update_value_to_nullopt)
{
  eversion_t v1(1, 1);
  ceph::buffer::list bl1;
  bl1.append("value1");
  
  ECOmapValue value(v1, bl1);
  ASSERT_TRUE(value.value.has_value());
  
  eversion_t v2(1, 2);
  value.update_value(v2, std::nullopt);
  
  ASSERT_EQ(v2, value.version);
  ASSERT_FALSE(value.value.has_value());
}

TEST(ecomapvalue, multiple_updates)
{
  eversion_t v1(1, 1);
  ECOmapValue value(v1, std::nullopt);
  
  for (int i = 2; i <= 5; ++i) {
    eversion_t v(1, i);
    ceph::buffer::list bl;
    bl.append(std::to_string(i));
    value.update_value(v, bl);
    ASSERT_EQ(v, value.version);
  }
  
  ASSERT_EQ(eversion_t(1, 5), value.version);
}

TEST(ecomapremovedranges, add_range_non_overlapping)
{
  ECOmapRemovedRanges ranges(eversion_t(1, 1));
  
  ranges.add_range("key_010", "key_020");
  ranges.add_range("key_030", "key_040");
  
  ASSERT_EQ(2u, ranges.ranges.size());
}

TEST(ecomapremovedranges, add_range_overlapping)
{
  ECOmapRemovedRanges ranges(eversion_t(1, 1));
  
  ranges.add_range("key_010", "key_030");
  ranges.add_range("key_020", "key_040");
  
  // Should merge into single range
  ASSERT_EQ(1u, ranges.ranges.size());
  EXPECT_EQ("key_010", ranges.ranges.front().first);
  EXPECT_EQ("key_040", *ranges.ranges.front().second);
}

TEST(ecomapremovedranges, add_range_adjacent)
{
  ECOmapRemovedRanges ranges(eversion_t(1, 1));
  
  ranges.add_range("key_010", "key_020");
  ranges.add_range("key_020", "key_030");
  
  // Should merge into single range
  ASSERT_EQ(1u, ranges.ranges.size());
  EXPECT_EQ("key_010", ranges.ranges.front().first);
  EXPECT_EQ("key_030", *ranges.ranges.front().second);
}

TEST(ecomapremovedranges, add_range_with_nullopt_end)
{
  ECOmapRemovedRanges ranges(eversion_t(1, 1));
  
  ranges.add_range("key_010", "key_020");
  ranges.add_range("key_015", std::nullopt);
  
  // Should merge and extend to end
  ASSERT_EQ(1u, ranges.ranges.size());
  EXPECT_EQ("key_010", ranges.ranges.front().first);
  EXPECT_FALSE(ranges.ranges.front().second.has_value());
}

TEST(ecomapremovedranges, clear_omap_test)
{
  ECOmapRemovedRanges ranges(eversion_t(1, 1));
  
  ranges.add_range("key_010", "key_020");
  ranges.add_range("key_030", "key_040");
  ASSERT_EQ(2u, ranges.ranges.size());
  
  ranges.clear_omap();
  
  // Should replace all ranges with single range from "" to nullopt
  ASSERT_EQ(1u, ranges.ranges.size());
  EXPECT_EQ("", ranges.ranges.front().first);
  EXPECT_FALSE(ranges.ranges.front().second.has_value());
}

TEST(ecomapheader, update_header_with_data)
{
  eversion_t v1(1, 1);
  ceph::buffer::list bl1;
  bl1.append("header1");
  
  ECOmapHeader header(v1, bl1);
  ASSERT_EQ(v1, header.version);
  ASSERT_TRUE(header.header.has_value());
  
  eversion_t v2(1, 2);
  ceph::buffer::list bl2;
  bl2.append("header2");
  
  header.update_header(v2, bl2);
  ASSERT_EQ(v2, header.version);
  ASSERT_TRUE(header.header.has_value());
  ASSERT_EQ(bl2, *header.header);
}

TEST(ecomapheader, update_header_to_nullopt)
{
  eversion_t v1(1, 1);
  ceph::buffer::list bl1;
  bl1.append("header1");
  
  ECOmapHeader header(v1, bl1);
  ASSERT_TRUE(header.header.has_value());
  
  eversion_t v2(1, 2);
  header.update_header(v2, std::nullopt);
  
  ASSERT_EQ(v2, header.version);
  ASSERT_FALSE(header.header.has_value());
}

TEST(ecomapheader, default_constructor)
{
  ECOmapHeader header;
  ASSERT_EQ(eversion_t(), header.version);
  ASSERT_FALSE(header.header.has_value());
}

TEST(ecomapjournal, remove_entry_on_empty_journal)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_empty", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  ECOmapJournalEntry entry(eversion_t(1, 1), false, std::nullopt, {});
  bool res = journal.remove_entry(test_hoid, entry);
  
  ASSERT_FALSE(res);
}

TEST(ecomapjournal, remove_entry_by_version_on_empty_journal)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_empty2", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  bool res = journal.remove_entry_by_version(test_hoid, eversion_t(1, 1));
  
  ASSERT_FALSE(res);
}

TEST(ecomapjournal, get_value_updates_after_clear)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_clear_updates", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  journal.add_entry(test_hoid, create_insert_entry(eversion_t(1, 1), 1, 10));
  journal.clear(test_hoid);
  
  auto [updates, ranges] = journal.get_value_updates(test_hoid);
  ASSERT_TRUE(updates.empty());
  ASSERT_TRUE(ranges.empty());
}

TEST(ecomapjournal, get_updated_header_after_clear)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_clear_header", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  ceph::buffer::list header_bl;
  header_bl.append("header");
  ECOmapJournalEntry entry(eversion_t(1, 1), false, header_bl, {});
  journal.add_entry(test_hoid, entry);
  
  journal.clear(test_hoid);
  
  std::optional<ceph::buffer::list> header = journal.get_updated_header(test_hoid);
  ASSERT_FALSE(header.has_value());
}

TEST(ecomapjournal, entries_with_empty_omap_updates)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_empty_updates", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  ECOmapJournalEntry entry(eversion_t(1, 1), false, std::nullopt, {});
  journal.add_entry(test_hoid, entry);
  
  auto [updates, ranges] = journal.get_value_updates(test_hoid);
  ASSERT_TRUE(updates.empty());
  ASSERT_TRUE(ranges.empty());
}

TEST(ecomapjournal, trim_delete_nonexistent_version)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_trim_nonexist", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  journal.append_delete(test_hoid, 5, false);
  
  // Trim non-existent version should not crash
  journal.trim_delete(test_hoid, 10);
  
  auto [gen, lost] = journal.get_generation(test_hoid);
  ASSERT_EQ(5u, gen);
}

TEST(ecomapjournal, get_generation_after_clear_all)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_gen_clear_all", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  journal.append_delete(test_hoid, 5, false);
  journal.clear_all();
  
  auto [gen, lost] = journal.get_generation(test_hoid);
  ASSERT_EQ(UINT64_MAX, gen);
  ASSERT_FALSE(lost);
}

TEST(ecomapjournal, multiple_objects_interleaved_operations)
{
  ECOmapJournal journal;
  const hobject_t hoid1("obj1", CEPH_NOSNAP, 1, 0, "ns");
  const hobject_t hoid2("obj2", CEPH_NOSNAP, 1, 0, "ns");
  const hobject_t hoid3("obj3", CEPH_NOSNAP, 1, 0, "ns");
  
  // Interleave operations on multiple objects
  journal.add_entry(hoid1, create_insert_entry(eversion_t(1, 1), 1, 10));
  journal.add_entry(hoid2, create_insert_entry(eversion_t(1, 1), 20, 30));
  journal.append_delete(hoid3, 5, false);
  journal.add_entry(hoid1, create_insert_entry(eversion_t(1, 2), 11, 20));
  journal.append_whiteout(hoid2);
  
  // Verify each object's state
  ASSERT_EQ(2u, journal.entries_size(hoid1));
  ASSERT_EQ(0u, journal.entries_size(hoid2));
  ASSERT_EQ(0u, journal.entries_size(hoid3));
  
  auto [gen3, lost3] = journal.get_generation(hoid3);
  ASSERT_EQ(5u, gen3);
}

TEST(ecomapjournal, iterator_over_empty_entries)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_iter_empty", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  auto begin = journal.begin_entries(test_hoid);
  auto end = journal.end_entries(test_hoid);
  
  ASSERT_EQ(begin, end);
}

TEST(ecomapjournal, iterator_over_single_entry)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_iter_single", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  ECOmapJournalEntry entry(eversion_t(1, 1), false, std::nullopt, {});
  journal.add_entry(test_hoid, entry);
  
  auto begin = journal.begin_entries(test_hoid);
  auto end = journal.end_entries(test_hoid);
  
  ASSERT_NE(begin, end);
  ASSERT_EQ(entry.version, begin->version);
  ASSERT_EQ(end, ++begin);
}

TEST(ecomapjournal, iterator_over_multiple_entries)
{
  ECOmapJournal journal;
  const hobject_t test_hoid("test_iter_multiple", CEPH_NOSNAP, 1, 0, "test_namespace");
  
  ECOmapJournalEntry entry1(eversion_t(1, 1), false, std::nullopt, {});
  ECOmapJournalEntry entry2(eversion_t(1, 2), false, std::nullopt, {});
  ECOmapJournalEntry entry3(eversion_t(1, 3), false, std::nullopt, {});
  
  journal.add_entry(test_hoid, entry1);
  journal.add_entry(test_hoid, entry2);
  journal.add_entry(test_hoid, entry3);
  
  int count = 0;
  for (auto it = journal.begin_entries(test_hoid); it != journal.end_entries(test_hoid); ++it) {
    count++;
  }
  
  ASSERT_EQ(3, count);
}
