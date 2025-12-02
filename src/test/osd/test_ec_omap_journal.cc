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

  // A new journal should start empty
  ASSERT_EQ(0u, journal.size());
}

TEST(ecomapjournal, add_entry)
{
  ECOmapJournal journal;

  ECOmapJournalEntry entry1(eversion_t(1, 1), false, std::nullopt, {});
  journal.add_entry(entry1);

  // The journal should contain the added entry
  ASSERT_EQ(1u, journal.size());
  ASSERT_TRUE(journal.begin()->version == entry1.version);
}

TEST(ecomapjournal, remove_entry)
{
  ECOmapJournal journal;

  ECOmapJournalEntry entry1(eversion_t(1, 1), false, std::nullopt, {});
  ECOmapJournalEntry entry2(eversion_t(1, 2), false, std::nullopt, {});
  ECOmapJournalEntry entry3(eversion_t(1, 3), false, std::nullopt, {});
  journal.add_entry(entry1);
  journal.add_entry(entry2);
  journal.add_entry(entry3);
  bool res = journal.remove_entry(entry1);
  ASSERT_TRUE(res);

  // The journal should have 2 entries in it after removal
  ASSERT_EQ(2u, journal.size());
  ASSERT_TRUE(journal.begin()->version == entry2.version);
  ASSERT_TRUE((++journal.begin())->version == entry3.version);
}

TEST(ecomapjournal, remove_entry_by_version)
{
  ECOmapJournal journal;

  ECOmapJournalEntry entry1(eversion_t(1, 1), false, std::nullopt, {});
  ECOmapJournalEntry entry2(eversion_t(1, 2), false, std::nullopt, {});
  ECOmapJournalEntry entry3(eversion_t(1, 3), false, std::nullopt, {});
  journal.add_entry(entry1);
  journal.add_entry(entry2);
  journal.add_entry(entry3);
  bool res = journal.remove_entry_by_version(entry2.version);
  ASSERT_TRUE(res);

  // The journal should have 2 entries in it after removal
  ASSERT_EQ(2u, journal.size());
  ASSERT_TRUE(journal.begin()->version == entry1.version);
  ASSERT_TRUE((++journal.begin())->version == entry3.version);
}

TEST(ecomapjournal, clear_journal)
{
  ECOmapJournal journal;

  ECOmapJournalEntry entry1(eversion_t(1, 1), false, std::nullopt, {});
  ECOmapJournalEntry entry2(eversion_t(1, 2), false, std::nullopt, {});
  journal.add_entry(entry1);
  journal.add_entry(entry2);
  journal.clear();

  // The journal should be empty after clearing
  ASSERT_EQ(0u, journal.size());
}

TEST(ecomapjournal, remove_bad_entry)
{
  ECOmapJournal journal;

  ECOmapJournalEntry entry1(eversion_t(1, 1), false, std::nullopt, {});
  ECOmapJournalEntry entry2(eversion_t(1, 2), false, std::nullopt, {});
  journal.add_entry(entry1);

  // Attempting to remove an entry not in the journal should fail
  bool res = journal.remove_entry(entry2);
  ASSERT_FALSE(res);

  // The journal should still have 1 entry in it after failed removal
  ASSERT_EQ(1u, journal.size());
  ASSERT_TRUE(journal.begin()->version == entry1.version);
}

TEST(ecomapjournal, remove_bad_entry_by_version)
{
  ECOmapJournal journal;

  ECOmapJournalEntry entry1(eversion_t(1, 1), false, std::nullopt, {});
  ECOmapJournalEntry entry2(eversion_t(1, 2), false, std::nullopt, {});
  journal.add_entry(entry1);

  // Attempting to remove an entry not in the journal should fail
  bool res = journal.remove_entry_by_version(entry2.version);
  ASSERT_FALSE(res);

  // The journal should still have 1 entry in it after failed removal
  ASSERT_EQ(1u, journal.size());
  ASSERT_TRUE(journal.begin()->version == entry1.version);
}