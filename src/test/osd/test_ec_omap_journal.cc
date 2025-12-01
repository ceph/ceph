#include <gtest/gtest.h>
#include "test/unit.cc"

#include "osd/ECOmapJournal.h"

TEST(ecomapjournalentry, construct_without_id)
{
  ECOmapJournalEntry::global_id_counter = 0;
  ASSERT_EQ(0u, ECOmapJournalEntry::global_id_counter);

  ECOmapJournalEntry entry(false, std::nullopt, {});

  // The id should be set to 1 on first construction
  ASSERT_EQ(1u, entry.id);
}

TEST(ecomapjournalentry, construct_with_id)
{
  ECOmapJournalEntry::global_id_counter = 0;
  ASSERT_EQ(0u, ECOmapJournalEntry::global_id_counter);

  ECOmapJournalEntry entry(5, false, std::nullopt, {});

  // The id should be set to the provided value
  ASSERT_EQ(5u, entry.id);
}

TEST(ecomapjournalentry, equality)
{
  ECOmapJournalEntry::global_id_counter = 0;
  ASSERT_EQ(0u, ECOmapJournalEntry::global_id_counter);

  ECOmapJournalEntry entry1(false, std::nullopt, {});
  ECOmapJournalEntry entry2(entry1.id, false, std::nullopt, {});

  // Two journal entries with the same id value are equal
  ASSERT_TRUE(entry1 == entry2);
}

TEST(ecomapjournalentry, id_increments)
{
  ECOmapJournalEntry::global_id_counter = 0;
  ASSERT_EQ(0u, ECOmapJournalEntry::global_id_counter);

  ECOmapJournalEntry entry1(false, std::nullopt, {});
  ECOmapJournalEntry entry2(false, std::nullopt, {});
  ECOmapJournalEntry entry3(false, std::nullopt, {});

  // Each journal entry should have an incremented id value
  ASSERT_EQ(1u, entry1.id);
  ASSERT_EQ(2u, entry2.id);
  ASSERT_EQ(3u, entry3.id);
}

TEST(ecomapjournalentry, inequality)
{
  ECOmapJournalEntry::global_id_counter = 0;
  ASSERT_EQ(0u, ECOmapJournalEntry::global_id_counter);

  ECOmapJournalEntry entry1(false, std::nullopt, {});
  ECOmapJournalEntry entry2(false, std::nullopt, {});
  ECOmapJournalEntry entry3(false, std::nullopt, {});

  // Journal entries should be unequal if their id values differ
  ASSERT_TRUE(entry1 != entry2);
  ASSERT_TRUE(entry2 != entry3);
  ASSERT_TRUE(entry1 != entry3);
}

TEST(ecomapjournalentry, attributes_retained)
{
  ECOmapJournalEntry::global_id_counter = 0;
  ASSERT_EQ(0u, ECOmapJournalEntry::global_id_counter);

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

  ECOmapJournalEntry entry(clear_omap, omap_header_bl, omap_updates);

  // Attributes should be retained correctly
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

  ECOmapJournalEntry entry1(false, std::nullopt, {});
  journal.add_entry(entry1);

  // The journal should contain the added entry
  ASSERT_EQ(1u, journal.size());
  ASSERT_TRUE(journal.begin()->id == entry1.id);
}

TEST(ecomapjournal, remove_entry)
{
  ECOmapJournal journal;

  ECOmapJournalEntry entry1(false, std::nullopt, {});
  ECOmapJournalEntry entry2(false, std::nullopt, {});
  ECOmapJournalEntry entry3(false, std::nullopt, {});
  journal.add_entry(entry1);
  journal.add_entry(entry2);
  journal.add_entry(entry3);
  bool res = journal.remove_entry(entry1);
  ASSERT_TRUE(res);

  // The journal should have 2 entries in it after removal
  ASSERT_EQ(2u, journal.size());
  ASSERT_TRUE(journal.begin()->id == entry2.id);
  ASSERT_TRUE((++journal.begin())->id == entry3.id);
}

TEST(ecomapjournal, remove_entry_by_id)
{
  ECOmapJournal journal;

  ECOmapJournalEntry entry1(false, std::nullopt, {});
  ECOmapJournalEntry entry2(false, std::nullopt, {});
  ECOmapJournalEntry entry3(false, std::nullopt, {});
  journal.add_entry(entry1);
  journal.add_entry(entry2);
  journal.add_entry(entry3);
  bool res = journal.remove_entry_by_id(entry2.id);
  ASSERT_TRUE(res);

  // The journal should have 2 entries in it after removal
  ASSERT_EQ(2u, journal.size());
  ASSERT_TRUE(journal.begin()->id == entry1.id);
  ASSERT_TRUE((++journal.begin())->id == entry3.id);
}

TEST(ecomapjournal, clear_journal)
{
  ECOmapJournal journal;

  ECOmapJournalEntry entry1(false, std::nullopt, {});
  ECOmapJournalEntry entry2(false, std::nullopt, {});
  journal.add_entry(entry1);
  journal.add_entry(entry2);
  journal.clear();

  // The journal should be empty after clearing
  ASSERT_EQ(0u, journal.size());
}

TEST(ecomapjournal, remove_bad_entry)
{
  ECOmapJournal journal;

  ECOmapJournalEntry entry1(false, std::nullopt, {});
  ECOmapJournalEntry entry2(false, std::nullopt, {});
  journal.add_entry(entry1);

  // Attempting to remove an entry not in the journal should fail
  bool res = journal.remove_entry(entry2);
  ASSERT_FALSE(res);

  // The journal should still have 1 entry in it after failed removal
  ASSERT_EQ(1u, journal.size());
  ASSERT_TRUE(journal.begin()->id == entry1.id);
}

TEST(ecomapjournal, remove_bad_entry_by_id)
{
  ECOmapJournal journal;

  ECOmapJournalEntry entry1(false, std::nullopt, {});
  ECOmapJournalEntry entry2(false, std::nullopt, {});
  journal.add_entry(entry1);

  // Attempting to remove an entry not in the journal should fail
  bool res = journal.remove_entry_by_id(entry2.id);
  ASSERT_FALSE(res);

  // The journal should still have 1 entry in it after failed removal
  ASSERT_EQ(1u, journal.size());
  ASSERT_TRUE(journal.begin()->id == entry1.id);
}