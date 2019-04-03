// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sstream>
#include <list>
#include <gtest/gtest.h>

#include "include/Context.h"
#include "tools/immutable_object_cache/SimplePolicy.h"

using namespace ceph::immutable_obj_cache;

std::string generate_file_name(uint64_t index) {
  std::string pre_name("object_cache_file_");
  std::ostringstream oss;
  oss << index;
  return pre_name + oss.str();
}

class TestSimplePolicy :public ::testing::Test {
public:
  SimplePolicy* m_simple_policy;
  const uint64_t m_cache_size;
  uint64_t m_entry_index;
  std::vector<std::string> m_promoted_lru;
  std::vector<std::string> m_promoting_lru;

  TestSimplePolicy() : m_cache_size(100), m_entry_index(0) {}
  ~TestSimplePolicy() {}
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}
  void SetUp() override {
    m_simple_policy = new SimplePolicy(g_ceph_context, m_cache_size, 128, 0.1);
    // populate 50 entries
    for (uint64_t i = 0; i < m_cache_size / 2; i++, m_entry_index++) {
      insert_entry_into_promoted_lru(generate_file_name(m_entry_index));
    }
  }
  void TearDown() override {
    while(m_promoted_lru.size()) {
      ASSERT_TRUE(m_simple_policy->get_evict_entry() == m_promoted_lru.front());
      m_simple_policy->evict_entry(m_simple_policy->get_evict_entry());
      m_promoted_lru.erase(m_promoted_lru.begin());
    }
    delete m_simple_policy;
  }

  void insert_entry_into_promoted_lru(std::string cache_file_name) {
    ASSERT_EQ(m_cache_size - m_promoted_lru.size(), m_simple_policy->get_free_size());
    ASSERT_EQ(m_promoting_lru.size(), m_simple_policy->get_promoting_entry_num());
    ASSERT_EQ(m_promoted_lru.size(), m_simple_policy->get_promoted_entry_num());
    ASSERT_EQ(OBJ_CACHE_NONE, m_simple_policy->get_status(cache_file_name));

    m_simple_policy->lookup_object(cache_file_name);
    ASSERT_EQ(OBJ_CACHE_SKIP, m_simple_policy->get_status(cache_file_name));
    ASSERT_EQ(m_cache_size - m_promoted_lru.size(), m_simple_policy->get_free_size());
    ASSERT_EQ(m_promoting_lru.size() + 1, m_simple_policy->get_promoting_entry_num());
    ASSERT_EQ(m_promoted_lru.size(), m_simple_policy->get_promoted_entry_num());

    m_simple_policy->update_status(cache_file_name, OBJ_CACHE_PROMOTED, 1);
    m_promoted_lru.push_back(cache_file_name);
    ASSERT_EQ(OBJ_CACHE_PROMOTED, m_simple_policy->get_status(cache_file_name));

    ASSERT_EQ(m_cache_size - m_promoted_lru.size(), m_simple_policy->get_free_size());
    ASSERT_EQ(m_promoting_lru.size(), m_simple_policy->get_promoting_entry_num());
    ASSERT_EQ(m_promoted_lru.size(), m_simple_policy->get_promoted_entry_num());
  }

  void insert_entry_into_promoting_lru(std::string cache_file_name) {
    ASSERT_EQ(m_cache_size - m_promoted_lru.size(), m_simple_policy->get_free_size());
    ASSERT_EQ(m_promoting_lru.size(), m_simple_policy->get_promoting_entry_num());
    ASSERT_EQ(m_promoted_lru.size(), m_simple_policy->get_promoted_entry_num());
    ASSERT_EQ(OBJ_CACHE_NONE, m_simple_policy->get_status(cache_file_name));

    m_simple_policy->lookup_object(cache_file_name);
    m_promoting_lru.push_back(cache_file_name);
    ASSERT_EQ(OBJ_CACHE_SKIP, m_simple_policy->get_status(cache_file_name));
    ASSERT_EQ(m_cache_size - m_promoted_lru.size(), m_simple_policy->get_free_size());
    ASSERT_EQ(m_promoting_lru.size(), m_simple_policy->get_promoting_entry_num());
    ASSERT_EQ(m_promoted_lru.size(), m_simple_policy->get_promoted_entry_num());
  }
};

TEST_F(TestSimplePolicy, test_lookup_miss_and_no_free) {
  // exhaust cache space
  uint64_t left_entry_num = m_cache_size - m_promoted_lru.size();
  for (uint64_t i = 0; i < left_entry_num; i++, ++m_entry_index) {
    insert_entry_into_promoted_lru(generate_file_name(m_entry_index));
  }
  ASSERT_TRUE(0 == m_simple_policy->get_free_size());
  ASSERT_TRUE(m_simple_policy->lookup_object("no_this_cache_file_name") == OBJ_CACHE_SKIP);
}

TEST_F(TestSimplePolicy, test_lookup_miss_and_have_free) {
  ASSERT_TRUE(m_cache_size - m_promoted_lru.size() == m_simple_policy->get_free_size());
  ASSERT_TRUE(m_simple_policy->lookup_object("miss_but_have_free_space_file_name") == OBJ_CACHE_NONE);
  ASSERT_TRUE(m_simple_policy->get_status("miss_but_have_free_space_file_name") == OBJ_CACHE_SKIP);
}

TEST_F(TestSimplePolicy, test_lookup_hit_and_promoting) {
  ASSERT_TRUE(m_cache_size - m_promoted_lru.size() == m_simple_policy->get_free_size());
  insert_entry_into_promoting_lru("promoting_file_1");
  insert_entry_into_promoting_lru("promoting_file_2");
  insert_entry_into_promoted_lru(generate_file_name(++m_entry_index));
  insert_entry_into_promoted_lru(generate_file_name(++m_entry_index));
  insert_entry_into_promoting_lru("promoting_file_3");
  insert_entry_into_promoting_lru("promoting_file_4");

  ASSERT_TRUE(m_simple_policy->get_promoting_entry_num() == 4);
  ASSERT_TRUE(m_simple_policy->get_status("promoting_file_1") == OBJ_CACHE_SKIP);
  ASSERT_TRUE(m_simple_policy->get_status("promoting_file_2") == OBJ_CACHE_SKIP);
  ASSERT_TRUE(m_simple_policy->get_status("promoting_file_3") == OBJ_CACHE_SKIP);
  ASSERT_TRUE(m_simple_policy->get_status("promoting_file_4") == OBJ_CACHE_SKIP);
  ASSERT_TRUE(m_simple_policy->lookup_object("promoting_file_1") == OBJ_CACHE_SKIP);
  ASSERT_TRUE(m_simple_policy->lookup_object("promoting_file_2") == OBJ_CACHE_SKIP);
  ASSERT_TRUE(m_simple_policy->lookup_object("promoting_file_3") == OBJ_CACHE_SKIP);
  ASSERT_TRUE(m_simple_policy->lookup_object("promoting_file_4") == OBJ_CACHE_SKIP);
}

TEST_F(TestSimplePolicy, test_lookup_hit_and_promoted) {
  ASSERT_TRUE(m_promoted_lru.size() == m_simple_policy->get_promoted_entry_num());
  for (uint64_t index = 0; index < m_entry_index; index++) {
    ASSERT_TRUE(m_simple_policy->get_status(generate_file_name(index)) == OBJ_CACHE_PROMOTED);
  }
}

TEST_F(TestSimplePolicy, test_update_state_from_promoting_to_none) {
  ASSERT_TRUE(m_cache_size - m_promoted_lru.size() == m_simple_policy->get_free_size());
  insert_entry_into_promoting_lru("promoting_to_none_file_1");
  insert_entry_into_promoting_lru("promoting_to_none_file_2");
  insert_entry_into_promoted_lru(generate_file_name(++m_entry_index));
  insert_entry_into_promoting_lru("promoting_to_none_file_3");
  insert_entry_into_promoting_lru("promoting_to_none_file_4");

  ASSERT_TRUE(m_simple_policy->get_promoting_entry_num() == 4);
  ASSERT_TRUE(m_simple_policy->get_status("promoting_to_none_file_1") == OBJ_CACHE_SKIP);
  ASSERT_TRUE(m_simple_policy->get_status("promoting_to_none_file_2") == OBJ_CACHE_SKIP);
  ASSERT_TRUE(m_simple_policy->get_status("promoting_to_none_file_3") == OBJ_CACHE_SKIP);
  ASSERT_TRUE(m_simple_policy->get_status("promoting_to_none_file_4") == OBJ_CACHE_SKIP);

  m_simple_policy->update_status("promoting_to_none_file_1", OBJ_CACHE_NONE);
  ASSERT_TRUE(m_simple_policy->get_promoting_entry_num() == 3);
  ASSERT_TRUE(m_simple_policy->get_status("promoting_to_none_file_1") == OBJ_CACHE_NONE);
  ASSERT_TRUE(m_simple_policy->get_status("promoting_to_none_file_2") == OBJ_CACHE_SKIP);
  ASSERT_TRUE(m_simple_policy->get_status("promoting_to_none_file_3") == OBJ_CACHE_SKIP);
  ASSERT_TRUE(m_simple_policy->get_status("promoting_to_none_file_4") == OBJ_CACHE_SKIP);

  m_simple_policy->update_status("promoting_to_none_file_2", OBJ_CACHE_NONE);
  ASSERT_TRUE(m_simple_policy->get_promoting_entry_num() == 2);
  ASSERT_TRUE(m_simple_policy->get_status("promoting_to_none_file_1") == OBJ_CACHE_NONE);
  ASSERT_TRUE(m_simple_policy->get_status("promoting_to_none_file_2") == OBJ_CACHE_NONE);
  ASSERT_TRUE(m_simple_policy->get_status("promoting_to_none_file_3") == OBJ_CACHE_SKIP);
  ASSERT_TRUE(m_simple_policy->get_status("promoting_to_none_file_4") == OBJ_CACHE_SKIP);

  m_simple_policy->update_status("promoting_to_none_file_3", OBJ_CACHE_NONE);
  ASSERT_TRUE(m_simple_policy->get_promoting_entry_num() == 1);
  ASSERT_TRUE(m_simple_policy->get_status("promoting_to_none_file_1") == OBJ_CACHE_NONE);
  ASSERT_TRUE(m_simple_policy->get_status("promoting_to_none_file_2") == OBJ_CACHE_NONE);
  ASSERT_TRUE(m_simple_policy->get_status("promoting_to_none_file_3") == OBJ_CACHE_NONE);
  ASSERT_TRUE(m_simple_policy->get_status("promoting_to_none_file_4") == OBJ_CACHE_SKIP);

  m_simple_policy->update_status("promoting_to_none_file_4", OBJ_CACHE_NONE);
  ASSERT_TRUE(m_simple_policy->get_promoting_entry_num() == 0);
  ASSERT_TRUE(m_simple_policy->get_status("promoting_to_none_file_1") == OBJ_CACHE_NONE);
  ASSERT_TRUE(m_simple_policy->get_status("promoting_to_none_file_2") == OBJ_CACHE_NONE);
  ASSERT_TRUE(m_simple_policy->get_status("promoting_to_none_file_3") == OBJ_CACHE_NONE);
  ASSERT_TRUE(m_simple_policy->get_status("promoting_to_none_file_4") == OBJ_CACHE_NONE);
}

TEST_F(TestSimplePolicy, test_update_state_from_promoted_to_none) {
  ASSERT_TRUE(m_promoted_lru.size() == m_simple_policy->get_promoted_entry_num());
  for (uint64_t index = 0; index < m_entry_index; index++) {
    ASSERT_TRUE(m_simple_policy->get_status(generate_file_name(index)) == OBJ_CACHE_PROMOTED);
    m_simple_policy->update_status(generate_file_name(index), OBJ_CACHE_NONE);
    ASSERT_TRUE(m_simple_policy->get_status(generate_file_name(index)) == OBJ_CACHE_NONE);
    ASSERT_TRUE(m_simple_policy->get_promoted_entry_num() == m_promoted_lru.size() - index - 1);
  }
  m_promoted_lru.clear();
}

TEST_F(TestSimplePolicy, test_update_state_from_promoting_to_promoted) {
  ASSERT_TRUE(m_cache_size - m_promoted_lru.size() == m_simple_policy->get_free_size());
  insert_entry_into_promoting_lru("promoting_to_promoted_file_1");
  insert_entry_into_promoting_lru("promoting_to_promoted_file_2");
  insert_entry_into_promoting_lru("promoting_to_promoted_file_3");
  insert_entry_into_promoting_lru("promoting_to_promoted_file_4");
  ASSERT_TRUE(4 == m_simple_policy->get_promoting_entry_num());

  m_simple_policy->update_status("promoting_to_promoted_file_1", OBJ_CACHE_PROMOTED);
  ASSERT_TRUE(3 == m_simple_policy->get_promoting_entry_num());
  ASSERT_TRUE(m_simple_policy->get_status("promoting_to_promoted_file_1") == OBJ_CACHE_PROMOTED);

  m_simple_policy->update_status("promoting_to_promoted_file_2", OBJ_CACHE_PROMOTED);
  ASSERT_TRUE(2 == m_simple_policy->get_promoting_entry_num());
  ASSERT_TRUE(m_simple_policy->get_status("promoting_to_promoted_file_2") == OBJ_CACHE_PROMOTED);

  m_simple_policy->update_status("promoting_to_promoted_file_3", OBJ_CACHE_PROMOTED);
  ASSERT_TRUE(1 == m_simple_policy->get_promoting_entry_num());
  ASSERT_TRUE(m_simple_policy->get_status("promoting_to_promoted_file_3") == OBJ_CACHE_PROMOTED);

  m_simple_policy->update_status("promoting_to_promoted_file_4", OBJ_CACHE_PROMOTED);
  ASSERT_TRUE(0 == m_simple_policy->get_promoting_entry_num());
  ASSERT_TRUE(m_simple_policy->get_status("promoting_to_promoted_file_4") == OBJ_CACHE_PROMOTED);

  m_promoted_lru.push_back("promoting_to_promoted_file_1");
  m_promoted_lru.push_back("promoting_to_promoted_file_2");
  m_promoted_lru.push_back("promoting_to_promoted_file_3");
  m_promoted_lru.push_back("promoting_to_promoted_file_4");
}

TEST_F(TestSimplePolicy, test_evict_list_0) {
  std::list<std::string> evict_entry_list;
  // 0.1 is watermark
  ASSERT_TRUE((float)m_simple_policy->get_free_size() > m_cache_size*0.1);
  m_simple_policy->get_evict_list(&evict_entry_list);
  ASSERT_TRUE(evict_entry_list.size() == 0);
}

TEST_F(TestSimplePolicy, test_evict_list_10) {
  uint64_t left_entry_num = m_cache_size - m_promoted_lru.size();
  for (uint64_t i = 0; i < left_entry_num; i++, ++m_entry_index) {
    insert_entry_into_promoted_lru(generate_file_name(m_entry_index));
  }
  ASSERT_TRUE(0 == m_simple_policy->get_free_size());
  std::list<std::string> evict_entry_list;
  m_simple_policy->get_evict_list(&evict_entry_list);
  // evict 10% of old entries
  ASSERT_TRUE(m_cache_size*0.1 == evict_entry_list.size());
  ASSERT_TRUE(m_cache_size - m_cache_size*0.1  == m_simple_policy->get_promoted_entry_num());

  for (auto it = evict_entry_list.begin(); it != evict_entry_list.end(); it++) {
    ASSERT_TRUE(*it == m_promoted_lru.front());
    m_promoted_lru.erase(m_promoted_lru.begin());
  }
}
