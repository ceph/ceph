#include <fmt/format.h>
#include <sqlite3.h>

#include <chrono>
#include <expected>
#include <random>
#include <thread>

#include "common/debug.h"
#include "include/rados/librados.hpp"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "gtest/gtest.h"
#include "include/libcephsqlite.h"
#include "include/uuid.h"

#include "common/AuditDB.h"

#define dout_subsys ceph_subsys_client
#undef dout_prefix
#define dout_prefix *_dout << "test_audit_db: "

static boost::intrusive_ptr<CephContext> cct;

using milliseconds = std::chrono::milliseconds;
using high_resolution_clock = std::chrono::high_resolution_clock;

namespace {
time_t
get_time()
{
  return high_resolution_clock::to_time_t(high_resolution_clock::now());
}

} // namespace

class AuditDBTest : public ::testing::Test {
public:
  inline static const std::string pool = ".test_audit_db";

  static void
  SetUpTestSuite()
  {
    librados::Rados cluster;
    ASSERT_LE(0, cluster.init_with_context(cct.get()));
    ASSERT_LE(0, cluster.connect());
    if (int rc = cluster.pool_create(pool.c_str()); rc < 0 && rc != -EEXIST) {
      ASSERT_EQ(0, rc);
    }
    cluster.shutdown();
    sleep(5);
  }

  void
  SetUp() override
  {
    u.generate_random();
    uri = fmt::format(
        "file:///{}:/test_audit_{}.db?vfs=ceph", pool, u.to_string());
    ASSERT_LE(0, cluster.init_with_context(cct.get()));
    ASSERT_LE(0, cluster.connect());
    ASSERT_LE(0, cluster.wait_for_latest_osdmap());

    db = std::make_unique<AuditDB>(cct.get(), uri.c_str(), true, "audit_log");
    ASSERT_TRUE(db->init().has_value());
    ASSERT_TRUE(db->is_initialised());
  }

  void
  TearDown() override
  {
    db.reset();
    cluster.shutdown();
  }

protected:
  std::string uri;
  uuid_d u;
  std::unique_ptr<AuditDB> db;
  librados::Rados cluster;
};

TEST_F(AuditDBTest, TwoPhaseCommitRoundTrip)
{
  int64_t seq = 1;
  std::string cmd = "osd pool create foo";
  std::string cmd_args = "8 8";
  time_t start_time = get_time();

  ASSERT_TRUE(db->first_phase_commit(seq, cmd, cmd_args, start_time).has_value());

  time_t comp_time = get_time();
  ASSERT_TRUE(db->second_phase_commit(seq, comp_time, "success", 0).has_value());

  AuditQuery q;
  auto rows = db->query(q);
  ASSERT_TRUE(rows.has_value());
  ASSERT_EQ(rows->size(), 1);
  EXPECT_EQ((*rows)[0].seq, seq);
  EXPECT_EQ((*rows)[0].cmd, cmd);
  EXPECT_EQ((*rows)[0].init_time, start_time);
  EXPECT_EQ((*rows)[0].comp_time.value(), comp_time);
  EXPECT_EQ((*rows)[0].status.value(), "success");
  EXPECT_EQ((*rows)[0].retval.value(), 0);

  db->clear_logs();
  rows = db->query(q);
  ASSERT_TRUE(rows.has_value());
  ASSERT_EQ(rows->size(), 0);
}

TEST_F(AuditDBTest, RejectsDuplicateSeq)
{
  ASSERT_TRUE(db->first_phase_commit(5, "a", "b", get_time()).has_value());
  auto r = db->first_phase_commit(5, "b", "c", get_time());
  ASSERT_FALSE(r.has_value());
  ASSERT_EQ(r.error().code, AuditDBErr::sqlite_step_failed);
  ASSERT_EQ(r.error().detail, SQLITE_CONSTRAINT_PRIMARYKEY);
}

TEST_F(AuditDBTest, AllowsNonMonotonicSeq)
{
  ASSERT_TRUE(db->first_phase_commit(10, "ceph osd ls", "8 8", get_time()).has_value());
  ASSERT_TRUE(db->first_phase_commit(3, "ceph lspools", "{}", get_time()).has_value());
}

TEST_F(AuditDBTest, ReadUnfinishedCommandAuditLog)
{
  int64_t seq = 1;
  std::string cmd = "osd pool ls";
  std::string cmd_args = "detail";
  time_t init_time = get_time();

  ASSERT_TRUE(db->first_phase_commit(seq, cmd, cmd_args, init_time).has_value());

  AuditQuery q;
  auto rows = db->query(q);
  EXPECT_TRUE(rows.has_value());
  ASSERT_EQ(rows->size(), 1);
  EXPECT_EQ((*rows)[0].seq, 1);
  EXPECT_EQ((*rows)[0].cmd, cmd);
  EXPECT_EQ((*rows)[0].cmd_args, cmd_args);
  EXPECT_EQ((*rows)[0].init_time, init_time);
  EXPECT_FALSE((*rows)[0].comp_time.has_value());
  EXPECT_FALSE((*rows)[0].status.has_value());
  EXPECT_FALSE((*rows)[0].retval.has_value());
}

TEST_F(AuditDBTest, InvalidTableNameTest)
{
  struct InvalidTableNameCase {
    std::string name;
    std::string reason;
  };
  
  std::vector<InvalidTableNameCase> invalid_names = {
    // Reserved name
    {"schema_version", "reserved name"},
    
    // Too long (>63 chars)
    {"a" + std::string(63, 'x'), "too long (64 chars)"},
    
    // Empty
    {"", "empty string"},
    
    // Starts with number
    {"123_table", "starts with number"},
    
    // Contains special characters
    {"table-name", "contains hyphen"},
    {"table.name", "contains dot"},
    {"table name", "contains space"},
    {"table;DROP", "contains semicolon (SQL injection attempt)"},
    {"table'OR'1'='1", "contains quotes (SQL injection attempt)"},
    
    // Contains SQL keywords as injection
    {"audit; DROP TABLE audit--", "SQL injection with DROP"},
    {"audit/*comment*/", "SQL injection with comment"},
  };
  
  for (const auto& test_case : invalid_names) {
    std::cout << "Testing invalid table name: '" << test_case.name 
              << "' (" << test_case.reason << ")\n";
    
    auto invalid_db = std::make_unique<AuditDB>(
        cct.get(), uri.c_str(), true, test_case.name);
    
    auto init_result = invalid_db->init();
    
    EXPECT_FALSE(init_result.has_value()) 
        << "Table name '" << test_case.name << "' should be rejected";
    
    if (!init_result.has_value()) {
      EXPECT_EQ(init_result.error().code, AuditDBErr::invalid_table_name)
          << "Expected invalid_table_name error for '" << test_case.name << "'";
    }
  }
  
  // Test valid edge cases
  std::vector<std::string> valid_names = {
    "a",                          // 1 char (minimum)
    "_table",                     // starts with underscore
    "Table123",                   // mixed case with numbers
    std::string(63, 'a'),        // 63 chars (maximum)
    "audit_log_2024",            // typical valid name
  };
  
  for (const auto& valid_name : valid_names) {
    std::cout << "Testing valid table name: '" << valid_name << "'\n";
    
    // Create unique URI for each test
    uuid_d u;
    u.generate_random();
    std::string test_uri = fmt::format(
        "file:///{}:/test_valid_{}.db?vfs=ceph", pool, u.to_string());
    
    auto valid_db = std::make_unique<AuditDB>(
        cct.get(), test_uri.c_str(), true, valid_name);
    
    auto init_result = valid_db->init();
    
    EXPECT_TRUE(init_result.has_value()) 
        << "Table name '" << valid_name << "' should be accepted";
    
    if (init_result.has_value()) {
      EXPECT_TRUE(valid_db->is_initialised());
    }
  }
}

TEST_F(AuditDBTest, NegativeSequenceNumberTest)
{
  int64_t negative_seq = -1;
  std::string cmd = "test_command";
  std::string cmd_args = cmd + " dummy_args";
  time_t init_time = get_time();
  
  auto result = db->first_phase_commit(negative_seq, cmd, cmd_args, init_time);
  
  EXPECT_FALSE(result.has_value());
  if (!result.has_value()) {
    EXPECT_EQ(result.error().code, AuditDBErr::invalid_seq)
        << "Negative sequence should fail";
  }
  
  // Verify database is still empty
  AuditQuery q;
  auto rows = db->query(q);
  ASSERT_TRUE(rows.has_value());
  EXPECT_EQ(rows->size(), 0) << "No entries should be added with invalid sequences";
  
  // Verify we can still add valid sequences
  ASSERT_TRUE(db->first_phase_commit(1, cmd, cmd_args, init_time).has_value());
  ASSERT_TRUE(db->second_phase_commit(1, get_time(), "ok", 0).has_value());
  
  rows = db->query(q);
  ASSERT_TRUE(rows.has_value());
  EXPECT_EQ(rows->size(), 1);
  EXPECT_EQ((*rows)[0].seq, 1);
}

TEST_F(AuditDBTest, ReadEmptyDatabaseTest)
{
  // Verify database is initialized but empty
  ASSERT_TRUE(db->is_initialised());
  
  // Test query on empty database
  AuditQuery q;
  auto rows = db->query(q);
  ASSERT_TRUE(rows.has_value()) << "Query should succeed on empty database";
  EXPECT_EQ(rows->size(), 0) << "Empty database should return no rows";
  
  // Test count on empty database
  auto count_result = db->count(q);
  ASSERT_TRUE(count_result.has_value()) << "Count should succeed on empty database";
  EXPECT_EQ(*count_result, 0) << "Empty database should have count of 0";
  
  // Test get_last_committed_seq on empty database
  auto seq_result = db->get_last_committed_seq();
  EXPECT_FALSE(seq_result.has_value()) << "Empty database should not have a last seq";
  if (!seq_result.has_value()) {
    EXPECT_EQ(seq_result.error().code, AuditDBErr::empty_table)
        << "Should return empty_table error";
  }
  
  // Test trim on empty database
  time_t now = get_time();
  auto trim_result = db->trim(now);
  ASSERT_TRUE(trim_result.has_value()) << "Trim should succeed on empty database";
  EXPECT_EQ(*trim_result, 0) << "Trim should delete 0 rows from empty database";
  
  // Test query with various filters on empty database
  AuditQuery filtered_q;
  filtered_q.after_seq = 100;
  filtered_q.before_seq = 200;
  filtered_q.since = now - 3600;
  filtered_q.until = now;
  filtered_q.status = "success";
  
  rows = db->query(filtered_q);
  ASSERT_TRUE(rows.has_value()) << "Filtered query should succeed on empty database";
  EXPECT_EQ(rows->size(), 0) << "Filtered query on empty database should return no rows";
  
  count_result = db->count(filtered_q);
  ASSERT_TRUE(count_result.has_value()) << "Filtered count should succeed on empty database";
  EXPECT_EQ(*count_result, 0) << "Filtered count on empty database should be 0";
}

TEST_F(AuditDBTest, TrimAndQueryDeletedRangeTest)
{
  constexpr int64_t TOTAL_ENTRIES = 100;
  constexpr int64_t ENTRIES_TO_DELETE = 20;
  
  // Write 100 entries with timestamps spread over time
  time_t base_time = get_time() - 1000; // Start 1000 seconds ago
  
  for (int64_t seq = 1; seq <= TOTAL_ENTRIES; ++seq) {
    std::string cmd = fmt::format("command_{}", seq);
    std::string cmd_args = cmd + " dummy_args";
    time_t init_time = base_time + (seq * 10); // 10 seconds apart
    
    ASSERT_TRUE(db->first_phase_commit(seq, cmd, cmd_args, init_time).has_value());
    ASSERT_TRUE(db->second_phase_commit(seq, init_time + 1, "success", 0).has_value());
  }
  
  // Verify all 100 entries exist
  AuditQuery q;
  q.limit = TOTAL_ENTRIES + 10;
  auto rows = db->query(q);
  ASSERT_TRUE(rows.has_value());
  EXPECT_EQ(rows->size(), TOTAL_ENTRIES);
  
  // Calculate trim time to delete oldest 20 entries
  // Entry 20 has init_time = base_time + (20 * 10)
  // Entry 21 has init_time = base_time + (21 * 10)
  time_t trim_time = base_time + (ENTRIES_TO_DELETE * 10) + 5; // Between entry 20 and 21
  
  // Trim oldest 20 entries
  auto trim_result = db->trim(trim_time);
  ASSERT_TRUE(trim_result.has_value());
  EXPECT_EQ(*trim_result, ENTRIES_TO_DELETE) 
      << "Should have deleted exactly " << ENTRIES_TO_DELETE << " entries";
  
  // Verify total count is now 80
  auto count_result = db->count(q);
  ASSERT_TRUE(count_result.has_value());
  EXPECT_EQ(*count_result, TOTAL_ENTRIES - ENTRIES_TO_DELETE);
  
  // Try to query the deleted range (seq 1-20)
  AuditQuery deleted_range_q;
  deleted_range_q.after_seq = 0;
  deleted_range_q.before_seq = ENTRIES_TO_DELETE + 1;
  deleted_range_q.limit = 50;
  
  rows = db->query(deleted_range_q);
  ASSERT_TRUE(rows.has_value());
  EXPECT_EQ(rows->size(), 0) 
      << "Deleted entries (seq 1-20) should not be returned";
  
  // Verify remaining entries start from seq 21
  AuditQuery remaining_q;
  remaining_q.limit = 10;
  remaining_q.ascending = true;
  remaining_q.order_by = "seq";
  
  rows = db->query(remaining_q);
  ASSERT_TRUE(rows.has_value());
  ASSERT_GT(rows->size(), 0);
  EXPECT_EQ((*rows)[0].seq, ENTRIES_TO_DELETE + 1) 
      << "First remaining entry should be seq " << (ENTRIES_TO_DELETE + 1);
  
  // Verify we can query specific remaining entries
  for (int64_t seq = ENTRIES_TO_DELETE + 1; seq <= TOTAL_ENTRIES; ++seq) {
    AuditQuery specific_q;
    specific_q.after_seq = seq - 1;
    specific_q.before_seq = seq + 1;
    
    rows = db->query(specific_q);
    ASSERT_TRUE(rows.has_value());
    ASSERT_EQ(rows->size(), 1) << "Should find entry with seq " << seq;
    EXPECT_EQ((*rows)[0].seq, seq);
    EXPECT_EQ((*rows)[0].cmd, fmt::format("command_{}", seq));
  }
  
  // Verify deleted entries are truly gone by checking specific sequences
  for (int64_t seq = 1; seq <= ENTRIES_TO_DELETE; ++seq) {
    AuditQuery deleted_q;
    deleted_q.after_seq = seq - 1;
    deleted_q.before_seq = seq + 1;
    
    rows = db->query(deleted_q);
    ASSERT_TRUE(rows.has_value());
    EXPECT_EQ(rows->size(), 0) 
        << "Deleted entry with seq " << seq << " should not be found";
  }
  
  // Test get_last_committed_seq still returns the highest seq (100)
  auto last_seq = db->get_last_committed_seq();
  ASSERT_TRUE(last_seq.has_value());
  EXPECT_EQ(*last_seq, TOTAL_ENTRIES) 
      << "Last committed seq should still be " << TOTAL_ENTRIES;
}

TEST_F(AuditDBTest, DeleteAndRecreateDatabaseTest)
{
  // Add some entries
  for (int64_t seq = 1; seq <= 10; ++seq) {
    std::string cmd = fmt::format("ceph osd pool create {}", seq);
    std::string cmd_args = "8 8";
    ASSERT_TRUE(db->first_phase_commit(seq, cmd, cmd_args, get_time()).has_value());
    ASSERT_TRUE(db->second_phase_commit(seq, get_time(), "ok", 0).has_value());
  }
  
  // Verify entries exist
  AuditQuery q;
  auto rows = db->query(q);
  ASSERT_TRUE(rows.has_value());
  EXPECT_EQ(rows->size(), 10);
  
  // Close the database
  db.reset();
  
  // Delete the database file using VFS
  std::string db_path = fmt::format("{}:/test_audit_{}.db", pool, u);
  auto delete_result = AuditDB::delete_db_file(cct.get(), cluster, db_path.c_str());
  ASSERT_TRUE(delete_result.has_value());
  
  // Recreate database instance
  db = std::make_unique<AuditDB>(cct.get(), uri.c_str(), true, "audit_log");
  ASSERT_TRUE(db->init().has_value());
  
  // Try to query - should return empty or handle gracefully
  rows = db->query(q);
  ASSERT_TRUE(rows.has_value());
  EXPECT_EQ(rows->size(), 0);
  
  // Verify we can still write
  ASSERT_TRUE(db->first_phase_commit(1, "ceph osd pool create foo", "8 8", get_time()).has_value());
  
  rows = db->query(q);
  ASSERT_TRUE(rows.has_value());
  EXPECT_EQ(rows->size(), 1);
}

TEST_F(AuditDBTest, PerformanceHeavyWriteAndTrim)
{
  constexpr int64_t NUM_ENTRIES = 5000;
  
  // Measure write performance
  auto write_start = high_resolution_clock::now();
  
  for (int64_t seq = 1; seq <= NUM_ENTRIES; ++seq) {
    std::string cmd = fmt::format("test_command_{}", seq);
    std::string cmd_args = cmd + " dummy_args";
    time_t init_time = get_time();
    
    ASSERT_TRUE(db->first_phase_commit(seq, cmd, cmd_args, init_time).has_value());

    time_t comp_time = get_time();
    ASSERT_TRUE(db->second_phase_commit(seq, comp_time, "success", 0).has_value());
  }
  
  auto write_end = high_resolution_clock::now();
  auto write_duration = std::chrono::duration_cast<milliseconds>(
      write_end - write_start);
  
  std::cout << "Write Performance:\n"
            << "  Entries: " << NUM_ENTRIES << "\n"
            << "  Duration: " << write_duration.count() << " ms\n"
            << "  Rate: " << (NUM_ENTRIES * 1000.0 / write_duration.count()) 
            << " entries/sec\n";
  
  // Verify count
  AuditQuery q;
  q.limit = NUM_ENTRIES + 1;
  auto count_result = db->count(q);
  ASSERT_TRUE(count_result.has_value());
  EXPECT_EQ(*count_result, NUM_ENTRIES);
  
  // Measure trim performance
  auto clear_start = high_resolution_clock::now();
  
  auto clear_result = db->clear_logs();
  
  auto clear_end = high_resolution_clock::now();
  auto trim_duration = std::chrono::duration_cast<milliseconds>(
      clear_end - clear_start);
  
  ASSERT_TRUE(clear_result.has_value());
  EXPECT_EQ(*clear_result, NUM_ENTRIES);
  
  std::cout << "Trim Performance:\n"
            << "  Deleted: " << *clear_result << " entries\n"
            << "  Duration: " << trim_duration.count() << " ms\n"
            << "  Rate: " << (*clear_result * 1000.0 / trim_duration.count()) 
            << " entries/sec\n";
  
  // Verify empty
  count_result = db->count(q);
  ASSERT_TRUE(count_result.has_value());
  EXPECT_EQ(*count_result, 0);
}

TEST_F(AuditDBTest, MultipleConcurrentReadersTest)
{
  for (int64_t seq = 1; seq <= 1000; ++seq) {
    ASSERT_TRUE(db->first_phase_commit(seq, "cmd", "cmd_args", get_time()).has_value());
    ASSERT_TRUE(db->second_phase_commit(seq, get_time(), "ok", 0).has_value());
  }
  
  constexpr int NUM_READERS = 5;
  std::vector<std::thread> readers;
  std::atomic<int> successful_reads{0};
  
  for (int i = 0; i < NUM_READERS; ++i) {
    readers.emplace_back([&, i]() {
      auto reader_db = std::make_unique<AuditDB>(
          cct.get(), uri.c_str(), true, "audit_log");
      EXPECT_TRUE(reader_db->init().has_value());
      
      AuditQuery q;
      q.limit = 100;
      q.after_seq = i * 100;
      
      auto rows = reader_db->query(q);
      if (rows.has_value()) {
        successful_reads++;
      }
    });
  }
  
  for (auto& t : readers) {
    t.join();
  }
  
  EXPECT_EQ(successful_reads, NUM_READERS);
}

TEST_F(AuditDBTest, SQLiteBusyRetryTest)
{
  // This test simulates high contention scenarios
  constexpr int NUM_WRITERS = 3;
  constexpr int WRITES_PER_THREAD = 100;
  
  std::vector<std::thread> writers;
  std::atomic<int> successful_writes{0};
  std::atomic<int> busy_errors{0};
  
  for (int thread_id = 0; thread_id < NUM_WRITERS; ++thread_id) {
    writers.emplace_back([&, thread_id]() {
      auto writer_db = std::make_unique<AuditDB>(
          cct.get(), uri.c_str(), true, "audit_log");
      EXPECT_TRUE(writer_db->init().has_value());
      
      for (int i = 0; i < WRITES_PER_THREAD; ++i) {
        int64_t seq = thread_id * WRITES_PER_THREAD + i + 1;
        std::string cmd = fmt::format("thread_{}_cmd_{}", thread_id, i);
        std::string cmd_args = cmd + " dummy_args";
        
        // Retry logic for SQLITE_BUSY
        int retries = 0;
        const int MAX_RETRIES = 10;
        bool success = false;
        
        while (retries < MAX_RETRIES && !success) {
          auto result = writer_db->first_phase_commit(seq, cmd, cmd_args, get_time());
          
          if (result.has_value()) {
            auto comp_result = writer_db->second_phase_commit(
                seq, get_time(), "ok", 0);
            if (comp_result.has_value()) {
              successful_writes++;
              success = true;
            } else if (comp_result.error().detail == SQLITE_BUSY) {
              busy_errors++;
              std::this_thread::sleep_for(milliseconds(50));
              retries++;
            }
          } else if (result.error().detail == SQLITE_BUSY) {
            busy_errors++;
            std::this_thread::sleep_for(milliseconds(50));
            retries++;
          } else {
            // Other error, don't retry
            break;
          }
        }
      }});
    }
  
  for (auto& t : writers) {
    t.join();
  }
  
  std::cout << "SQLITE_BUSY Test Results:\n"
            << "  Successful writes: " << successful_writes << "\n"
            << "  BUSY errors encountered: " << busy_errors << "\n";
  
  EXPECT_EQ(successful_writes, NUM_WRITERS * WRITES_PER_THREAD);
}

TEST_F(AuditDBTest, SecondPhaseFailsWhenSeqMissing) {
  ASSERT_TRUE(db->first_phase_commit(1, "cmd", "cmd_args", get_time()).has_value());
  auto r = db->second_phase_commit(2, get_time(), "ok", 0);
  EXPECT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code, AuditDBErr::row_count_mismatch);
}
 
TEST_F(AuditDBTest, SecondPhaseFailsWhenAlreadyCommitted) {  
  ASSERT_TRUE(db->first_phase_commit(5, "cmd", "cmd_args", get_time()).has_value());
  ASSERT_TRUE(db->second_phase_commit(5, get_time(), "ok", 0).has_value());
  auto r = db->second_phase_commit(5, get_time(), "ok", 0);
  EXPECT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code, AuditDBErr::row_count_mismatch);
}

TEST_F(AuditDBTest, TrimWithMaxDeleteTest) {
  // Add 100 entries
  for (int64_t seq = 1; seq <= 100; ++seq) {
    ASSERT_TRUE(db->first_phase_commit(seq, "cmd", "cmd_args", get_time()).has_value());
    ASSERT_TRUE(db->second_phase_commit(seq, get_time(), "ok", 0).has_value());
  }
  
  // Trim with max_delete = 10
  auto result = db->trim(get_time() + 1, 10);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, 10);
  
  // Verify 90 remain
  AuditQuery q;
  auto count = db->count(q);
  ASSERT_TRUE(count.has_value());
  EXPECT_EQ(*count, 90);
  
  // Test max_delete = 0
  result = db->trim(get_time() + 1, 0);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, 90);

  // Should report 0
  count = db->count(q);
  ASSERT_TRUE(count.has_value());
  EXPECT_EQ(*count, 0);
}

TEST_F(AuditDBTest, DatabasePersistenceTest) {
  // Write entries
  for (int64_t seq = 1; seq <= 10; ++seq) {
    ASSERT_TRUE(db->first_phase_commit(seq, "cmd", "cmd_args", get_time()).has_value());
    ASSERT_TRUE(db->second_phase_commit(seq, get_time(), "ok", 0).has_value());
  }
  
  // Close database
  db.reset();
  
  // Reopen same database
  db = std::make_unique<AuditDB>(cct.get(), uri.c_str(), true, "audit_log");
  ASSERT_TRUE(db->init().has_value());
  
  // Verify data persisted
  AuditQuery q;
  auto rows = db->query(q);
  ASSERT_TRUE(rows.has_value());
  EXPECT_EQ(rows->size(), 10);
  
  // Verify last committed seq
  auto seq = db->get_last_committed_seq();
  EXPECT_TRUE(seq.has_value());
  EXPECT_EQ(*seq, 10);
  
  // Verify we can continue with seq 11
  ASSERT_TRUE(db->first_phase_commit(11, "cmd", "cmd_args", get_time()).has_value());
}

TEST_F(AuditDBTest, CountWithFiltersTest) {
  // Add entries with different statuses
  for (int64_t seq = 1; seq <= 50; ++seq) {
    ASSERT_TRUE(db->first_phase_commit(seq, "cmd", "cmd_args", get_time()).has_value());
    std::string status = (seq % 2 == 0) ? "success" : "failure";
    ASSERT_TRUE(db->second_phase_commit(seq, get_time(), status, 0).has_value());
  }
  
  // Count with status filter
  AuditQuery q;
  q.status = "success";
  auto count = db->count(q);
  ASSERT_TRUE(count.has_value());
  EXPECT_EQ(*count, 25);
  
  // Count with seq range
  q.status.reset();
  q.after_seq = 10;
  q.before_seq = 21;
  count = db->count(q);
  ASSERT_TRUE(count.has_value());
  EXPECT_EQ(*count, 10);
}

TEST_F(AuditDBTest, QueryFilteringTest)
{
  // Setup: Create entries with varied attributes for comprehensive filtering tests
  constexpr int64_t TOTAL_ENTRIES = 50;
  time_t base_time = get_time() - 1000; // Start 1000 seconds ago
  
  // Create entries with different statuses and times
  for (int64_t seq = 1; seq <= TOTAL_ENTRIES; ++seq) {
    std::string cmd = fmt::format("command_{}", seq);
    std::string cmd_args = cmd + " dummy_args";
    time_t init_time = base_time + (seq * 10); // 10 seconds apart
    
    ASSERT_TRUE(db->first_phase_commit(seq, cmd, cmd_args, init_time).has_value());
    
    // Vary completion times and statuses
    time_t comp_time = init_time + (seq % 5) + 1; // 1-5 seconds after init
    std::string status;
    int32_t retval;
    
    if (seq % 3 == 0) {
      status = "success";
      retval = 0;
    } else if (seq % 3 == 1) {
      status = "failure";
      retval = -EINVAL;
    } else {
      status = "timeout";
      retval = -ETIMEDOUT;
    }
    
    ASSERT_TRUE(db->second_phase_commit(seq, comp_time, status, retval).has_value());
  }
  
  // Test 1: Time-based filtering (since)
  {
    time_t since_time = base_time + 250; // After seq 25
    AuditQuery q;
    q.since = since_time;
    q.limit = 100;
    
    auto rows = db->query(q);
    ASSERT_TRUE(rows.has_value());
    EXPECT_GT(rows->size(), 0);
    
    // Verify all returned entries are after since_time
    for (const auto& entry : *rows) {
      EXPECT_GE(entry.init_time, since_time)
          << "Entry seq=" << entry.seq << " has init_time before since filter";
    }
  }
  
  // Test 2: Time-based filtering (until)
  {
    time_t until_time = base_time + 250; // Before seq 26
    AuditQuery q;
    q.until = until_time;
    q.limit = 100;
    
    auto rows = db->query(q);
    ASSERT_TRUE(rows.has_value());
    EXPECT_GT(rows->size(), 0);
    
    // Verify all returned entries are before until_time
    for (const auto& entry : *rows) {
      EXPECT_LE(entry.init_time, until_time)
          << "Entry seq=" << entry.seq << " has init_time after until filter";
    }
  }
  
  // Test 3: Time-based filtering (since AND until - range)
  {
    time_t since_time = base_time + 100; // After seq 10
    time_t until_time = base_time + 300; // Before seq 31
    AuditQuery q;
    q.since = since_time;
    q.until = until_time;
    q.limit = 100;
    
    auto rows = db->query(q);
    ASSERT_TRUE(rows.has_value());
    EXPECT_GT(rows->size(), 0);
    
    // Verify all entries are within the time range
    for (const auto& entry : *rows) {
      EXPECT_GE(entry.init_time, since_time);
      EXPECT_LE(entry.init_time, until_time);
    }
    
    // Verify count matches
    auto count = db->count(q);
    ASSERT_TRUE(count.has_value());
    EXPECT_EQ(*count, rows->size());
  }
  
  // Test 4: Status filtering (success)
  {
    AuditQuery q;
    q.status = "success";
    q.limit = 100;
    
    auto rows = db->query(q);
    ASSERT_TRUE(rows.has_value());
    EXPECT_GT(rows->size(), 0);
    
    // Verify all entries have success status
    for (const auto& entry : *rows) {
      ASSERT_TRUE(entry.status.has_value());
      EXPECT_EQ(*entry.status, "success")
          << "Entry seq=" << entry.seq << " has wrong status";
    }
    
    // Count should match (every 3rd entry is success)
    auto count = db->count(q);
    ASSERT_TRUE(count.has_value());
    EXPECT_EQ(*count, TOTAL_ENTRIES / 3);
  }
  
  // Test 5: Status filtering (failure)
  {
    AuditQuery q;
    q.status = "failure";
    q.limit = 100;
    
    auto rows = db->query(q);
    ASSERT_TRUE(rows.has_value());
    
    for (const auto& entry : *rows) {
      ASSERT_TRUE(entry.status.has_value());
      EXPECT_EQ(*entry.status, "failure");
      ASSERT_TRUE(entry.retval.has_value());
      EXPECT_EQ(*entry.retval, -EINVAL);
    }
  }
  
  // Test 6: Sequence filtering (after_seq)
  {
    AuditQuery q;
    q.after_seq = 25;
    q.limit = 100;
    
    auto rows = db->query(q);
    ASSERT_TRUE(rows.has_value());
    
    for (const auto& entry : *rows) {
      EXPECT_GT(entry.seq, 25);
    }
  }
  
  // Test 7: Sequence filtering (before_seq)
  {
    AuditQuery q;
    q.before_seq = 26;
    q.limit = 100;
    
    auto rows = db->query(q);
    ASSERT_TRUE(rows.has_value());
    
    for (const auto& entry : *rows) {
      EXPECT_LT(entry.seq, 26);
    }
  }
  
  // Test 8: Sequence range (after_seq AND before_seq)
  {
    AuditQuery q;
    q.after_seq = 10;
    q.before_seq = 21;
    q.limit = 100;
    
    auto rows = db->query(q);
    ASSERT_TRUE(rows.has_value());
    EXPECT_EQ(rows->size(), 10); // seq 11-20
    
    for (const auto& entry : *rows) {
      EXPECT_GT(entry.seq, 10);
      EXPECT_LT(entry.seq, 21);
    }
  }
  
  // Test 9: Order by seq, ascending
  {
    AuditQuery q;
    q.order_by = "seq";
    q.ascending = true;
    q.limit = 10;
    
    auto rows = db->query(q);
    ASSERT_TRUE(rows.has_value());
    ASSERT_EQ(rows->size(), 10);
    
    // Verify ascending order
    for (size_t i = 1; i < rows->size(); ++i) {
      EXPECT_LT((*rows)[i-1].seq, (*rows)[i].seq)
          << "Entries not in ascending seq order";
    }
    
    // First entry should be seq=1
    EXPECT_EQ((*rows)[0].seq, 1);
  }
  
  // Test 10: Order by seq, descending (default)
  {
    AuditQuery q;
    q.order_by = "seq";
    q.ascending = false;
    q.limit = 10;
    
    auto rows = db->query(q);
    ASSERT_TRUE(rows.has_value());
    ASSERT_EQ(rows->size(), 10);
    
    // Verify descending order
    for (size_t i = 1; i < rows->size(); ++i) {
      EXPECT_GT((*rows)[i-1].seq, (*rows)[i].seq)
          << "Entries not in descending seq order";
    }
    
    // First entry should be seq=50
    EXPECT_EQ((*rows)[0].seq, TOTAL_ENTRIES);
  }
  
  // Test 11: Order by init_time, ascending
  {
    AuditQuery q;
    q.order_by = "init_time";
    q.ascending = true;
    q.limit = 10;
    
    auto rows = db->query(q);
    ASSERT_TRUE(rows.has_value());
    ASSERT_EQ(rows->size(), 10);
    
    // Verify ascending order by init_time
    for (size_t i = 1; i < rows->size(); ++i) {
      EXPECT_LE((*rows)[i-1].init_time, (*rows)[i].init_time)
          << "Entries not in ascending init_time order";
    }
  }
  
  // Test 12: Order by init_time, descending
  {
    AuditQuery q;
    q.order_by = "init_time";
    q.ascending = false;
    q.limit = 10;
    
    auto rows = db->query(q);
    ASSERT_TRUE(rows.has_value());
    ASSERT_EQ(rows->size(), 10);
    
    // Verify descending order by init_time
    for (size_t i = 1; i < rows->size(); ++i) {
      EXPECT_GE((*rows)[i-1].init_time, (*rows)[i].init_time)
          << "Entries not in descending init_time order";
    }
  }
  
  // Test 13: Order by comp_time, ascending
  {
    AuditQuery q;
    q.order_by = "comp_time";
    q.ascending = true;
    q.limit = 10;
    
    auto rows = db->query(q);
    ASSERT_TRUE(rows.has_value());
    ASSERT_EQ(rows->size(), 10);
    
    // Verify ascending order by comp_time
    for (size_t i = 1; i < rows->size(); ++i) {
      ASSERT_TRUE((*rows)[i-1].comp_time.has_value());
      ASSERT_TRUE((*rows)[i].comp_time.has_value());
      EXPECT_LE(*(*rows)[i-1].comp_time, *(*rows)[i].comp_time)
          << "Entries not in ascending comp_time order";
    }
  }
  
  // Test 14: Limit and pagination
  {
    AuditQuery q;
    q.order_by = "seq";
    q.ascending = true;
    q.limit = 10;
    
    // First page
    auto page1 = db->query(q);
    ASSERT_TRUE(page1.has_value());
    ASSERT_EQ(page1->size(), 10);
    EXPECT_EQ((*page1)[0].seq, 1);
    EXPECT_EQ((*page1)[9].seq, 10);
    
    // Second page
    q.after_seq = 10;
    auto page2 = db->query(q);
    ASSERT_TRUE(page2.has_value());
    ASSERT_EQ(page2->size(), 10);
    EXPECT_EQ((*page2)[0].seq, 11);
    EXPECT_EQ((*page2)[9].seq, 20);
    
    // Third page
    q.after_seq = 20;
    auto page3 = db->query(q);
    ASSERT_TRUE(page3.has_value());
    ASSERT_EQ(page3->size(), 10);
    EXPECT_EQ((*page3)[0].seq, 21);
    EXPECT_EQ((*page3)[9].seq, 30);
  }
  
  // Test 15: Combined filters (time + status + seq range)
  {
    time_t since_time = base_time + 100;
    time_t until_time = base_time + 400;
    
    AuditQuery q;
    q.since = since_time;
    q.until = until_time;
    q.status = "success";
    q.after_seq = 5;
    q.before_seq = 45;
    q.limit = 100;
    
    auto rows = db->query(q);
    ASSERT_TRUE(rows.has_value());
    
    // Verify all filters are applied
    for (const auto& entry : *rows) {
      EXPECT_GE(entry.init_time, since_time);
      EXPECT_LE(entry.init_time, until_time);
      ASSERT_TRUE(entry.status.has_value());
      EXPECT_EQ(*entry.status, "success");
      EXPECT_GT(entry.seq, 5);
      EXPECT_LT(entry.seq, 45);
    }
    
    // Verify count matches
    auto count = db->count(q);
    ASSERT_TRUE(count.has_value());
    EXPECT_EQ(*count, rows->size());
  }
  
  // Test 16: Empty result set with filters
  {
    AuditQuery q;
    q.status = "nonexistent_status";
    q.limit = 100;
    
    auto rows = db->query(q);
    ASSERT_TRUE(rows.has_value());
    EXPECT_EQ(rows->size(), 0);
    
    auto count = db->count(q);
    ASSERT_TRUE(count.has_value());
    EXPECT_EQ(*count, 0);
  }
  
  // Test 17: Limit of 0 (should return empty)
  {
    AuditQuery q;
    q.limit = 0;
    
    auto rows = db->query(q);
    ASSERT_TRUE(rows.has_value());
    EXPECT_EQ(rows->size(), 0);
  }
  
  // Test 18: Very large limit (should return all matching entries)
  {
    AuditQuery q;
    q.limit = 10000;
    
    auto rows = db->query(q);
    ASSERT_TRUE(rows.has_value());
    EXPECT_EQ(rows->size(), TOTAL_ENTRIES);
  }
}

TEST_F(AuditDBTest, GetDbSizeTest)
{
  auto size = db->get_db_size();
  ASSERT_TRUE(size.has_value());
  auto empty_size = *size;
  std::cout << "empty size : " << empty_size << std::endl;
  EXPECT_GT(empty_size, 0) << "Schema alone should occupy 448 KiB";

  for (int64_t seq = 1; seq <= 1500; ++seq) {
    ASSERT_TRUE(db->first_phase_commit(
      seq, "osd pool create foo", "8 8", get_time()).has_value());
    ASSERT_TRUE(db->second_phase_commit(
      seq, get_time(), "success", 0).has_value());
  }

  size = db->get_db_size();
  ASSERT_TRUE(size.has_value());
  EXPECT_GT(*size, empty_size) << "Size should grow to 576 KiB";
  std::cout << "total size is: " << *size << " bytes" << std::endl;

  // Clear and verify size shrinks
  auto cleared = db->clear_logs();
  ASSERT_TRUE(cleared.has_value());
  EXPECT_EQ(*cleared, 1500);

  size = db->get_db_size();
  ASSERT_TRUE(size.has_value());
  EXPECT_EQ(*size, empty_size)
      << "Size after clear + vacuum should be equal to empty schema size";
}

TEST_F(AuditDBTest, StandaloneFirstPhaseAllocatesSeq)
{
  // build a standalone-mode AuditDB
  auto sa_db = std::make_unique<AuditDB>(
      cct.get(), uri.c_str(), true, "audit_log", /*is_standalone=*/true);
  ASSERT_TRUE(sa_db->init().has_value());

  auto r1 = sa_db->first_phase_commit("cmd", "args", get_time());
  ASSERT_TRUE(r1.has_value());
  EXPECT_EQ(*r1, 1);               

  auto r2 = sa_db->first_phase_commit("cmd2", "args2", get_time());
  ASSERT_TRUE(r2.has_value());
  EXPECT_EQ(*r2, 2);
}

TEST_F(AuditDBTest, StandaloneRejectsCallerSeq)
{
  auto sa_db = std::make_unique<AuditDB>(
      cct.get(), uri.c_str(), true, "audit_log", true);
  ASSERT_TRUE(sa_db->init().has_value());

  auto r = sa_db->first_phase_commit(42, "cmd", "args", get_time());
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code, AuditDBErr::seq_not_allowed_standalone);
}

TEST_F(AuditDBTest, DaemonRejectsMissingSeq)
{
  // existing fixture is daemon-mode (is_standalone defaults to false)
  auto r = db->first_phase_commit("cmd", "args", get_time());
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code, AuditDBErr::seq_required);
}

TEST_F(AuditDBTest, RejectsEmptyCmd)
{
  auto r = db->first_phase_commit(1, "", "args", get_time());
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code, AuditDBErr::sqlite_step_failed);
  EXPECT_EQ(r.error().detail, SQLITE_CONSTRAINT_CHECK);
}

TEST_F(AuditDBTest, RejectsWhitespaceOnlyCmd)
{
  auto r = db->first_phase_commit(1, "   ", "args", get_time());
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().detail, SQLITE_CONSTRAINT_CHECK);
}

TEST_F(AuditDBTest, RejectsZeroOrNegativeInitTime)
{
  auto r = db->first_phase_commit(1, "cmd", "args", 0);
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().detail, SQLITE_CONSTRAINT_CHECK);
}

TEST_F(AuditDBTest, RejectsCompTimeBeforeInitTime)
{
  time_t init = get_time();
  ASSERT_TRUE(db->first_phase_commit(1, "cmd", "args", init).has_value());
  auto r = db->second_phase_commit(1, init - 1, "ok", 0);
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().detail, SQLITE_CONSTRAINT_CHECK);
}

TEST_F(AuditDBTest, RejectsEmptyStatus)
{
  ASSERT_TRUE(db->first_phase_commit(1, "cmd", "args", get_time()).has_value());
  auto r = db->second_phase_commit(1, get_time(), "", 0);
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().detail, SQLITE_CONSTRAINT_CHECK);
}

TEST_F(AuditDBTest, MethodsFailBeforeInit)
{
  auto fresh = std::make_unique<AuditDB>(cct.get(), "file:///nonexistent.db?vfs=ceph", true, "t");
  // do NOT call init()
  EXPECT_FALSE(fresh->is_initialised());

  auto r = fresh->first_phase_commit(1, "c", "a", get_time());
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code, AuditDBErr::not_initialised);

  r = fresh->second_phase_commit(1, get_time(), "ok", 0);
  ASSERT_FALSE(r);
  EXPECT_EQ(r.error().code, AuditDBErr::not_initialised);
}

TEST_F(AuditDBTest, ClearLogsAllowsSeqReuse)
{
  ASSERT_TRUE(db->first_phase_commit(1, "c", "a", get_time()).has_value());
  ASSERT_TRUE(db->second_phase_commit(1, get_time(), "ok", 0).has_value());
  ASSERT_TRUE(db->clear_logs().has_value());
  // seq=1 must be reusable now
  ASSERT_TRUE(db->first_phase_commit(1, "c2", "a2", get_time()).has_value());
}

TEST_F(AuditDBTest, RejectsSeqZero)
{
  auto r = db->first_phase_commit(0, "c", "a", get_time());
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code, AuditDBErr::invalid_seq);
}

int
main(int argc, char** argv)
{
  auto args = argv_to_vec(argc, argv);

  std::string conf_file_list;
  std::string cluster;
  CephInitParameters iparams = ceph_argparse_early_args(
      args, CEPH_ENTITY_TYPE_CLIENT, &cluster, &conf_file_list);
  cct = boost::intrusive_ptr<CephContext>(
      common_preinit(iparams, CODE_ENVIRONMENT_UTILITY, 0), false);
  cct->_conf.parse_config_files(
      conf_file_list.empty() ? nullptr : conf_file_list.c_str(), &std::cerr, 0);
  cct->_conf.parse_env(cct->get_module_type()); // environment variables override
  cct->_conf.parse_argv(args);
  cct->_conf.apply_changes(nullptr);
  common_init_finish(cct.get());

  if (int rc = sqlite3_config(SQLITE_CONFIG_URI, 1); rc) {
    lderr(cct) << "sqlite3 config failed: " << rc << dendl;
    exit(EXIT_FAILURE);
  }

  sqlite3_auto_extension((void (*)())sqlite3_cephsqlite_init);
  sqlite3* db = nullptr;
  if (int rc = sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE, nullptr);
      rc == SQLITE_OK) {
    sqlite3_close(db);
  } else {
    lderr(cct) << "could not open sqlite3: " << rc << dendl;
    exit(EXIT_FAILURE);
  }
  if (int rc = cephsqlite_setcct(cct.get(), nullptr); rc < 0) {
    lderr(cct) << "could not set cct: " << rc << dendl;
    exit(EXIT_FAILURE);
  }

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
