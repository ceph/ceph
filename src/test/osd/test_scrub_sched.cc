// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/// \file testing the scrub scheduling algorithm

#include "./test_scrub_sched.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <map>

#include "common/async/context_pool.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "include/utime_fmt.h"
#include "mon/MonClient.h"
#include "msg/Messenger.h"
#include "os/ObjectStore.h"
#include "osd/PG.h"
#include "osd/osd_types.h"
#include "osd/osd_types_fmt.h"
#include "osd/scrubber/osd_scrub_sched.h"
#include "osd/scrubber/scrub_queue.h"
#include "osd/scrubber_common.h"

using namespace std::chrono;
using namespace std::chrono_literals;
using namespace std::literals;


int main(int argc, char** argv)
{
  std::map<std::string, std::string> defaults = {
      // make sure we have 3 copies, or some tests won't work
      {"osd_pool_default_size", "3"},
      // our map is flat, so just try and split across OSDs, not hosts or
      // whatever
      {"osd_crush_chooseleaf_type", "0"},
      {"osd_scrub_retry_busy_replicas", "10"},
  };
  std::vector<const char*> args(argv, argv + argc);
  auto cct = global_init(
      &defaults, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
      CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

using sched_conf_t = Scrub::sched_conf_t;
using SchedEntry = Scrub::SchedEntry;
using ScrubJob = Scrub::ScrubJob;
using urgency_t = Scrub::urgency_t;
using delay_cause_t = Scrub::delay_cause_t;


/**
 * providing the small number of OSD services used when scheduling
 * a scrub
 */
class FakeOsd : public Scrub::ScrubSchedListener {
 public:
  constexpr explicit FakeOsd(int osd_num) : m_osd_num(osd_num) {}

  int get_nodeid() const final { return m_osd_num; }

  std::optional<PGLockWrapper> get_locked_pg(spg_t pgid) final
  {
    return std::nullopt;
  }

  void send_sched_recalc_to_pg(spg_t pgid) final { std::ignore = pgid; }

  ~FakeOsd() override = default;

 private:
  int m_osd_num;
};

struct qu_entries_dt_t {
  spg_t pg_id;
  SchedEntry shallow;
  SchedEntry deep;
};


// ///////////////////////////////////////////////////
// ScrubQueue

class ScrubQueueTestWrapper : public ScrubQueue {
 public:
  ScrubQueueTestWrapper(Scrub::ScrubSchedListener& osds)
      : ScrubQueue(g_ceph_context, osds)
  {}


  /**
   * unit-test support for faking the current time. When
   * not activated specifically - the default is to use ceph_clock_now()
   */
  void set_time_for_testing(long faked_now)
  {
    m_time_for_testing = utime_t{timeval{faked_now}};
  }
  void clear_time_for_testing() { m_time_for_testing.reset(); }
  mutable std::optional<utime_t> m_time_for_testing;

  utime_t time_now() const final
  {
    if (m_time_for_testing) {
      m_time_for_testing->tv.tv_nsec += 1'000'000;
    }
    return m_time_for_testing.value_or(ceph_clock_now());
  }

  void queue_test_entries(const qu_entries_dt_t& dt)
  {
    queue_entries(dt.pg_id, dt.shallow, dt.deep);
  }

  // note: copies the top element, then removes it
  std::optional<SchedEntry> popped_front()
  {
    if (to_scrub.empty()) {
      return std::nullopt;
    }
    auto ret = to_scrub.front();
    to_scrub.pop_front();
    return ret;
  }

  // verify that the queue (removed) top is equal to the given entry
  void test_popped_front(const schedentry_blueprint_t& bp)
  {
    auto top = popped_front();
    EXPECT_TRUE(top);
    EXPECT_TRUE(bp.is_equiv(top));
  }

  int get_queue_size() const { return to_scrub.size(); }

  bool normalize_the_queue() { return ScrubQueue::normalize_the_queue(); }

  void cout_queue(std::string_view title, int cnt) const
  {
    for (int i = 0; const auto& e : to_scrub) {
      std::cout << fmt::format(
	  "\t{} {}: {} {}\n", title, i,
	  (e.is_ripe(scrub_clock_now()) ? "<+>" : "<->"), e);
      if (++i == cnt) {
	break;
      }
    }
  }


  ~ScrubQueueTestWrapper() override = default;
};


class TestScrubQueue : public ::testing::Test {
 public:
  TestScrubQueue() = default;

  void init(utime_t starting_at, std::vector<qu_entries_dt_t> dt)
  {
    m_queue = std::make_unique<ScrubQueueTestWrapper>(m_osd);
    m_queue->set_time_for_testing(starting_at.sec());
    for (auto& [pgid, shallow, deep] : dt) {
      m_queue->queue_entries(pgid, shallow, deep);
    }
  }

 protected:
  FakeOsd m_osd{1};

  std::unique_ptr<ScrubQueueTestWrapper> m_queue;
};

// ///////////////////////////////////////////////////////////////////////////
// test data.


namespace {

// the times used during the tests are offset to 1.1.2000, so that
// utime_t formatting will treat them as absolute (not as a relative time)
static const auto epoch_2000 = 946'684'800;

spg_t pg_tst1_a{pg_t{1, 1}};

// a regular-periodic target
schedentry_blueprint_t tst1_a_shallow{
    pg_tst1_a,
    scrub_level_t::shallow,
    urgency_t::periodic_regular,
    utime_t{epoch_2000 + 1'000'000, 0},	  // not-before
    utime_t{epoch_2000 + 11'000'000, 0},  // deadline
    utime_t{epoch_2000 + 1'000'000, 0}	  // target
};
schedentry_blueprint_t tst1_a_deep{
    pg_tst1_a,
    scrub_level_t::deep,
    urgency_t::periodic_regular,
    utime_t{epoch_2000 + 1'000'000 + 3 * 24 * 3600, 0},	   // not-before
    utime_t{epoch_2000 + 11'000'000 + 10 * 24 * 3600, 0},  // deadline
    utime_t{epoch_2000 + 1'000'000 + 3 * 24 * 3600, 0}	   // target
};

qu_entries_dt_t tst1_a{
    pg_tst1_a, tst1_a_shallow.make_entry(), tst1_a_deep.make_entry()};

// a 2'nd regular-periodic target
spg_t pg_tst1_b{pg_t{3, 3}};

schedentry_blueprint_t tst1_b_shallow{
    pg_tst1_b,
    scrub_level_t::shallow,
    urgency_t::periodic_regular,
    utime_t{epoch_2000 + 1'000'000, 0},	  // not-before
    utime_t{epoch_2000 + 11'000'000, 0},  // deadline
    utime_t{epoch_2000 + 1'000'000, 0}	  // target
};
schedentry_blueprint_t tst1_b_deep{
    pg_tst1_b,
    scrub_level_t::deep,
    urgency_t::periodic_regular,
    utime_t{epoch_2000 + 500'000, 0},			   // not-before
    utime_t{epoch_2000 + 11'000'000 + 10 * 24 * 3600, 0},  // deadline
    utime_t{epoch_2000 + 500'000 + 3 * 24 * 3600, 0}	   // target
};

qu_entries_dt_t tst1_b{
    pg_tst1_b, tst1_b_shallow.make_entry(), tst1_b_deep.make_entry()};

// a high-priority target
spg_t pg_tst1_c{pg_t{3, 3}};

schedentry_blueprint_t tst1_c_shallow{
    pg_tst1_c,
    scrub_level_t::shallow,
    urgency_t::must,
    utime_t{epoch_2000 + 1'000'000, 0},	  // not-before
    utime_t{epoch_2000 + 11'000'000, 0},  // deadline
    utime_t{epoch_2000 + 1'000'000, 0}	  // target
};
schedentry_blueprint_t tst1_c_deep{
    pg_tst1_c,
    scrub_level_t::deep,
    urgency_t::periodic_regular,
    utime_t{epoch_2000 + 500'000, 0},			   // not-before
    utime_t{epoch_2000 + 11'000'000 + 10 * 24 * 3600, 0},  // deadline
    utime_t{epoch_2000 + 500'000 + 3 * 24 * 3600, 0}	   // target
};

qu_entries_dt_t tst1_c{
    pg_tst1_c, tst1_c_shallow.make_entry(), tst1_c_deep.make_entry()};

}  // namespace


// //////////////////////////// tests ////////////////////////////////////////

/// three PGs: two periodic and one high-priority
TEST_F(TestScrubQueue, test_queueing)
{
  std::vector<qu_entries_dt_t> dt{tst1_a, tst1_b};
  init(utime_t{epoch_2000 + 36'000, 0}, dt);
  std::cout << fmt::format("\ntime is now: {}\n", m_queue->scrub_clock_now());

  m_queue->normalize_the_queue();
  m_queue->cout_queue("test_queueing A", 6);
  EXPECT_EQ(m_queue->get_queue_size(), 4);
  m_queue->test_popped_front(tst1_b_deep);

  // add a priority entry - and check that it's in front
  m_queue->queue_test_entries(tst1_c);
  m_queue->normalize_the_queue();
  m_queue->cout_queue("test_queueing B", 6);
  EXPECT_EQ(m_queue->get_queue_size(), 5);
  m_queue->test_popped_front(tst1_c_shallow);
}


/// populate_config_params() combines data from the pool configuration
/// and the OSD configuration.
TEST_F(TestScrubQueue, populate_1)
{
  poolopts_blueprint_t poolopt1{
      .min_interval = 3'600.0,
      .deep_interval = 0.0,
      .max_interval = 14 * 24 * 3'600.0};
  auto pool_conf = poolopt1.make_conf();

  std::vector<qu_entries_dt_t> dt{tst1_a, tst1_b};
  init(utime_t{epoch_2000 + 36'000, 0}, dt);

  auto scrub_conf = m_queue->populate_config_params(pool_conf);
  std::cout << fmt::format("populate_1: {}\n", scrub_conf);
  EXPECT_EQ(scrub_conf.shallow_interval, 3'600.0);
  EXPECT_EQ(scrub_conf.max_deep, scrub_conf.max_shallow.value());
}

TEST_F(TestScrubQueue, populate_2)
{
  poolopts_blueprint_t poolopt2{
      .min_interval = 0.0, .deep_interval = 12 * 3'600.0, .max_interval = 0.0};
  auto pool_conf = poolopt2.make_conf();

  std::vector<qu_entries_dt_t> dt{tst1_a, tst1_b};
  init(utime_t{epoch_2000 + 36'000, 0}, dt);

  auto scrub_conf = m_queue->populate_config_params(pool_conf);
  std::cout << fmt::format("populate_2: {}\n", scrub_conf);
  EXPECT_EQ(scrub_conf.deep_interval, 12 * 3'600.0);
  // populate_2: periods: s:86400/604800 d:43200/604800 iv-ratio:0.5 on-inv:t
}
