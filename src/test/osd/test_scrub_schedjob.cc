// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/// \file testing the ScrubJob object (and the ScrubQueue indirectly)

#include "./test_scrub_sched.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <deque>
#include <map>
#include <string>

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
#include "osd/scrubber/scrub_queue_if.h"
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
using string = std::string;


/**
 *  implementing the ScrubQueue services used by the ScrubJob
 *
 *  Note - in this unit-testing implementation, elements are erased
 * forthwith, not whited-out.
 */
struct ScrubQueueOpsImp : public Scrub::ScrubQueueOps {
  ~ScrubQueueOpsImp() override = default;

  // clock issues

  void set_time_for_testing(long faked_now)
  {
    m_time_for_testing = utime_t{timeval{faked_now}};
  }

  void clear_time_for_testing() { m_time_for_testing.reset(); }

  mutable std::optional<utime_t> m_time_for_testing;

  utime_t scrub_clock_now() const final
  {
    if (m_time_for_testing) {
      m_time_for_testing->tv.tv_nsec += 1'000'000;
    }
    return m_time_for_testing.value_or(ceph_clock_now());
  }


  sched_conf_t populate_config_params(
      const pool_opts_t& pool_conf) const override
  {
    return m_config;
  }

  void remove_entry(spg_t pgid, scrub_level_t s_or_d)
  {
    auto it = std::find_if(
	m_queue.begin(), m_queue.end(), [pgid, s_or_d](const SchedEntry& e) {
	  return e.pgid == pgid && e.level == s_or_d;
	});
    if (it != m_queue.end()) {
      m_queue.erase(it);
    }
  }

  /**
   * add both targets to the queue (but only if urgency>off)
   * Note: modifies the entries (setting 'is_valid') before queuing them.
   * \todo when implementing a queue w/o the need for white-out support -
   * restore to const&.
   */
  bool queue_entries(spg_t pgid, SchedEntry shallow, SchedEntry deep) override
  {
    EXPECT_TRUE(shallow.level == scrub_level_t::shallow);
    EXPECT_TRUE(deep.level == scrub_level_t::deep);

    if (shallow.urgency == urgency_t::off || deep.urgency == urgency_t::off) {
      return false;
    }

    // we should not have these entries in the queue already
    EXPECT_TRUE(!is_in_queue(pgid, scrub_level_t::shallow));
    EXPECT_TRUE(!is_in_queue(pgid, scrub_level_t::deep));

    // set the 'is_valid' flag
    shallow.is_valid = true;
    deep.is_valid = true;

    // and queue
    m_queue.push_back(shallow);
    m_queue.push_back(deep);
    return true;
  }

  void cp_and_queue_target(SchedEntry t) override
  {
    EXPECT_TRUE(t.urgency != urgency_t::off);
    t.is_valid = true;
    m_queue.push_back(t);
  }

  // test support

  void set_sched_conf(const sched_conf_t& conf) { m_config = conf; }
  const sched_conf_t& get_sched_conf() const { return m_config; }
  int get_queue_size() const { return m_queue.size(); }

 private:
  std::deque<SchedEntry> m_queue;

  sched_conf_t m_config;

  bool is_in_queue(spg_t pgid, scrub_level_t level)
  {
    auto it = std::find_if(
	m_queue.begin(), m_queue.end(), [pgid, level](const SchedEntry& e) {
	  return e.pgid == pgid && e.level == level && e.is_valid;
	});
    return it != m_queue.end();
  }
};


// ///////////////////////////////////////////////////
// ScrubJob

class ScrubJobTestWrapper : public ScrubJob {
 public:
  ScrubJobTestWrapper(ScrubQueueOpsImp& qops, const spg_t& pg, int osd_num)
      : ScrubJob(qops, g_ceph_context, pg, osd_num)
  {}

  int dequeue_targets() { return ScrubJob::dequeue_targets(); }
  Scrub::SchedTarget& dequeue_target(scrub_level_t lvl)
  {
    return ScrubJob::dequeue_target(lvl);
  }

  void verify_sched_entry(const SchedEntry& e, const expected_entry_t& tst)
  {
    auto now_is = scrub_queue.scrub_clock_now();
    EXPECT_EQ(tst.tst_is_valid.value_or(e.is_valid), e.is_valid);
    EXPECT_EQ(tst.tst_is_ripe.value_or(e.is_ripe(now_is)), e.is_ripe(now_is));
    EXPECT_EQ(tst.tst_level.value_or(e.level), e.level);
    EXPECT_EQ(tst.tst_urgency.value_or(e.urgency), e.urgency);
    EXPECT_EQ(tst.tst_target_time.value_or(e.target), e.target);
    EXPECT_LE(tst.tst_target_time_min.value_or(e.target), e.target);
    EXPECT_GE(tst.tst_target_time_max.value_or(e.target), e.target);
    EXPECT_EQ(tst.tst_nb_time.value_or(e.not_before), e.not_before);
    EXPECT_LE(tst.tst_nb_time_min.value_or(e.not_before), e.not_before);
    EXPECT_GE(tst.tst_nb_time_max.value_or(e.not_before), e.not_before);
  }

  void verify_target(scrub_level_t selector, const expected_target_t tst)
  {
    const auto& e = get_target(selector);
    if (tst.tst_sched_info) {
      verify_sched_entry(e.queued_element(), tst.tst_sched_info.value());
    }
    EXPECT_EQ(tst.tst_in_queue.value_or(e.is_queued()), e.is_queued());
    EXPECT_EQ(tst.tst_is_viable.value_or(!e.is_off()), !e.is_off());
    auto now_is = scrub_queue.scrub_clock_now();
    EXPECT_EQ(
	tst.tst_is_overdue.value_or(e.over_deadline(now_is)),
	e.over_deadline(now_is));
    EXPECT_EQ(tst.tst_delay_cause.value_or(e.delay_cause()), e.delay_cause());
  }

  template <typename T>
  T get_conf_val(std::string c_name) const
  {
    return g_conf().get_val<T>(c_name);
  }
};


class TestScrubSchedJob : public ::testing::Test {
 public:
  TestScrubSchedJob() = default;

  void init(const sjob_config_t& dt, utime_t starting_at)
  {
    // the pg-info is queried for stats validity and for the last-scrub-stamp
    pg_info = create_pg_info(dt);

    m_qops.set_sched_conf(dt.sched_cnf);
    m_qops.set_time_for_testing(starting_at);

    // the scrub-job is created with the initial configuration
    m_sjob = std::make_unique<ScrubJobTestWrapper>(m_qops, dt.spg, 1);
  }

  void set_initial_targets()
  {
    ASSERT_TRUE(m_sjob);
    m_sjob->init_and_queue_targets(
	pg_info, m_qops.get_sched_conf(), m_qops.scrub_clock_now());
  }

  pg_info_t create_pg_info(const sjob_config_t& s)
  {
    pg_info_t pginf;
    pginf.pgid = s.spg;
    pginf.history.last_scrub_stamp = s.levels[0].history_stamp;
    pginf.stats.last_scrub_stamp = s.levels[0].history_stamp;
    pginf.history.last_deep_scrub_stamp = s.levels[1].history_stamp;
    pginf.stats.last_deep_scrub_stamp = s.levels[1].history_stamp;
    pginf.stats.stats_invalid = !s.are_stats_valid;
    return pginf;
  }

 protected:
  int m_num_osds{3};
  int my_osd_id{1};

  spg_t tested_pg;
  ScrubQueueOpsImp m_qops;
  std::unique_ptr<ScrubJobTestWrapper> m_sjob;

  /// the pg-info is queried for stats validity and for the last-scrub-stamp
  pg_info_t pg_info{};

  /// the pool configuration holds some per-pool scrub timing settings
  pool_opts_t pool_opts{};
};


// ///////////////////////////////////////////////////////////////////////////
// test data.

namespace {

// the times used during the tests are offset to 1.1.2000, so that
// utime_t formatting will treat them as absolute (not as a relative time)
static const auto epoch_2000 = 946'684'800;


sched_conf_t t1{
    .shallow_interval = 24 * 3600.0,
    .deep_interval = 7 * 24 * 3600.0,
    .max_shallow = 2 * 24 * 3600.0,
    .max_deep = 7 * 24 * 3600.0,
    .interval_randomize_ratio = 0.2};

sjob_config_t sjob_config_1{
    spg_t{pg_t{1, 1}},
    true,	    // PG has valid stats
    t1,		    // sched_conf_t
    {level_config_t{// shallow
		    utime_t{std::time_t(epoch_2000 + 1'050'000), 0}},

     level_config_t{						       // deep
		    utime_t{std::time_t(epoch_2000 + 1'000'000), 0}}}  // fmt
};

expected_entry_t for_t1_shallow{
    .tst_is_valid = true,
    .tst_is_ripe = true,
    .tst_level = scrub_level_t::shallow,
    .tst_urgency = urgency_t::periodic_regular,
    .tst_target_time_min = utime_t{std::time_t(epoch_2000 + 1'050'000), 0}
    // for fmt
};

static const auto t2_base_time = epoch_2000 + 10'000'000;

// invalid stats
sjob_config_t sjob_config_2{
    spg_t{pg_t{3, 1}},
    false,	    // invalid stats
    t1,		    // sched_conf_t
    {level_config_t{// shallow
		    utime_t{std::time_t(epoch_2000 + 1'050'000), 0}},

     level_config_t{						       // deep
		    utime_t{std::time_t(epoch_2000 + 1'000'000), 0}}}  // fmt
};

static const expected_entry_t for_t2_deep{
    .tst_is_valid = true,
    .tst_is_ripe = false,
    .tst_level = scrub_level_t::deep,
    .tst_urgency = urgency_t::periodic_regular,
    .tst_target_time_min =
	utime_t{std::time_t(t2_base_time + 3 * t1.shallow_interval), 0},
    .tst_nb_time_max = utime_t{std::time_t(t2_base_time + 1'050'000), 0}
    // line-break for fmt
};

static const expected_entry_t for_t2_shallow{
    .tst_is_valid = true,
    .tst_is_ripe = true,
    .tst_level = scrub_level_t::shallow,
    .tst_urgency = urgency_t::must,
    .tst_target_time_min = utime_t{std::time_t(t2_base_time), 0},
    .tst_nb_time_max =
	utime_t{std::time_t(t2_base_time + 3 * t1.shallow_interval), 0}
    // line-break for fmt
};

}  // anonymous namespace


// //////////////////////////// tests ////////////////////////////////////////


TEST_F(TestScrubSchedJob, targets_creation)
{
  utime_t t_at_start{epoch_2000 + 1'000'000, 0};
  init(sjob_config_1, t_at_start);
  std::cout << "\ntime is now: " << m_qops.scrub_clock_now() << std::endl;

  set_initial_targets();
  auto f = Formatter::create_unique("json-pretty");

  m_sjob->dump(f.get());
  f.get()->flush(std::cout);

  EXPECT_EQ(m_qops.get_queue_size(), 2);

  // set a time after both targets are due
  utime_t t_after{epoch_2000 + 1'600'000, 0};
  m_qops.set_time_for_testing(t_after);
  std::cout << "\ntime is now: " << m_qops.scrub_clock_now() << std::endl;

  // the scheduling state should be:
  std::string exp1 = "queued for scrub";
  EXPECT_EQ(exp1, m_sjob->scheduling_state());

  m_sjob->verify_sched_entry(
      m_sjob->shallow_target.queued_element(), for_t1_shallow);
}


TEST_F(TestScrubSchedJob, invalid_history)
{
  utime_t t_at_start{t2_base_time, 0};
  init(sjob_config_2, t_at_start);
  std::cout << "\ntime is now: " << m_qops.scrub_clock_now() << std::endl;

  set_initial_targets();
  auto f = Formatter::create_unique("json-pretty");

  m_sjob->dump(f.get());
  f.get()->flush(std::cout);

  EXPECT_EQ(m_qops.get_queue_size(), 2);

  utime_t t_after{t2_base_time + 100, 0};
  m_qops.set_time_for_testing(t_after);
  std::cout << "\ntime is now: " << m_qops.scrub_clock_now() << std::endl;

  auto nrst = m_sjob->closest_target(m_qops.scrub_clock_now());
  m_sjob->verify_sched_entry(nrst.queued_element(), for_t2_shallow);
  m_sjob->verify_sched_entry(m_sjob->deep_target.queued_element(), for_t2_deep);

  // the scheduling state should be:
  std::string exp1 = "queued for scrub";
  EXPECT_EQ(exp1, m_sjob->scheduling_state());
}

namespace {

sched_conf_t no_rand_conf{
    .shallow_interval = 24 * 3600.0,
    .deep_interval = 7 * 24 * 3600.0,
    .max_shallow = 2 * 24 * 3600.0,
    .max_deep = 7 * 24 * 3600.0,
    .interval_randomize_ratio = 0.0};

static const auto resfail_base_time = epoch_2000 + 31 * 24 * 3600;

static const sjob_config_t sjob_config_resfail{
    spg_t{pg_t{5, 1}},
    true,  // the stats are valid
    no_rand_conf,
    {level_config_t{// shallow
		    utime_t{std::time_t(resfail_base_time - 3'600), 0}},

     level_config_t{
	 // deep
	 utime_t{std::time_t(resfail_base_time + 100'000), 0}}}	 // fmt
};

static const expected_entry_t exp_resfail_b4_dp{
    .tst_is_valid = true,
    .tst_is_ripe = false,
    .tst_level = scrub_level_t::deep,
    .tst_urgency = urgency_t::periodic_regular,
    .tst_target_time_min =
	utime_t{std::time_t(resfail_base_time + 3 * t1.shallow_interval), 0},
    .tst_nb_time_max = utime_t{std::time_t(resfail_base_time + 1'050'000), 0}
    // line-break for fmt
};

static const expected_entry_t exp_resfail_shl_penlzd{
    .tst_is_valid = true,
    .tst_is_ripe = false,
    .tst_level = scrub_level_t::shallow,
    .tst_urgency = urgency_t::penalized,
    .tst_nb_time_min = utime_t{std::time_t(resfail_base_time + 10), 0},
    .tst_nb_time_max = utime_t{std::time_t(resfail_base_time + 20), 0},
    // for fmt
};

static const expected_target_t for_resfail_shallow{
    .tst_sched_info = exp_resfail_shl_penlzd,
    .tst_in_queue = true,
    .tst_is_viable = true,
    .tst_is_overdue = false,
    .tst_delay_cause = delay_cause_t::replicas
    //
};

}  // namespace

TEST_F(TestScrubSchedJob, reservationFail)
{
  utime_t t_at_start{resfail_base_time, 0};
  init(sjob_config_resfail, t_at_start);
  std::cout << "\ntime is now: " << m_qops.scrub_clock_now() << std::endl;

  set_initial_targets();
#ifdef DEV_EXTRA
  {
    auto f = Formatter::create_unique("json-pretty");
    m_sjob->dump(f.get());
    f.get()->flush(std::cout);
  }
#endif

  EXPECT_EQ(m_qops.get_queue_size(), 2);
  m_sjob->scrubbing = true;  // required by the Scrub-Sched code
  m_sjob->dequeue_target(scrub_level_t::shallow);
  m_sjob->dequeue_target(scrub_level_t::deep);

  // suppose a shallow scrub was initiated, and failed repl reservation:
  EXPECT_EQ(m_qops.get_queue_size(), 0);

#ifdef DEV_EXTRA
  std::cout << "conf: "
	    << m_sjob->get_conf_val<int64_t>("osd_scrub_retry_busy_replicas")
	    << std::endl;
#endif

  auto active_target = m_sjob->get_moved_target(scrub_level_t::shallow);
  {
    std::cout << fmt::format("active: {}\n", active_target);
    auto f = Formatter::create_unique("json-pretty");
    m_sjob->dump(f.get());
    f.get()->flush(std::cout);
  }

  auto was_penl =
      m_sjob->on_reservation_failure(200s, std::move(active_target));

#ifdef DEV_EXTRA
  {
    auto f = Formatter::create_unique("json-pretty");
    m_sjob->dump(f.get());
    f.get()->flush(std::cout);
  }
#endif

  // the target wasn't a priority one, thus:
  EXPECT_EQ(true, was_penl);
  m_sjob->verify_target(scrub_level_t::shallow, for_resfail_shallow);
}
