// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/// \file testing the scrub scheduling algorithm

#include <gtest/gtest.h>

#include <algorithm>
#include <map>

#include "common/async/context_pool.h"
#include "common/ceph_argparse.h"
#include "common/Finisher.h"
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
#include "osd/scrubber_common.h"

int main(int argc, char** argv)
{
  std::map<std::string, std::string> defaults = {
    // make sure we have 3 copies, or some tests won't work
    {"osd_pool_default_size", "3"},
    // our map is flat, so just try and split across OSDs, not hosts or whatever
    {"osd_crush_chooseleaf_type", "0"},
  };
  std::vector<const char*> args(argv, argv + argc);
  auto cct = global_init(&defaults,
			 args,
			 CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

using schedule_result_t = Scrub::schedule_result_t;
using ScrubJobRef = Scrub::ScrubJobRef;
using qu_state_t = Scrub::qu_state_t;
using scrub_schedule_t = Scrub::scrub_schedule_t;
using ScrubQContainer = Scrub::ScrubQContainer;

/// enabling access into ScrubQueue internals
class ScrubSchedTestWrapper : public ScrubQueue {
 public:
  ScrubSchedTestWrapper(Scrub::ScrubSchedListener& osds)
      : ScrubQueue(g_ceph_context, osds)
  {}

  void rm_unregistered_jobs()
  {
    ScrubQueue::rm_unregistered_jobs(to_scrub);
  }

  ScrubQContainer collect_ripe_jobs()
  {
    return ScrubQueue::collect_ripe_jobs(
	to_scrub, Scrub::OSDRestrictions{}, time_now());
  }

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

  ~ScrubSchedTestWrapper() override = default;
};


/**
 * providing the small number of OSD services used when scheduling
 * a scrub
 */
class FakeOsd : public Scrub::ScrubSchedListener {
 public:
  FakeOsd(int osd_num) : m_osd_num(osd_num) {}

  int get_nodeid() const final { return m_osd_num; }

  void set_initiation_response(spg_t pgid, schedule_result_t result)
  {
    m_next_response[pgid] = result;
  }

  std::optional<PGLockWrapper> get_locked_pg(spg_t pgid)
  {
    std::ignore = pgid;
    return std::nullopt;
  }

  AsyncReserver<spg_t, Finisher>& get_scrub_reserver() final
  {
    return m_scrub_reserver;
  }

 private:
  int m_osd_num;
  std::map<spg_t, schedule_result_t> m_next_response;
  Finisher reserver_finisher{g_ceph_context};
  AsyncReserver<spg_t, Finisher> m_scrub_reserver{
      g_ceph_context, &reserver_finisher, 1};
};


/// the static blueprint for creating a scrub job in the scrub queue
struct sjob_config_t {
  spg_t spg;
  bool are_stats_valid;

  utime_t history_scrub_stamp;
  std::optional<double> pool_conf_min;
  std::optional<double> pool_conf_max;
  bool is_must;
  bool is_need_auto;
  scrub_schedule_t initial_schedule;
};


/**
 * the runtime configuration for a scrub job. Created basde on the blueprint
 * above (sjob_config_t)
 */
struct sjob_dynamic_data_t {
  sjob_config_t initial_config;
  pg_info_t mocked_pg_info;
  pool_opts_t mocked_pool_opts;
  requested_scrub_t request_flags;
  ScrubJobRef job;
};

class TestScrubSched : public ::testing::Test {
 public:
  TestScrubSched() = default;

 protected:
  int m_osd_num{1};
  FakeOsd m_osds{m_osd_num};
  std::unique_ptr<ScrubSchedTestWrapper> m_sched{
    new ScrubSchedTestWrapper(m_osds)};

  /// the pg-info is queried for stats validity and for the last-scrub-stamp
  pg_info_t pg_info{};

  /// the pool configuration holds some per-pool scrub timing settings
  pool_opts_t pool_opts{};

  /**
   * the scrub-jobs created for the tests, along with their corresponding
   * "pg info" and pool configuration. In real life - the scrub jobs
   * are owned by the respective PGs.
   */
  std::vector<sjob_dynamic_data_t> m_scrub_jobs;

 protected:
  sjob_dynamic_data_t create_scrub_job(const sjob_config_t& sjob_data)
  {
    sjob_dynamic_data_t dyn_data;
    dyn_data.initial_config = sjob_data;

    // populate the 'pool options' object with the scrub timing settings
    if (sjob_data.pool_conf_min) {
      dyn_data.mocked_pool_opts.set<double>(pool_opts_t::SCRUB_MIN_INTERVAL,
					    sjob_data.pool_conf_min.value());
    }
    if (sjob_data.pool_conf_max) {
      dyn_data.mocked_pool_opts.set(pool_opts_t::SCRUB_MAX_INTERVAL,
				    sjob_data.pool_conf_max.value());
    }

    // create the 'pg info' object with the stats
    dyn_data.mocked_pg_info = pg_info_t{sjob_data.spg};

    dyn_data.mocked_pg_info.history.last_scrub_stamp =
      sjob_data.history_scrub_stamp;
    dyn_data.mocked_pg_info.stats.stats_invalid = !sjob_data.are_stats_valid;

    // fake hust the required 'requested-scrub' flags
    std::cout << "request_flags: sjob_data.is_must " << sjob_data.is_must
	      << std::endl;
    dyn_data.request_flags.must_scrub = sjob_data.is_must;
    dyn_data.request_flags.need_auto = sjob_data.is_need_auto;

    // create the scrub job
    dyn_data.job = ceph::make_ref<Scrub::ScrubJob>(g_ceph_context,
							sjob_data.spg,
							m_osd_num);
    m_scrub_jobs.push_back(dyn_data);
    return dyn_data;
  }

  void register_job_set(const std::vector<sjob_config_t>& job_configs)
  {
    std::for_each(job_configs.begin(),
		  job_configs.end(),
		  [this](const sjob_config_t& sj) {
		    auto dynjob = create_scrub_job(sj);
		    m_sched->register_with_osd(
		      dynjob.job,
		      m_sched->determine_scrub_time(dynjob.request_flags,
						    dynjob.mocked_pg_info,
						    dynjob.mocked_pool_opts));
		  });
  }

  /// count the scrub-jobs that are currently in a specific state
  int count_scrub_jobs_in_state(qu_state_t state)
  {
    return std::count_if(m_scrub_jobs.begin(),
			 m_scrub_jobs.end(),
			 [state](const sjob_dynamic_data_t& sj) {
			   return sj.job->state == state;
			 });
  }

  void list_testers_jobs(std::string hdr)
  {
    std::cout << fmt::format("{}: {} jobs created for the test:",
			     hdr,
			     m_scrub_jobs.size())
	      << std::endl;
    for (const auto& job : m_scrub_jobs) {
      std::cout << fmt::format("\t{}: job {}", hdr, *job.job) << std::endl;
    }
  }

  void print_all_states(std::string hdr)
  {
    std::cout << fmt::format(
		   "{}: Created:{}. Per state: not-reg:{} reg:{} unreg:{}",
		   hdr,
		   m_scrub_jobs.size(),
		   count_scrub_jobs_in_state(qu_state_t::not_registered),
		   count_scrub_jobs_in_state(qu_state_t::registered),
		   count_scrub_jobs_in_state(qu_state_t::unregistering))
	      << std::endl;
  }

  void debug_print_jobs(std::string hdr,
			const ScrubQContainer& jobs)
  {
    std::cout << fmt::format("{}: time now {}", hdr, m_sched->time_now())
	      << std::endl;
    for (const auto& job : jobs) {
      std::cout << fmt::format(
		     "\t{}: job {} ({}): scheduled {}",
		     hdr,
		     job->pgid,
		     job->scheduling_state(m_sched->time_now(), false),
		     job->get_sched_time())
		<< std::endl;
    }
  }
};

// ///////////////////////////////////////////////////////////////////////////
// test data. Scrub-job creation requires a PG-id, and a set of 'scrub request'
// flags

namespace {

// the times used during the tests are offset to 1.1.2000, so that
// utime_t formatting will treat them as absolute (not as a relative time)
static const auto epoch_2000 = 946'684'800;

std::vector<sjob_config_t> sjob_configs = {
  {
    spg_t{pg_t{1, 1}},
    true,					      // PG has valid stats
    utime_t{std::time_t(epoch_2000 + 1'000'000), 0},  // last-scrub-stamp
    100.0,			    // min scrub delay in pool config
    std::nullopt,		    // max scrub delay in pool config
    false,			    // must-scrub
    false,			    // need-auto
    scrub_schedule_t{}  // initial schedule
  },

  {spg_t{pg_t{4, 1}},
   true,
   utime_t{epoch_2000 + 1'000'000, 0},
   100.0,
   std::nullopt,
   true,
   false,
   scrub_schedule_t{}},

  {spg_t{pg_t{7, 1}},
   true,
   utime_t{},
   1.0,
   std::nullopt,
   false,
   false,
   scrub_schedule_t{}},

  {spg_t{pg_t{5, 1}},
   true,
   utime_t{epoch_2000 + 1'900'000, 0},
   1.0,
   std::nullopt,
   false,
   false,
   scrub_schedule_t{}}};

}  // anonymous namespace

// //////////////////////////// tests ////////////////////////////////////////

/// basic test: scheduling simple jobs, validating their calculated schedule
TEST_F(TestScrubSched, populate_queue)
{
  ASSERT_EQ(0, m_sched->list_registered_jobs().size());

  auto dynjob_0 = create_scrub_job(sjob_configs[0]);
  auto suggested = m_sched->determine_scrub_time(dynjob_0.request_flags,
						 dynjob_0.mocked_pg_info,
						 dynjob_0.mocked_pool_opts);
  m_sched->register_with_osd(dynjob_0.job, suggested);
  std::cout << fmt::format("scheduled at: {}", dynjob_0.job->get_sched_time())
	    << std::endl;

  auto dynjob_1 = create_scrub_job(sjob_configs[1]);
  suggested = m_sched->determine_scrub_time(dynjob_1.request_flags,
					    dynjob_1.mocked_pg_info,
					    dynjob_1.mocked_pool_opts);
  m_sched->register_with_osd(dynjob_1.job, suggested);
  std::cout << fmt::format("scheduled at: {}", dynjob_1.job->get_sched_time())
	    << std::endl;

  EXPECT_EQ(dynjob_1.job->get_sched_time(), utime_t(1, 1));
  EXPECT_EQ(2, m_sched->list_registered_jobs().size());
}

/// validate the states of the scrub-jobs (as set in the jobs themselves)
TEST_F(TestScrubSched, states)
{
  m_sched->set_time_for_testing(epoch_2000);
  register_job_set(sjob_configs);
  list_testers_jobs("testing states");
  EXPECT_EQ(sjob_configs.size(), m_sched->list_registered_jobs().size());

  // check the initial state of the jobs
  print_all_states("<initial state>");
  m_sched->rm_unregistered_jobs();
  EXPECT_EQ(0, count_scrub_jobs_in_state(qu_state_t::not_registered));

  // now - remove a couple of them
  m_sched->remove_from_osd_queue(m_scrub_jobs[2].job);
  m_sched->remove_from_osd_queue(m_scrub_jobs[1].job);
  m_sched->remove_from_osd_queue(m_scrub_jobs[2].job);	// should have no effect

  print_all_states("<w/ 2 jobs removed>");
  EXPECT_EQ(2, count_scrub_jobs_in_state(qu_state_t::registered));
  EXPECT_EQ(2, count_scrub_jobs_in_state(qu_state_t::unregistering));

  m_sched->rm_unregistered_jobs();
  EXPECT_EQ(2, count_scrub_jobs_in_state(qu_state_t::not_registered));
  std::cout << fmt::format("inp size: {}. In list-registered: {}",
			   sjob_configs.size(),
			   m_sched->list_registered_jobs().size())
	    << std::endl;
  EXPECT_EQ(sjob_configs.size() - 2, m_sched->list_registered_jobs().size());
}

/// jobs that are ripe should be in the ready list, sorted by their scheduled
/// time
TEST_F(TestScrubSched, ready_list)
{
  m_sched->set_time_for_testing(epoch_2000 + 900'000);
  register_job_set(sjob_configs);
  list_testers_jobs("testing states");
  EXPECT_EQ(sjob_configs.size(), m_sched->list_registered_jobs().size());

  m_sched->set_time_for_testing(epoch_2000 + 1'000'000);
  auto all_reg_jobs = m_sched->list_registered_jobs();
  debug_print_jobs("registered", all_reg_jobs);

  auto ripe_jobs = m_sched->collect_ripe_jobs();
  EXPECT_EQ(2, ripe_jobs.size());
  debug_print_jobs("ready_list", ripe_jobs);

  m_sched->set_time_for_testing(epoch_2000 + 3'000'000);
  // all jobs should be in the ready list
  ripe_jobs = m_sched->collect_ripe_jobs();
  EXPECT_EQ(4, ripe_jobs.size());
  debug_print_jobs("ready_list", ripe_jobs);
}
