// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "./scrub_queue.h"

#include "osd/OSD.h"
#include "osd/scrubber/not_before_queue.h"

#include "osd_scrub_sched.h"

using namespace std::chrono;
using namespace std::chrono_literals;
using namespace std::literals;


class ScrubQueueImp : public ScrubQueueImp_IF {
  using SchedEntry = Scrub::SchedEntry;

 public:
  ScrubQueueImp() {}

  ~ScrubQueueImp() = default;

  void enqueue_entry(const SchedEntry& entry) final;

  bool remove_entry(spg_t pgid, scrub_level_t s_or_d) final;

  ScrubQueueStats get_stats(utime_t scrub_clock_now) final;

  std::optional<SchedEntry> pop_ready_pg(utime_t scrub_clock_now) final;

  void dump_scrubs(ceph::Formatter* f) const final;

  std::set<spg_t> get_pgs(EntryPred) const final;

  std::vector<SchedEntry> get_entries(EntryPred) const final;

 private:
  not_before_queue_t<SchedEntry> to_scrub;
};


#define dout_context (cct)
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix_target(_dout, this)

template <class T>
static ostream& _prefix_target(std::ostream* _dout, T* t)
{
  return t->gen_prefix(*_dout);
}

ScrubQueue::ScrubQueue(CephContext* cct, Scrub::ScrubSchedListener& osds)
    : cct{cct}
    , osd_service{osds}
    , m_osd_resources{[this](std::string msg) { log_fwd(msg); }, cct->_conf}
    , m_queue_impl{std::make_unique<ScrubQueueImp>()}
    , m_load_tracker{cct, cct->_conf, osd_service.get_nodeid()}
{
  log_prefix = fmt::format("osd.{} scrub-queue::", osd_service.get_nodeid());
}

std::ostream& ScrubQueue::gen_prefix(std::ostream& out) const
{
  return out << log_prefix;
}

utime_t ScrubQueue::scrub_clock_now() const
{
  return ceph_clock_now();
}


// ////////////////////////////////////////////////////////////////////////// //
// queue manipulation - implementing the ScrubQueueOps interface

using SchedEntry = Scrub::SchedEntry;
using urgency_t = Scrub::urgency_t;

void ScrubQueue::enqueue_targets(
    spg_t pgid,
    const SchedEntry& shallow,
    const SchedEntry& deep)
{
  dout(20) << fmt::format(
		  "{}: pg[{}]: queuing <{}> & <{}>", __func__, pgid, shallow,
		  deep)
	   << dendl;
  ceph_assert(shallow.pgid == pgid && deep.pgid == pgid);
  // urgency is only set to 'off' when the PG is removed from the queue:
  ceph_assert(shallow.urgency != urgency_t::off);
  ceph_assert(deep.urgency != urgency_t::off);

  std::unique_lock l{jobs_lock};
  m_queue_impl->enqueue_entry(shallow);
  m_queue_impl->enqueue_entry(deep);
}

void ScrubQueue::remove_entry(spg_t pgid, scrub_level_t s_or_d)
{
  dout(20) << fmt::format(
		  "{}: removing {}/{} from the scrub-queue", __func__, pgid,
		  s_or_d)
	   << dendl;
  std::unique_lock l{jobs_lock};
  std::ignore = m_queue_impl->remove_entry(pgid, s_or_d);
}

void ScrubQueue::enqueue_target(SchedEntry t)
{
  dout(20) << fmt::format("{}: restoring {} to the scrub-queue", __func__, t)
	   << dendl;
  ceph_assert(t.urgency > urgency_t::off);
  std::unique_lock l{jobs_lock};
  m_queue_impl->enqueue_entry(t);
}

// note: a sub-optimal implementation, as only for debugging
// (to be replaced with a queue-class based code)
size_t ScrubQueue::count_queued(spg_t pgid) const
{
  auto select_pg = [pgid](const SchedEntry& se, bool _) -> bool {
    return se.pgid == pgid;
  };
  std::lock_guard lck(jobs_lock);
  auto pg_targes = m_queue_impl->get_entries(select_pg);
  return pg_targes.size();
}

void ScrubQueue::dump_scrubs(ceph::Formatter* f) const
{
  std::lock_guard lck(jobs_lock);
  m_queue_impl->dump_scrubs(f);
}

Scrub::ScrubResources& ScrubQueue::resource_bookkeeper()
{
  return m_osd_resources;
}

const Scrub::ScrubResources& ScrubQueue::resource_bookkeeper() const
{
  return m_osd_resources;
}

void ScrubQueue::log_fwd(std::string_view text)
{
  dout(20) << text << dendl;
}


// ////////////////////////////////////////////////////////////////////////// //
// initiating a scrub

using OSDRestrictions = Scrub::OSDRestrictions;
using schedule_result_t = Scrub::schedule_result_t;

void ScrubQueue::initiate_a_scrub(
    const ceph::common::ConfigProxy& config,
    bool is_recovery_active)
{
  utime_t scrub_tick_time = scrub_clock_now();
  dout(10) << fmt::format(
		  "time now:{}, is_recovery_active:{}", scrub_tick_time,
		  is_recovery_active)
	   << dendl;

  std::scoped_lock both_locks(jobs_lock, m_loop_lock);

  // is there already an active 'scrub-loop'? (i.e. - are we in the middle of
  // the asynchronous process of going over the ready-to-scrub PGs, trying
  // them one by one?)
  if (m_initiation_loop) {
    /// \todo add a timeout here
    dout(10)
	<< fmt::format(
	       "{}: already looking for scrub candidate (since {}). skipping",
	       __func__, m_initiation_loop->loop_id)
	<< dendl;
    return;
  }

  ceph_assert(m_queue_impl);
  auto queue_stats = m_queue_impl->get_stats(scrub_tick_time);
  dout(20) << fmt::format(
		  "{}: in queue: {} ready, {} future, {} total", __func__,
		  queue_stats.num_ready,
		  queue_stats.num_total - queue_stats.num_ready,
		  queue_stats.num_total)
	   << dendl;

  // for debug logs - list all jobs in the queue
  debug_log_queue(queue_stats);
  if (queue_stats.num_ready == 0) {
    dout(10) << fmt::format(
		    "{}: no eligible scrub targets among the {} queued",
		    __func__, queue_stats.num_total)
	     << dendl;
    return;
  }

  // check the OSD-wide environment conditions (scrub resources, time, etc.).
  // These may restrict the type of scrubs we are allowed to start, or just
  // prevent us from starting any scrub at all.
  auto env_restrictions =
      restrictions_on_scrubbing(config, is_recovery_active, scrub_tick_time);
  if (!env_restrictions) {
    return;
  }

  auto candidate = m_queue_impl->pop_ready_pg(scrub_tick_time);
  ceph_assert(candidate);  // we did check that the queue is not empty
  dout(20) << fmt::format(
		  "{}: scrub candidate is pg[{}] ({}). There are {} eligible "
		  "and {} future targets behind it "
		  "in the queue",
		  __func__, candidate->pgid, *candidate,
		  queue_stats.num_ready - 1,
		  queue_stats.num_total - queue_stats.num_ready)
	   << dendl;

  // as we will be (asynchronously) going over the ready-to-scrub PGs, let us
  // maintain the 'loop' object. It will be used to identify the current
  // 'scrub-loop' (and will be reset when the loop ends). It would also be
  // used to limit the number of PGs tried.

  auto max_pgs_to_try = std::min<uint32_t>(queue_stats.num_ready, 40U);
#ifdef SCRUB_QUEUE_TESTING
  max_pgs_to_try = queue_stats.num_ready + 5;
#endif
  m_initiation_loop = std::make_optional<ScrubStartLoop>(
      scrub_tick_time, max_pgs_to_try, *env_restrictions, candidate->pgid,
      candidate->level);

  // send a message to that top-of-the-queue PG to start scrubbing
  osd_service.queue_for_scrub_initiation(
      candidate->pgid, candidate->level, m_initiation_loop->loop_id,
      *env_restrictions);
}

// called with 'jobs_lock' held
void ScrubQueue::debug_log_queue(ScrubQueueStats queue_stats) const
{
  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    const int max_to_log = 6;

    auto select_ready = [](const SchedEntry&, bool is_eligible) -> bool {
      return is_eligible;
    };
    auto ready_jobs = m_queue_impl->get_entries(select_ready);
    dout(20) << fmt::format("{}: ready scrub targets:", __func__) << dendl;
    std::for_each_n(
	ready_jobs.begin(), std::min<int>(max_to_log, ready_jobs.size()),
	[this](const auto& sj) {
	  dout(20) << fmt::format(" scrub-queue job: {}", sj) << dendl;
	});


    auto select_not_ready = [](const SchedEntry&, bool is_eligible) -> bool {
      return !is_eligible;
    };
    auto not_ready_jobs = m_queue_impl->get_entries(select_not_ready);
    dout(20) << fmt::format("{}: not-ready scrub targets:", __func__) << dendl;
    std::for_each_n(
	not_ready_jobs.begin(),
	std::min<int>(max_to_log, not_ready_jobs.size()),
	[this](const auto& sj) {
	  dout(20) << fmt::format(" scrub-queue job: {}", sj) << dendl;
	});

    // and for even higher debug level - list all ready PGs in the queue
    if (g_conf()->subsys.should_gather<ceph_subsys_osd, 25>()) {
      dout(20) << fmt::format("{}: eligible PGs in the queue:", __func__)
	       << dendl;
      auto ready_pgs = m_queue_impl->get_pgs(select_ready);
      std::for_each_n(
	  ready_pgs.begin(), std::min<int>(max_to_log, ready_pgs.size()),
	  [this](const auto& rpg) {
	    dout(20) << fmt::format(" scrub-queue: pg[{}]", rpg) << dendl;
	  });
    }
  }
}


void ScrubQueue::scrub_next_in_queue(utime_t loop_id)
{
  dout(10) << fmt::format("{}: scrub-loop (with ID {}) cont...", __func__,
                          loop_id)
           << dendl;
  std::scoped_lock both_locks(jobs_lock, m_loop_lock);

  // are we indeed in the middle of a 'scrub-loop'?
  if (!m_initiation_loop) {
    dout(20) << fmt::format(
		    "{}: no active scrub-loop (suggested loop ID:{:s})", __func__,
		    loop_id)
	     << dendl;
    return;
  }

  // verify that we are not receiving a message from a previous loop
  if (m_initiation_loop->loop_id != loop_id) {
    dout(10) << fmt::format(
		    "{}: loop-id mismatch. skipping. ({:s} != {:s})", __func__,
		    m_initiation_loop->loop_id, loop_id)
	     << dendl;
    return;
  }

  // are we allowed to continue the loop?
  if (++m_initiation_loop->attempted >= m_initiation_loop->retries_budget) {
    dout(10) << fmt::format(
		    "{}: reached the max number of PGs to try. ending the loop",
		    __func__)
	     << dendl;
    m_initiation_loop.reset();
    return;
  }

  const utime_t scrub_tick_time = scrub_clock_now();
  auto queue_stats = m_queue_impl->get_stats(scrub_tick_time);
  if (queue_stats.num_ready == 0) {
    dout(10) << fmt::format(
		    "{}: no eligible scrub targets among the {} queued",
		    __func__, queue_stats.num_total)
	     << dendl;
    m_initiation_loop.reset();
    return;
  }

  auto candidate = m_queue_impl->pop_ready_pg(scrub_tick_time);
  ceph_assert(candidate);

  // a sanity check: do not try the same PG twice
  if (candidate->pgid == m_initiation_loop->first_pg_tried &&
      candidate->level == m_initiation_loop->first_level_tried) {
    dout(10) << fmt::format(
		    "{}: retrying the first sched target. ending the loop",
		    __func__)
	     << dendl;
    m_initiation_loop.reset();
    // must requeue!
    m_queue_impl->enqueue_entry(*candidate);
    return;
  }
  dout(10) << fmt::format(
		  "{}: scrub candidate is {}. {} candidates in the queue",
		  __func__, candidate->pgid, queue_stats.num_total)
	   << dendl;

  // send a message to that PG to start scrubbing
  osd_service.queue_for_scrub_initiation(
      candidate->pgid, candidate->level, m_initiation_loop->loop_id,
      m_initiation_loop->env_restrictions);
}


void ScrubQueue::initiation_loop_done(Scrub::loop_token_t loop_id)
{
  std::scoped_lock lock(m_loop_lock);

  // are we indeed in the middle of a 'scrub-loop'?
  if (!m_initiation_loop) {
    dout(10) << fmt::format("{}: no active scrub-loop. skipping", __func__)
	     << dendl;
    return;
  }
  // verify that we are not seeing a message from a previous loop
  if (m_initiation_loop->loop_id != loop_id) {
    dout(10) << fmt::format(
		    "{}: loop-id mismatch. skipping. ({:s} != {:s})", __func__,
		    m_initiation_loop->loop_id, loop_id)
	     << dendl;
    return;
  }

  dout(20) << fmt::format("{}: scrub-loop (with ID {}) done", __func__, loop_id)
	   << dendl;
  m_initiation_loop.reset();
}


std::optional<Scrub::OSDRestrictions> ScrubQueue::restrictions_on_scrubbing(
    const ceph::common::ConfigProxy& config,
    bool is_recovery_active,
    utime_t scrub_clock_now) const
{
  if (auto blocked_pgs = get_blocked_pgs_count(); blocked_pgs > 0) {
    // some PGs managed by this OSD were blocked by a locked object during
    // scrub. This means we might not have the resources needed to scrub now.
    dout(10) << fmt::format(
		    "{}: PGs are blocked while scrubbing due to locked objects "
		    "({} PGs)",
		    __func__, blocked_pgs)
	     << dendl;
  }

  // sometimes we just skip the scrubbing
  if (Scrub::random_bool_with_probability(config->osd_scrub_backoff_ratio)) {
    dout(20) << fmt::format(
		    "{}: lost coin flip, randomly backing off (ratio: {:f})",
		    __func__, config->osd_scrub_backoff_ratio)
	     << dendl;
    return std::nullopt;
  }

  // our local OSD may already be running too many scrubs
  if (!resource_bookkeeper().can_inc_scrubs()) {
    dout(10) << fmt::format("{}: OSD cannot inc scrubs", __func__) << dendl;
    return std::nullopt;
  }

  // if there is a PG that is just now trying to reserve scrub replica resources
  // - we should wait and not initiate a new scrub
  if (is_reserving_now()) {
    dout(10) << fmt::format(
		    "{}: scrub resources reservation in progress", __func__)
	     << dendl;
    return std::nullopt;
  }

  Scrub::OSDRestrictions env_conditions;
  env_conditions.time_permit = scrub_time_permit();
  env_conditions.load_is_low = m_load_tracker.scrub_load_below_threshold();
  env_conditions.only_deadlined =
      !env_conditions.time_permit || !env_conditions.load_is_low;

  if (is_recovery_active && !config->osd_scrub_during_recovery) {
    if (!config->osd_repair_during_recovery) {
      dout(15) << fmt::format(
		      "{}: not scheduling scrubs due to active recovery",
		      __func__)
	       << dendl;
      return std::nullopt;
    }

    dout(10) << fmt::format(
		    "{}: will only schedule explicitly requested repair due to "
		    "active recovery",
		    __func__)
	     << dendl;
    env_conditions.allow_requested_repair_only = true;
  }

  return env_conditions;
}

/**
 * on_config_times_change() (the refactored "OSD::sched_all_scrubs()")
 *
 * Scans the queue for entries that are "periodic", and messages the PGs
 * named in those entries to recalculate their scrub scheduling
 */
void ScrubQueue::on_config_times_change()
{
  std::unique_lock l{jobs_lock};
  auto to_notify = m_queue_impl->get_pgs(
      [](const SchedEntry& e, [[maybe_unused]] bool is_rdy) -> bool {
	return e.urgency == urgency_t::periodic_regular;
      });
  l.unlock();

  for (const auto& p : to_notify) {
    dout(15) << fmt::format("{}: rescheduling {}", __func__, p) << dendl;
    auto locked_pg = osd_service.get_locked_pg(p);
    if (!locked_pg)
      continue;

    dout(15) << fmt::format(
		    "{}: updating scrub schedule on {}", __func__,
		    (locked_pg->pg())->get_pgid())
	     << dendl;
    locked_pg->pg()->on_scrub_schedule_input_change();
  }
}

// ////////////////////////////////////////////////////////////////////////// //
// CPU load tracking and related

ScrubQueue::LoadTracker::LoadTracker(
    CephContext* cct,
    const ceph::common::ConfigProxy& config,
    int node_id)
    : cct{cct}
    , cnf(config)
{
  log_prefix = fmt::format("osd.{} scrub-queue::load-tracker::", node_id);

  // initialize the daily loadavg with current 15min loadavg
  if (double loadavgs[3]; getloadavg(loadavgs, 3) == 3) {
    daily_loadavg = loadavgs[2];
  } else {
    derr << "OSD::init() : couldn't read loadavgs\n" << dendl;
    daily_loadavg = 1.0;
  }
}

std::optional<double> ScrubQueue::LoadTracker::update_load_average()
{
  int hb_interval = cnf->osd_heartbeat_interval;
  int n_samples = 60 * 24 * 24;
  if (hb_interval > 1) {
    n_samples = std::max(n_samples / hb_interval, 1);
  }

  double loadavg;
  if (getloadavg(&loadavg, 1) == 1) {
    daily_loadavg = (daily_loadavg * (n_samples - 1) + loadavg) / n_samples;
    return 100 * loadavg;
  }

  return std::nullopt;
}

bool ScrubQueue::LoadTracker::scrub_load_below_threshold() const
{
  double loadavgs[3];
  if (getloadavg(loadavgs, 3) != 3) {
    dout(10) << fmt::format("{}: couldn't read loadavgs", __func__) << dendl;
    return false;
  }

  // allow scrub if below configured threshold
  long cpus = sysconf(_SC_NPROCESSORS_ONLN);
  double loadavg_per_cpu = cpus > 0 ? loadavgs[0] / cpus : loadavgs[0];
  if (loadavg_per_cpu < cnf->osd_scrub_load_threshold) {
    dout(20) << fmt::format(
	"loadavg per cpu {} < max {} = yes", loadavg_per_cpu,
	cnf->osd_scrub_load_threshold) << dendl;
    return true;
  }

  // allow scrub if below daily avg and currently decreasing
  if (loadavgs[0] < daily_loadavg && loadavgs[0] < loadavgs[2]) {
    dout(20) << fmt::format(
	"loadavg {} < daily_loadavg {} and < 15m avg {} = yes", loadavgs[0],
	daily_loadavg, loadavgs[2]) << dendl;
    return true;
  }

  dout(10) << fmt::format(
      "loadavg {} >= max {} and ( >= daily_loadavg {} or >= 15m "
      "avg {} ) = no",
      loadavgs[0], cnf->osd_scrub_load_threshold, daily_loadavg,
      loadavgs[2]) << dendl;
  return false;
}

std::ostream& ScrubQueue::LoadTracker::gen_prefix(std::ostream& out) const
{
  return out << log_prefix;
}


// checks for half-closed ranges. Modify the (p<till)to '<=' to check for
// closed.
static inline bool isbetween_modulo(int64_t from, int64_t till, int p)
{
  // the 1st condition is because we have defined from==till as "always true"
  return (till == from) || ((till >= from) ^ (p >= from) ^ (p < till));
}

bool ScrubQueue::scrub_time_permit(utime_t now) const
{
  const time_t tt = now.sec();
  tm bdt;
  localtime_r(&tt, &bdt);

  bool day_permit = isbetween_modulo(
      conf()->osd_scrub_begin_week_day, conf()->osd_scrub_end_week_day,
      bdt.tm_wday);
  if (!day_permit) {
    dout(20) << fmt::format(
		    "{}: should run between week day {} - {} now {} - no",
		    __func__, conf()->osd_scrub_begin_week_day,
		    conf()->osd_scrub_end_week_day, bdt.tm_wday)
	     << dendl;
    return false;
  }

  bool time_permit = isbetween_modulo(
      conf()->osd_scrub_begin_hour, conf()->osd_scrub_end_hour, bdt.tm_hour);
  dout(20) << fmt::format(
		  "{}: should run between {} - {} now {} = {}", __func__,
		  conf()->osd_scrub_begin_hour, conf()->osd_scrub_end_hour,
		  bdt.tm_hour, (time_permit ? "yes" : "no"))
	   << dendl;
  return time_permit;
}

bool ScrubQueue::scrub_time_permit() const
{
  return scrub_time_permit(scrub_clock_now());
}

milliseconds ScrubQueue::required_sleep_time(bool high_priority_scrub) const
{
  const milliseconds regular_sleep_period =
      milliseconds{int64_t(1000 * conf()->osd_scrub_sleep)};

  if (high_priority_scrub || scrub_time_permit()) {
    return regular_sleep_period;
  }

  // relevant if scrubbing started during allowed time, but continued into
  // forbidden hours
  const milliseconds extended_sleep =
      milliseconds{int64_t(1000 * conf()->osd_scrub_extended_sleep)};
  dout(20)
      << fmt::format(
	     "{}: scrubbing started during allowed time, but continued into "
	     "forbidden hours. regular_sleep_period {} extended_sleep {}",
	     __func__, regular_sleep_period, extended_sleep)
      << dendl;
  return std::max(extended_sleep, regular_sleep_period);
}

// ////////////////////////////////////////////////////////////////////////// //
// auxiliaries

Scrub::sched_conf_t ScrubQueue::populate_config_params(
    const pool_opts_t& pool_conf) const
{
  Scrub::sched_conf_t configs;

  // deep-scrub optimal interval
  configs.deep_interval =
      pool_conf.value_or(pool_opts_t::DEEP_SCRUB_INTERVAL, 0.0);
  if (configs.deep_interval <= 0.0) {
    configs.deep_interval = conf()->osd_deep_scrub_interval;
  }

  // shallow-scrub interval
  configs.shallow_interval =
      pool_conf.value_or(pool_opts_t::SCRUB_MIN_INTERVAL, 0.0);
  if (configs.shallow_interval <= 0.0) {
    configs.shallow_interval = conf()->osd_scrub_min_interval;
  }

  // the max allowed delay between scrubs.
  // For deep scrubs - there is no equivalent of scrub_max_interval. Per the
  // documentation, once deep_scrub_interval has passed, we are already
  // "overdue", at least as far as the "ignore allowed load" window is
  // concerned.

  configs.max_deep = configs.deep_interval + configs.shallow_interval;

  auto max_shallow = pool_conf.value_or(pool_opts_t::SCRUB_MAX_INTERVAL, 0.0);
  if (max_shallow <= 0.0) {
    max_shallow = conf()->osd_scrub_max_interval;
  }
  if (max_shallow > 0.0) {
    configs.max_shallow = max_shallow;
    // otherwise - we're left with the default nullopt
  }

  // but seems like our tests require: \todo fix!
  configs.max_deep =
      std::max(configs.max_shallow.value_or(0.0), configs.deep_interval);

  configs.interval_randomize_ratio = conf()->osd_scrub_interval_randomize_ratio;
  configs.deep_randomize_ratio = conf()->osd_deep_scrub_randomize_ratio;
  configs.mandatory_on_invalid = conf()->osd_scrub_invalid_stats;

  dout(15) << fmt::format("updated config:{}", configs) << dendl;
  return configs;
}


// used in ut/debug logs
constexpr int ordering_as_int(std::weak_ordering cmp) noexcept
{
  return (cmp < 0) ? -1 : ((cmp == 0) ? 0 : 1);
}

void ScrubQueue::clear_pg_scrub_blocked(spg_t blocked_pg)
{
  dout(5) << fmt::format("{}: pg[{}] is unblocked", __func__, blocked_pg)
	  << dendl;
  --blocked_scrubs_cnt;
  ceph_assert(blocked_scrubs_cnt >= 0);
}

void ScrubQueue::mark_pg_scrub_blocked(spg_t blocked_pg)
{
  dout(5) << fmt::format(
		 "{}: pg[{}] is blocked on an object", __func__, blocked_pg)
	  << dendl;
  ++blocked_scrubs_cnt;
}

int ScrubQueue::get_blocked_pgs_count() const
{
  return blocked_scrubs_cnt;
}


// ////////////////////////////////////////////////////////////////////////// //
// SchedLoopHolder

using SchedLoopHolder = Scrub::SchedLoopHolder;

SchedLoopHolder::~SchedLoopHolder()
{
  // we may have failed without explicitly handling the sched-loop
  // state. Let's just ignore it (without trying the next in queue)
  conclude_candidates_selection();
}

void SchedLoopHolder::conclude_candidates_selection()
{
  if (m_loop_id) {
    m_queue.initiation_loop_done(*m_loop_id);
    m_loop_id.reset();
  }
}

void SchedLoopHolder::go_for_next_in_queue()
{
  if (m_loop_id) {
    // we must have failed to schedule a scrub
    m_queue.scrub_next_in_queue(*m_loop_id);
    m_loop_id.reset();
  }
}


// ////////////////////////////////////////////////////////////////////////// //
// ScrubQueueImp: container low-level operations

void ScrubQueueImp::enqueue_entry(const SchedEntry& entry)
{
  to_scrub.enqueue(entry);
}

ScrubQueueStats ScrubQueueImp::get_stats(utime_t scrub_clock_now)
{
  to_scrub.advance_time(scrub_clock_now);
  return ScrubQueueStats{
      static_cast<uint16_t>(to_scrub.eligible_count()),
      static_cast<uint16_t>(to_scrub.total_count())};
}

std::optional<SchedEntry> ScrubQueueImp::pop_ready_pg(utime_t scrub_clock_now)
{
  to_scrub.advance_time(scrub_clock_now);
  return to_scrub.dequeue();
}

void ScrubQueueImp::dump_scrubs(ceph::Formatter* f) const
{
  f->open_array_section("scrubs");
  to_scrub.for_each(
      [&f](const SchedEntry& se, [[maybe_unused]] bool is_eligible) {
	se.dump("sched-target", f);
      });
  f->close_section();
}

bool ScrubQueueImp::remove_entry(spg_t pgid, scrub_level_t s_or_d)
{
  auto same_lvl = [s_or_d](const SchedEntry& e) { return e.level == s_or_d; };
  return to_scrub.remove_if_by_class<spg_t, decltype(same_lvl)>(
      pgid, std::move(same_lvl), 1);
}

std::set<spg_t> ScrubQueueImp::get_pgs(ScrubQueueImp_IF::EntryPred pred) const
{
  using acc_t = std::set<spg_t>;
  auto extract_pg =
      [pred](acc_t&& acc, const SchedEntry& se, bool is_eligible) mutable {
	if (pred(se, is_eligible)) {
	  acc.insert(se.pgid);
	}
	return std::move(acc);
      };

  return to_scrub.accumulate<acc_t, decltype(extract_pg)>(
      std::move(extract_pg));
}

std::vector<SchedEntry> ScrubQueueImp::get_entries(EntryPred pred) const
{
  using acc_t = std::vector<SchedEntry>;
  auto by_pred = [pred](
		     acc_t&& acc, const SchedEntry& se,
		     bool is_eligible) mutable -> acc_t {
    if (pred(se, is_eligible)) {
      acc.push_back(se);
    }
    return std::move(acc);
  };
  return to_scrub.accumulate<acc_t, decltype(by_pred)>(std::move(by_pred));
}
