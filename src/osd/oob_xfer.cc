// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "osd/oob_xfer.h"

#include <algorithm>
#include <cerrno>

#include <fmt/format.h>

#include "common/HeartbeatMap.h"
#include "common/Thread.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/config_cacher.h"
#include "common/debug.h"
#include "common/Formatter.h"
#include "common/perf_counters.h"
#include "common/rdma_token.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "oob_xfer: "

namespace ceph::osd::oob {

namespace {
/// how often a transfer thread with nothing to run calls idle()
constexpr std::chrono::milliseconds IDLE_INTERVAL{100};

double secs(ceph::signedspan d)
{
  return std::chrono::duration<double>(d).count();
}

ceph::timespan unsigned_span(ceph::signedspan d)
{
  return std::chrono::duration_cast<ceph::timespan>(
    std::max(d, ceph::signedspan::zero()));
}

/// sleep for as long as a debug delay option says, from now. The value
/// is read again every 100 ms, so that a test that lowers the option
/// while a transfer sleeps ends the sleep early
void debug_sleep(const md_config_cacher_t<std::chrono::milliseconds>& delay)
{
  const auto start = ceph::mono_clock::now();
  while (true) {
    const auto until = start + *delay;
    const auto now = ceph::mono_clock::now();
    if (now >= until) {
      return;
    }
    std::this_thread::sleep_for(
      std::min<ceph::timespan>(until - now, std::chrono::milliseconds(100)));
  }
}
}

bounds_t make_bounds(ceph::mono_clock::time_point received, double lease,
		     double drain,
		     std::optional<ceph::signedspan> readable_until,
		     ceph::signedspan mnow)
{
  bounds_t b;
  b.land_by = received + delivery_window(lease, drain);
  b.post_by = received + ceph::make_timespan(lease);
  if (readable_until) {
    // within an interval readable_until only grows, so the PG's lease
    // as it is now is a safe bound
    b.post_by = std::min(b.post_by,
			 ceph::mono_clock::now() + (*readable_until - mnow));
  }
  return b;
}

bool lease_expired(const bounds_t& b)
{
  return ceph::mono_clock::now() > b.post_by;
}

std::chrono::milliseconds budget_left(const bounds_t& b)
{
  const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
    b.land_by - ceph::mono_clock::now());
  return std::max(left, std::chrono::milliseconds::zero());
}

struct debug_delays_t {
  md_config_cacher_t<std::chrono::milliseconds> post;
  md_config_cacher_t<std::chrono::milliseconds> done;

  explicit debug_delays_t(ConfigProxy& conf)
    : post(conf, "osd_debug_oob_delay_post_ms"),
      done(conf, "osd_debug_oob_delay_done_ms") {}
};

void xfer_job::debug_delay_done() const
{
  if (delays) {
    debug_sleep(delays->done);
  }
}

Transfers::Transfers(CephContext* cct, unsigned threads, unsigned max_jobs,
		     uint64_t max_bytes, ceph::timespan thread_grace,
		     std::function<void()> idle)
  : cct(cct), nthreads(threads), max_jobs(max_jobs), max_bytes(max_bytes),
    thread_grace(thread_grace), idle(std::move(idle)),
    delays(std::make_unique<debug_delays_t>(cct->_conf))
{
  PerfCountersBuilder b(cct, "oob_xfer", l_oob_xfer_first, l_oob_xfer_last);
  b.set_prio_default(PerfCountersBuilder::PRIO_USEFUL);
  b.add_u64(l_oob_xfer_queued, "queued",
	    "Out-of-band transfers waiting for a transfer thread");
  b.add_u64(l_oob_xfer_running, "running",
	    "Out-of-band transfers running");
  b.add_u64_counter(l_oob_xfer_refused, "refused",
		    "Out-of-band transfers refused over a cap or while "
		    "stopping: a push is delivered inline", nullptr,
		    PerfCountersBuilder::PRIO_USEFUL);
  b.add_u64_counter(l_oob_xfer_expired, "expired",
		    "Out-of-band transfers taken past the time they could "
		    "start: a push is delivered inline", nullptr,
		    PerfCountersBuilder::PRIO_USEFUL);
  b.add_u64_counter(l_oob_xfer_shed, "shed",
		    "Out-of-band transfers still queued when the OSD stopped",
		    nullptr, PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_oob_xfer_queue_lat, "queue_lat",
		 "Time an out-of-band transfer waited for a transfer thread",
		 nullptr, PerfCountersBuilder::PRIO_USEFUL);
  b.add_time_avg(l_oob_xfer_push_lat, "push_lat",
		 "Time an out-of-band push took", nullptr,
		 PerfCountersBuilder::PRIO_USEFUL);
  logger = {b.create_perf_counters(), cct};
  cct->get_perfcounters_collection()->add(logger.get());
}

Transfers::~Transfers()
{
  stop();
}

void Transfers::start()
{
  dout(1) << nthreads << " transfer threads, at most " << max_jobs
	  << " jobs and " << max_bytes << " bytes" << dendl;
  for (unsigned i = 0; i < nthreads; i++) {
    threads.push_back(make_named_thread("oob_xfer", &Transfers::worker,
					this, i));
  }
}

int Transfers::submit(std::unique_ptr<xfer_job>& job)
{
  ceph_assert(job);
  std::unique_lock l(lock);
  const auto now = ceph::mono_clock::now();
  const uint64_t bytes = job->pinned_bytes();
  if (nthreads == 0 || job->inline_only()) {
    // on this thread, before submit() returns; the caps bound what
    // waits for a thread, which this does not, and start_stopping() does
    // not refuse it: the OSD drains this thread, an op thread, before it
    // calls stop()
    if (stop_threads) {
      logger->inc(l_oob_xfer_refused);
      return -ESHUTDOWN;
    }
    auto r = running.insert(running.end(),
			    running_t{job.get(), now, now, bytes, true});
    inline_running++;
    logger->set(l_oob_xfer_running, running.size());
    l.unlock();
    auto j = std::move(job);
    dispatch(*j);
    l.lock();
    // dump() reads the jobs in running
    running.erase(r);
    logger->set(l_oob_xfer_running, running.size());
    l.unlock();
    // the job lets go of its payload, and of its op, without the lock,
    // and before stop() returns
    j.reset();
    l.lock();
    inline_running--;
    if (stop_threads && inline_running == 0) {
      done_cond.notify_all();
    }
    return 0;
  }
  if (stopping) {
    logger->inc(l_oob_xfer_refused);
    return -ESHUTDOWN;
  }
  // pinned never passes max_bytes
  if (queue.size() + threads_running >= max_jobs ||
      bytes > max_bytes - pinned) {
    dout(10) << "refusing a job of " << bytes << " bytes: "
	     << queue.size() << " queued, " << threads_running
	     << " running, " << pinned << " bytes pinned" << dendl;
    logger->inc(l_oob_xfer_refused);
    return -EBUSY;
  }
  pinned += bytes;
  queue.push_back({std::move(job), now, bytes});
  logger->set(l_oob_xfer_queued, queue.size());
  cond.notify_one();
  return 0;
}

void Transfers::dispatch(xfer_job& job)
{
  // the time counts against the lease, as a slow op thread's would
  debug_sleep(delays->post);
  const auto start = ceph::mono_clock::now();
  if (start > job.post_by()) {
    // an op answered inline costs its client the out-of-band transfer:
    // a job waits while it can still start, and only then answers
    // without its transfer
    dout(10) << "job " << &job << " expired "
	     << secs(start - job.post_by()) << "s past its post_by" << dendl;
    logger->inc(l_oob_xfer_expired);
    job.expire(-ETIMEDOUT);
    return;
  }
  job.delays = delays.get();
  job.run();
  logger->tinc(l_oob_xfer_push_lat,
	       unsigned_span(ceph::mono_clock::now() - start));
}

void Transfers::worker(unsigned i)
{
  auto hb = cct->get_heartbeat_map()->add_worker(
    fmt::format("oob_xfer thread {}", i), pthread_self());
  std::unique_lock l(lock);
  while (!stop_threads) {
    if (queue.empty()) {
      cct->get_heartbeat_map()->reset_timeout(hb, thread_grace,
					      ceph::timespan::zero());
      if (cond.wait_for(l, IDLE_INTERVAL) == std::cv_status::timeout &&
	  queue.empty() && !stop_threads) {
	// what timed-out transfers left on this thread's resources
	l.unlock();
	idle();
	l.lock();
      }
      continue;
    }
    auto q = std::move(queue.front());
    queue.pop_front();
    const auto now = ceph::mono_clock::now();
    auto r = running.insert(running.end(),
			    running_t{q.job.get(), q.submitted, now, q.bytes,
				      false});
    threads_running++;
    logger->set(l_oob_xfer_queued, queue.size());
    logger->set(l_oob_xfer_running, running.size());
    l.unlock();

    logger->tinc(l_oob_xfer_queue_lat, unsigned_span(now - q.submitted));
    // the grace covers the whole transfer: it ends by its land_by, plus
    // a late cut-off's tolerance, and a stalled peer can take all of it
    cct->get_heartbeat_map()->reset_timeout(hb, thread_grace,
					    ceph::timespan::zero());
    dispatch(*q.job);

    l.lock();
    // dump() reads the jobs in running
    running.erase(r);
    logger->set(l_oob_xfer_running, running.size());
    l.unlock();
    // the job lets go of its payload, and of its op, without the lock
    q.job.reset();
    l.lock();
    pinned -= q.bytes;
    threads_running--;
  }
  l.unlock();
  cct->get_heartbeat_map()->remove_worker(hb);
}

void Transfers::start_stopping()
{
  std::lock_guard l(lock);
  stopping = true;
}

void Transfers::stop()
{
  std::deque<queued_t> shed;
  {
    std::lock_guard l(lock);
    stopping = true;
    stop_threads = true;
    shed.swap(queue);
    for (const auto& q : shed) {
      pinned -= q.bytes;
    }
    logger->set(l_oob_xfer_queued, 0);
    if (!shed.empty() || !running.empty()) {
      dout(1) << "stopping: shedding " << shed.size() << " queued jobs, "
	      << "waiting for " << running.size() << " running" << dendl;
    }
    cond.notify_all();
  }
  // none of them started: each answers as a refused submit
  for (auto& q : shed) {
    logger->inc(l_oob_xfer_shed);
    q.job->expire(-ESHUTDOWN);
    q.job.reset();
  }
  for (auto& t : threads) {
    t.join();
  }
  threads.clear();
  // and the jobs that run on the threads that submitted them, until they
  // are destroyed
  std::unique_lock l(lock);
  done_cond.wait(l, [this] { return inline_running == 0; });
}

void Transfers::dump(ceph::Formatter* f) const
{
  std::lock_guard l(lock);
  const auto now = ceph::mono_clock::now();
  f->dump_unsigned("threads", nthreads);
  f->dump_bool("stopping", stopping);
  f->dump_unsigned("max_jobs", max_jobs);
  f->dump_unsigned("max_bytes", max_bytes);
  f->dump_unsigned("queued", queue.size());
  f->dump_unsigned("pinned_bytes", pinned);
  f->open_array_section("running");
  for (const auto& r : running) {
    f->open_object_section("job");
    f->dump_string("kind", "push");
    f->dump_bool("inline", r.here);
    f->dump_float("age", secs(now - r.submitted));
    f->dump_float("queued_for", secs(r.started - r.submitted));
    f->dump_float("post_by_in", secs(r.job->post_by() - now));
    f->dump_unsigned("pinned_bytes", r.bytes);
    r.job->dump(f);
    f->close_section();
  }
  f->close_section();
}

} // namespace ceph::osd::oob
