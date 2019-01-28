
#include "common/async/completion.h"
#include "rgw_dmclock_async_scheduler.h"
#include "rgw_dmclock_scheduler.h"

namespace rgw::dmclock {

AsyncScheduler::~AsyncScheduler()
{
  cancel();
  if (observer) {
    cct->_conf.remove_observer(this);
  }
}

const char** AsyncScheduler::get_tracked_conf_keys() const
{
  if (observer) {
    return observer->get_tracked_conf_keys();
  }
  static const char* keys[] = { "rgw_max_concurrent_requests", nullptr };
  return keys;
}

void AsyncScheduler::handle_conf_change(const ConfigProxy& conf,
                                        const std::set<std::string>& changed)
{
  if (observer) {
    observer->handle_conf_change(conf, changed);
  }
  if (changed.count("rgw_max_concurrent_requests")) {
    auto new_max = conf.get_val<int64_t>("rgw_max_concurrent_requests");
    max_requests = new_max > 0 ? new_max : std::numeric_limits<int64_t>::max();
  }
  queue.update_client_infos();
  schedule(crimson::dmclock::TimeZero);
}

int AsyncScheduler::schedule_request_impl(const client_id& client,
                                          const ReqParams& params,
                                          const Time& time, const Cost& cost,
                                          optional_yield yield_ctx)
{
    ceph_assert(yield_ctx);

    auto &yield = yield_ctx.get_yield_context();
    boost::system::error_code ec;
    async_request(client, params, time, cost, yield[ec]);

    if (ec){
      if (ec == boost::system::errc::resource_unavailable_try_again)
        return -EAGAIN;
      else
        return -ec.value();
    }

    return 0;
}

void AsyncScheduler::request_complete()
{
  --outstanding_requests;
  schedule(crimson::dmclock::TimeZero);
}

void AsyncScheduler::cancel()
{
  ClientSums sums;

  queue.remove_by_req_filter([&] (RequestRef&& request) {
      inc(sums, request->client, request->cost);
      auto c = static_cast<Completion*>(request.release());
      Completion::dispatch(std::unique_ptr<Completion>{c},
                           boost::asio::error::operation_aborted,
                           PhaseType::priority);
      return true;
    });
  timer.cancel();

  for (size_t i = 0; i < client_count; i++) {
    if (auto c = counters(static_cast<client_id>(i))) {
      on_cancel(c, sums[i]);
    }
  }
}

void AsyncScheduler::cancel(const client_id& client)
{
  ClientSum sum;

  queue.remove_by_client(client, false, [&] (RequestRef&& request) {
      sum.count++;
      sum.cost += request->cost;
      auto c = static_cast<Completion*>(request.release());
      Completion::dispatch(std::unique_ptr<Completion>{c},
                           boost::asio::error::operation_aborted,
                           PhaseType::priority);
    });
  if (auto c = counters(client)) {
    on_cancel(c, sum);
  }
  schedule(crimson::dmclock::TimeZero);
}

void AsyncScheduler::schedule(const Time& time)
{
  timer.expires_at(Clock::from_double(time));
  timer.async_wait([this] (boost::system::error_code ec) {
      // process requests unless the wait was canceled. note that a canceled
      // wait may execute after this AsyncScheduler destructs
      if (ec != boost::asio::error::operation_aborted) {
        process(get_time());
      }
    });
}

void AsyncScheduler::process(const Time& now)
{
  // must run in the executor. we should only invoke completion handlers if the
  // executor is running
  assert(get_executor().running_in_this_thread());

  ClientSums rsums, psums;

  while (outstanding_requests < max_requests) {
    auto pull = queue.pull_request(now);

    if (pull.is_none()) {
      // no pending requests, cancel the timer
      timer.cancel();
      break;
    }
    if (pull.is_future()) {
      // update the timer based on the future time
      schedule(pull.getTime());
      break;
    }
    ++outstanding_requests;

    // complete the request
    auto& r = pull.get_retn();
    auto client = r.client;
    auto phase = r.phase;
    auto started = r.request->started;
    auto cost = r.request->cost;
    auto c = static_cast<Completion*>(r.request.release());
    Completion::post(std::unique_ptr<Completion>{c},
                     boost::system::error_code{}, phase);

    if (auto c = counters(client)) {
      auto lat = Clock::from_double(now) - Clock::from_double(started);
      if (phase == PhaseType::reservation) {
        inc(rsums, client, cost);
        c->tinc(queue_counters::l_res_latency, lat);
      } else {
        inc(psums, client, cost);
        c->tinc(queue_counters::l_prio_latency, lat);
      }
    }
  }

  if (outstanding_requests >= max_requests) {
    if(auto c = counters(client_id::count)){
      c->inc(throttle_counters::l_throttle);
    }
  }

  for (size_t i = 0; i < client_count; i++) {
    if (auto c = counters(static_cast<client_id>(i))) {
      on_process(c, rsums[i], psums[i]);
    }
  }
}

} // namespace rgw::dmclock
