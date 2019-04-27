#include "rgw_dmclock_scheduler.h"
#include "rgw_dmclock_sync_scheduler.h"
#include "rgw_dmclock_scheduler_ctx.h"

namespace rgw::dmclock {

SyncScheduler::~SyncScheduler()
{
  cancel();
}

int SyncScheduler::add_request(const client_id& client, const ReqParams& params,
                               const Time& time, Cost cost)
{
  std::mutex req_mtx;
  std::condition_variable req_cv;
  ReqState rstate {ReqState::Wait};
  auto req = SyncRequest{client, time, cost, req_mtx, req_cv, rstate, counters};
  int r = queue.add_request_time(req, client, params, time, cost);
  if (r == 0) {
    if (auto c = counters(client)) {
      c->inc(queue_counters::l_qlen);
      c->inc(queue_counters::l_cost, cost);
    }
    queue.request_completed();
    // Perform a blocking wait until the request callback is called
    if (std::unique_lock<std::mutex> lk(req_mtx); rstate != ReqState::Wait) {
      req_cv.wait(lk, [&rstate] {return rstate != ReqState::Wait;});
    }
    if (rstate == ReqState::Cancelled) {
      //FIXME: decide on error code for cancelled request
      r = -ECONNABORTED;
    }
  } else {
      // post the error code
    if (auto c = counters(client)) {
      c->inc(queue_counters::l_limit);
      c->inc(queue_counters::l_limit_cost, cost);
    }
  }
  return r;
}

void SyncScheduler::handle_request_cb(const client_id &c,
                                      std::unique_ptr<SyncRequest> req,
                                      PhaseType phase, Cost cost)
{
  { std::lock_guard<std::mutex> lg(req->req_mtx);
    req->req_state = ReqState::Ready;
    req->req_cv.notify_one();
  }

  if (auto ctr = req->counters(c)) {
    auto lat = Clock::from_double(get_time()) - Clock::from_double(req->started);
    if (phase == PhaseType::reservation){
      ctr->tinc(queue_counters::l_res_latency, lat);
      ctr->inc(queue_counters::l_res);
      if (cost) ctr->inc(queue_counters::l_res_cost);
    } else if (phase == PhaseType::priority){
      ctr->tinc(queue_counters::l_prio_latency, lat);
      ctr->inc(queue_counters::l_prio);
      if (cost) ctr->inc(queue_counters::l_prio_cost);
    }
    ctr->dec(queue_counters::l_qlen);
    if (cost) ctr->dec(queue_counters::l_cost);
  }
}


void SyncScheduler::cancel(const client_id& client)
{
  ClientSum sum;

  queue.remove_by_client(client, false, [&](RequestRef&& request)
    {
      sum.count++;
      sum.cost += request->cost;
      {
        std::lock_guard <std::mutex> lg(request->req_mtx);
        request->req_state = ReqState::Cancelled;
        request->req_cv.notify_one();
      }
    });
  if (auto c = counters(client)) {
    on_cancel(c, sum);
  }

  queue.request_completed();
}

void SyncScheduler::cancel()
{
  ClientSums sums;

  queue.remove_by_req_filter([&](RequestRef&& request) -> bool
           {
             inc(sums, request->client, request->cost);
             {
               std::lock_guard<std::mutex> lg(request->req_mtx);
               request->req_state = ReqState::Cancelled;
               request->req_cv.notify_one();
             }
             return true;
           });

  for (size_t i = 0; i < client_count; i++) {
    if (auto c = counters(static_cast<client_id>(i))) {
      on_cancel(c, sums[i]);
    }
  }
}

} // namespace rgw::dmclock
