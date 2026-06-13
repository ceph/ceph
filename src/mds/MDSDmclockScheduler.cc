// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 LINE
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "Server.h"
#include "SessionMap.h"
#include "MDSDmclockScheduler.h"
#include "mds/MDSMap.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << get_nodeid() << ".dmclock_scheduler "

using std::string;
using TOPNSPC::common::cmd_getval;

ostream& operator<<(ostream& os, VolumeInfo* vi)
{
  if (vi) {
    os << "VolumeInfo: (session_cnt " << vi->get_session_cnt() << ") "
        << " reservation " << vi->get_reservation()
        << " weight" << vi->get_weight()
        << " limit " << vi->get_limit();
  } else {
    os << "VolumeInfo has nullptr";
  }
  return os;
}

const VolumeId MDSDmclockScheduler::convert_subvol_root(const VolumeId &volume_id)
{
  filepath subvol_root_path(volume_id);

  if (subvol_root_path.depth() > SUBVOL_ROOT_DEPTH) {
    return "/" + subvol_root_path.prefixpath(SUBVOL_ROOT_DEPTH).get_path();
  }

  return volume_id;
}

const VolumeId MDSDmclockScheduler::get_volume_id(Session *session)
{
  ceph_assert(session != nullptr);
  auto client_root_entry = session->info.client_metadata.find("root");
  if (client_root_entry == session->info.client_metadata.end() || client_root_entry->second == "") {
    return "";
  }
  return convert_subvol_root(client_root_entry->second);
}

const VolumeId MDSDmclockScheduler::get_session_id(Session *session)
{
  ceph_assert(session != nullptr);
  return std::to_string(session->info.inst.name.num());
}

template<typename R>
void MDSDmclockScheduler::enqueue_client_request(const R &mds_req, VolumeId volume_id, Cost cost)
{
  dout(10) << __func__ << " volume_id " << volume_id << dendl;

  std::unique_lock<std::mutex> lock(queue_mutex);
  request_queue.emplace_back(new ClientRequest(mds_req, volume_id, crimson::dmclock::get_time(), cost));
  /* wake up*/
  lock.unlock();
  queue_cvar.notify_all();
}

void MDSDmclockScheduler::handle_client_request(const MDSReqRef &mds_req)
{
  dout(10) << __func__ << " " << *mds_req << dendl;

  if (mds->mds_dmclock_scheduler->default_conf.is_enabled() == true &&
      mds_req->get_orig_source().is_client() &&
      mds->is_active() &&
      check_volume_info_updated(get_volume_id(mds->get_session(mds_req))) &&
      check_volume_info_validity(get_volume_id(mds->get_session(mds_req)))) {
    Cost cost = get_op_cost(mds_req->get_op());
    enqueue_client_request<MDSReqRef>(mds_req, get_volume_id(mds->get_session(mds_req)), cost);
  } else {
    mds->server->handle_client_request(mds_req);
  }
}

void MDSDmclockScheduler::submit_request_to_mds(const VolumeId& vid, std::unique_ptr<ClientRequest>&& request,
                                                const PhaseType& phase_type, const uint64_t cost)
{
  dout(10) << __func__ << " volume_id " << vid << dendl;

  hit_volume_throttle(vid, request->time);

  const MDSReqRef& req = request->mds_req_ref;

  mds_lock();

  mds->server->handle_client_request(req);

  mds_unlock();

  decrease_inflight_request(request->get_volume_id());
}

void MDSDmclockScheduler::shutdown()
{
  dout(10) << __func__ << " state " << get_state_str() << dendl;

  if (default_conf.is_enabled() == true) {
    disable_qos_feature();
  }

  std::unique_lock<std::mutex> lock(queue_mutex);
  state = SchedulerState::FINISHING;
  lock.unlock();
  queue_cvar.notify_all();

  if (scheduler_thread.joinable()) {
    scheduler_thread.join();
  }

  state = SchedulerState::SHUTDOWN;

  dout(10) << __func__ << " state " << get_state_str() << dendl;
}

MDSDmclockScheduler::~MDSDmclockScheduler()
{
  dout(10) << __func__ << dendl;
  shutdown();
  delete dmclock_queue;
}

const ClientInfo *MDSDmclockScheduler::get_client_info(const VolumeId &vid)
{
  dout(10) << __func__ << " volume_id " << vid << dendl;
  std::lock_guard lock(volume_info_lock);

  auto vi = get_volume_info_ptr(vid);
  const ClientInfo *ci = nullptr;
  if (vi != nullptr) {
    if (vi->is_use_default() == true) {
      dout(15) << __func__ << " default QoS " << *default_conf.get_qos_info() << dendl;
      ci = default_conf.get_qos_info();
    } else {
      dout(15) << __func__ <<  " per client specific QoS " << vi->get_qos_info() << dendl;
      ci = vi->get_qos_info();
    }
  }
  return ci;
}

void MDSDmclockScheduler::dump(Formatter *f) const
{
  f->open_object_section("qos_info");

  f->open_object_section("qos_state");
  f->dump_bool("qos_enabled", default_conf.is_enabled());
  if (default_conf.is_enabled()) {
    f->dump_string("state", get_state_str());
    f->dump_float("default_reservation", default_conf.get_reservation());
    f->dump_float("default_weight", default_conf.get_weight());
    f->dump_float("default_limit", default_conf.get_limit());
    f->dump_int("mds_dmclock_queue_size", get_request_queue_size());
    f->dump_int("inflight_requests", total_inflight_requests);
  }
  f->close_section(); // qos_state

  f->open_array_section("volume_infos");
  std::lock_guard lock(volume_info_lock);
  for (auto it = volume_info_map.begin(); it != volume_info_map.end(); it++) {
    auto vol_info = it->second;
    f->open_object_section("volume_info");
    vol_info.dump(f, it->first);
    f->close_section();
  }
  f->close_section(); // volume_infos

  f->close_section(); // qos_info
}

VolumeInfo *MDSDmclockScheduler::get_volume_info_ptr(const VolumeId &vid)
{
  auto it = volume_info_map.find(vid);
  if (it != volume_info_map.end()) {
    return &it->second;
  }
  return nullptr;
}

bool MDSDmclockScheduler::copy_volume_info(const VolumeId &vid, VolumeInfo &vi)
{
  std::lock_guard lock(volume_info_lock);
  auto it = volume_info_map.find(vid);
  if (it != volume_info_map.end()) {
    vi = it->second;
    return true;
  }
  return false;
}

bool MDSDmclockScheduler::check_volume_info_existence(const VolumeId &vid)
{
  std::lock_guard lock(volume_info_lock);
  if (get_volume_info_ptr(vid) != nullptr) {
    return true;
  }
  return false;
}

bool MDSDmclockScheduler::check_volume_info_updated(const VolumeId &vid)
{
  std::lock_guard lock(volume_info_lock);
  VolumeInfo* vi = get_volume_info_ptr(vid);
  if (vi != nullptr && vi->get_updated()) {
    return true;
  }
  return false;
}

void MDSDmclockScheduler::hit_volume_throttle(const VolumeId &vid, Time arrival)
{
  std::lock_guard lock(volume_info_lock);
  VolumeInfo* vi = get_volume_info_ptr(vid);

  if (vi != nullptr && vi->get_updated()) {
    double expected_lat;
    double expected_iops;

    if (vi->is_use_default()) {
      expected_lat = (double)1.0 / default_conf.get_limit();
      expected_iops = default_conf.get_limit();
    } else {
      expected_lat = (double)1.0 / vi->get_limit();
      expected_iops = vi->get_limit();
    }

    double latency = crimson::dmclock::get_time() - arrival;

    dout(10) << __func__ << " Volume " << vid <<
      " current latency " << latency <<
      " expected latency " << expected_lat <<
      " inflight request " << vi->get_inflight_request() <<
      " expected iops " << expected_iops << dendl;

    if (latency > expected_lat && vi->get_inflight_request() > expected_iops) {
      vi->hit_throttle();
    }

    vi->update_latency(latency);
  }
}

bool MDSDmclockScheduler::check_volume_info_validity(const VolumeId &vid)
{
  std::lock_guard lock(volume_info_lock);
  VolumeInfo* vi = get_volume_info_ptr(vid);
  if (vi != nullptr) {
    if (vi->is_use_default() == true) {
      const ClientInfo *ci = default_conf.get_qos_info();
      if (ci->reservation > 0.0 &&
          ci->weight > 0.0 &&
          ci->limit > 0.0) {
        return true;
      }

    } else {
      if (vi->get_reservation() > 0.0 &&
          vi->get_weight() > 0.0 &&
          vi->get_limit() > 0.0)
      return true;
    }
  }
  return false;
}

void MDSDmclockScheduler::set_volume_info_updated(const VolumeId &vid)
{
  std::lock_guard lock(volume_info_lock);
  VolumeInfo* vi = get_volume_info_ptr(vid);
  if (vi != nullptr) {
    vi->set_updated(true);
  }
}

void MDSDmclockScheduler::increase_inflight_request(const VolumeId &vid)
{
  std::lock_guard lock(volume_info_lock);
  VolumeInfo* vi = get_volume_info_ptr(vid);
  ceph_assert(vi!=nullptr);
  vi->increase_inflight_request();
  total_inflight_requests++;
}

void MDSDmclockScheduler::decrease_inflight_request(const VolumeId &vid)
{
  std::lock_guard lock(volume_info_lock);
  VolumeInfo* vi = get_volume_info_ptr(vid);
  ceph_assert(vi!=nullptr);
  vi->decrease_inflight_request();
  total_inflight_requests--;
}

int MDSDmclockScheduler::get_inflight_request(const VolumeId &vid)
{
  std::lock_guard lock(volume_info_lock);
  VolumeInfo* vi = get_volume_info_ptr(vid);
  ceph_assert(vi!=nullptr);
  return vi->get_inflight_request();
}

void MDSDmclockScheduler::create_volume_info(const VolumeId &vid, const ClientInfo &client_info,
                                              const bool use_default)
{
  dout(10) << __func__ << " volume_id " << vid << " client_info "
          << client_info << " use_default" <<  use_default << dendl;

  std::lock_guard lock(volume_info_lock);
  VolumeInfo* vi = get_volume_info_ptr(vid);

  if (vi == nullptr) {
    auto [it, success]  = volume_info_map.insert(std::make_pair(std::move(vid), VolumeInfo()));
    ceph_assert(success==true);
    vi = &it->second;
  }

  enqueue_update_request(vid, client_info, use_default);
}

void MDSDmclockScheduler::add_session_to_volume_info(const VolumeId &vid, const SessionId &sid)
{
  dout(10) << __func__ << " volume_id " << vid << " session_id " << sid << dendl;

  std::lock_guard lock(volume_info_lock);
  VolumeInfo* vi = get_volume_info_ptr(vid);
  ceph_assert(vi!=nullptr);
  vi->add_session(sid);
}

void MDSDmclockScheduler::delete_session_from_volume_info(const VolumeId &vid, const SessionId &sid)
{
  dout(10) << __func__ << " volume_id " << vid << " session_id " << sid << dendl;

  std::lock_guard lock(volume_info_lock);

  auto it = volume_info_map.find(vid);
  if (it != volume_info_map.end()) {
    auto vi = &it->second;
    vi->remove_session(sid);
    if (vi->get_session_cnt() == 0) {
      dout(15) << __func__ << " erase volume info due to no sessions (volume_id  "
                <<  vid << " session_id " << sid << ")" << dendl;
      ceph_assert(vi->get_inflight_request()==0);
      volume_info_map.erase(it);
    }
    /* the dmclock library supports only removal of idle clients in the background */
  }
}

void MDSDmclockScheduler::update_volume_info(const VolumeId &vid, const ClientInfo& client_info, const bool use_default)
{
  dout(10) << __func__ << " volume_id " << vid << " " << client_info << " use_default " << use_default << dendl;

  std::lock_guard lock(volume_info_lock);
  
  VolumeInfo* vi = get_volume_info_ptr(vid);
  if (vi) {
    vi->update(client_info, use_default);
  } else {
    dout(5) << " VolumeInfo is unavaiable (vid = " << vid << ")" << dendl;
  }
}

void MDSDmclockScheduler::set_default_volume_info(const VolumeId &vid)
{
  ClientInfo client_info(0.0, 0.0, 0.0);
  dout(10) << __func__ << " vid " << vid << " " << client_info << dendl;
  enqueue_update_request(vid, client_info, true);
}

void MDSDmclockScheduler::add_or_remove_session_map(const bool is_add)
{
  auto sessionmap = get_session_map();
  if (sessionmap == nullptr) {
    dout(10) << __func__ << " sessionmap has nullptr" << dendl;
    return;
  }
  dout(0) << __func__ << dendl;

  std::list<int> session_state_list = {Session::STATE_OPEN, Session::STATE_STALE};

  for (int session_state : session_state_list) {
      dout(0) << " session state " << session_state << dendl;
    if (auto it = sessionmap->by_state.find(session_state); it != sessionmap->by_state.end()) {
      for (const auto &session : *(it->second)) {
        dout(0) << " session " << session << dendl;
        if (is_add) {
          add_session(session);
        } else {
          dout(0) << " remove session " << session << dendl;
          remove_session(session);
        }
      }
    }
  }
}

void MDSDmclockScheduler::add_session(Session *session)
{
  if (get_default_conf().is_enabled() == false) {
    return;
  }

  if (!get_mds_is_active()) {
    dout(5) << __func__ << " Session cannot be added as mds is not active" << dendl;
  }

  if (session == nullptr) {
    dout(5) << __func__ << " session is nullptr" << dendl;
    return;
  }

  VolumeId vid = get_volume_id(session);
  SessionId sid = get_session_id(session);

  dout(10) << __func__ << " volume_id " << vid << " session_id " << sid <<  dendl;

  if (check_volume_info_existence(vid) == false) {
    ClientInfo client_info(0.0, 0.0, 0.0);
    bool use_default = true;

    create_volume_info(vid, client_info, use_default);
  } 

  add_session_to_volume_info(vid, sid);
}

void MDSDmclockScheduler::remove_session(Session *session)
{
  if (get_default_conf().is_enabled() == false) {
    return;
  }
  VolumeId vid = get_volume_id(session);
  SessionId sid = get_session_id(session);
  dout(10) << __func__ << " volume_id " << vid << " session_id " << sid << dendl;
  delete_session_from_volume_info(vid, sid);
}

uint32_t MDSDmclockScheduler::get_request_queue_size() const
{
  std::unique_lock<std::mutex> lock(queue_mutex);
  return request_queue.size();
}

void MDSDmclockScheduler::enqueue_update_request(const VolumeId& vid, const ClientInfo& client_info, const bool use_default)
{
    dout(10) << __func__ <<  " volume_id " << vid << dendl;

    std::unique_lock<std::mutex> lock(queue_mutex);

    request_queue.emplace_back(new UpdateRequest(vid, client_info, use_default));

    /* wake up*/
    lock.unlock();
    queue_cvar.notify_all();
}

void MDSDmclockScheduler::enqueue_update_request(const VolumeId& vid, const ClientInfo& client_info, const bool use_default, RequestCB cb_func)
{
    dout(10) << __func__ <<  " volume_id " << vid << dendl;

    std::unique_lock<std::mutex> lock(queue_mutex);

    request_queue.emplace_back(new UpdateRequest(vid, client_info, use_default, cb_func));
    /* wake up*/
    lock.unlock();
    queue_cvar.notify_all();
}

void MDSDmclockScheduler::process_request_handler()
{
  std::unique_lock<std::mutex> lock(queue_mutex);

  if (request_queue.size() == 0) {
    queue_cvar.wait(lock);
  }

  while (request_queue.size()) {
    std::unique_ptr<Request> request = std::move(request_queue.front());
    request_queue.erase(request_queue.begin());

    dout(10) << __func__ << " Process request type " << request->get_request_type_str() << dendl;

    lock.unlock();

    switch(request->get_request_type()) {
      case RequestType::CLIENT_REQUEST:
      {
        std::unique_ptr<ClientRequest> c_request(static_cast<ClientRequest *>(request.release()));

        dout(10) << " Process client request (queue size " << request_queue.size()
                << " volume_id " << c_request->get_volume_id()
                << " time " << c_request->time
                << " cost " << c_request->cost << ")" << dendl;

        increase_inflight_request(c_request->get_volume_id());

        dmclock_queue->add_request(std::move(c_request), std::move(c_request->get_volume_id()),
            {0, 0}, c_request->time, c_request->cost);

        break;
      }
      case RequestType::UPDATE_REQUEST:
      {
        std::unique_ptr<UpdateRequest> c_request(static_cast<UpdateRequest *>(request.release()));

        dout(10) << " Process update request (queue size " << request_queue.size()
                  << " volume_id " << c_request->get_volume_id() << ")" << dendl;

        update_volume_info(c_request->get_volume_id(), c_request->client_info, c_request->use_default);
        dmclock_queue->update_client_info(c_request->get_volume_id());
        set_volume_info_updated(c_request->get_volume_id());

        if (c_request->cb_func) {
          c_request->cb_func();
        }
        break;
      }
      default:
        derr << __func__ << " received unknown message " << request->get_request_type_str() << dendl;
    }

    lock.lock();
  }
}

void MDSDmclockScheduler::process_request()
{
  dout(10) << __func__ << " thread has been invoked" << dendl;
  /*
   * process_request_handler() is responsible for handling request_queue.
   * The function uses mutex-based locking, and
   * if request_queue is empty, it waits until the request is queued.
   * Therefore, it prevents excessive use of CPU by not busy waiting.
   */
  while (state == SchedulerState::RUNNING) {
    process_request_handler();
  }
  dout(10) << __func__ << " thread has been joined" << dendl;
}

void MDSDmclockScheduler::begin_schedule_thread()
{
  scheduler_thread = std::thread([this](){process_request();});
}

void MDSDmclockScheduler::enable_qos_feature()
{
  dout(10) << __func__ << dendl;

  default_conf.set_status(true);

  if (!get_mds_is_active()) {
    dout(0) << " MDS Rank is not active and QoS feature cannot be enabled." << dendl;
    return;
  }

  add_or_remove_session_map(true);
}

void MDSDmclockScheduler::cancel_inflight_request()
{
  dout(10) << __func__ << dendl;
  std::lock_guard lock(volume_info_lock);
  std::list<Queue::RequestRef> req_list;

  auto accum_f = [&req_list] (Queue::RequestRef&& r)
                  {
                    req_list.push_front(std::move(r));
                  };

  for (auto it : volume_info_map) {
    if (it.second.get_inflight_request()) {
      dmclock_queue->remove_by_client(it.first, true, accum_f);
    }
  }

  dout(10) << __func__ << " canceled requests " << req_list.size() << dendl;

  for (auto& it : req_list) {
    dout(10) << " canceled request volume_id " << it->get_volume_id() << " " << *it->mds_req_ref << dendl;
    handle_request_func(it->get_volume_id(), std::move(it), PhaseType::reservation, 1);
  }
  ceph_assert(dmclock_queue->empty() == true);
}

void MDSDmclockScheduler::disable_qos_feature()
{
  uint32_t queue_size;
  bool dmclock_empty;

  dout(10) << __func__ << dendl;

  default_conf.set_status(false);

  do
  {
    mds_unlock();
    queue_cvar.notify_all();
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    queue_size = get_request_queue_size();
    mds_lock();
  } while(queue_size);

  do
  {
    mds_unlock();
    cancel_inflight_request();
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    dmclock_empty = dmclock_queue->empty();
    mds_lock();
  } while(!dmclock_empty);

  add_or_remove_session_map(false);
}

void MDSDmclockScheduler::try_enable_qos_feature()
{
  if (g_conf().get_val<bool>("mds_dmclock_enable")) {
    enable_qos_feature();
  }
}

void MDSDmclockScheduler::try_disable_qos_feature()
{
  if (g_conf().get_val<bool>("mds_dmclock_enable")) {
    disable_qos_feature();
  }
}

void MDSDmclockScheduler::handle_conf_change(const std::set<std::string>& changed)
{
  dout(10) << __func__ << dendl;

  if (changed.count("mds_dmclock_enable")) {
    bool new_val = g_conf().get_val<bool>("mds_dmclock_enable");
    if (default_conf.is_enabled() != new_val)
    {
      if (new_val == true) {
        enable_qos_feature();
      } else {
        disable_qos_feature();
      }
    }
  }

  if (changed.count("mds_dmclock_reservation") || default_conf.is_enabled() == true) {
    dout(10) << " set reservation " << g_conf().get_val<double>("mds_dmclock_reservation") << dendl;
    default_conf.set_reservation(g_conf().get_val<double>("mds_dmclock_reservation"));
    ceph_assert(default_conf.get_reservation() == g_conf().get_val<double>("mds_dmclock_reservation"));
  }
  if (changed.count("mds_dmclock_weight") || default_conf.is_enabled() == true) {
    dout(10) << " set weight " << g_conf().get_val<double>("mds_dmclock_weight") << dendl;
    default_conf.set_weight(g_conf().get_val<double>("mds_dmclock_weight"));
    ceph_assert(default_conf.get_weight() == g_conf().get_val<double>("mds_dmclock_weight"));
  }
  if (changed.count("mds_dmclock_limit") || default_conf.is_enabled() == true) {
    dout(10) << " set limit " << g_conf().get_val<double>("mds_dmclock_limit") << dendl;
    default_conf.set_limit(g_conf().get_val<double>("mds_dmclock_limit"));
    ceph_assert(default_conf.get_limit() == g_conf().get_val<double>("mds_dmclock_limit"));
  }

  /* need to check whether conf is updated from ceph.conf when the MDS is restarted */
  dout(10) << __func__ <<  " enable " << default_conf.is_enabled()
            << " reservation " << default_conf.get_reservation()
            << " weight " << default_conf.get_weight()
            << " limit " << default_conf.get_limit() << dendl;
}

mds_rank_t MDSDmclockScheduler::get_nodeid()
{
  if (mds != nullptr) {
    return mds->get_nodeid();
  }
  return 0;
}

void MDSDmclockScheduler::set_mds_is_active(bool value)
{
  mds_is_active = value;
}

bool MDSDmclockScheduler::get_mds_is_active()
{
  return mds_is_active;
}
SessionMap *MDSDmclockScheduler::get_session_map()
{
  if (mds != nullptr) {
    return &mds->sessionmap;
  }
  return 0;
}

void MDSDmclockScheduler::mds_lock()
{
  if (mds != nullptr) {
    mds->mds_lock.lock();
  }
}

void MDSDmclockScheduler::mds_unlock()
{
  if (mds != nullptr) {
    mds->mds_lock.unlock();
  }
}

std::string_view MDSDmclockScheduler::get_state_str() const
{
  switch(state) {
    case SchedulerState::INIT:
      return "INIT";
    case SchedulerState::RUNNING:
      return "RUNNING";
    case SchedulerState::FINISHING:
      return "FINISHING";
    case SchedulerState::SHUTDOWN:
      return "SHUTDOWN";
    default:
      return "UNKONWN";
  }
}

Cost MDSDmclockScheduler::get_op_cost(const int op)
{
  switch (op) {
    case CEPH_MDS_OP_LOOKUP:
    case CEPH_MDS_OP_LOOKUPHASH:
    case CEPH_MDS_OP_LOOKUPPARENT:
    case CEPH_MDS_OP_LOOKUPINO:
    case CEPH_MDS_OP_LOOKUPNAME:
    case CEPH_MDS_OP_GETATTR:
    case CEPH_MDS_OP_READDIR:
    case CEPH_MDS_OP_OPEN:
    case CEPH_MDS_OP_LOOKUPSNAP:
    case CEPH_MDS_OP_LSSNAP:
    case CEPH_MDS_OP_GETFILELOCK:
      return 1;
    case CEPH_MDS_OP_SETXATTR:
    case CEPH_MDS_OP_SETATTR:
    case CEPH_MDS_OP_RMXATTR:
    case CEPH_MDS_OP_SETLAYOUT:
    case CEPH_MDS_OP_SETDIRLAYOUT:
    case CEPH_MDS_OP_MKNOD:
    case CEPH_MDS_OP_LINK:
    case CEPH_MDS_OP_UNLINK:
    case CEPH_MDS_OP_RENAME:
    case CEPH_MDS_OP_MKDIR:
    case CEPH_MDS_OP_RMDIR:
    case CEPH_MDS_OP_SYMLINK:
    case CEPH_MDS_OP_CREATE:
    case CEPH_MDS_OP_MKSNAP:
    case CEPH_MDS_OP_RMSNAP:
    case CEPH_MDS_OP_RENAMESNAP:
    case CEPH_MDS_OP_SETFILELOCK:
    default:
      return 3;
  }
}

bool MDSDmclockScheduler::_process_asok_qos_set(const cmdmap_t &cmdmap, std::ostream &ss)
{
  bool rc;
  string path;
  rc = cmd_getval(cmdmap, "path", path);
  if (!rc) {
    ss << "malformed path";
    return false;
  }

  int64_t reservation;
  rc = cmd_getval(cmdmap, "reservation", reservation);
  if (!rc || reservation < 1) {
    ss << "malformed reservation";
    return false;
  }

  int64_t weight;
  rc = cmd_getval(cmdmap, "weight", weight);
  if (!rc || weight < 1) {
    ss << "malformed weight";
    return false;
  }

  int64_t limit;
  rc = cmd_getval(cmdmap, "limit", limit);
  if (!rc || limit < 1) {
    ss << "malformed limit";
    return false;
  }

  if (reservation > limit) {
    ss << "reservation cannot be greater than limit";
    return false;
  }

  if (check_volume_info_existence(path) == false) {
    ss << "volume_info doesn't exist (" << path << ")" ;
    return false;
  }

  dout(5) << "dmclock path " << path << " reservation=" << reservation
    << " weight=" << weight
    << " limit=" << limit <<dendl;

  ClientInfo info(reservation, weight, limit);

  enqueue_update_request(path, info, false);

  return true;
}

void MDSDmclockScheduler::process_asok_qos_set(const cmdmap_t &cmdmap, std::ostream &ss, Formatter *f)
{
  bool success = _process_asok_qos_set(cmdmap, ss);
  f->open_object_section("results");
  f->dump_int("return_code", int(success));
  f->close_section(); // results
}

bool MDSDmclockScheduler::_process_asok_qos_rm(const cmdmap_t &cmdmap, std::ostream &ss)
{
  string path;
  if(!cmd_getval(cmdmap, "path", path)) {
    ss << "malformed path";
    return false;
  }

  if (check_volume_info_existence(path) == false) {
    ss << "volume_info doesn't exist (" << path << ")" ;
    return false;
  }

  dout(5) << "dmclock path " << path << dendl;

  set_default_volume_info(path);

  return true;
}


void MDSDmclockScheduler::process_asok_qos_rm(const cmdmap_t &cmdmap, std::ostream &ss, Formatter *f)
{
  bool success = _process_asok_qos_rm(cmdmap, ss);
  f->open_object_section("results");
  f->dump_int("return_code", int(success));
  f->close_section(); // results
}

void MDSDmclockScheduler::process_asok_qos_get(const cmdmap_t &cmdmap, std::ostream &ss, Formatter *f)
{
  string path;
  if(!cmd_getval(cmdmap, "path", path)) {
    ss << "malformed path";
    return;
  }

  VolumeInfo vi;

  if (!copy_volume_info(path, vi)) {
    ss << "volume_info doesn't exist (" << path << ")" ;
    return;
  }

  f->open_object_section("volume_qos_info");
  vi.dump(f, path);
  f->close_section();
}
