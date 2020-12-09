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

#include "SessionMap.h"
#include "MDSDmclockScheduler.h"
#include "mds/MDSMap.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << get_nodeid() << ".dmclock_scheduler "

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
  ceph_assert(client_root_entry != session->info.client_metadata.end());
  return convert_subvol_root(client_root_entry->second);
}

const VolumeId MDSDmclockScheduler::get_session_id(Session *session)
{
  ceph_assert(session != nullptr);
  return to_string(session->info.inst.name.num());
}

template<typename R>
void MDSDmclockScheduler::enqueue_client_request(const R &mds_req, VolumeId volume_id)
{
  dout(10) << __func__ << " volume_id " << volume_id << dendl;

  std::unique_lock<std::mutex> lock(queue_mutex);
  request_queue.emplace_back(new ClientRequest(mds_req, volume_id, crimson::dmclock::get_time(), 1));
  /* wake up*/
  lock.unlock();
  queue_cvar.notify_all();
}

void MDSDmclockScheduler::handle_mds_request(const MDSReqRef &mds_req)
{
  ceph_assert(mds_is_locked_by_me());
  dout(10) << __func__ << " " << *mds_req << dendl;

  if (mds->mds_dmclock_scheduler->default_conf.is_enabled() == true
      && mds_req->get_orig_source().is_client()) {
    auto volume_id = get_volume_id(mds->get_session(mds_req));
    enqueue_client_request<MDSReqRef>(mds_req, volume_id);
  } else {
    mds->server->handle_client_request(mds_req);
  }
}

void MDSDmclockScheduler::submit_request_to_mds(const VolumeId& vid, std::unique_ptr<ClientRequest>&& request,
                                                const PhaseType& phase_type, const uint64_t cost)
{
  dout(10) << __func__ << " volume_id " << vid << dendl;

  const MDSReqRef& req = request->mds_req_ref;

  ceph_assert(!mds_is_locked_by_me());

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
  f->open_array_section("qos_info");

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
    auto [it, success]  = volume_info_map.insert(std::make_pair(std::move(vid), std::move(VolumeInfo())));
    ceph_assert(success==true);
    vi = &it->second;
  }
  vi->update(client_info, use_default);

  enqueue_update_request(vid);
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
    /* the dmclock library supports only removal of idle clients in the backround */
  }
}

void MDSDmclockScheduler::update_volume_info(const VolumeId &vid, const ClientInfo& client_info, const bool use_default)
{
  dout(10) << __func__ << " volume_id " << vid << " " << client_info << " use_default " << use_default << dendl;

  std::lock_guard lock(volume_info_lock);
  
  VolumeInfo* vi = get_volume_info_ptr(vid);
  if (vi) {
    vi->update(client_info, use_default);
    enqueue_update_request(vid);
  } else {
    dout(5) << " VolumeInfo is unavaiable (vid = " << vid << ")" << dendl;
  }
}

void MDSDmclockScheduler::set_default_volume_info(const VolumeId &vid)
{
  ClientInfo client_info(0.0, 0.0, 0.0);
  dout(10) << __func__ << " vid " << vid << " " << client_info << dendl;
  update_volume_info(vid, client_info, true);
}

void MDSDmclockScheduler::add_session(Session *session)
{
  if (get_default_conf().is_enabled() == false) {
    return;
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

    auto qos_msg = make_message<MDSDmclockQoS>(mds->get_nodeid(), convert_subvol_root(vid),
        dmclock_info_t(), MDSDmclockQoS::PATH_TRAVERSE);
    mds->send_message_mds(qos_msg, mds->get_nodeid());
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

void MDSDmclockScheduler::broadcast_qos_info_update_to_mds(const VolumeId& vid, const dmclock_info_t &dmclock_info)
{
  dout(10) << __func__ << " volume_id " << vid << " from " << mds->get_nodeid() << dendl;

  std::set<mds_rank_t> actives;
  mds->get_mds_map()->get_active_mds_set(actives);

  for (auto it : actives) {
    dout(10) << " send MDSDmclockQoS message (" << vid  << ") to MDS" << it << dendl;
    auto qos_msg = make_message<MDSDmclockQoS>(mds->get_nodeid(), convert_subvol_root(vid),
                                              dmclock_info, MDSDmclockQoS::BROADCAST_TO_ALL);
    mds->send_message_mds(qos_msg, it);
  }
}

CInode *MDSDmclockScheduler::traverse_path_inode(const cref_t<MDSDmclockQoS> &m)
{
  CF_MDS_RetryMessageFactory cf(mds, m);
  MDRequestRef null_ref;
  int flags = MDS_TRAVERSE_DISCOVER;
  const filepath refpath(m->get_volume_id());
  vector<CDentry*> trace;
  CInode *in;

  int r = mds->mdcache->path_traverse(null_ref, cf, refpath, flags, &trace, &in);
  if (r > 0) {
    dout(5) << __func__ << " path_traverse() returns " << r << dendl;
    return nullptr;
  }
  if (r < 0) {
    dout(7) << "failed to discover or not dir " << m->get_volume_id() << ", NAK" << dendl;
    ceph_abort();
  }
  return in;
}

void MDSDmclockScheduler::handle_qos_info_update_message(const cref_t<MDSDmclockQoS> &m)
{
  dmclock_info_t dmclock_info;
  CInode *in;

  dout(10) << __func__ << " message " << m->get_sub_op_str() << " from " << m->get_mds_from()
    << " volume_id " << m->get_volume_id() << dendl;

  assert(mds_is_locked_by_me());

  switch(m->get_sub_op()) {
    case MDSDmclockQoS::REQUEST_TO_AUTH:
    {
      ceph_assert(mds->get_nodeid() != m->get_mds_from());

      in = traverse_path_inode(m);
      if (in == nullptr) {
        return;
      }
      ceph_assert(in->is_auth());
      dmclock_info = in->get_projected_inode()->dmclock_info;
      auto qos_msg = make_message<MDSDmclockQoS>(mds->get_nodeid(), m->get_volume_id(),
                                                dmclock_info, MDSDmclockQoS::BROADCAST_TO_ALL);
      mds->send_message_mds(qos_msg, m->get_mds_from());

      dout(15) << " inode is_auth " << in->is_auth() << " authority " << in->authority().first << dendl;
      dout(15) << " auth dmclock_info reservation " << dmclock_info.mds_reservation
          << " weight " << dmclock_info.mds_weight
          << " limit " << dmclock_info.mds_limit << dendl;
      return;
    }
    case MDSDmclockQoS::BROADCAST_TO_ALL:
      dmclock_info = m->get_dmclock_info();
      break;
    case MDSDmclockQoS::PATH_TRAVERSE:
    {
      ceph_assert(mds->get_nodeid() == m->get_mds_from());

      in = traverse_path_inode(m);
      if (in == nullptr) {
        return;
      }

      dmclock_info = in->get_projected_inode()->dmclock_info;

      dout(15) << " inode is_auth " << in->is_auth() << " authority " << in->authority().first << dendl;
      if (!in->is_auth()) {
        dout(10) << __func__ << " request dmclock_info to MDS" << in->authority().first << dendl;
        auto qos_msg = make_message<MDSDmclockQoS>(mds->get_nodeid(), m->get_volume_id(),
                                                    dmclock_info_t(), MDSDmclockQoS::REQUEST_TO_AUTH);
        mds->send_message_mds(qos_msg, in->authority().first);
        return;
      }
      dout(20) << " found inode: " << *in << dendl;
      break;
    }
    default:
      dout(0) << __func__ << " unkown message type " << m->get_sub_op() << dendl;
      ceph_abort();
  }

  if (check_volume_info_existence(m->get_volume_id()) == false) {
    dout(5) << " MDS doesn't maintain client info for volume_id " << m->get_volume_id() << dendl;
    return;
  }

  dout(15) << " dmclock_info reservation " << dmclock_info.mds_reservation
          << " weight " << dmclock_info.mds_weight
          << " limit " << dmclock_info.mds_limit << dendl;

  ClientInfo info(0.0, 0.0, 0.0);
  bool use_default = true;

  if (dmclock_info.is_valid()) {
    info.update(dmclock_info.mds_reservation,
                dmclock_info.mds_weight,
                dmclock_info.mds_limit);
    use_default = false;
  }

  update_volume_info(m->get_volume_id(), info, use_default);
}

void MDSDmclockScheduler::proc_message(const cref_t<Message> &m)
{
  switch (m->get_type()) {
    case MSG_MDS_DMCLOCK_QOS:
      handle_qos_info_update_message(ref_cast<MDSDmclockQoS>(m));
      break;
  default:
    derr << " dmClock QoS unknown message " << m->get_type() << dendl_impl;
    ceph_abort_msg("dmClock QoS unknown message");
  }
}

uint32_t MDSDmclockScheduler::get_request_queue_size() const
{
  std::unique_lock<std::mutex> lock(queue_mutex);
  return request_queue.size();
}

void MDSDmclockScheduler::enqueue_update_request(const VolumeId& vid)
{
    dout(10) << __func__ <<  " volume_id " << vid << dendl;

    std::unique_lock<std::mutex> lock(queue_mutex);

    request_queue.emplace_back(new UpdateRequest(vid));

    /* wake up*/
    lock.unlock();
    queue_cvar.notify_all();
}

void MDSDmclockScheduler::enqueue_update_request(const VolumeId& vid, RequestCB cb_func)
{
    dout(10) << __func__ <<  " volume_id " << vid << dendl;

    std::unique_lock<std::mutex> lock(queue_mutex);

    request_queue.emplace_back(new UpdateRequest(vid, cb_func));
    /* wake up*/
    lock.unlock();
    queue_cvar.notify_all();
}

void MDSDmclockScheduler::process_request_handler()
{
  std::unique_lock<std::mutex> lock(queue_mutex);
  queue_cvar.wait(lock);

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

        dmclock_queue->update_client_info(c_request->get_volume_id());

        if (c_request->cb_func) {
          c_request->cb_func();
        }
        break;
      }
    }

    lock.lock();
  }
}

void MDSDmclockScheduler::process_request()
{
  dout(10) << __func__ << " thread has been invoked" << dendl;
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

  auto sessionmap = get_session_map();

  if (sessionmap == nullptr) {
    dout(10) << __func__ << " sessionmap has nullptr" << dendl;
    return;
  }

  if (auto it = sessionmap->by_state.find(Session::STATE_OPEN); it != sessionmap->by_state.end()) {
    for (const auto &session : *(it->second)) {
      add_session(session);
    }
  }
  if (auto it = sessionmap->by_state.find(Session::STATE_STALE); it != sessionmap->by_state.end()) {
    for (const auto &session : *(it->second)) {
      add_session(session);
    }
  }
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
    dout(15) << " canceled request volume_id " << it->get_volume_id() << dendl;
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

  auto sessionmap = get_session_map();

  if (sessionmap == nullptr) {
    dout(10) << __func__ << " sessionmap has nullptr" <<  dendl;
    return;
  }

  if (auto it = sessionmap->by_state.find(Session::STATE_OPEN); it != sessionmap->by_state.end()) {
    for (const auto &session : *(it->second)) {
      remove_session(session);
    }
  }

  if (auto it = sessionmap->by_state.find(Session::STATE_STALE); it != sessionmap->by_state.end()) {
    for (const auto &session : *(it->second)) {
      remove_session(session);
    }
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

int MDSDmclockScheduler::mds_is_locked_by_me()
{
  if (mds != nullptr) {
    return mds->mds_lock.is_locked_by_me();
  }
  return 1;
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
