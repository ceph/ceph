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

#ifndef MDS_DMCLOCK_SCHEDULER_H_
#define MDS_DMCLOCK_SCHEDULER_H_

#include <string>
#include <chrono>
#include <functional>
#include <map>
#include <mutex>
#include <deque>

#include "include/types.h"
#include "mdstypes.h"

#include "MDSRank.h"
#include "messages/MClientReply.h"
#include "messages/MClientRequest.h"
#include "messages/MClientSession.h"
#include "messages/MMDSDmclockQoS.h"
#include "msg/Messenger.h"
#include "dmclock/src/dmclock_server.h"
#include "CInode.h"

class ClientRequest;

using MDSReqRef = cref_t<MClientRequest>;
using crimson::dmclock::ClientInfo;
using crimson::dmclock::AtLimit;
using crimson::dmclock::PhaseType;
using crimson::dmclock::ReqParams;
using Time = double;
using ClientId = std::string;
using VolumeId = ClientId;
using SessionId = std::string;
using std::placeholders::_1;
using std::placeholders::_2;
using std::placeholders::_3;
using std::placeholders::_4;

using Queue = crimson::dmclock::PushPriorityQueue<VolumeId, ClientRequest>;

enum class RequestType {
  CLIENT_REQUEST,
  UPDATE_REQUEST
};

using RequestCB = std::function<void()>;

class Request {
private:
  RequestType type;
  VolumeId volume_id;
public:
  Request(RequestType _type, VolumeId _volume_id) :
    type(_type), volume_id(_volume_id) {};

  Request(RequestType _type, VolumeId _volume_id, RequestCB _cb_func) :
    type(_type), volume_id(_volume_id), cb_func(_cb_func) {};

  RequestType get_request_type() const
  {
    return type;
  }

  std::string_view get_request_type_str() const
  {
    switch(type) {
      case RequestType::CLIENT_REQUEST:
        return "CLIENT_REQUEST";
      case RequestType::UPDATE_REQUEST:
        return "UPDATE_REQUEST";
      default:
        return "UNKOWN_REQUEST";
    }
  }

  const VolumeId& get_volume_id() const
  {
    return volume_id;
  }
  RequestCB cb_func;
};

class ClientRequest : public Request {
public:
  const MDSReqRef mds_req_ref;
  Time time;
  uint32_t cost;
  explicit ClientRequest(const MDSReqRef &_mds_req_ref, VolumeId _id,
      double _time, uint32_t _cost) :
      Request(RequestType::CLIENT_REQUEST, _id),
      mds_req_ref(_mds_req_ref), time(_time), cost(_cost) {};
};

class UpdateRequest : public Request {
public:
  UpdateRequest(VolumeId _id):
    Request(RequestType::UPDATE_REQUEST, _id) {};
  UpdateRequest(VolumeId _id, RequestCB _cb_func):
    Request(RequestType::UPDATE_REQUEST, _id, _cb_func) {};
};

class QoSInfo : public ClientInfo {
public:
  explicit QoSInfo(const double reservation, const double weight, const double limit) :
    ClientInfo(reservation, weight, limit) {};

  void set_reservation(const double _reservation)
  {
    reservation = _reservation;
    reservation_inv = 1.0 / _reservation;
  }

  void set_weight(const double _weight)
  {
    weight = _weight;
    weight_inv = 1.0 / _weight;
  }

  void set_limit(const double _limit)
  {
    limit = _limit;
    limit_inv = 1.0 / _limit;
  }

  double get_reservation() const
  {
    return reservation;
  }

  double get_weight() const
  {
    return weight;
  }

  double get_limit() const
  {
    return limit;
  }

  const ClientInfo* get_qos_info() const
  {
    return this;
  }
};

class VolumeInfo : public QoSInfo {
private:
  bool use_default;
  std::set<SessionId> session_list;
  int inflight_requests;

public:
  explicit VolumeInfo():
    QoSInfo(0.0, 0.0, 0.0), use_default(true), inflight_requests(0)  {};

  int32_t get_session_cnt() const
  {
    return session_list.size();
  }

  bool is_use_default() const
  {
    return use_default;
  }
  void set_use_default(bool _use_default)
  {
    use_default = _use_default;
  }

  void update(const ClientInfo& client_info, const bool use_default)
  {
    set_reservation(client_info.reservation);
    set_weight(client_info.weight);
    set_limit(client_info.limit);
    set_use_default(use_default);
  }

  void add_session(const SessionId &sid)
  {
    session_list.insert(sid);
  }

  void remove_session(const SessionId &sid)
  {
    auto it = session_list.find(sid);
    if (it != session_list.end()) {
      session_list.erase(it);
    }
  }

  int get_inflight_request() const
  {
    return inflight_requests;
  }

  void increase_inflight_request()
  {
    inflight_requests++;
  }

  void decrease_inflight_request()
  {
    inflight_requests--;
  }

  void dump(Formatter *f, const std::string &vid) const
  {
    f->dump_string("volume_id", vid);
    f->dump_bool("use_default", is_use_default());
    if (!is_use_default()) {
      f->dump_float("reservation", get_reservation());
      f->dump_float("weight", get_weight());
      f->dump_float("limit", get_limit());
    }
    f->dump_int("inflight_requests", get_inflight_request());
    f->dump_int("session_cnt", get_session_cnt());
    {
      f->open_array_section("session_list");
      for (auto &it : session_list) {
        f->dump_string("session_id", it);
      }
      f->close_section();
    }
  }
};

ostream& operator<<(ostream& os, const VolumeInfo* vi);

class mds_dmclock_conf : public QoSInfo {
private:
  bool enabled;

public:
  mds_dmclock_conf(): QoSInfo(0.0, 0.0, 0.0), enabled(false){};

  bool get_status() const
  {
    return enabled;
  }

  bool is_enabled() const
  {
    return enabled;
  }

  void set_status(const bool _enabled)
  {
    enabled = _enabled;
  }
};

enum class SchedulerState {
  INIT,
  RUNNING,
  FINISHING,
  SHUTDOWN,
};

class MDSRank;

class MDSDmclockScheduler {
private:
  SchedulerState state;
  mds_dmclock_conf default_conf;
  int total_inflight_requests;
  MDSRank *mds;
  Queue *dmclock_queue;
  std::map<VolumeId, VolumeInfo> volume_info_map;
  mutable std::mutex volume_info_lock;
  static constexpr uint32_t SUBVOL_ROOT_DEPTH = 3;

public:

  std::map<VolumeId, VolumeInfo> &get_volume_info_map()
  {
    return volume_info_map;
  }
  mds_dmclock_conf get_default_conf()
  {
    return default_conf;
  }

  /* volume QoS info management */
  void create_volume_info(const VolumeId &vid, const ClientInfo &client_info, const bool use_default);
  void add_session_to_volume_info(const VolumeId &vid, const SessionId &sid);
  void update_volume_info(const VolumeId &vid, const ClientInfo& client_info, const bool use_default);
  VolumeInfo *get_volume_info_ptr(const VolumeId &vid);
  bool copy_volume_info(const VolumeId &vid, VolumeInfo &vi);
  bool check_volume_info_existence(const VolumeId &vid);
  void delete_session_from_volume_info(const VolumeId &vid, const SessionId &sid);
  void set_default_volume_info(const VolumeId &vid);
  void dump(Formatter *f) const;

  void add_session(Session *session);
  void remove_session(Session *session);

  /* multi MDS broadcast message */
  void broadcast_qos_info_update_to_mds(const VolumeId& vid, const dmclock_info_t &dmclock_info);
  void handle_qos_info_update_message(const cref_t<MDSDmclockQoS> &m);
  void proc_message(const cref_t<Message> &m);
  CInode* traverse_path_inode(const cref_t<MDSDmclockQoS> &m);

  void handle_mds_request(const MDSReqRef &req);
  template<typename R>
  void enqueue_client_request(const R &mds_req, VolumeId volume_id);
  void submit_request_to_mds(const VolumeId &, std::unique_ptr<ClientRequest> &&, const PhaseType&, const uint64_t);
  const ClientInfo *get_client_info(const VolumeId &vid);

  void handle_conf_change(const std::set<std::string>& changed);

  void enable_qos_feature();
  void disable_qos_feature();

  CInode *read_xattrs(const VolumeId vid);

  /* request event handler */
  void begin_schedule_thread();
  void process_request();
  void process_request_handler();
  std::thread scheduler_thread;
  mutable std::mutex queue_mutex;
  std::condition_variable queue_cvar;

  std::deque<std::unique_ptr<Request>> request_queue;
  void enqueue_update_request(const VolumeId& vid);
  void enqueue_update_request(const VolumeId& vid, RequestCB cb_func);
  uint32_t get_request_queue_size() const;

  const VolumeId get_volume_id(Session *session);
  const SessionId get_session_id(Session *session);
  const VolumeId convert_subvol_root(const VolumeId& volume_id);

  using RejectThreshold = Time;
  using AtLimitParam = boost::variant<AtLimit, RejectThreshold>;

  Queue::ClientInfoFunc client_info_func;
  Queue::CanHandleRequestFunc can_handle_func;
  Queue::HandleRequestFunc handle_request_func;

  MDSDmclockScheduler(MDSRank *m, const Queue::ClientInfoFunc _client_info_func,
      const Queue::CanHandleRequestFunc _can_handle_func,
      const Queue::HandleRequestFunc _handle_request_func) : mds(m)
  {
    if (_client_info_func) {
      client_info_func = _client_info_func;
    } else {
      client_info_func = std::bind(&MDSDmclockScheduler::get_client_info, this, _1);
    }

    if (_can_handle_func) {
      can_handle_func = _can_handle_func;
    } else {
      can_handle_func = []()->bool{ return true;};
    }

    if (_handle_request_func) {
      handle_request_func = _handle_request_func;
    } else {
      handle_request_func = std::bind(&MDSDmclockScheduler::submit_request_to_mds, this, _1, _2, _3, _4);
    }

    dmclock_queue = new Queue(client_info_func,
        can_handle_func,
        handle_request_func);

    state = SchedulerState::RUNNING;
    total_inflight_requests = 0;

    begin_schedule_thread();

    default_conf.set_reservation(g_conf().get_val<double>("mds_dmclock_reservation"));
    default_conf.set_weight(g_conf().get_val<double>("mds_dmclock_weight"));
    default_conf.set_limit(g_conf().get_val<double>("mds_dmclock_limit"));
    default_conf.set_status(g_conf().get_val<bool>("mds_dmclock_enable"));
  }

  MDSDmclockScheduler(MDSRank *m) :
    MDSDmclockScheduler(m,
      Queue::ClientInfoFunc(),
      Queue::CanHandleRequestFunc(),
      Queue::HandleRequestFunc())
  {
    // empty
  }

  ~MDSDmclockScheduler();

  SessionMap *get_session_map();
  mds_rank_t get_nodeid();
  void mds_lock();
  void mds_unlock();
  int mds_is_locked_by_me();
  Queue *get_dmclock_queue()
  {
    return dmclock_queue;
  }

  void cancel_inflight_request();
  void increase_inflight_request(const VolumeId &vid);
  void decrease_inflight_request(const VolumeId &vid);
  int get_inflight_request(const VolumeId &vid);

  void shutdown();
  friend ostream& operator<<(ostream& os, const VolumeInfo* vi);

  std::string_view get_state_str() const;
};

#endif // MDS_DMCLOCK_SCHEDULER_H_
