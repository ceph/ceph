// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 New Dream Network/Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef OPREQUEST_H_
#define OPREQUEST_H_

#include "osd/osd_types.h"
#include "common/TrackedOp.h"

/**
 * The OpRequest takes in a Message* and takes over a single reference
 * to it, which it puts() when destroyed.
 */
struct OpRequest : public TrackedOp {
  friend class OpTracker;

  // rmw flags
  int rmw_flags;

  bool check_rmw(int flag) const ;
  bool may_read() const;
  bool may_write() const;
  bool may_cache() const;
  bool rwordered_forced() const;
  bool rwordered() const;
  bool includes_pg_op();
  bool need_read_cap() const;
  bool need_write_cap() const;
  bool need_promote();
  bool need_skip_handle_cache();
  bool need_skip_promote();
  void set_read();
  void set_write();
  void set_cache();
  void set_class_read();
  void set_class_write();
  void set_pg_op();
  void set_promote();
  void set_skip_handle_cache();
  void set_skip_promote();
  void set_force_rwordered();

  struct ClassInfo {
    ClassInfo(std::string&& class_name, std::string&& method_name,
              bool read, bool write, bool whitelisted) :
      class_name(std::move(class_name)), method_name(std::move(method_name)),
      read(read), write(write), whitelisted(whitelisted)
    {}
    const std::string class_name;
    const std::string method_name;
    const bool read, write, whitelisted;
  };

  void add_class(std::string&& class_name, std::string&& method_name,
                 bool read, bool write, bool whitelisted) {
    classes_.emplace_back(std::move(class_name), std::move(method_name),
                          read, write, whitelisted);
  }

  std::vector<ClassInfo> classes() const {
    return classes_;
  }

  void _dump(ceph::Formatter *f) const override;

  bool has_feature(uint64_t f) const {
    return request->get_connection()->has_feature(f);
  }

private:
  Message *request; /// the logical request we are tracking
  osd_reqid_t reqid;
  entity_inst_t req_src_inst;
  uint8_t hit_flag_points;
  uint8_t latest_flag_point;
  utime_t dequeued_time;
  static const uint8_t flag_queued_for_pg=1 << 0;
  static const uint8_t flag_reached_pg =  1 << 1;
  static const uint8_t flag_delayed =     1 << 2;
  static const uint8_t flag_started =     1 << 3;
  static const uint8_t flag_sub_op_sent = 1 << 4;
  static const uint8_t flag_commit_sent = 1 << 5;

  std::vector<ClassInfo> classes_;

  OpRequest(Message *req, OpTracker *tracker);

protected:
  void _dump_op_descriptor_unlocked(std::ostream& stream) const override;
  void _unregistered() override;
  bool filter_out(const std::set<std::string>& filters) override;

public:
  ~OpRequest() override {
    request->put();
  }

  bool check_send_map = true; ///< true until we check if sender needs a map
  epoch_t sent_epoch = 0;     ///< client's map epoch
  epoch_t min_epoch = 0;      ///< min epoch needed to handle this msg

  bool hitset_inserted;
  const Message *get_req() const { return request; }
  Message *get_nonconst_req() { return request; }

  entity_name_t get_source() {
    if (request) {
      return request->get_source();
    } else {
      return entity_name_t();
    }
  }

  std::string_view state_string() const override {
    switch(latest_flag_point) {
    case flag_queued_for_pg: return "queued for pg";
    case flag_reached_pg: return "reached pg";
    case flag_delayed: return "delayed";
    case flag_started: return "started";
    case flag_sub_op_sent: return "waiting for sub ops";
    case flag_commit_sent: return "commit sent; apply or cleanup";
    default: break;
    }
    return "no flag points reached";
  }

  void mark_queued_for_pg() {
    mark_flag_point(flag_queued_for_pg, "queued_for_pg");
  }
  void mark_reached_pg() {
    mark_flag_point(flag_reached_pg, "reached_pg");
  }
  void mark_delayed(const std::string& s) {
    mark_flag_point_string(flag_delayed, s);
  }
  void mark_started() {
    mark_flag_point(flag_started, "started");
  }
  void mark_sub_op_sent(const std::string& s) {
    mark_flag_point_string(flag_sub_op_sent, s);
  }
  void mark_commit_sent() {
    mark_flag_point(flag_commit_sent, "commit_sent");
  }

  utime_t get_dequeued_time() const {
    return dequeued_time;
  }
  void set_dequeued_time(utime_t deq_time) {
    dequeued_time = deq_time;
  }

  osd_reqid_t get_reqid() const {
    return reqid;
  }

  typedef boost::intrusive_ptr<OpRequest> Ref;

private:
  void set_rmw_flags(int flags);
  void mark_flag_point(uint8_t flag, const char *s);
  void mark_flag_point_string(uint8_t flag, const std::string& s);
};

typedef OpRequest::Ref OpRequestRef;

std::ostream& operator<<(std::ostream& out, const OpRequest::ClassInfo& i);

#endif /* OPREQUEST_H_ */
