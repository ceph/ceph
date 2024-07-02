// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef OPREQUEST_H_
#define OPREQUEST_H_

#include "common/TrackedOp.h"
#include "common/tracer.h"
/**
 * The MgrOpRequest takes in a MessageRef and takes over a single reference
 * to it, which it puts() when destroyed.
 */
struct MgrOpRequest : public TrackedOp {
  friend class OpTracker;

public:
  void _dump(ceph::Formatter *f) const override;

private:
  MessageRef request; /// the logical request we are tracking
  entity_inst_t req_src_inst;
  uint8_t hit_flag_points;
  uint8_t latest_flag_point;
  const char* last_event_detail = nullptr;

  static const uint8_t flag_started =              1 << 0;
  static const uint8_t flag_queued_for_module =    1 << 1;
  static const uint8_t flag_reached_module =       1 << 2;
  static const uint8_t flag_start_mon_command =    1 << 3;
  static const uint8_t flag_finish_mon_command =   1 << 4;

  MgrOpRequest(MessageRef req, OpTracker *tracker);

protected:
  void _dump_op_descriptor(std::ostream& stream) const override;
  void _unregistered() override;
  bool filter_out(const std::set<std::string>& filters) override;

public:
  ~MgrOpRequest() override {
    request->put();
  }

  template<class T>
  const T* get_req() const { return static_cast<const T*>(request); }

  const MessageRef get_req() const { return request; }
  MessageRef get_nonconst_req() { return request; }

  entity_name_t get_source() {
    if (request) {
      return request->get_source();
    } else {
      return {};
    }
  }
  uint8_t state_flag() const {
    return latest_flag_point;
  }

  std::string _get_state_string() const override {
    switch(latest_flag_point) {
    case flag_started: return "started";
    case flag_queued_for_module: return "queued for module";
    case flag_reached_module: return last_event_detail;
    case flag_start_mon_command: return "start mon command";
    case flag_finish_mon_command: return "mon command finished";
    default: break;
    }
    return "no flag points reached";
  }

  static std::string get_state_string(uint8_t flag) {
    std::string flag_point;
    switch(flag) {
      case flag_started:
        flag_point = "started";
        break;
      case flag_queued_for_module:
        flag_point = "queued for module";
        break;
      case flag_reached_module:
        flag_point = "reached module";
        break;
      case flag_start_mon_command:
        flag_point = "start mon command";
        break;
      case flag_finish_mon_command:
        flag_point = "mon command finished";
        break;
    }
    return flag_point;
  }

  void mark_started() {
    mark_flag_point(flag_started, "started");
  }
  void mark_queued_for_module() {
    mark_flag_point(flag_queued_for_module, "queued_for_module");
  }
  void mark_reached(const char *s) {
    mark_flag_point(flag_reached_module, s);
  }
  void mark_start_mon_command() {
    mark_flag_point(flag_start_mon_command, "start_mon_command");
  }
  void mark_finish_mon_command() {
    mark_flag_point(flag_start_mon_command, "mon_command_finished");
  }

  typedef boost::intrusive_ptr<MgrOpRequest> Ref;

private:
  void mark_flag_point(uint8_t flag, const char *s);
  void mark_flag_point_string(uint8_t flag, const std::string& s);
};

typedef MgrOpRequest::Ref MgrOpRequestRef;

#endif /* OPREQUEST_H_ */
