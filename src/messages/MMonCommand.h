// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_MMONCOMMAND_H
#define CEPH_MMONCOMMAND_H

#include "messages/PaxosServiceMessage.h"
#include "common/cmdparse.h" // for cmdmap_from_json()
#include "include/encoding_string.h"
#include "include/encoding_vector.h"

#include <vector>
#include <string>

class MMonCommand final : public PaxosServiceMessage {
public:
  // weird note: prior to octopus, MgrClient would leave fsid blank when
  // sending commands to the mgr.  Starting with octopus, this is either
  // populated with a valid fsid (tell command) or an MMgrCommand is sent
  // instead.
  uuid_d fsid;
  std::vector<std::string> cmd;

  MMonCommand() : PaxosServiceMessage{MSG_MON_COMMAND, 0} {}
  MMonCommand(const uuid_d &f)
    : PaxosServiceMessage{MSG_MON_COMMAND, 0},
      fsid(f)
  { }

  MMonCommand(const MMonCommand &other)
    : PaxosServiceMessage(MSG_MON_COMMAND, 0),
      fsid(other.fsid),
      cmd(other.cmd) {
    set_tid(other.get_tid());
    set_data(other.get_data());
  }

  ~MMonCommand() final {}

public:
  std::string_view get_type_name() const override { return "mon_command"; }
  void print(std::ostream& o) const override;

  void encode_payload(uint64_t features) override;
  void decode_payload() override;

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
