/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Cloudbase Solutions
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef WNBD_PER_RES_H
#define WNBD_PER_RES_H

#include <wnbd.h>

#include "include/encoding.h"
#include "include/rbd/librbd.hpp"

std::string get_pr_initiator();

// persistent registration info
struct per_reg {
  uint64_t key;
  std::string initiator;

  void encode(bufferlist &bl) const
  {
    using ceph::encode;
    ENCODE_START(1, 1, bl);
    encode(key, bl);
    encode(initiator, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &bl)
  {
    using ceph::decode;
    DECODE_START(1, bl);
    decode(key, bl);
    decode(initiator, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(per_reg)

// persistent reservation
struct per_res {
  uint64_t key;
  std::string initiator;
  uint8_t type;

  void encode(bufferlist &bl) const
  {
    using ceph::encode;
    ENCODE_START(1, 1, bl);
    encode(key, bl);
    encode(initiator, bl);
    encode(type, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &bl)
  {
    using ceph::decode;
    DECODE_START(1, bl);
    decode(key, bl);
    decode(initiator, bl);
    decode(type, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(per_res)

// persistent registration and reservation info
class RbdPrInfo
{
private:
  librados::IoCtx &rados_ctx;
  librbd::Image &image;

  // The last retrieved state, used for "compare and write"
  // operations.
  bufferlist last_bl;

  std::string get_header_obj_name();
public:
  uint32_t generation;
  // persistent registrations
  std::vector<per_reg> regs;
  // persistent reservation
  std::optional<per_res> res;

  void encode(bufferlist &bl) const
  {
    using ceph::encode;
    ENCODE_START(1, 1, bl);
    encode(generation, bl);
    encode(regs, bl);
    encode(res, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &bl)
  {
    using ceph::decode;
    DECODE_START(1, bl);
    decode(generation, bl);
    decode(regs, bl);
    decode(res, bl);
    DECODE_FINISH(bl);
  }

  int create();
  int retrieve();
  int retrieve_or_create();
  // Performs an atomic "compare and write" operation, checking the last known
  // state of the xattr data, which must be explicitly retrieved first.
  int safe_replace();

  // get registration
  std::optional<per_reg> get_reg(const std::string &initiator) const;
  bool is_res_holder(
    const std::string &initiator,
    uint64_t res_key,
    bool check_reg) const;
  bool has_reservation() const;
  bool all_registrants_access() const;

  friend std::ostream &operator<<(std::ostream &os, const RbdPrInfo &pr_info);

  RbdPrInfo(librbd::IoCtx& _rados_ctx,
            librbd::Image& _image)
    : rados_ctx(_rados_ctx)
    , image(_image)
    , generation(0)
  {
  }
};

std::ostream &operator<<(std::ostream &os, const RbdPrInfo &pr_info);

// WNBD PERSISTENT RESERVATION IN operation
class WnbdPerResInOperation
{
private:
  librados::IoCtx &rados_ctx;
  librbd::Image &image;
  std::string initiator;
  uint8_t service_action;

  bufferlist& out_buff;
  PWNBD_STATUS wnbd_status;

  int read_keys();
  int read_reservations();

public:
  WnbdPerResInOperation(librbd::IoCtx& _rados_ctx,
                        librbd::Image& _image,
                        std::string& _initiator,
                        uint16_t _service_action,
                        bufferlist& _out_buff,
                        PWNBD_STATUS _wnbd_status)
    : rados_ctx(_rados_ctx)
    , image(_image)
    , initiator(_initiator)
    , service_action(_service_action)
    , out_buff(_out_buff)
    , wnbd_status(_wnbd_status)
  {
  }

  int execute();
};

// WNBD PERSISTENT RESERVATION OUT operation
class WnbdPerResOutOperation
{
private:
  librados::IoCtx &rados_ctx;
  librbd::Image &image;
  std::string initiator;
  uint8_t service_action;
  uint8_t scope;
  uint8_t type;

  bufferlist& in_buff;
  PWNBD_STATUS wnbd_status;

  // common parameter list fields
  uint64_t res_key;
  uint64_t sv_act_res_key;
  uint32_t scope_specif_addr;
  bool aptpl;
  bool all_tg_pt;
  bool spec_i_pt;

  int parse_param_list();
  int do_execute();

  int register_key(bool ignore_existing);
  void remove_own_reg(RbdPrInfo& pr_info, bool& found);
  void preempt_reg(RbdPrInfo& pr_info, bool& found);
  int reserve();
  void do_reserve(RbdPrInfo& pr_info);
  int release();
  int clear();
  int preempt();

public:
  WnbdPerResOutOperation(librbd::IoCtx& _rados_ctx,
                        librbd::Image& _image,
                        std::string& _initiator,
                        uint8_t _service_action,
                        uint8_t _scope,
                        uint8_t _type,
                        bufferlist& _in_buff,
                        PWNBD_STATUS _wnbd_status)
    : rados_ctx(_rados_ctx)
    , image(_image)
    , initiator(_initiator)
    , service_action(_service_action)
    , scope(_scope)
    , type(_type)
    , in_buff(_in_buff)
    , wnbd_status(_wnbd_status)
  {
  }

  int execute();
};

int check_pr_conflict(
  librbd::IoCtx& rados_ctx,
  librbd::Image& image,
  WnbdRequestType req_type,
  std::string& initiator,
  PWNBD_STATUS wnbd_status);

#endif // WNBD_PER_RES_H
