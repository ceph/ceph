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

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd

#include "per_res.h"

#define _NTSCSI_USER_MODE_
#include <rpc.h>
#include <ddk/scsi.h>

#ifndef RESERVATION_TYPE_WRITE_EXCLUSIVE_ALLREG
#define RESERVATION_TYPE_WRITE_EXCLUSIVE_ALLREG 0x07
#endif

#ifndef RESERVATION_TYPE_EXCLUSIVE_ALLREG
#define RESERVATION_TYPE_EXCLUSIVE_ALLREG 0x08
#endif

#ifndef SCSI_ADSENSE_UNRECOVERED_ERROR
#define SCSI_ADSENSE_UNRECOVERED_ERROR 0x11
#endif

#include <boost/endian/conversion.hpp>

#include "common/debug.h"
#include "common/errno.h"
#include "common/hostname.h"

#include "global/global_context.h"

#define WNBD_PR_INFO_XATTR_KEY      "pr_info.win32"

#define RBD_HEADER_PREFIX "rbd_header."
#define RBD_SUFFIX        ".rbd"

// The number of PR OUT attempts, retrying when receiving EAGAIN errors caused
// by concurrent PR updates.
#define WNBD_PR_OUT_TRIES 5

#define CLASS_NAME typeid(*this).name()

// The scsi.h structure is missing a few SPC-3 fields so we'll define our own.
typedef struct {
  UCHAR ReservationKey[8];
  UCHAR ServiceActionReservationKey[8];
  UCHAR ScopeSpecificAddress[4];
  UCHAR ActivatePersistThroughPowerLoss:1;
  UCHAR Reserved1:1;
  UCHAR AllTargetPorts:1;
  UCHAR SpecifyInitiatorPorts:1;
  UCHAR Reserved2:4;
  UCHAR Reserved3;
  UCHAR Obsolete[2];
} RBD_PRO_PARAMETER_LIST, *PRBD_PRO_PARAMETER_LIST;
static_assert(sizeof(RBD_PRO_PARAMETER_LIST) == sizeof(PRO_PARAMETER_LIST),
              "Invalid structure size");

std::string RbdPrInfo::get_header_obj_name()
{
  std::string image_id;
  auto r = image.get_id(&image_id);
  if (r < 0) {
    // old format
    std::string image_name;
    r = image.get_name(&image_name);
    // should always return 0
    ceph_assert(!r);
    return image_name + RBD_SUFFIX;
  } else {
    return RBD_HEADER_PREFIX + image_id;
  }
}

std::string get_pr_initiator() {
  // target_core_rbd includes the initiator name and target wwn in the i_t nexus
  // buffer, which looks like this:
  // iqn.1991-05.com.microsoft:ws2k22-node2.mydomain.local,i,0x3430303030313337,
  //   iqn.2016-05.com.org:00002,t,0x1
  //
  // We don't receive much information about the initiator or target, however we
  // know that there's at most one WNBD virtual SCSI adapter per host and that
  // WNBD doesn't allow mapping the same image twice.
  //
  // We also know that we're only receiving SCSI Persistent Reservation
  // commands emitted by this host, which is why the hostname will be the only
  // i_t information that we'll use.
  //
  // Note that the Windows hostname might change after joining a domain, which
  // might impact persistent reservations.
  return ceph_get_hostname();
}

int RbdPrInfo::retrieve()
{
  dout(20) << CLASS_NAME << "::" << __func__ << ": start" << dendl;

  auto object_name = get_header_obj_name();
  auto r = rados_ctx.getxattr(object_name, WNBD_PR_INFO_XATTR_KEY, last_bl);
  if (r < 0) {
    if (r == -ENODATA) {
      dout(5) << CLASS_NAME << "::" << __func__
        << ": no pr xattr specified" << dendl;
    } else {
      derr << CLASS_NAME << "::" << __func__
         << ": failed to retrieve PR info xattr"
         << ": error: " << r << dendl;
    }
    return r;
  }

  bufferlist::const_iterator ci = last_bl.begin();

  try {
    decode(ci);
  } catch (buffer::error& err) {
    derr << CLASS_NAME << "::" << __func__
         << ": failed to decode PR xattr: "
         << err.what() << dendl;
    return -EINVAL;
  }

  dout(6) << CLASS_NAME << "::" << __func__ << ": retrieved: " << *this << dendl;
  return 0;
}

int RbdPrInfo::retrieve_or_create()
{
  dout(20) << CLASS_NAME << "::" << __func__ << ": start" << dendl;

  auto r = retrieve();
  if (r == -ENODATA) {
    r = create();
  }

  return r;
}

std::optional<per_reg> RbdPrInfo::get_reg(const std::string &initiator) const
{
  for (auto reg : regs) {
    if (initiator == reg.initiator) {
      return reg;
    }
  }
  return {};
}

bool RbdPrInfo::all_registrants_access() const {
  if (!res.has_value()) {
    return false;
  }
  switch (res.value().type) {
  case RESERVATION_TYPE_WRITE_EXCLUSIVE_ALLREG:
  case RESERVATION_TYPE_EXCLUSIVE_ALLREG:
    return true;
  default:
    return false;
  }
}

bool RbdPrInfo::is_res_holder(
  const std::string &initiator,
  uint64_t res_key,
  bool check_reg) const
{
  if (check_reg) {
    auto existing_reg = get_reg(initiator);
    if (!existing_reg || existing_reg->key != res_key) {
      return false;
    }
  }

  if (all_registrants_access()) {
    return true;
  }

  return res.has_value() &&
    res.value().initiator == initiator &&
    res.value().key == res_key;
}

bool RbdPrInfo::has_reservation() const
{
  return res.has_value();
}

int RbdPrInfo::create()
{
  dout(20) << CLASS_NAME << "::" << __func__ << ": start" << dendl;

  try {
    encode(last_bl);
  } catch (buffer::error& err) {
    derr << CLASS_NAME << "::" << __func__
         << ": failed to encode PR xattr: "
         << err.what() << dendl;
    return -EINVAL;
  }

  auto object_name = get_header_obj_name();
  auto r = rados_ctx.setxattr(object_name, WNBD_PR_INFO_XATTR_KEY, last_bl);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RbdPrInfo::safe_replace()
{
  dout(20) << CLASS_NAME << "::" << __func__ << ": start" << dendl;

  bufferlist bl;
  try {
    encode(bl);
  } catch (buffer::error& err) {
    derr << CLASS_NAME << "::" << __func__
         << ": failed to encode PR xattr: "
         << err.what() << dendl;
    return -EINVAL;
  }

  dout(5) << CLASS_NAME << "::" << __func__ << ": applying: " << *this << dendl;

  librados::ObjectWriteOperation o;
  o.cmpxattr(WNBD_PR_INFO_XATTR_KEY, CEPH_OSD_CMPXATTR_OP_EQ, last_bl);
  o.setxattr(WNBD_PR_INFO_XATTR_KEY, bl);

  auto object_name = get_header_obj_name();
  auto r = rados_ctx.operate(object_name, &o);
  if (r < 0) {
    dout(5) << CLASS_NAME << "::" << __func__
            << ": couldn't apply PR: " << *this
            << ", error: " << r << dendl;
    return r;
  }

  return 0;
}

std::ostream &operator<<(std::ostream &os, const RbdPrInfo &pr_info) {
  os << std::hex
     << "RbdPrInfo("
     << "generation=0x" << pr_info.generation;

  if (pr_info.has_reservation()) {
    os << ", reservation=("
       << "key=0x" << pr_info.res.value().key
       << ", initiator=\"" << pr_info.res.value().initiator
       << "\", type=0x" << (uint) pr_info.res.value().type
       << ")";
  } else {
    os << ", reservation=None";
  }

  os << ", registrations=[";
  if (!pr_info.regs.empty()) {
    bool first = true;
    for (auto reg: pr_info.regs) {
      if (!first) {
        os << ", ";
      }
      os << "("
         << "key=0x" << reg.key
         << ", initiator=\"" << reg.initiator
         << "\")";
      first = false;
    }
  }

  os << "])";

  return os;
}

int WnbdPerResInOperation::read_keys()
{
  dout(20) << CLASS_NAME << "::" << __func__ << ": start" << dendl;

  auto pr_info = RbdPrInfo(rados_ctx, image);
  int r = pr_info.retrieve_or_create();
  if (r < 0) {
    return r;
  }

  uint32_t generation_be = boost::endian::native_to_big(pr_info.generation);
  out_buff.append(
    reinterpret_cast<const char*>(&generation_be), sizeof(generation_be));

  uint32_t additional_length_be = boost::endian::native_to_big(
    uint32_t(pr_info.regs.size() * sizeof(uint64_t)));
  out_buff.append(
    reinterpret_cast<const char*>(&additional_length_be),
    sizeof(additional_length_be));

  for (auto reg : pr_info.regs) {
    auto key_be = boost::endian::native_to_big(reg.key);
    out_buff.append(
      reinterpret_cast<const char*>(&key_be), sizeof(key_be));
  }

  return 0;
}

int WnbdPerResInOperation::read_reservations()
{
  dout(20) << CLASS_NAME << "::" << __func__ << ": start" << dendl;

  auto pr_info = RbdPrInfo(rados_ctx, image);
  int r = pr_info.retrieve_or_create();
  if (r < 0) {
    return r;
  }

  uint32_t additional_length = 0;
  if (pr_info.has_reservation()) {
    additional_length += sizeof(PRI_RESERVATION_DESCRIPTOR);
  }

  out_buff.push_back(
    ceph::buffer::create(sizeof(PRI_RESERVATION_LIST) + additional_length));
  auto o_res_list = (PPRI_RESERVATION_LIST) out_buff.c_str();

  ((uint32_t*)o_res_list->Generation)[0] = boost::endian::native_to_big(
    pr_info.generation);
  ((uint32_t*)o_res_list->AdditionalLength)[0] = boost::endian::native_to_big(
    additional_length);

  if (pr_info.has_reservation()) {
    PPRI_RESERVATION_DESCRIPTOR o_res = o_res_list->Reservations;

    ((uint64_t*)o_res->ReservationKey)[0] = boost::endian::native_to_big(
      pr_info.res.value().key);
    o_res->Type = pr_info.res.value().type;
    o_res->Scope = RESERVATION_SCOPE_LU;
  }

  return 0;
}

int WnbdPerResInOperation::execute()
{
  dout(5) << std::hex
          << "WnbdPerResInOperation: "
          << "action=0x" << (uint) service_action
          << ", initiator=\"" << initiator << "\""
          << ": start" << dendl;

  int r = 0;
  switch (service_action) {
  case RESERVATION_ACTION_READ_KEYS:
    r = read_keys();
    break;
  case RESERVATION_ACTION_READ_RESERVATIONS:
    r = read_reservations();
    break;
  // TODO: consider implementing REPORT CAPABILITIES service action
  default:
    derr << "Unsupported Persistent Reservation IN service action: "
         << std::hex << "0x" << (uint) service_action << dendl;
    WnbdSetSense(
      wnbd_status,
      SCSI_SENSE_ILLEGAL_REQUEST,
      SCSI_ADSENSE_ILLEGAL_COMMAND);
    return -ENOTSUP;
  }

  if (r < 0 && !wnbd_status->ScsiStatus) {
    WnbdSetSense(
      wnbd_status,
      SCSI_SENSE_MEDIUM_ERROR,
      SCSI_ADSENSE_UNRECOVERED_ERROR);
  }

  return r;
}

int WnbdPerResOutOperation::parse_param_list()
{
  if (in_buff.length() < sizeof(RBD_PRO_PARAMETER_LIST)) {
    derr << "Invalid PR OUT parameter list size: "
         << in_buff.length() << " < "
         << sizeof(RBD_PRO_PARAMETER_LIST) << dendl;
    WnbdSetSense(
      wnbd_status,
      SCSI_SENSE_ILLEGAL_REQUEST,
      SCSI_ADSENSE_PARAMETER_LIST_LENGTH);
    return -EOVERFLOW;
  }

  PRBD_PRO_PARAMETER_LIST params = (PRBD_PRO_PARAMETER_LIST) in_buff.c_str();

  res_key = boost::endian::big_to_native(
    *reinterpret_cast<uint64_t*>(params->ReservationKey));
  sv_act_res_key = boost::endian::big_to_native(
    *reinterpret_cast<uint64_t*>(params->ServiceActionReservationKey));
  scope_specif_addr = boost::endian::big_to_native(
    *reinterpret_cast<uint64_t*>(params->ScopeSpecificAddress));
  aptpl = params->ActivatePersistThroughPowerLoss;
  all_tg_pt = params->AllTargetPorts;
  spec_i_pt = params->SpecifyInitiatorPorts;
  return 0;
}

int WnbdPerResOutOperation::register_key(bool ignore_existing)
{
  dout(20) << CLASS_NAME << "::" << __func__ << ": start" << dendl;

  if (all_tg_pt) {
    derr << "unsupported PR flag: all_tg_pt" << dendl;
    WnbdSetSense(
      wnbd_status,
      SCSI_SENSE_ILLEGAL_REQUEST,
      SCSI_ADSENSE_INVALID_CDB);
    return -EINVAL;
  }

  if (spec_i_pt) {
    derr << "unsupported PR flag: spec_i_pt" << dendl;
    WnbdSetSense(
      wnbd_status,
      SCSI_SENSE_ILLEGAL_REQUEST,
      SCSI_ADSENSE_INVALID_CDB);
    return -EINVAL;
  }

  auto pr_info = RbdPrInfo(rados_ctx, image);
  int r = pr_info.retrieve_or_create();
  if (r < 0) {
    return r;
  }

  per_reg* existing_reg = nullptr;
  for (auto& reg : pr_info.regs) {
    if (initiator == reg.initiator) {
      existing_reg = &reg;
    }
  }

  if (!existing_reg) {
    if (!ignore_existing && (res_key != 0)) {
      derr << "Reservation conflict. "
           << "New initiator, yet an old key was provided."
           << dendl;
      wnbd_status->ScsiStatus = SCSISTAT_RESERVATION_CONFLICT;
      return -EEXIST;
    }
    if (sv_act_res_key == 0) {
      // delete no-op
      dout(20) << CLASS_NAME << "::" << __func__
               << ": was requested to delete registration but the initiator "
               << "is unregistered, no-op" << dendl;
      return 0;
    }
  } else {
    // found existing registration
    if (!ignore_existing && (res_key != existing_reg->key)) {
      derr << "Reservation conflict. "
           << "Existing initiator, old key mismatch: "
           << std::hex << "0x" << res_key << " != "
           << "0x" << existing_reg->key << dendl;
      wnbd_status->ScsiStatus = SCSISTAT_RESERVATION_CONFLICT;
      return -EEXIST;
    }

    if (sv_act_res_key == 0) {
      dout(20) << CLASS_NAME << "::" << __func__
               << ": removing registration" << dendl;
      bool reg_found = false;
      remove_own_reg(pr_info, reg_found);

      pr_info.generation++;
      return pr_info.safe_replace();
    }
  }

  if (!existing_reg) {
    dout(20) << CLASS_NAME << "::" << __func__
             << ": adding new registration" << dendl;
    per_reg new_reg = {
      .key = sv_act_res_key,
      .initiator = initiator,
    };
    pr_info.regs.push_back(new_reg);
  } else {
    dout(20) << CLASS_NAME << "::" << __func__
             << ": changing registration key" << dendl;
    existing_reg->key = sv_act_res_key;

    if (pr_info.has_reservation() &&
        initiator == pr_info.res.value().initiator) {
      pr_info.res.value().key = sv_act_res_key;
    }
  }

  pr_info.generation++;
  return pr_info.safe_replace();
}

void WnbdPerResOutOperation::remove_own_reg(
  RbdPrInfo& pr_info,
  bool& found)
{
  dout(20) << CLASS_NAME << "::" << __func__ << ": start" << dendl;

  for (auto it = pr_info.regs.begin(); it != pr_info.regs.end();) {
    if (initiator == it->initiator) {
      dout(20) << CLASS_NAME << "::" << __func__ << ": removing registration: "
               << std::hex
               << "initiator: " << initiator
               << ", key: 0x" << it->key << dendl;
      if (!pr_info.all_registrants_access() &&
          pr_info.is_res_holder(initiator, res_key, false)) {
        dout(5) << CLASS_NAME << "::" << __func__
                << std::hex
                << "cleaning up reservation while removing registration"
                << ", type: 0x" << (uint) pr_info.res.value().type
                << dendl;
        pr_info.res.reset();
      }

      found = true;
      it = pr_info.regs.erase(it);
    } else {
      it++;
    }
  }
}

void WnbdPerResOutOperation::preempt_reg(
  RbdPrInfo& pr_info,
  bool& found)
{
  dout(20) << CLASS_NAME << "::" << __func__ << ": start" << dendl;

  for (auto it = pr_info.regs.begin(); it != pr_info.regs.end();) {
    if (initiator == it->initiator) {
      it++;
      continue;
    }

    if (sv_act_res_key && (it->key != sv_act_res_key)) {
      it++;
      continue;
    }

    found = true;
    it = pr_info.regs.erase(it);
    dout(5) << CLASS_NAME << "::" << __func__
            << ": preempted registration" << dendl;
  }
  if (!found) {
    dout(5) << CLASS_NAME << "::" << __func__
            << ": couldn't find registration to preempt" << dendl;
  }
}

void WnbdPerResOutOperation::do_reserve(RbdPrInfo& pr_info)
{
  dout(20) << CLASS_NAME << "::" << __func__ << ": start" << dendl;

  per_res reservation = {
    .key = res_key,
    .initiator = initiator,
    .type = type,
  };
  pr_info.res.emplace(reservation);
}

int WnbdPerResOutOperation::reserve()
{
  dout(20) << CLASS_NAME << "::" << __func__ << ": start" << dendl;

  auto pr_info = RbdPrInfo(rados_ctx, image);
  int r = pr_info.retrieve_or_create();
  if (r < 0) {
    return r;
  }

  auto existing_reg = pr_info.get_reg(initiator);

  if (!existing_reg) {
    derr << CLASS_NAME << "::" << __func__
         << ": reserve without registration" << dendl;
    WnbdSetSense(
      wnbd_status,
      SCSI_SENSE_ILLEGAL_REQUEST,
      SCSI_ADSENSE_LUN_COMMUNICATION);
    return -EINVAL;
  }

  if (res_key != existing_reg->key) {
    derr << CLASS_NAME << "::" << __func__
         << ": PR key mismatch: "
         << std::hex << "0x" << res_key << " != "
         << "0x" << existing_reg->key << dendl;
    wnbd_status->ScsiStatus = SCSISTAT_RESERVATION_CONFLICT;
    return -EEXIST;
  }

  if (pr_info.has_reservation()) {
    if (!pr_info.is_res_holder(initiator, res_key, false)) {
      derr << CLASS_NAME << "::" << __func__
         << ": unable to acquire reservation, already held by "
         << pr_info.res.value().initiator
         << ". registration type: 0x" << std::hex << pr_info.res.value().type
         << dendl;
      wnbd_status->ScsiStatus = SCSISTAT_RESERVATION_CONFLICT;
      return -EEXIST;
    }

    if (pr_info.res.value().type != type) {
      derr << CLASS_NAME << "::" << __func__
         << ": reservation request with mismatching type: "
         << std::hex
         << "0x" << (uint) pr_info.res.value().type << " != "
         << "0x" << (uint) type << dendl;
      wnbd_status->ScsiStatus = SCSISTAT_RESERVATION_CONFLICT;
      return -EEXIST;
    }

    dout(20) << CLASS_NAME << "::" << __func__
             << ": found matching reservation, no-op" << dendl;
    return 0;
  }

  do_reserve(pr_info);
  return pr_info.safe_replace();
}

int WnbdPerResOutOperation::release()
{
  dout(20) << CLASS_NAME << "::" << __func__ << ": start" << dendl;

  auto pr_info = RbdPrInfo(rados_ctx, image);
  int r = pr_info.retrieve_or_create();
  if (r < 0) {
    return r;
  }

  if (!pr_info.has_reservation()) {
    dout(20) << CLASS_NAME << "::" << __func__
             << ": no reservation, no-op" << dendl;
    return 0;
  }

  auto existing_reg = pr_info.get_reg(initiator);

  if (!existing_reg) {
    derr << CLASS_NAME << "::" << __func__
         << ": release without registration" << dendl;
    WnbdSetSense(
      wnbd_status,
      SCSI_SENSE_ILLEGAL_REQUEST,
      SCSI_ADSENSE_LUN_COMMUNICATION);
    return -EINVAL;
  }

  if (!pr_info.is_res_holder(initiator, res_key, false)) {
    derr << CLASS_NAME << "::" << __func__
       << ": not a registration holder, no-op "
       << dendl;
    return 0;
  }

  if (res_key != existing_reg->key) {
    derr << CLASS_NAME << "::" << __func__
         << ": PR key mismatch: "
         << std::hex << "0x" << res_key << " != "
         << "0x" << existing_reg->key << dendl;
    wnbd_status->ScsiStatus = SCSISTAT_RESERVATION_CONFLICT;
    return -EEXIST;
  }

  if (pr_info.res.value().type != type) {
    derr << CLASS_NAME << "::" << __func__
         << ": release request with mismatching type: "
         << std::hex
         << "0x" << (uint) pr_info.res.value().type << " != "
         << "0x" << (uint) type << dendl;
    wnbd_status->ScsiStatus = SCSISTAT_RESERVATION_CONFLICT;
    return -EEXIST;
  }

  // TODO: notify other reservation holders about the released reservation
  // using SCSI SENSE.
  dout(5) << CLASS_NAME << "::" << __func__
          << ": releasing reservation" << dendl;
  pr_info.res.reset();
  return pr_info.safe_replace();
}

int WnbdPerResOutOperation::clear()
{
  dout(20) << CLASS_NAME << "::" << __func__ << ": start" << dendl;

  auto pr_info = RbdPrInfo(rados_ctx, image);
  int r = pr_info.retrieve_or_create();
  if (r < 0) {
    return r;
  }

  auto existing_reg = pr_info.get_reg(initiator);

  if (!existing_reg) {
    derr << CLASS_NAME << "::" << __func__
         << ": release without registration" << dendl;
    WnbdSetSense(
      wnbd_status,
      SCSI_SENSE_ILLEGAL_REQUEST,
      SCSI_ADSENSE_LUN_COMMUNICATION);
    return -EINVAL;
  }

  if (res_key != existing_reg->key) {
    derr << CLASS_NAME << "::" << __func__
         << ": PR key mismatch: "
         << std::hex << "0x" << res_key << " != "
         << "0x" << existing_reg->key << dendl;
    wnbd_status->ScsiStatus = SCSISTAT_RESERVATION_CONFLICT;
    return -EEXIST;
  }

  dout(5) << CLASS_NAME << "::" << __func__
          << ": clearing reservation and registrations" << dendl;
  // TODO: notify other initiators about the preempted reservations
  // using SCSI SENSE.
  pr_info.regs.clear();
  pr_info.res.reset();
  pr_info.generation++;
  return pr_info.safe_replace();
}

// SCSI SPC-3 compliant (5.6.10.4 Preempting)
int WnbdPerResOutOperation::preempt()
{
  dout(20) << CLASS_NAME << "::" << __func__ << ": start" << dendl;

  auto pr_info = RbdPrInfo(rados_ctx, image);
  int r = pr_info.retrieve_or_create();
  if (r < 0) {
    return r;
  }

  auto existing_reg = pr_info.get_reg(initiator);

  if (!existing_reg) {
    derr << CLASS_NAME << "::" << __func__
         << ": release without registration" << dendl;
    WnbdSetSense(
      wnbd_status,
      SCSI_SENSE_ILLEGAL_REQUEST,
      SCSI_ADSENSE_LUN_COMMUNICATION);
    return -EINVAL;
  }

  if (res_key != existing_reg->key) {
    derr << CLASS_NAME << "::" << __func__
         << ": PR key mismatch: "
         << std::hex << "0x" << res_key << " != "
         << "0x" << existing_reg->key << dendl;
    wnbd_status->ScsiStatus = SCSISTAT_RESERVATION_CONFLICT;
    return -EEXIST;
  }

  bool regs_found = false;

  if (!pr_info.has_reservation()) {
    dout(20) << CLASS_NAME << "::" << __func__
             << ": preempt requested, no reservation found" << dendl;
    if (!sv_act_res_key) {
      derr << CLASS_NAME << "::" << __func__
           << ": reservation conflict"
           << ": no existing reservation and no new key specified"
           << dendl;
      WnbdSetSense(
        wnbd_status,
        SCSI_SENSE_ILLEGAL_REQUEST,
        SCSI_ADSENSE_INVALID_FIELD_PARAMETER_LIST);
      return -EINVAL;
    }

    preempt_reg(pr_info, regs_found);
    if (!regs_found) {
      derr << CLASS_NAME << "::" << __func__
           << ": reservation conflict"
           << ": couldn't find the registration to preempt"
           << dendl;
      wnbd_status->ScsiStatus = SCSISTAT_RESERVATION_CONFLICT;
      return -EEXIST;
    }

    goto commit;
  }

  if (pr_info.all_registrants_access()) {
    dout(20) << CLASS_NAME << "::" << __func__
             << ": reservation applies to all registrants"
             << dendl;
    preempt_reg(pr_info, regs_found);
    if (!regs_found) {
      derr << CLASS_NAME << "::" << __func__
           << ": reservation conflict"
           << ": couldn't find the registration to preempt"
           << dendl;
      wnbd_status->ScsiStatus = SCSISTAT_RESERVATION_CONFLICT;
      return -EEXIST;
    }

    if (!sv_act_res_key) {
      dout(5) << CLASS_NAME << "::" << __func__
              << ": replacing reservation"
              << dendl;
      do_reserve(pr_info);
    }
    goto commit;
  }

  if (pr_info.res.value().key != sv_act_res_key) {
    if (!sv_act_res_key) {
      derr << CLASS_NAME << "::" << __func__
           << ": reservation conflict"
           << ": didn't specify which reservation to preempt, "
           << " the existing reservation doesn't apply to all registrants"
           << dendl;
      WnbdSetSense(
        wnbd_status,
        SCSI_SENSE_ILLEGAL_REQUEST,
        SCSI_ADSENSE_INVALID_FIELD_PARAMETER_LIST);
      return -EINVAL;
    }

    dout(5) << CLASS_NAME << "::" << __func__
            << ": the existing reservation doesn't match the specified key "
            << " and it doesn't apply to all registrants. The reservation "
            << " will be left in place while the specified registration "
            << " will be preempted."
            << dendl;

    preempt_reg(pr_info, regs_found);
    if (!regs_found) {
      derr << CLASS_NAME << "::" << __func__
           << ": reservation conflict"
           << ": couldn't find the registration to preempt"
           << dendl;
      wnbd_status->ScsiStatus = SCSISTAT_RESERVATION_CONFLICT;
      return -EEXIST;
    }
    goto commit;
  }

  preempt_reg(pr_info, regs_found);
  if (!regs_found && initiator != pr_info.res.value().initiator) {
    derr << CLASS_NAME << "::" << __func__
         << ": reservation conflict"
         << ": couldn't find the registration to preempt"
         << dendl;
    wnbd_status->ScsiStatus = SCSISTAT_RESERVATION_CONFLICT;
    return -EEXIST;
  }

  dout(5) << CLASS_NAME << "::" << __func__
          << ": replacing reservation"
          << dendl;
  do_reserve(pr_info);

commit:
  // TODO: notify other initiators about the preempted reservation
  // using SCSI SENSE.
  pr_info.generation++;
  return pr_info.safe_replace();
}

int WnbdPerResOutOperation::do_execute()
{
  int r = parse_param_list();
  if (r != 0) {
    return r;
  }

  dout(5) << std::hex
          << "WnbdPerResOutOperation: "
          << "action=0x" << (uint) service_action
          << ", scope=0x" << (uint) scope
          << ", type=0x" << (uint) type
          << ", initiator=\"" << initiator << "\""
          << ", res_key=0x" << res_key
          << ", sv_act_res_key=0x" << sv_act_res_key
          << ", scope_specif_addr=0x" << scope_specif_addr
          << ": start" << dendl;

  if (scope != RESERVATION_SCOPE_LU) {
    derr << CLASS_NAME << "::" << __func__
         << std::hex
         << ": unsupported scope: 0x" << (uint) scope << dendl;
    WnbdSetSense(
      wnbd_status,
      SCSI_SENSE_ILLEGAL_REQUEST,
      SCSI_ADSENSE_LUN_COMMUNICATION);
    return -EINVAL;
  }

  switch (service_action) {
  case RESERVATION_ACTION_REGISTER:
    return register_key(false);
  case RESERVATION_ACTION_REGISTER_IGNORE_EXISTING:
    return register_key(true);
  case RESERVATION_ACTION_RESERVE:
    return reserve();
  case RESERVATION_ACTION_RELEASE:
    return release();
  case RESERVATION_ACTION_CLEAR:
    return clear();
  case RESERVATION_ACTION_PREEMPT:
    return preempt();
  default:
    dout(5) << "Unsupported Persistent Reservation OUT service action: "
            << std::hex << "0x" << service_action << dendl;
    WnbdSetSense(
      wnbd_status,
      SCSI_SENSE_ILLEGAL_REQUEST,
      SCSI_ADSENSE_ILLEGAL_COMMAND);
    return -ENOTSUP;
  }
  return 0;
}

int WnbdPerResOutOperation::execute()
{
  int r = -EAGAIN;
  for (int attempt=0;
       r == -EAGAIN && attempt < WNBD_PR_OUT_TRIES;
       attempt++){
    r = do_execute();

    if (r == -EAGAIN && attempt < WNBD_PR_OUT_TRIES) {
      dout(5) << "encountered conflict while trying to update "
              << "persistent reservations. Tries left: "
              << WNBD_PR_OUT_TRIES - attempt - 1
              << dendl;
      // clear wnbd status before retrying
      *wnbd_status = {0};
    }
  }

  if (r < 0 && !wnbd_status->ScsiStatus) {
    WnbdSetSense(
      wnbd_status,
      SCSI_SENSE_MEDIUM_ERROR,
      SCSI_ADSENSE_UNRECOVERED_ERROR);
  }

  return r;
}

int check_pr_conflict(
  librbd::IoCtx& rados_ctx,
  librbd::Image& image,
  WnbdRequestType req_type,
  std::string &initiator,
  PWNBD_STATUS wnbd_status)
{
  dout(20) << __func__ << ": start" << dendl;

  auto pr_info = RbdPrInfo(rados_ctx, image);
  int r = pr_info.retrieve();
  if (r == -ENODATA) {
    dout(20) << __func__ << ": no pr data" << dendl;
    return 0;
  }

  if (r < 0) {
    WnbdSetSense(
      wnbd_status,
      SCSI_SENSE_MEDIUM_ERROR,
      SCSI_ADSENSE_UNRECOVERED_ERROR);
    return r;
  }

  bool is_write = false;
  switch (req_type) {
  case WnbdReqTypePersistResIn:
  case WnbdReqTypePersistResOut:
    // PR command conflicts are checked separately, let's avoid
    // duplication
    return 0;
  case WnbdReqTypeRead:
    break;
  case WnbdReqTypeWrite:
  case WnbdReqTypeFlush:
  case WnbdReqTypeUnmap:
    // we're treating flush and unmap the same way as writes
    is_write = true;
    break;
  default:
    derr << __func__
      << ": unsupported wnbd request type: 0x"
      << std::hex << req_type << dendl;
    WnbdSetSense(
      wnbd_status,
      SCSI_SENSE_ILLEGAL_REQUEST,
      SCSI_ADSENSE_LUN_COMMUNICATION);
    return -EINVAL;
  }

  if (!pr_info.has_reservation()) {
    dout(20) << __func__
      << ": no persistent reservation" << dendl;
    return 0;
  }

  if (initiator == pr_info.res.value().initiator) {
    dout(20) << __func__
      << ": explicit reservation holder" << dendl;
    return 0;
  }

  auto reg = pr_info.get_reg(initiator);
  bool has_reg = reg.has_value();

  bool reg_nexuses = false;
  bool write_exclusive = false;

  switch (pr_info.res.value().type) {
  case RESERVATION_TYPE_WRITE_EXCLUSIVE:
    write_exclusive = true;
  case RESERVATION_TYPE_EXCLUSIVE:
    break;
  case RESERVATION_TYPE_WRITE_EXCLUSIVE_REGISTRANTS:
  case RESERVATION_TYPE_WRITE_EXCLUSIVE_ALLREG:
    write_exclusive = true;
  case RESERVATION_TYPE_EXCLUSIVE_REGISTRANTS:
  case RESERVATION_TYPE_EXCLUSIVE_ALLREG:
    reg_nexuses = true;
    break;
  default:
    derr << __func__
      << ": unsupported reservation type: 0x"
      << std::hex << pr_info.res.value().type
      << dendl;
    WnbdSetSense(
      wnbd_status,
      SCSI_SENSE_ILLEGAL_REQUEST,
      SCSI_ADSENSE_LUN_COMMUNICATION);
    return -EINVAL;
  }

  if (!is_write) {
    if (write_exclusive) {
      dout(20) << __func__
        << ": allowing non-write operation with write exclusive pr"
        << dendl;
      return 0;
    }
  }

  if (reg_nexuses) {
    if (has_reg) {
      dout(20) << __func__
        << ": allowing operation"
        << ": reg present, RR rsv"
        << dendl;
      return 0;
    } else {
      dout(10) << __func__
        << ": reservation conflict"
        << ": no reg, exclusive rsv"
        << dendl;
    }
  } else {
    dout(10) << __func__
      << ": reservation conflict"
      << ": exclusive rsv, not rsv owner"
      << dendl;
  }

  // conflict by default
  wnbd_status->ScsiStatus = SCSISTAT_RESERVATION_CONFLICT;
  return -EINVAL;
}
