// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "ECInject.h"
#include "common/ceph_mutex.h"

#include <map>

namespace ECInject {

  // Error inject interfaces
  static ceph::recursive_mutex lock =
    ceph::make_recursive_mutex("ECCommon::lock");
  static std::map<ghobject_t,std::pair<int64_t,int64_t>> read_failures0;
  static std::map<ghobject_t,std::pair<int64_t,int64_t>> read_failures1;
  static std::map<ghobject_t,std::pair<int64_t,int64_t>> write_failures0;
  static std::map<ghobject_t,std::pair<int64_t,int64_t>> write_failures1;
  static std::map<ghobject_t,std::pair<int64_t,int64_t>> write_failures2;
  static std::map<ghobject_t,std::pair<int64_t,int64_t>> write_failures3;
  static std::map<ghobject_t,shard_id_t> write_failures0_shard;
  static std::set<osd_reqid_t> write_failures0_reqid;

  /**
   * Configure a read error inject that typically forces additional reads of
   * shards in an EC pool to recover data using the redundancy. With multiple
   * errors it is possible to force client reads to fail.
   *
   * Type 0 - Simulate a medium error. Fail a read with -EIO to force
   * additional reads and a decode
   *
   * Type 1 - Simulate a missing OSD. Dont even try to read a shard
   *
   * @brief Set up a read error inject for an object in an EC pool.
   * @param o Target object for the error inject.
   * @param when Error inject starts after this many object store reads.
   * @param duration Error inject affects this many object store reads.
   * @param type Type of error inject 0 = EIO, 1 = missing shard.
   * @return string Result of configuring the error inject.
   */
  std::string read_error(const ghobject_t& o,
				   const int64_t type,
				   const int64_t when,
				   const int64_t duration) {
    std::lock_guard<ceph::recursive_mutex> l(lock);
    ghobject_t os = o;
    if (os.hobj.oid.name == "*") {
      os.hobj.set_hash(0);
    }
    switch (type) {
    case 0:
      read_failures0[os] = std::pair(when, duration);
      return "ok - read returns EIO";
    case 1:
      read_failures1[os] = std::pair(when, duration);
      return "ok - read pretends shard is missing";
    default:
      break;
    }
    return "unrecognized error inject type";
  }

  /**
   * Configure a write error inject that either fails an OSD or causes a
   * client write operation to be rolled back.
   *
   * Type 0 - Tests rollback. Drop a write I/O to a shard, then simulate an OSD
   * down to force rollback to occur, lastly fail the retried write from the
   * client so the results of the rollback can be inspected.
   *
   * Type 1 - Drop a write I/O to a shard. Used on its own this will hang a
   * write I/O.
   *
   * Type 2 - Simulate an OSD down (ceph osd down) to force a new epoch. Usually
   * used together with type 1 to force a rollback
   *
   * Type 3 - Abort when an OSD processes a write I/O to a shard. Typically the
   * client write will be commited while the OSD is absent which will result in
   * recovery or backfill later when the OSD returns.
   *
   * @brief Set up a write error inject for an object in an EC pool.
   * @param o Target object for the error inject.
   * @param when Error inject starts after this many object store reads.
   * @param duration Error inject affects this many object store reads.
   * @param type Type of error inject 0 = EIO, 1 = missing shard.
   * @return string Result of configuring the error inect.
   */
  std::string write_error(const ghobject_t& o,
				    const int64_t type,
				    const int64_t when,
				    const int64_t duration) {
    std::lock_guard<ceph::recursive_mutex> l(lock);
    std::map<ghobject_t,std::pair<int64_t,int64_t>> *failures;
    ghobject_t os = o;
    bool no_shard = true;
    std::string result;
    switch (type) {
    case 0:
      failures = &write_failures0;
      result = "ok - drop write, sim OSD down and fail client retry with EINVAL";
      break;
    case 1:
      failures = &write_failures1;
      no_shard = false;
      result = "ok - drop write to shard";
      break;
    case 2:
      failures = &write_failures2;
      result = "ok - inject OSD down";
      break;
    case 3:
      if (duration != 1) {
        return "duration must be 1";
      }
      failures = &write_failures3;
      result = "ok - write abort OSDs";
      break;
    default:
      return "unrecognized error inject type";
    }
    if (no_shard) {
      os.set_shard(shard_id_t::NO_SHARD);
    }
    if (os.hobj.oid.name == "*") {
      os.hobj.set_hash(0);
    }
    (*failures)[os] = std::pair(when, duration);
    if (type == 0) {
      write_failures0_shard[os] = o.shard_id;
    }
    return result;
  }

  /**
   * @brief Clear a previously configured read error inject.
   * @param o Target object for the error inject.
   * @param type Type of error inject 0 = EIO, 1 = missing shard.
   * @return string Indication of how many errors were cleared.
   */
  std::string clear_read_error(const ghobject_t& o,
				         const int64_t type) {
    std::lock_guard<ceph::recursive_mutex> l(lock);
    std::map<ghobject_t,std::pair<int64_t,int64_t>> *failures;
    ghobject_t os = o;
    int64_t remaining = 0;
    switch (type) {
    case 0:
      failures = &read_failures0;
      break;
    case 1:
      failures = &read_failures1;
      break;
    default:
      return "unrecognized error inject type";
    }
    if (os.hobj.oid.name == "*") {
      os.hobj.set_hash(0);
    }
    auto it = failures->find(os);
    if (it != failures->end()) {
      remaining = it->second.second;
      failures->erase(it);
    }
    if (remaining == 0) {
      return "no outstanding error injects";
    } else if (remaining == 1) {
      return "ok - 1 inject cleared";
    }
    return "ok - " + std::to_string(remaining) + " injects cleared";
  }

  /**
   * @brief Clear a previously configured write error inject.
   * @param o Target object for the error inject.
   * @param type Type of error inject 0 = EIO, 1 = missing shard.
   * @return string Indication of how many errors were cleared.
   */
  std::string clear_write_error(const ghobject_t& o,
					  const int64_t type) {
    std::lock_guard<ceph::recursive_mutex> l(lock);
    std::map<ghobject_t,std::pair<int64_t,int64_t>> *failures;
    ghobject_t os = o;
    bool no_shard = true;
    int64_t remaining = 0;
    switch (type) {
    case 0:
      failures = &write_failures0;
      break;
    case 1:
      failures = &write_failures1;
      no_shard = false;
      break;
    case 2:
      failures = &write_failures2;
      break;
    case 3:
      failures = &write_failures3;
      break;
    default:
      return "unrecognized error inject type";
    }
    if (no_shard) {
      os.set_shard(shard_id_t::NO_SHARD);
    }
    if (os.hobj.oid.name == "*") {
      os.hobj.set_hash(0);
    }
    auto it = failures->find(os);
    if (it != failures->end()) {
      remaining = it->second.second;
      failures->erase(it);
      if (type == 0) {
        write_failures0_shard.erase(os);
      }
    }
    if (remaining == 0) {
      return "no outstanding error injects";
    } else if (remaining == 1) {
      return "ok - 1 inject cleared";
    }
    return "ok - " + std::to_string(remaining) + " injects cleared";
  }

  static bool test_error(const ghobject_t& o,
    std::map<ghobject_t,std::pair<int64_t,int64_t>> *failures)
  {
    std::lock_guard<ceph::recursive_mutex> l(lock);
    auto it = failures->find(o);
    if (it == failures->end()) {
      ghobject_t os = o;
      os.hobj.oid.name = "*";
      os.hobj.set_hash(0);
      it = failures->find(os);
    }
    if (it != failures->end()) {
      auto && [when,duration] = it->second;
      if (when > 0) {
        when--;
        return false;
      }
      if (--duration <= 0) {
        failures->erase(it);
      }
      return true;
    }
    return false;
  }

  bool test_read_error0(const ghobject_t& o)
  {
    return test_error(o, &read_failures0);
  }

  bool test_read_error1(const ghobject_t& o)
  {
    return test_error(o, &read_failures1);
  }

  bool test_write_error0(const hobject_t& o,
				   const osd_reqid_t& reqid) {
    std::lock_guard<ceph::recursive_mutex> l(lock);
    ghobject_t os = ghobject_t(o, ghobject_t::NO_GEN, shard_id_t::NO_SHARD);
    if (write_failures0_reqid.count(reqid)) {
      // Matched reqid of retried write - flag for failure
      write_failures0_reqid.erase(reqid);
      return true;
    }
    auto it = write_failures0.find(os);
    if (it == write_failures0.end()) {
      os.hobj.oid.name = "*";
      os.hobj.set_hash(0);
      it = write_failures0.find(os);
    }
    if (it != write_failures0.end()) {
      auto && [when, duration] = it->second;
      auto shard = write_failures0_shard.find(os)->second;
      if (when > 0) {
        when--;
      } else {
        if (--duration <= 0) {
	  write_failures0.erase(it);
	  write_failures0_shard.erase(os);
        }
        // Error inject triggered - save reqid
        write_failures0_reqid.insert(reqid);
        // Set up error inject to drop message to primary
        write_error(ghobject_t(o, ghobject_t::NO_GEN, shard), 1, 0, 1);
      }
    }
    return false;
  }

  bool test_write_error1(const ghobject_t& o) {
    bool rc = test_error(o, &write_failures1);
    if (rc) {
      // Set up error inject to generate OSD down
      write_error(o, 2, 0, 1);
    }
    return rc;
  }

  bool test_write_error2(const hobject_t& o) {
    return test_error(
      ghobject_t(o, ghobject_t::NO_GEN, shard_id_t::NO_SHARD),
      &write_failures2);
  }

  bool test_write_error3(const hobject_t& o) {
    return test_error(
      ghobject_t(o, ghobject_t::NO_GEN, shard_id_t::NO_SHARD),
      &write_failures3);
  }

} // ECInject