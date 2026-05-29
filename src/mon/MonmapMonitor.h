// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

/*
 * The Monmap Monitor is used to track the monitors in the cluster.
 */

#ifndef CEPH_MONMAPMONITOR_H
#define CEPH_MONMAPMONITOR_H

#include <iosfwd>
#include <set>
#include <string>

#include "include/buffer_fwd.h"

#include "PaxosService.h"
#include "MonMap.h"
#include "MonitorDBStore.h"
#include "mon/mon_types.h"

struct Subscription;

class MonmapMonitor : public PaxosService {
 public:
  MonmapMonitor(Monitor &mn, Paxos &p, const std::string& service_name)
    : PaxosService(mn, p, service_name)
  {
  }
  MonMap pending_map; //the pending map awaiting passage

  void create_initial() override;

  void update_from_paxos(bool *need_bootstrap) override;

  void create_pending() override;

  void encode_pending(MonitorDBStore::TransactionRef t) override;
  // we always encode the full map; we have no use for full versions
  void encode_full(MonitorDBStore::TransactionRef t) override { }

  void on_active() override;
  void apply_mon_features(const mon_feature_t& features,
			  ceph_release_t min_mon_release);

  void dump_info(ceph::Formatter *f);

  bool preprocess_query(MonOpRequestRef op) override;
  bool prepare_update(MonOpRequestRef op) override;

  bool preprocess_join(MonOpRequestRef op);
  bool prepare_join(MonOpRequestRef op);

  bool preprocess_command(MonOpRequestRef op);
  bool prepare_command(MonOpRequestRef op);

  int get_monmap(ceph::buffer::list &bl);

  /*
   * Since monitors are pretty
   * important, this implementation will just write 0.0.
   */
  bool should_propose(double& delay) override;

  void check_sub(Subscription *sub);

  void tick() override;

private:
  void check_subs();
  ceph::buffer::list monmap_bl;
  /**
   * Check validity of inputs and monitor state to
   * engage stretch mode. Designed to be used with
   * OSDMonitor::try_enable_stretch_mode() where we call both twice,
   * first with commit=false to validate.
   * @param ss: a stringstream to write errors into
   * @param okay: Filled to true if okay, false if validation fails
   * @param errcode: filled with -errno if there's a problem
   * @param commit: true if we should commit the change, false if just testing
   * @param tiebreaker_mon: the name of the monitor to declare tiebreaker (empty for auto-select)
   * @param dividing_bucket: the bucket type (eg 'dc') that divides the cluster
   * @param crush: the pending CrushWrapper for validating monitor locations against subtrees
   * @param set_global_stretch_mode: if true, set global_stretch_mode_enabled to true when committing
   *
   * Note: CRUSH bucket type and subtree count validation is performed by
   * OSDMonitor::try_enable_stretch_mode() to avoid redundancy.
   */

public:
  void try_enable_stretch_mode(std::stringstream& ss, bool *okay,
			       int *errcode, bool commit,
			       std::string tiebreaker_mon,
			       const std::string& dividing_bucket,
			       const CrushWrapper& crush,
             bool set_global_stretch_mode);

  /**
   * Static helper for validating and enabling stretch mode on a MonMap.
   * Extracted for testability - can be called from unit tests.
   * Parameters same as try_enable_stretch_mode above.
   * @param monmap: current committed monmap to validate against
   * @param pending_map: pending monmap to modify when commit=true
   */
  static void validate_and_enable_stretch_mode(
			       const MonMap& monmap,
			       MonMap& pending_map,
			       std::stringstream& ss, bool *okay,
			       int *errcode, bool commit,
			       std::string tiebreaker_mon,
			       const std::string& dividing_bucket,
			       const CrushWrapper& crush,
             bool set_global_stretch_mode);

public:
  /**
   * Set us to degraded stretch mode. Put the dead_mons in
   * the MonMap.
   */
  void trigger_degraded_stretch_mode(const std::set<std::string>& dead_mons);
  /**
   * Set us to healthy stretch mode: clear out the
   * down list to allow any non-tiebreaker mon to be the leader again.
   */
  void trigger_healthy_stretch_mode();
  /**
   * Ensure CONNECTIVITY election strategy is enabled (static version for testing).
   * @param pending_map: MonMap to potentially modify
   * @param mon_features: Monitor features to check for CONNECTIVITY support
   * @param ss: stringstream for error messages
   * @return true if changed pending_map, false otherwise
   */
  static bool ensure_connectivity_strategy(MonMap& pending_map,
                                           const mon_feature_t& mon_features,
                                           std::stringstream& ss);

  /**
   * Ensure CONNECTIVITY election strategy is enabled (instance version).
   * Called by OSDMonitor when per-pool stretch mode is needed (num_zones > 1).
   * @param ss: stringstream for error messages
   * @return true if changed pending_map (needs proposal), false otherwise
   */
  bool ensure_connectivity_strategy(std::stringstream& ss);

  /**
   * Clear per-pool stretch mode state from monmap.
   * Called by OSDMonitor when the last per-pool stretch mode pool is removed.
   * Only clears monmap state; OSDMonitor handles its own state cleanup.
   */
  void clear_stretch_mode_state();
};


#endif
