// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_PERIOD_PULLER_H
#define CEPH_RGW_PERIOD_PULLER_H

#include "rgw_period_history.h"

class RGWRados;
class RGWPeriod;

class RGWPeriodPuller : public RGWPeriodHistory::Puller {
  RGWRados *const store;
 public:
  RGWPeriodPuller(RGWRados* store) : store(store) {}

  int pull(const std::string& period_id, RGWPeriod& period) override;
};

#endif // CEPH_RGW_PERIOD_PULLER_H
