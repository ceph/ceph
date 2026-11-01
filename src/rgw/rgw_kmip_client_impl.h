// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <boost/intrusive/list.hpp>
#include <memory>
#include <vector>
#include "common/ceph_mutex.h"

struct RGWKmipWorker;
class RGWKMIPManagerImpl: public RGWKMIPManager {
protected:
  ceph::mutex lock = ceph::make_mutex("RGWKMIPManager");
  ceph::condition_variable cond;

  struct Request : boost::intrusive::list_base_hook<> {
    boost::intrusive::list_member_hook<> req_hook;
    RGWKMIPTransceiver &details;
    Request(RGWKMIPTransceiver &details) : details(details) {}
  };
  boost::intrusive::list<Request, boost::intrusive::member_hook< Request,
  boost::intrusive::list_member_hook<>, &Request::req_hook>> requests;
  bool going_down = false;
  std::vector<std::unique_ptr<RGWKmipWorker>> workers;
public:
  RGWKMIPManagerImpl(CephContext *cct);
  ~RGWKMIPManagerImpl() override;
  int add_request(RGWKMIPTransceiver *);
  int start();
  void stop();
  friend RGWKmipWorker;
};
