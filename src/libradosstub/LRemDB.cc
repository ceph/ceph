// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LibradosLRemStub.h"
#include "mod/db/LRemDBCluster.h"

namespace librados_stub {

static LRemClusterRef& cluster() {
  static LRemClusterRef s_cluster;
  return s_cluster;
}

LRemClusterRef get_cluster() {
  auto &cluster_ref = cluster();
  if (cluster_ref.get() == nullptr) {
    cluster_ref.reset(new librados::LRemDBCluster(g_ceph_context));
  }
  return cluster_ref;
}

}
