// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/rados/librados_fwd.hpp"
#include <boost/shared_ptr.hpp>

namespace neorados {
struct IOContext;
struct RADOS;
} // namespace neorados

namespace librados {

class LRemCluster;
class LRemClassHandler;

} // namespace librados

namespace librados_stub {

typedef boost::shared_ptr<librados::LRemCluster> LRemClusterRef;

void set_cluster(LRemClusterRef cluster);
LRemClusterRef get_cluster();

librados::LRemClassHandler* get_class_handler();

} // namespace librados_stub

