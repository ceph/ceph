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

class MockLRemMemIoCtxImpl;
class MockLRemMemRadosClient;
class LRemCluster;
class LRemClassHandler;

MockLRemMemIoCtxImpl &get_mock_io_ctx(IoCtx &ioctx);
MockLRemMemIoCtxImpl &get_mock_io_ctx(neorados::RADOS& rados,
                                      neorados::IOContext& io_context);

MockLRemMemRadosClient &get_mock_rados_client(neorados::RADOS& rados);

} // namespace librados

namespace librados_stub {

typedef boost::shared_ptr<librados::LRemCluster> LRemClusterRef;

void set_cluster(LRemClusterRef cluster);
LRemClusterRef get_cluster();

librados::LRemClassHandler* get_class_handler();

} // namespace librados_stub

