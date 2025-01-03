// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRADOS_TEST_STUB_H
#define LIBRADOS_TEST_STUB_H

#include "include/rados/librados_fwd.hpp"
#include <boost/shared_ptr.hpp>

namespace neorados {
struct IOContext;
struct RADOS;
} // namespace neorados

namespace librados {

class MockTestMemIoCtxImpl;
class MockTestMemRadosClient;
class TestCluster;
class TestClassHandler;

MockTestMemIoCtxImpl &get_mock_io_ctx(IoCtx &ioctx);
MockTestMemIoCtxImpl &get_mock_io_ctx(neorados::RADOS& rados,
                                      neorados::IOContext& io_context);

MockTestMemRadosClient &get_mock_rados_client(neorados::RADOS& rados);

} // namespace librados

namespace librados_test_stub {

typedef boost::shared_ptr<librados::TestCluster> TestClusterRef;

void set_cluster(TestClusterRef cluster);
TestClusterRef get_cluster();

librados::TestClassHandler* get_class_handler();

} // namespace librados_test_stub


#endif // LIBRADOS_TEST_STUB_H
