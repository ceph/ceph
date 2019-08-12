// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRADOS_TEST_STUB_H
#define LIBRADOS_TEST_STUB_H

#include "include/rados/librados_fwd.hpp"
#include <boost/shared_ptr.hpp>

namespace librados {

class MockTestMemIoCtxImpl;
class TestCluster;

MockTestMemIoCtxImpl &get_mock_io_ctx(IoCtx &ioctx);

} // namespace librados

namespace librados_test_stub {

typedef boost::shared_ptr<librados::TestCluster> TestClusterRef;

void set_cluster(TestClusterRef cluster);
TestClusterRef get_cluster();

} // namespace librados_test_stub


#endif // LIBRADOS_TEST_STUB_H
