// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRADOS_TEST_STUB_H
#define LIBRADOS_TEST_STUB_H

#include <boost/shared_ptr.hpp>

namespace librados {
class IoCtx;
class TestRadosClient;
class MockTestMemIoCtxImpl;

MockTestMemIoCtxImpl &get_mock_io_ctx(IoCtx &ioctx);
}

namespace librados_test_stub {

typedef boost::shared_ptr<librados::TestRadosClient> TestRadosClientPtr;

void set_rados_client(const TestRadosClientPtr &rados_client);

TestRadosClientPtr get_rados_client();

} // namespace librados_test_stub

#endif // LIBRADOS_TEST_STUB_H
