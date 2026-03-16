// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "test/librados/test_pool_types.h"

namespace ceph {
namespace test {

// Static member definitions
librados::Rados ClsTestFixture::rados;
librados::Rados ClsTestFixtureEC::rados;

} // namespace test
} // namespace ceph
