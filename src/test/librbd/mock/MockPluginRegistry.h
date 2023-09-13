// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_PLUGIN_REGISTRY_H
#define CEPH_TEST_LIBRBD_MOCK_PLUGIN_REGISTRY_H

#include <gmock/gmock.h>

class Context;

namespace librbd {

struct MockPluginRegistry{
  MOCK_METHOD2(init, void(const std::string&, Context*));
  MOCK_METHOD1(acquired_exclusive_lock, void(Context*));
  MOCK_METHOD1(prerelease_exclusive_lock, void(Context*));
};

} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_PLUGIN_REGISTRY_H
