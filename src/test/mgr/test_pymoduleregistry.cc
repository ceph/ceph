// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "TestMgr.h"
#include "mgr/PyModuleRegistry.h"
#include "mon/MgrMap.h"

TEST_F(PyModuleRegistryTestHelper, BasicCreation) {
  ASSERT_NE(registry, nullptr);
  ASSERT_NE(cct, nullptr);
  ASSERT_NE(clog, nullptr);
}

TEST_F(PyModuleRegistryTestHelper, GetModulesEmpty) {
  auto modules = registry->get_modules();
  ASSERT_TRUE(modules.empty());
}

TEST_F(PyModuleRegistryTestHelper, GetNonExistentModule) {
  auto module = registry->get_module("nonexistent_module");
  ASSERT_FALSE(module);
}

TEST_F(PyModuleRegistryTestHelper, RegisterClient) {
  entity_addrvec_t addrs;
  entity_addr_t addr;
  addr.parse("127.0.0.1:6789", nullptr);
  addrs.v.push_back(addr);

  registry->register_client("test_client", addrs, false);

  auto clients = registry->get_clients();
  ASSERT_EQ(clients.size(), 1);
  ASSERT_EQ(clients.count("test_client"), 1);
}

TEST_F(PyModuleRegistryTestHelper, RegisterClientReplace) {
  entity_addrvec_t addrs1, addrs2;
  entity_addr_t addr1, addr2;

  addr1.parse("127.0.0.1:6789", nullptr);
  addrs1.v.push_back(addr1);

  addr2.parse("127.0.0.1:6790", nullptr);
  addrs2.v.push_back(addr2);

  registry->register_client("test_client", addrs1, false);
  auto clients = registry->get_clients();
  ASSERT_EQ(clients.size(), 1);

  // clients is a multimap: a second insert with replace=false adds a second entry
  registry->register_client("test_client", addrs2, false);
  clients = registry->get_clients();
  ASSERT_EQ(clients.size(), 2); // Both should exist

  // replace=true erases all entries for the name, then inserts one
  registry->register_client("test_client", addrs2, true);
  clients = registry->get_clients();
  ASSERT_EQ(clients.size(), 1); // Only one should remain
}

TEST_F(PyModuleRegistryTestHelper, UnregisterClient) {
  entity_addrvec_t addrs;
  entity_addr_t addr;
  addr.parse("127.0.0.1:6789", nullptr);
  addrs.v.push_back(addr);

  registry->register_client("test_client", addrs, false);
  auto clients = registry->get_clients();
  ASSERT_EQ(clients.size(), 1);

  registry->unregister_client("test_client", addrs);
  clients = registry->get_clients();
  ASSERT_TRUE(clients.empty());
}

TEST_F(PyModuleRegistryTestHelper, MultipleClients) {
  entity_addrvec_t addrs1, addrs2;
  entity_addr_t addr1, addr2;

  addr1.parse("127.0.0.1:6789", nullptr);
  addrs1.v.push_back(addr1);

  addr2.parse("127.0.0.1:6790", nullptr);
  addrs2.v.push_back(addr2);

  registry->register_client("client1", addrs1, false);
  registry->register_client("client2", addrs2, false);

  auto clients = registry->get_clients();
  ASSERT_EQ(clients.size(), 2);
  ASSERT_EQ(clients.count("client1"), 1);
  ASSERT_EQ(clients.count("client2"), 1);
}

TEST_F(PyModuleRegistryTestHelper, HandleMgrMapFirstSeen) {
  // Internal epoch starts at 0; the first call takes the epoch==0 branch,
  // which sets enabled flags on modules and always returns false.
  MgrMap mgr_map;
  mgr_map.epoch = 1;

  bool needs_restart = registry->handle_mgr_map(mgr_map);
  ASSERT_FALSE(needs_restart);
}

TEST_F(PyModuleRegistryTestHelper, HandleMgrMapUpdate) {
  // Prime the registry with epoch 1 (epoch==0 branch).
  MgrMap mgr_map;
  mgr_map.epoch = 1;
  registry->handle_mgr_map(mgr_map);

  // A second call with the same module sets takes the else branch.
  // No module sets changed, so needs_restart must be false.
  MgrMap mgr_map2;
  mgr_map2.epoch = 2;
  bool needs_restart = registry->handle_mgr_map(mgr_map2);
  ASSERT_FALSE(needs_restart);
}

TEST_F(PyModuleRegistryTestHelper, StandbyModulesState) {
  ASSERT_FALSE(registry->have_standby_modules());
  ASSERT_FALSE(registry->is_standby_running());
}
