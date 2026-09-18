// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "TestMgr.h"
#include "mgr/PyModuleRunner.h"
#include "mgr/PyModule.h"
#include "common/LogClient.h"

// AddGlobalTestEnvironment must be called before RUN_ALL_TESTS(); a file-scope
// pointer is the standard way to ensure this runs during static initialisation.
::testing::Environment* const python_env =
    ::testing::AddGlobalTestEnvironment(new PythonEnv);

TEST_F(TestMgr, PyModuleRunnerCephContextInitialized) {
  ASSERT_NE(cct, nullptr);
  EXPECT_NE(cct.get(), nullptr);
}

TEST_F(TestMgr, PyModuleRunner_PyModuleCreation) {
  auto py_module = std::make_shared<PyModule>("test_module");
  ASSERT_NE(py_module, nullptr);
  EXPECT_EQ(py_module->get_name(), "test_module");
}

TEST_F(TestMgr, PyModuleRunner_PyModuleNames) {
  auto py_module1 = std::make_shared<PyModule>("module1");
  auto py_module2 = std::make_shared<PyModule>("module2");
  auto py_module3 = std::make_shared<PyModule>("very_long_module_name");

  EXPECT_EQ(py_module1->get_name(), "module1");
  EXPECT_EQ(py_module2->get_name(), "module2");
  EXPECT_EQ(py_module3->get_name(), "very_long_module_name");
}

TEST_F(TestMgr, PyModuleRunner_PyModuleEmptyName) {
  auto py_module = std::make_shared<PyModule>("");
  ASSERT_NE(py_module, nullptr);
  EXPECT_EQ(py_module->get_name(), "");
}

TEST_F(TestMgr, PyModuleRunner_PyModuleSpecialChars) {
  auto py_module = std::make_shared<PyModule>("test-module_123");
  ASSERT_NE(py_module, nullptr);
  EXPECT_EQ(py_module->get_name(), "test-module_123");
}

TEST_F(TestMgr, PyModuleRunner_LogChannelCreation) {
  auto clog = std::make_shared<LogChannel>(cct.get(), nullptr, "cluster");
  ASSERT_NE(clog, nullptr);
  EXPECT_NE(clog.get(), nullptr);
}

TEST_F(TestMgr, PyModuleRunner_ThreadMonitorCreation) {
  auto thread_monitor = std::make_unique<ThreadMonitor>(cct.get());
  ASSERT_NE(thread_monitor, nullptr);
  EXPECT_NE(thread_monitor.get(), nullptr);
}

TEST_F(TestMgr, PyModuleRunner_MultipleLogChannels) {
  auto clog1 = std::make_shared<LogChannel>(cct.get(), nullptr, "cluster");
  auto clog2 = std::make_shared<LogChannel>(cct.get(), nullptr, "audit");

  ASSERT_NE(clog1, nullptr);
  ASSERT_NE(clog2, nullptr);
  EXPECT_NE(clog1.get(), clog2.get());
}

TEST_F(TestMgr, PyModuleRunner_PyModuleRefCounting) {
  auto py_module = std::make_shared<PyModule>("test_module");
  EXPECT_EQ(py_module.use_count(), 1);

  auto py_module_copy = py_module;
  EXPECT_EQ(py_module.use_count(), 2);
  EXPECT_EQ(py_module_copy.use_count(), 2);
}

TEST_F(TestMgr, PyModuleRunner_CephContextShared) {
  ASSERT_NE(cct, nullptr);
  EXPECT_NE(cct.get(), nullptr);
}
