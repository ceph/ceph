// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "TestMgr.h"
#include "mgr/PyModuleRunner.h"
#include "mgr/PyModule.h"
#include "mgr/ThreadMonitor.h"
#include "common/LogClient.h"

::testing::Environment* const python_env =
    ::testing::AddGlobalTestEnvironment(new PythonEnv);

TEST_F(PyModuleRunnerTestHelper, BasicConstruction) {
  ASSERT_NE(cct, nullptr);
  auto py_module = make_test_module("test_module");
  auto clog = std::make_shared<LogChannel>(cct.get(), nullptr, "cluster");
  ThreadMonitor monitor(cct.get());

  PyModuleRunner runner(py_module, clog, &monitor);
  EXPECT_EQ(runner.get_name(), "test_module");
  EXPECT_STREQ(runner.get_thread_name(), "test_module");
  EXPECT_EQ(runner.get_native_tid(), 0);
}

TEST_F(PyModuleRunnerTestHelper, WithoutMonitor) {
  auto py_module = make_test_module("mgr_runner");
  auto clog = std::make_shared<LogChannel>(cct.get(), nullptr, "cluster");

  PyModuleRunner runner(py_module, clog);
  EXPECT_EQ(runner.get_name(), "mgr_runner");
  EXPECT_STREQ(runner.get_thread_name(), "mgr_runner");
  EXPECT_EQ(runner.get_native_tid(), 0);
}

TEST_F(PyModuleRunnerTestHelper, SetAndGetNativeTid) {
  auto py_module = make_test_module("tid_test_module");
  auto clog = std::make_shared<LogChannel>(cct.get(), nullptr, "cluster");

  PyModuleRunner runner(py_module, clog);
  EXPECT_EQ(runner.get_native_tid(), 0);

  runner.set_native_tid(4242);
  EXPECT_EQ(runner.get_native_tid(), 4242);

  runner.set_native_tid(99999);
  EXPECT_EQ(runner.get_native_tid(), 99999);
}

TEST_F(PyModuleRunnerTestHelper, ThreadNameTruncation) {
  std::string long_name = "extremely_long_module_name_exceeding_fifteen_chars";
  auto py_module = make_test_module(long_name);
  auto clog = std::make_shared<LogChannel>(cct.get(), nullptr, "cluster");

  PyModuleRunner runner(py_module, clog);
  EXPECT_EQ(runner.get_name(), long_name);
  EXPECT_EQ(std::string(runner.get_thread_name()), long_name.substr(0, 15));
  EXPECT_LE(strlen(runner.get_thread_name()), 15u);
}

TEST_F(PyModuleRunnerTestHelper, MultipleRunners) {
  auto mod1 = make_test_module("module_one");
  auto mod2 = make_test_module("module_two");
  auto clog = std::make_shared<LogChannel>(cct.get(), nullptr, "cluster");

  PyModuleRunner runner1(mod1, clog);
  PyModuleRunner runner2(mod2, clog);

  EXPECT_EQ(runner1.get_name(), "module_one");
  EXPECT_EQ(runner2.get_name(), "module_two");
  EXPECT_STREQ(runner1.get_thread_name(), "module_one");
  EXPECT_STREQ(runner2.get_thread_name(), "module_two");

  runner1.set_native_tid(101);
  runner2.set_native_tid(202);
  EXPECT_EQ(runner1.get_native_tid(), 101);
  EXPECT_EQ(runner2.get_native_tid(), 202);
}
