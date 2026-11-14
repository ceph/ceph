// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <Python.h>

#include <vector>

#include "gtest/gtest.h"

#include "global/global_init.h"
#include "global/global_context.h"
#include "common/ceph_argparse.h"

#include "mgr/PyModule.h"

// ~PyModule() acquires the GIL through Gil, which logs via g_ceph_context, so a
// CephContext must exist for the lifetime of the suite.
class PyModuleTest : public ::testing::Test {
public:
  static void SetUpTestSuite() {
    if (!cct) {
      std::vector<const char*> args = {"unittest_mgr_pymodule"};
      cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                        CODE_ENVIRONMENT_UTILITY,
                        CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
      common_init_finish(cct.get());
    }
  }

protected:
  static inline boost::intrusive_ptr<CephContext> cct;
};

// Initialise and tear down the embedded Python interpreter around the suite.
struct PythonEnv : public ::testing::Environment {
  void SetUp() override { Py_Initialize(); }
  void TearDown() override { Py_Finalize(); }
};

// AddGlobalTestEnvironment must run before RUN_ALL_TESTS(); a file-scope pointer
// is the standard way to ensure this happens during static initialisation.
::testing::Environment* const python_env =
    ::testing::AddGlobalTestEnvironment(new PythonEnv);

// Smoke test confirming the scaffolding links PyModule against its full mgr
// object closure and brings up the embedded Python interpreter for the suite.
TEST_F(PyModuleTest, InterpreterInitialised) {
  EXPECT_TRUE(Py_IsInitialized());
}

TEST_F(PyModuleTest, DestructorDoesNotEndMainInterpreter) {
  ASSERT_TRUE(Py_IsInitialized());

  PyThreadState *main_ts = PyThreadState_Get();
  ASSERT_NE(main_ts, nullptr);

  auto *mod = new PyModule("test_main_interpreter_module");
  mod->use_main_interpreter = true;
  mod->pMyThreadState.set(main_ts);

  PyThreadState *saved = PyEval_SaveThread();
  delete mod;
  PyEval_RestoreThread(saved);

  EXPECT_TRUE(Py_IsInitialized())
      << "Current: Py_EndInterpreter called on main interpreter thread state "
         "(process crash); "
         "Expected: Py_EndInterpreter not called on main interpreter";

  EXPECT_EQ(PyRun_SimpleString("1 + 1"), 0)
      << "Current: Py_EndInterpreter called on main interpreter thread state "
         "(process crash); "
         "Expected: Py_EndInterpreter not called on main interpreter";
}

TEST_F(PyModuleTest, DestructorTearsDownSubInterpreterWithoutCrash) {
  ASSERT_TRUE(Py_IsInitialized());

  PyThreadState *main_ts = PyThreadState_Get();
  ASSERT_NE(main_ts, nullptr);

  PyThreadState *sub_ts = Py_NewInterpreter();
  ASSERT_NE(sub_ts, nullptr)
      << "Py_NewInterpreter() failed; sub-interpreter unavailable";

  PyThreadState_Swap(main_ts);

  auto *mod = new PyModule("test_sub_interpreter_module");
  mod->use_main_interpreter = false;
  mod->pMyThreadState.set(sub_ts);

  PyEval_SaveThread();
  delete mod;
  // Py_EndInterpreter() released GIL; restore main interpreter thread state.
  PyEval_RestoreThread(main_ts);

  EXPECT_TRUE(Py_IsInitialized())
      << "Current: Py_EndInterpreter called while wrong thread state is current "
         "-> fatal Python error; "
         "Expected: sub-interpreter torn down cleanly, main interpreter intact";

  EXPECT_EQ(PyRun_SimpleString("1 + 1"), 0)
      << "Current: Py_EndInterpreter called while wrong thread state is current "
         "-> fatal Python error; "
         "Expected: sub-interpreter torn down cleanly, main interpreter intact";
}
