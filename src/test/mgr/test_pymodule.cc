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

// A normal mgr module runs in the shared main interpreter
// (use_main_interpreter == true) and, per PyModule::load(), reuses the main
// interpreter's thread state rather than creating a sub-interpreter.
// ~PyModule() must therefore NOT call Py_EndInterpreter() on that shared state:
// doing so is a Python fatal error that crashes the ceph-mgr process.  Only
// sub-interpreter modules (use_main_interpreter == false) own a thread state
// created by Py_NewInterpreter() that must be torn down.
// See https://tracker.ceph.com/issues/81339.
TEST_F(PyModuleTest, DestructorDoesNotEndMainInterpreter) {
  ASSERT_TRUE(Py_IsInitialized());

  // Mirror the main-interpreter path of PyModule::load(): pMyThreadState points
  // at the main interpreter's thread state and use_main_interpreter is true.
  PyThreadState *main_ts = PyThreadState_Get();
  ASSERT_NE(main_ts, nullptr);

  auto *mod = new PyModule("test_main_interpreter_module");
  mod->use_main_interpreter = true;
  mod->pMyThreadState.set(main_ts);

  // Release the GIL so that ~PyModule()'s Gil can re-acquire it, then destroy
  // the module.  On the unfixed code the inverted guard calls
  // Py_EndInterpreter() on the main interpreter's thread state here, which
  // aborts the process.
  PyThreadState *saved = PyEval_SaveThread();
  delete mod;
  PyEval_RestoreThread(saved);

  // If we reach this point the main interpreter survived the destructor.
  EXPECT_TRUE(Py_IsInitialized())
      << "Current: Py_EndInterpreter called on main interpreter thread state "
         "(process crash); "
         "Expected: Py_EndInterpreter not called on main interpreter";

  // The main interpreter must still be functional.
  EXPECT_EQ(PyRun_SimpleString("1 + 1"), 0)
      << "Current: Py_EndInterpreter called on main interpreter thread state "
         "(process crash); "
         "Expected: Py_EndInterpreter not called on main interpreter";
}
