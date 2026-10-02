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

// ~PyModule() for a sub-interpreter instance (use_main_interpreter == false)
// must call Py_EndInterpreter() with the sub-interpreter's thread state as the
// *current* thread state.  The previous code used Gil(pMyThreadState, true)
// which restores a *new* temporary thread state (not pMyThreadState.ts) as
// current, so Py_EndInterpreter() would crash with "thread is not current".
// The corrected teardown uses PyEval_RestoreThread(pMyThreadState.ts) first so
// that pMyThreadState.ts is current when Py_EndInterpreter() runs.
// See https://tracker.ceph.com/issues/81339.
TEST_F(PyModuleTest, DestructorTearsDownSubInterpreterWithoutCrash) {
  ASSERT_TRUE(Py_IsInitialized());

  // Capture the main interpreter's thread state before creating a sub-interp.
  PyThreadState *main_ts = PyThreadState_Get();
  ASSERT_NE(main_ts, nullptr);

  // Create a real sub-interpreter, mirroring PyModule::load()'s sub-interp path.
  // Py_NewInterpreter() requires and holds the GIL, and makes sub_ts current.
  PyThreadState *sub_ts = Py_NewInterpreter();
  ASSERT_NE(sub_ts, nullptr)
      << "Py_NewInterpreter() failed; sub-interpreter unavailable";

  // Swap back to the main interpreter's thread state so the test body runs in a
  // known-good context and so that ~PyModule() must acquire the GIL itself.
  PyThreadState_Swap(main_ts);

  auto *mod = new PyModule("test_sub_interpreter_module");
  mod->use_main_interpreter = false;
  mod->pMyThreadState.set(sub_ts);

  // Release the GIL.  ~PyModule() must re-acquire it, swap in sub_ts as
  // current, DECREF Python objects, and call Py_EndInterpreter(sub_ts).
  // On the unfixed code (Gil(pMyThreadState,true) + Py_EndInterpreter) this
  // crashes with "Fatal Python error: thread is not current" (exit 134/SIGABRT).
  PyEval_SaveThread();
  delete mod;
  // After Py_EndInterpreter() the GIL has been released; re-acquire it on the
  // main interpreter's thread state so the test framework can continue.
  PyEval_RestoreThread(main_ts);

  // If we reach this point the sub-interpreter was torn down without a crash.
  EXPECT_TRUE(Py_IsInitialized())
      << "Current: Py_EndInterpreter called while wrong thread state is current "
         "-> fatal Python error; "
         "Expected: sub-interpreter torn down cleanly, main interpreter intact";

  // The main interpreter must still be functional after the sub-interpreter
  // was destroyed.
  EXPECT_EQ(PyRun_SimpleString("1 + 1"), 0)
      << "Current: Py_EndInterpreter called while wrong thread state is current "
         "-> fatal Python error; "
         "Expected: sub-interpreter torn down cleanly, main interpreter intact";
}
