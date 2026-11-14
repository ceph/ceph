// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <Python.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iterator>
#include <string_view>
#include <thread>

#include <fmt/format.h>
#include <unistd.h>

#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/perf_counters.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "gtest/gtest.h"
#include "log/Log.h"
#include "mgr/Gil.h"
#include "mgr/mgr_perf_counters.h"

using namespace std::literals;

namespace fs = std::filesystem;

static SafeThreadState main_ts;
static fs::path module_root;

// Take the GIL on a new thread and keep it for hold_for after
// signalling that it is held.
static std::thread hold_gil(std::chrono::milliseconds hold_for,
                            std::promise<void> &held)
{
  return std::thread([hold_for, &held] {
    Gil gil(main_ts, true, "holder");
    held.set_value();
    std::this_thread::sleep_for(hold_for);
  });
}

TEST(Gil, SlowAcquireIsCounted)
{
  const auto [sum_before, count_before] =
    perfcounter->get_tavg_ns(l_mgr_gil_acquire_lat);
  const auto slow_before = perfcounter->get(l_mgr_gil_slow_waits);

  std::promise<void> held;
  auto holder = hold_gil(50ms, held);
  held.get_future().wait();
  {
    Gil gil(main_ts, true, "waiter");
  }
  holder.join();

  const auto [sum_after, count_after] =
    perfcounter->get_tavg_ns(l_mgr_gil_acquire_lat);
  EXPECT_EQ(count_after - count_before, 2u);
  EXPECT_GE(sum_after - sum_before, uint64_t(25ms / 1ns));
  EXPECT_GE(perfcounter->get(l_mgr_gil_slow_waits) - slow_before, 1u);
}

TEST(Gil, SlowReacquireIsCounted)
{
  Gil gil(main_ts, true, "waiter");
  const auto [sum_before, count_before] =
    perfcounter->get_tavg_ns(l_mgr_gil_reacquire_lat);
  const auto slow_before = perfcounter->get(l_mgr_gil_slow_waits);

  std::promise<void> held;
  std::thread holder;
  {
    without_gil_t no_gil;
    holder = hold_gil(50ms, held);
    held.get_future().wait();
  }

  const auto [sum_after, count_after] =
    perfcounter->get_tavg_ns(l_mgr_gil_reacquire_lat);
  EXPECT_EQ(count_after - count_before, 1u);
  EXPECT_GE(sum_after - sum_before, uint64_t(25ms / 1ns));
  EXPECT_GE(perfcounter->get(l_mgr_gil_slow_waits) - slow_before, 1u);

  without_gil_t no_gil;
  holder.join();
}

// With the GIL held: release it while another thread takes it for 50ms,
// so the reacquire is a slow wait.
static void slow_reacquire()
{
  std::promise<void> held;
  std::thread holder;
  {
    without_gil_t no_gil;
    holder = hold_gil(50ms, held);
    held.get_future().wait();
  }
  without_gil_t no_gil;
  holder.join();
}

static PyObject *py_slow_reacquire(PyObject *, PyObject *)
{
  slow_reacquire();
  Py_RETURN_NONE;
}

// With the GIL held: call fakemod.<func>(py_slow_reacquire).
static void call_fakemod(const char *func)
{
  static PyMethodDef def = {"slow_reacquire", py_slow_reacquire,
                            METH_NOARGS, nullptr};
  PyObject *fn = PyCFunction_New(&def, nullptr);
  PyObject *mod = PyImport_ImportModule("fakemod");
  PyObject *ret = mod ? PyObject_CallMethod(mod, func, "O", fn) : nullptr;
  if (ret == nullptr) {
    PyErr_Print();
  }
  EXPECT_NE(ret, nullptr);
  EXPECT_EQ(PyErr_Occurred(), nullptr);
  Py_XDECREF(ret);
  Py_XDECREF(mod);
  Py_DECREF(fn);
}

static std::string read_log()
{
  g_ceph_context->_log->flush();
  std::ifstream in(module_root / "log");
  return {std::istreambuf_iterator<char>(in), {}};
}

// Slow waits are logged at most once per second.
static void wait_for_next_report()
{
  std::this_thread::sleep_for(1100ms);
}

// Runs fn on a new thread, so thread-local tags start out empty, and
// returns what it added to the log.
template <typename Fn>
static std::string log_from_new_thread(Fn &&fn)
{
  const auto before = read_log().size();
  std::thread(std::forward<Fn>(fn)).join();
  return read_log().substr(before);
}

static size_t count(const std::string &s, std::string_view what)
{
  size_t n = 0;
  for (auto pos = s.find(what); pos != s.npos; pos = s.find(what, pos + 1)) {
    ++n;
  }
  return n;
}

TEST(Gil, SlowWaitNamesOutermostModuleAndInnermostFrame)
{
  // fakemod.run_via_helper() -> helperpkg.call() -> C: the thread belongs
  // to fakemod, but the wait happened in helperpkg's call().
  const auto log = log_from_new_thread([] {
    wait_for_next_report();
    Gil gil(main_ts, true);
    call_fakemod("run_via_helper");
  });
  EXPECT_NE(log.find("module 'fakemod' waited"), log.npos) << log;
  EXPECT_NE(log.find("python frame: call (" +
                     (module_root / "helperpkg").string()), log.npos) << log;
}

TEST(Gil, SlowWaitSkipsFramesOutsideModules)
{
  // fakemod.run_via_wrapper() -> wrapper.call() -> C: wrapper.py is a
  // top-level file like mgr_module.py, so the frame named is fakemod's.
  const auto log = log_from_new_thread([] {
    wait_for_next_report();
    Gil gil(main_ts, true);
    call_fakemod("run_via_wrapper");
  });
  EXPECT_NE(log.find("python frame: run_via_wrapper ("), log.npos) << log;
}

TEST(Gil, ModuleTagIsRestored)
{
  // A thread tagged modb that calls into moda (like dispatch_remote)
  // goes back to modb afterwards.
  auto log = log_from_new_thread([] {
    gil_tag_thread_module("modb");
    wait_for_next_report();
    {
      Gil gil(main_ts, true, "moda");
      slow_reacquire();
    }
    wait_for_next_report();
    Gil gil(main_ts, true);
    slow_reacquire();
  });
  auto moda = log.find("module 'moda' waited");
  EXPECT_NE(moda, log.npos) << log;
  EXPECT_NE(log.find("module 'modb' waited", moda), log.npos) << log;
  // no Python code ran on that thread, so there is no frame to show
  EXPECT_EQ(log.find("python frame"), log.npos) << log;

  // An untagged thread is still attributed from its Python stack after
  // a tagged Gil goes away.
  log = log_from_new_thread([] {
    wait_for_next_report();
    {
      Gil gil(main_ts, true, "moda");
    }
    Gil gil(main_ts, true);
    call_fakemod("run");
  });
  EXPECT_NE(log.find("module 'fakemod' waited"), log.npos) << log;
}

TEST(Gil, SlowWaitLogIsRateLimited)
{
  const auto log = log_from_new_thread([] {
    Gil gil(main_ts, true, "moda");
    wait_for_next_report();
    for (int i = 0; i < 3; ++i) {
      slow_reacquire();
    }
    wait_for_next_report();
    slow_reacquire();
  });
  EXPECT_EQ(count(log, "for the GIL"), 2u) << log;
  EXPECT_NE(log.find("(2 slow waits not logged since the last report, "
                     "worst "), log.npos) << log;
  EXPECT_NE(log.find(" in 'moda')"), log.npos) << log;
}

TEST(Gil, EverySlowWaitLoggedAtDebug20)
{
  g_conf().set_val("debug_mgr", "20/20");
  g_conf().apply_changes(nullptr);
  const auto log = log_from_new_thread([] {
    Gil gil(main_ts, true, "moda");
    for (int i = 0; i < 3; ++i) {
      slow_reacquire();
    }
  });
  g_conf().set_val("debug_mgr", "5/5");
  g_conf().apply_changes(nullptr);
  EXPECT_EQ(count(log, "for the GIL"), 3u) << log;
  EXPECT_EQ(log.find("not logged"), log.npos) << log;
}

int main(int argc, char **argv)
{
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  // mgr_module_path can't change at runtime, so set it before startup
  module_root = fs::temp_directory_path() /
    fmt::format("unittest_mgr_gil.{}", getpid());
  g_conf().set_val_or_die("mgr_module_path", module_root.string());
  common_init_finish(g_ceph_context);
  mgr_perf_start(g_ceph_context);

  fs::create_directories(module_root / "fakemod");
  fs::create_directories(module_root / "helperpkg");
  std::ofstream(module_root / "fakemod" / "__init__.py")
    << "def run(f):\n"
    << "    return f()\n"
    << "def run_via_helper(f):\n"
    << "    import helperpkg\n"
    << "    return helperpkg.call(f)\n"
    << "def run_via_wrapper(f):\n"
    << "    import wrapper\n"
    << "    return wrapper.call(f)\n";
  for (const auto &file : {module_root / "helperpkg" / "__init__.py",
                           module_root / "wrapper.py"}) {
    std::ofstream(file) << "def call(f):\n"
                        << "    return f()\n";
  }
  g_conf().set_val("log_to_file", "true");
  g_conf().set_val("log_file", (module_root / "log").string());
  g_conf().set_val("debug_mgr", "5/5");
  g_conf().apply_changes(nullptr);

  Py_Initialize();
  PyObject *dir = PyUnicode_FromString(module_root.c_str());
  PyList_Insert(PySys_GetObject("path"), 0, dir);
  Py_DECREF(dir);
  main_ts.set(PyEval_SaveThread());

  ::testing::InitGoogleTest(&argc, argv);
  const int r = RUN_ALL_TESTS();
  fs::remove_all(module_root);
  return r;
}
