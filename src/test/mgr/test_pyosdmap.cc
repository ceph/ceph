// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "TestMgr.h"
#include "mgr/PyOSDMap.h"
#include "osd/OSDMap.h"
#include "crush/CrushWrapper.h"

::testing::Environment* const python_env =
    ::testing::AddGlobalTestEnvironment(new PythonEnv);

class PyOSDMapWrapperTest : public ::testing::Test {
public:
  static void SetUpTestSuite() {
    BasePyOSDMapType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&BasePyOSDMapType) < 0) {
      FAIL() << "BasePyOSDMapType initialization failed";
    }
    BasePyOSDMapIncrementalType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&BasePyOSDMapIncrementalType) < 0) {
      FAIL() << "BasePyOSDMapIncrementalType initialization failed";
    }
    BasePyCRUSHType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&BasePyCRUSHType) < 0) {
      FAIL() << "BasePyCRUSHType initialization failed";
    }
  }
};

TEST_F(PyOSDMapWrapperTest, BasePyOSDMapEpochAndCrushVersion) {
  OSDMap *osdmap = new OSDMap();
  osdmap->set_epoch(42);

  PyObject *capsule = PyCapsule_New(osdmap, nullptr, nullptr);
  ASSERT_NE(capsule, nullptr);
  PyObject *args = PyTuple_Pack(1, capsule);
  Py_DECREF(capsule);
  ASSERT_NE(args, nullptr);

  PyObject *py_osdmap = PyObject_CallObject((PyObject*)&BasePyOSDMapType, args);
  Py_DECREF(args);
  ASSERT_NE(py_osdmap, nullptr);

  PyObject *epoch_obj = PyObject_CallMethod(py_osdmap, "_get_epoch", nullptr);
  ASSERT_NE(epoch_obj, nullptr);
  EXPECT_TRUE(PyLong_Check(epoch_obj));
  EXPECT_EQ(PyLong_AsLong(epoch_obj), 42);
  Py_DECREF(epoch_obj);

  PyObject *crush_ver_obj = PyObject_CallMethod(py_osdmap, "_get_crush_version", nullptr);
  ASSERT_NE(crush_ver_obj, nullptr);
  EXPECT_TRUE(PyLong_Check(crush_ver_obj));
  EXPECT_EQ(PyLong_AsLong(crush_ver_obj), osdmap->get_crush_version());
  Py_DECREF(crush_ver_obj);

  PyObject *dump_obj = PyObject_CallMethod(py_osdmap, "_dump", nullptr);
  ASSERT_NE(dump_obj, nullptr);
  EXPECT_TRUE(PyDict_Check(dump_obj));
  Py_DECREF(dump_obj);

  Py_DECREF(py_osdmap);
}

TEST_F(PyOSDMapWrapperTest, BasePyOSDMapIncrementalEpochAndDump) {
  OSDMap::Incremental *inc = new OSDMap::Incremental(10);
  inc->epoch = 10;

  PyObject *capsule = PyCapsule_New(inc, nullptr, nullptr);
  ASSERT_NE(capsule, nullptr);
  PyObject *args = PyTuple_Pack(1, capsule);
  Py_DECREF(capsule);
  ASSERT_NE(args, nullptr);

  PyObject *py_inc = PyObject_CallObject((PyObject*)&BasePyOSDMapIncrementalType, args);
  Py_DECREF(args);
  ASSERT_NE(py_inc, nullptr);

  PyObject *epoch_obj = PyObject_CallMethod(py_inc, "_get_epoch", nullptr);
  ASSERT_NE(epoch_obj, nullptr);
  EXPECT_TRUE(PyLong_Check(epoch_obj));
  EXPECT_EQ(PyLong_AsLong(epoch_obj), 10);
  Py_DECREF(epoch_obj);

  PyObject *dump_obj = PyObject_CallMethod(py_inc, "_dump", nullptr);
  ASSERT_NE(dump_obj, nullptr);
  EXPECT_TRUE(PyDict_Check(dump_obj));
  Py_DECREF(dump_obj);

  Py_DECREF(py_inc);
}

TEST_F(PyOSDMapWrapperTest, BasePyCRUSHDumpAndRoots) {
  auto crush = std::make_shared<CrushWrapper>();
  crush->create();

  PyObject *capsule = PyCapsule_New(&crush, nullptr, nullptr);
  ASSERT_NE(capsule, nullptr);
  PyObject *args = PyTuple_Pack(1, capsule);
  Py_DECREF(capsule);
  ASSERT_NE(args, nullptr);

  PyObject *py_crush = PyObject_CallObject((PyObject*)&BasePyCRUSHType, args);
  Py_DECREF(args);
  ASSERT_NE(py_crush, nullptr);

  PyObject *dump_obj = PyObject_CallMethod(py_crush, "_dump", nullptr);
  ASSERT_NE(dump_obj, nullptr);
  EXPECT_TRUE(PyDict_Check(dump_obj));
  Py_DECREF(dump_obj);

  PyObject *roots_obj = PyObject_CallMethod(py_crush, "_find_roots", nullptr);
  ASSERT_NE(roots_obj, nullptr);
  EXPECT_TRUE(PyDict_Check(roots_obj));
  Py_DECREF(roots_obj);

  PyObject *takes_obj = PyObject_CallMethod(py_crush, "_find_takes", nullptr);
  ASSERT_NE(takes_obj, nullptr);
  EXPECT_TRUE(PyDict_Check(takes_obj));
  Py_DECREF(takes_obj);

  Py_DECREF(py_crush);
}

TEST_F(PyOSDMapWrapperTest, BasePyCRUSHItemNameAndWeight) {
  auto crush = std::make_shared<CrushWrapper>();
  crush->create();
  int item_id = 0;
  crush->set_item_name(item_id, "osd.0");

  PyObject *capsule = PyCapsule_New(&crush, nullptr, nullptr);
  ASSERT_NE(capsule, nullptr);
  PyObject *args = PyTuple_Pack(1, capsule);
  Py_DECREF(capsule);
  ASSERT_NE(args, nullptr);

  PyObject *py_crush = PyObject_CallObject((PyObject*)&BasePyCRUSHType, args);
  Py_DECREF(args);
  ASSERT_NE(py_crush, nullptr);

  PyObject *name_obj = PyObject_CallMethod(py_crush, "_get_item_name", "i", item_id);
  ASSERT_NE(name_obj, nullptr);
  EXPECT_TRUE(PyUnicode_Check(name_obj));
  const char *name_str = PyUnicode_AsUTF8(name_obj);
  ASSERT_NE(name_str, nullptr);
  EXPECT_STREQ(name_str, "osd.0");
  Py_DECREF(name_obj);

  PyObject *nonexistent_name = PyObject_CallMethod(py_crush, "_get_item_name", "i", 9999);
  ASSERT_NE(nonexistent_name, nullptr);
  EXPECT_EQ(nonexistent_name, Py_None);
  Py_DECREF(nonexistent_name);

  Py_DECREF(py_crush);
}
