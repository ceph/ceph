// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Tests for PyFormatterRO (read-only-on-the-fly builder):
 *  - lists -> tuples, sets -> frozensets
 *  - dicts remain dicts (JSON-friendly)
 *  - works with nested structures
 *  - can be json.dumps()-ed safely (for structures without sets)
 */

#include "gtest/gtest.h"
#include "mgr/PyFormatter.h"     // PyFormatterRO
#include <Python.h>

using namespace ceph;

class PyFormatterROTestHelper : public PyFormatterRO {
public:
  using PyFormatterRO::PyFormatterRO;
  
  // Expose the protected method for testing
  void test_dump_pyobject(std::string_view name, PyObject *p) {
    dump_pyobject(name, p);
  }
};

struct PyRuntime : public ::testing::Test {
  static void SetUpTestSuite() {
    if (!Py_IsInitialized()) {
      Py_Initialize();
    }
  }
  static void TearDownTestSuite() {
    if (Py_IsInitialized()) {
      Py_Finalize();
    }
  }
  void SetUp() override {
    gstate = PyGILState_Ensure();
    ASSERT_TRUE(Py_IsInitialized());
  }
  void TearDown() override {
    PyGILState_Release(gstate);
  }
private:
  PyGILState_STATE gstate{};
};

TEST_F(PyRuntime, PyFormatterRO_FreezesContainers) {
  PyFormatterROTestHelper ro; // default root is a dict

  // Build values
  PyObject* src_list = PyList_New(0);
  PyList_Append(src_list, PyLong_FromLong(1));
  PyList_Append(src_list, PyLong_FromLong(2));

  PyObject* src_set = PySet_New(nullptr);
  PySet_Add(src_set, PyLong_FromLong(7));
  PySet_Add(src_set, PyLong_FromLong(8));

  PyObject* src_dict = PyDict_New();
  PyDict_SetItemString(src_dict, "k", PyLong_FromLong(42));

  // Insert (RO conversion happens on insert)
  ro.test_dump_pyobject("alist", src_list);
  ro.test_dump_pyobject("aset",  src_set);
  ro.test_dump_pyobject("adict", src_dict);

  // We own the temps
  Py_DECREF(src_list);
  Py_DECREF(src_set);
  Py_DECREF(src_dict);

  PyObject* root = ro.get();  // new ref
  ASSERT_TRUE(PyDict_Check(root));

  // alist -> tuple
  {
    PyObject* alist = PyDict_GetItemString(root, "alist"); // borrowed
    ASSERT_NE(alist, nullptr);
    EXPECT_TRUE(PyTuple_Check(alist));
    EXPECT_EQ(PyTuple_Size(alist), 2);
  }

  // aset -> frozenset
  {
    PyObject* aset = PyDict_GetItemString(root, "aset"); // borrowed
    ASSERT_NE(aset, nullptr);
    EXPECT_TRUE(PyFrozenSet_Check(aset));
    EXPECT_EQ(PySet_Size(aset), 2);
  }

  // adict stays dict
  {
    PyObject* adict = PyDict_GetItemString(root, "adict"); // borrowed
    ASSERT_NE(adict, nullptr);
    EXPECT_TRUE(PyDict_Check(adict));
    EXPECT_EQ(PyDict_Size(adict), 1);
  }

  Py_DECREF(root);
}

TEST_F(PyRuntime, PyFormatterRO_NestedFreezingAndJsonDump) {
  PyFormatterROTestHelper ro;

  // Build: outer = [ {"k1": 1, "k2": 2}, [3, 4] ]
  PyObject* first_dict = PyDict_New();
  PyDict_SetItemString(first_dict, "k1", PyLong_FromLong(1));
  PyDict_SetItemString(first_dict, "k2", PyLong_FromLong(2));

  PyObject* second_list = PyList_New(0);
  PyList_Append(second_list, PyLong_FromLong(3));
  PyList_Append(second_list, PyLong_FromLong(4));

  PyObject* outer_list = PyList_New(0);
  PyList_Append(outer_list, first_dict);
  PyList_Append(outer_list, second_list);

  // Temps no longer needed
  Py_DECREF(first_dict);
  Py_DECREF(second_list);

  // Insert under key "outer" (list will be frozen to tuple)
  ro.test_dump_pyobject("outer", outer_list);
  Py_DECREF(outer_list);

  PyObject* root = ro.get(); // new ref
  ASSERT_TRUE(PyDict_Check(root));

  PyObject* outer = PyDict_GetItemString(root, "outer");
  ASSERT_NE(outer, nullptr);
  ASSERT_TRUE(PyTuple_Check(outer));
  ASSERT_EQ(PyTuple_Size(outer), 2);

  PyObject* outer0 = PyTuple_GetItem(outer, 0);
  PyObject* outer1 = PyTuple_GetItem(outer, 1);

  // Element 0 remains a dict
  ASSERT_TRUE(PyDict_Check(outer0));
  EXPECT_EQ(PyDict_Size(outer0), 2);

  // Element 1 is the forzened list -> tuple
  ASSERT_TRUE(PyTuple_Check(outer1));
  ASSERT_EQ(PyTuple_Size(outer1), 2);
  EXPECT_TRUE(PyLong_Check(PyTuple_GetItem(outer1, 0)));
  EXPECT_TRUE(PyLong_Check(PyTuple_GetItem(outer1, 1)));

  // json.dumps should work (dict + tuple are serializable; no sets here)
  PyObject* json_mod = PyImport_ImportModule("json");
  ASSERT_NE(json_mod, nullptr);
  PyObject* dumps = PyObject_GetAttrString(json_mod, "dumps");
  ASSERT_NE(dumps, nullptr);

  PyObject* args = PyTuple_Pack(1, root);
  PyObject* s = PyObject_CallObject(dumps, args);
  Py_DECREF(args);
  Py_DECREF(dumps);
  Py_DECREF(json_mod);

  ASSERT_NE(s, nullptr);
  EXPECT_TRUE(PyUnicode_Check(s));
  Py_DECREF(s);

  Py_DECREF(root);
}

TEST_F(PyRuntime, PyFormatterRO_ReadonlyBehavior) {
  PyFormatterROTestHelper ro;

  // Insert three things
  PyObject* src_list = PyList_New(0);
  PyList_Append(src_list, PyLong_FromLong(1));
  PyList_Append(src_list, PyLong_FromLong(2));

  PyObject* src_set = PySet_New(nullptr);
  PySet_Add(src_set, PyLong_FromLong(7));
  PySet_Add(src_set, PyLong_FromLong(8));

  PyObject* src_dict = PyDict_New();
  PyDict_SetItemString(src_dict, "k", PyLong_FromLong(42));

  ro.test_dump_pyobject("alist", src_list);
  ro.test_dump_pyobject("aset",  src_set);
  ro.test_dump_pyobject("adict", src_dict);

  Py_DECREF(src_list);
  Py_DECREF(src_set);
  Py_DECREF(src_dict);

  PyObject* root = ro.get(); // new ref
  ASSERT_TRUE(PyDict_Check(root));

  // alist -> tuple; mutation must fail
  {
    PyObject* alist = PyDict_GetItemString(root, "alist"); // borrowed
    ASSERT_NE(alist, nullptr);
    ASSERT_TRUE(PyTuple_Check(alist));

    PyObject* ninety_nine = PyLong_FromLong(99);
    int rc = PySequence_SetItem(alist, 0, ninety_nine);
    Py_DECREF(ninety_nine);
    //print the rc and error
    EXPECT_EQ(rc, -1);
    EXPECT_TRUE(PyErr_Occurred());

    PyErr_Clear();
  }

  // aset -> frozenset; adding must fail (no 'add' method)
  {
    PyObject* aset = PyDict_GetItemString(root, "aset"); // borrowed
    ASSERT_NE(aset, nullptr);
    ASSERT_TRUE(PyFrozenSet_Check(aset));

    PyObject* nine = PyLong_FromLong(9);
    PyObject* res = PyObject_CallMethod(aset, "add", "O", nine);
    Py_DECREF(nine);
    EXPECT_EQ(res, nullptr);      // no method on frozenset
    EXPECT_TRUE(PyErr_Occurred()); // AttributeError
    PyErr_Clear();
  }

  // adict stays dict (mutable) — allowed
  {
    PyObject* adict = PyDict_GetItemString(root, "adict"); // borrowed
    ASSERT_NE(adict, nullptr);
    ASSERT_TRUE(PyDict_Check(adict));

    int rc = PyDict_SetItemString(adict, "k2", PyLong_FromLong(5));
    EXPECT_EQ(rc, 0);
    EXPECT_EQ(PyDict_Size(adict), 2);
  }

  Py_DECREF(root);
}
