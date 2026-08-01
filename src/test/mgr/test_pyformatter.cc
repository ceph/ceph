// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <Python.h>

#include "global/global_init.h"
#include "gtest/gtest.h"
#include "mgr/PyFormatter.h"

#include "TestMgr.h"

TEST(PyFormatter, CreateAndGet)
{
  PyFormatter py_f;
  PyObject* py_obj = py_f.get();
  ASSERT_NE(py_obj, nullptr);
  Py_DECREF(py_obj);
}

TEST(PyFormatter, ArraySection)
{
  PyFormatter py_f;
  py_f.open_array_section("items");
  py_f.dump_int("item", 123);
  py_f.close_section();

  PyObject* py_obj = py_f.get();
  ASSERT_NE(py_obj, nullptr);
  ASSERT_TRUE(PyDict_Check(py_obj));

  PyObject* items = PyDict_GetItemString(py_obj, "items");
  ASSERT_NE(items, nullptr);
  ASSERT_TRUE(PyList_Check(items));

  Py_ssize_t list_size = PyList_Size(items);
  ASSERT_EQ(list_size, 1);

  PyObject* py_value = PyList_GetItem(items, 0);
  ASSERT_NE(py_value, nullptr);
  ASSERT_TRUE(PyLong_Check(py_value));
  ASSERT_EQ(PyLong_AsLong(py_value), 123);

  Py_DECREF(py_obj);
}

TEST(PyFormatter, ObjectSection)
{
  PyFormatter py_f;
  py_f.open_object_section("temp");
  py_f.dump_string("a", "b");
  py_f.dump_int("abc", 123);
  py_f.close_section();

  PyObject* py_obj = py_f.get();
  ASSERT_NE(py_obj, nullptr);
  ASSERT_TRUE(PyDict_Check(py_obj));

  PyObject* temp = PyDict_GetItemString(py_obj, "temp");
  ASSERT_NE(temp, nullptr);
  ASSERT_TRUE(PyDict_Check(temp));

  PyObject* py_str_value = PyDict_GetItemString(temp, "a");
  ASSERT_NE(py_str_value, nullptr);
  ASSERT_TRUE(PyUnicode_Check(py_str_value));

  const char* str_result = PyUnicode_AsUTF8(py_str_value);
  ASSERT_NE(str_result, nullptr);
  ASSERT_STREQ(str_result, "b");

  PyObject* py_int_value = PyDict_GetItemString(temp, "abc");
  ASSERT_NE(py_int_value, nullptr);
  ASSERT_TRUE(PyLong_Check(py_int_value));
  ASSERT_EQ(PyLong_AsLong(py_int_value), 123);

  Py_DECREF(py_obj);
}

TEST(PyFormatter, DumpNull)
{
  PyFormatter py_f;
  py_f.open_object_section("temp");
  py_f.dump_null("abc");
  py_f.close_section();

  PyObject* py_obj = py_f.get();
  ASSERT_NE(py_obj, nullptr);
  ASSERT_TRUE(PyDict_Check(py_obj));

  PyObject* temp = PyDict_GetItemString(py_obj, "temp");
  ASSERT_NE(temp, nullptr);
  ASSERT_TRUE(PyDict_Check(temp));

  PyObject* py_null_value = PyDict_GetItemString(temp, "abc");
  ASSERT_NE(py_null_value, nullptr);
  ASSERT_EQ(py_null_value, Py_None);

  Py_DECREF(py_obj);
}

TEST(PyFormatter, DumpUnsigned)
{
  PyFormatter py_f;
  py_f.open_object_section("temp");
  py_f.dump_unsigned("testUInt", 123u);
  py_f.close_section();

  PyObject* py_obj = py_f.get();
  ASSERT_NE(py_obj, nullptr);
  ASSERT_TRUE(PyDict_Check(py_obj));

  PyObject* temp = PyDict_GetItemString(py_obj, "temp");
  ASSERT_TRUE(temp != nullptr);

  PyObject* py_value = PyDict_GetItemString(temp, "testUInt");
  ASSERT_TRUE(py_value != nullptr);
  ASSERT_TRUE(PyLong_Check(py_value));
  ASSERT_EQ(PyLong_AsLong(py_value), 123);

  Py_DECREF(py_obj);
}

TEST(PyFormatter, DumpInt)
{
  PyFormatter py_f;
  py_f.open_object_section("temp");
  py_f.dump_int("testInt", -123);
  py_f.close_section();

  PyObject* py_obj = py_f.get();
  ASSERT_NE(py_obj, nullptr);
  ASSERT_TRUE(PyDict_Check(py_obj));

  PyObject* temp = PyDict_GetItemString(py_obj, "temp");
  ASSERT_TRUE(temp != nullptr);

  PyObject* py_value = PyDict_GetItemString(temp, "testInt");
  ASSERT_TRUE(py_value != nullptr);
  ASSERT_TRUE(PyLong_Check(py_value));
  ASSERT_EQ(PyLong_AsLong(py_value), -123);

  Py_DECREF(py_obj);
}

TEST(PyFormatter, DumpFloat)
{
  PyFormatter py_f;
  py_f.open_object_section("temp");
  py_f.dump_float("testFloat", 1.23);
  py_f.close_section();

  PyObject* py_obj = py_f.get();
  ASSERT_NE(py_obj, nullptr);
  ASSERT_TRUE(PyDict_Check(py_obj));

  PyObject* temp = PyDict_GetItemString(py_obj, "temp");
  ASSERT_TRUE(temp != nullptr);

  PyObject* py_value = PyDict_GetItemString(temp, "testFloat");
  ASSERT_TRUE(py_value != nullptr);
  ASSERT_TRUE(PyFloat_Check(py_value));
  ASSERT_DOUBLE_EQ(PyFloat_AsDouble(py_value), 1.23);

  Py_DECREF(py_obj);
}

TEST(PyFormatter, DumpString)
{
  PyFormatter py_f;
  py_f.open_object_section("temp");
  py_f.dump_string("testStr", "testing");
  py_f.close_section();

  PyObject* py_obj = py_f.get();
  ASSERT_NE(py_obj, nullptr);
  ASSERT_TRUE(PyDict_Check(py_obj));

  PyObject* temp = PyDict_GetItemString(py_obj, "temp");
  ASSERT_TRUE(temp != nullptr);

  PyObject* py_value = PyDict_GetItemString(temp, "testStr");
  ASSERT_TRUE(py_value != nullptr);
  ASSERT_TRUE(PyUnicode_Check(py_value));
  ASSERT_STREQ(PyUnicode_AsUTF8(py_value), "testing");

  Py_DECREF(py_obj);
}

TEST(PyFormatter, DumpBool)
{
  PyFormatter py_f;
  py_f.open_object_section("temp");
  py_f.dump_bool("testFlag", true);
  py_f.close_section();

  PyObject* py_obj = py_f.get();
  ASSERT_NE(py_obj, nullptr);
  ASSERT_TRUE(PyDict_Check(py_obj));

  PyObject* temp = PyDict_GetItemString(py_obj, "temp");
  ASSERT_TRUE(temp != nullptr);

  PyObject* py_value = PyDict_GetItemString(temp, "testFlag");
  ASSERT_TRUE(py_value != nullptr);
  ASSERT_TRUE(PyBool_Check(py_value));
  ASSERT_EQ(py_value, Py_True);

  Py_DECREF(py_obj);
}

TEST(PyFormatter, DumpStream)
{
  PyFormatter py_f;
  std::ostringstream oss;
  oss << "test stream value";
  py_f.open_object_section("temp");
  py_f.dump_stream(oss.str());
  py_f.close_section();
  PyObject* py_obj = py_f.get();
  ASSERT_NE(py_obj, nullptr);
  Py_DECREF(py_obj);
}

TEST(PyFormatter, DumpFormatVa)
{
  auto dump_format_va_helper = [](PyFormatter* py_formatter,
                                  std::string_view name, const char* ns,
                                  bool quoted, const char* py_formatstr, ...) {
    va_list args;
    va_start(args, py_formatstr);
    py_formatter->dump_format_va(name, ns, quoted, py_formatstr, args);
    va_end(args);
  };
  PyFormatter py_f;
  py_f.open_object_section("temp");
  dump_format_va_helper(
      &py_f, "testVa", nullptr, false, "testing %s %d", "abc", 123);
  py_f.close_section();

  PyObject* py_obj = py_f.get();
  ASSERT_NE(py_obj, nullptr);
  ASSERT_TRUE(PyDict_Check(py_obj));

  PyObject* temp = PyDict_GetItemString(py_obj, "temp");
  ASSERT_TRUE(temp != nullptr);

  PyObject* py_value = PyDict_GetItemString(temp, "testVa");
  ASSERT_TRUE(py_value != nullptr);
  ASSERT_TRUE(PyUnicode_Check(py_value));
  ASSERT_STREQ(PyUnicode_AsUTF8(py_value), "testing abc 123");

  Py_DECREF(py_obj);
}

/* Begin Negative Tests */

TEST(PyFormatter, DumpWithEmptyKey)
{
  PyFormatter py_f;
  py_f.open_object_section("temp");
  py_f.dump_string("", "value");
  py_f.close_section();

  PyObject* py_obj = py_f.get();
  ASSERT_NE(py_obj, nullptr);
  ASSERT_TRUE(PyDict_Check(py_obj));

  PyObject* temp = PyDict_GetItemString(py_obj, "temp");
  ASSERT_TRUE(temp != nullptr);

  PyObject* py_value = PyDict_GetItemString(temp, "");
  ASSERT_TRUE(py_value != nullptr);
  ASSERT_STREQ(PyUnicode_AsUTF8(py_value), "value");

  Py_DECREF(py_obj);
}

TEST(PyFormatter, DumpWithEmptyString)
{
  PyFormatter py_f;
  py_f.open_object_section("temp");
  py_f.dump_string("key", "");
  py_f.close_section();

  PyObject* py_obj = py_f.get();
  ASSERT_NE(py_obj, nullptr);
  ASSERT_TRUE(PyDict_Check(py_obj));

  PyObject* temp = PyDict_GetItemString(py_obj, "temp");
  ASSERT_TRUE(temp != nullptr);

  PyObject* py_value = PyDict_GetItemString(temp, "key");
  ASSERT_TRUE(py_value != nullptr);
  ASSERT_STREQ(PyUnicode_AsUTF8(py_value), "");

  Py_DECREF(py_obj);
}

TEST(PyFormatter, OpenSectionWithoutClosing)
{
  PyFormatter py_f;
  py_f.open_object_section("temp");
  py_f.dump_int("key", 123);
  //Not closing the section, testing that we handle incomplete nesting
  //Should still return valid data from get()

  PyObject* py_obj = py_f.get();
  ASSERT_NE(py_obj, nullptr);
  ASSERT_TRUE(PyDict_Check(py_obj));

  PyObject* temp = PyDict_GetItemString(py_obj, "temp");
  ASSERT_TRUE(temp != nullptr);
  ASSERT_TRUE(PyDict_Check(temp));

  PyObject* py_value = PyDict_GetItemString(temp, "key");
  ASSERT_TRUE(py_value != nullptr);
  ASSERT_TRUE(PyLong_Check(py_value));
  ASSERT_EQ(PyLong_AsLong(py_value), 123);

  Py_DECREF(py_obj);
}

TEST(PyFormatter, DumpExtremeInts)
{
  PyFormatter py_f;
  py_f.open_object_section("temp");
  py_f.dump_int("max", INT_MAX);
  py_f.dump_int("min", INT_MIN);
  py_f.close_section();
  //Testing for Integer overflow during conversion to/from Python objects

  PyObject* py_obj = py_f.get();
  ASSERT_NE(py_obj, nullptr);
  ASSERT_TRUE(PyDict_Check(py_obj));

  PyObject* temp = PyDict_GetItemString(py_obj, "temp");
  ASSERT_TRUE(temp != nullptr);
  ASSERT_TRUE(PyDict_Check(temp));

  PyObject* max_val = PyDict_GetItemString(temp, "max");
  ASSERT_TRUE(max_val != nullptr);
  ASSERT_TRUE(PyLong_Check(max_val));
  ASSERT_EQ(PyLong_AsLong(max_val), INT_MAX);

  PyObject* min_val = PyDict_GetItemString(temp, "min");
  ASSERT_TRUE(min_val != nullptr);
  ASSERT_TRUE(PyLong_Check(min_val));
  ASSERT_EQ(PyLong_AsLong(min_val), INT_MIN);

  Py_DECREF(py_obj);
}

TEST(PyFormatter, EmptySectionName)
{
  PyFormatter py_f;
  py_f.open_object_section("");
  py_f.dump_string("key", "test_val");
  py_f.close_section();
  //Test that "" is a valid key

  PyObject* py_obj = py_f.get();
  ASSERT_NE(py_obj, nullptr);
  ASSERT_TRUE(PyDict_Check(py_obj));

  PyObject* temp = PyDict_GetItemString(py_obj, "");
  ASSERT_TRUE(temp != nullptr);

  PyObject* py_value = PyDict_GetItemString(temp, "key");
  ASSERT_TRUE(py_value != nullptr);
  ASSERT_STREQ(PyUnicode_AsUTF8(py_value), "test_val");

  Py_DECREF(py_obj);
}

/* End Negative Tests */

/*
 * Tests for PyFormatterRO (read-only-on-the-fly builder):
 *  - lists -> tuples, sets -> frozensets
 *  - dicts remain dicts (JSON-friendly)
 *  - works with nested structures
 *  - can be json.dumps()-ed safely (for structures without sets)
 */

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

  // dump_pyobject steals the reference, do not Py_DECREF afterward
  ro.test_dump_pyobject("alist", src_list);
  ro.test_dump_pyobject("aset",  src_set);
  ro.test_dump_pyobject("adict", src_dict);

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

  // dump_pyobject steals the reference, do not Py_DECREF afterward
  ro.test_dump_pyobject("outer", outer_list);

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

  // dump_pyobject steals the reference, do not Py_DECREF afterward
  ro.test_dump_pyobject("alist", src_list);
  ro.test_dump_pyobject("aset",  src_set);
  ro.test_dump_pyobject("adict", src_dict);

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


int
main(int argc, char* argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::AddGlobalTestEnvironment(new PythonEnv);

  return RUN_ALL_TESTS();
}