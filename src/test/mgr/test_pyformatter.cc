// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "mgr/PyFormatter.h"
#include "gtest/gtest.h"
#include "TestMgr.h"

#include <Python.h>

TEST(PyFormatter, CreateAndGet)
{
  PyFormatter py_f;
  PyObject* py_obj = py_f.get();
  ASSERT_NE(py_obj, nullptr);
  Py_DECREF(py_obj);
}

TEST(PyFormatter, ArraySection)
{
  std::string expected = R"({'items': [123]})";
  PyFormatter py_f;
  long result;
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
  result = PyLong_AsLong(py_value);
  ASSERT_EQ(result, 123);

  Py_DECREF(py_obj);
}

TEST(PyFormatter, ObjectSection)
{
  std::string expected = R"({'items': [123]})";
  const char* str_result;
  long int_result;
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
  
  str_result = PyUnicode_AsUTF8(py_str_value);
  ASSERT_NE(str_result, nullptr);
  ASSERT_STREQ(str_result, "b");
  
  PyObject* py_int_value = PyDict_GetItemString(temp, "abc");
  ASSERT_NE(py_int_value, nullptr);
  ASSERT_TRUE(PyLong_Check(py_int_value));
  int_result = PyLong_AsLong(py_int_value);
  ASSERT_EQ(int_result, 123);

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
  auto dump_format_va_helper = [](PyFormatter* py_formatter, std::string_view name, const char* ns, bool quoted, const char* py_formatstr, ...)
  {
    va_list args;
    va_start(args, py_formatstr);
    py_formatter->dump_format_va(name, ns, quoted, py_formatstr, args);
    va_end(args);
  };
  PyFormatter py_f;
  py_f.open_object_section("temp");
  dump_format_va_helper(&py_f, "testVa", nullptr, false, "testing %s %d", "abc", 123);
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

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::AddGlobalTestEnvironment(new PythonEnv);

  return RUN_ALL_TESTS();
}
