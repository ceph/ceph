// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <Python.h>

#include "common/options.h"
#include "global/global_init.h"
#include "gtest/gtest.h"
#include "mgr/PyUtil.h"

#include "TestMgr.h"

TEST(PyUtil, IntType)
{
  PyObject* int_obj = get_python_typed_option_value(Option::TYPE_INT, "11");
  ASSERT_TRUE(PyLong_Check(int_obj));
  EXPECT_EQ(PyLong_AsLong(int_obj), 11);
  Py_DECREF(int_obj);
}

TEST(PyUtil, FloatType)
{
  PyObject* float_obj =
      get_python_typed_option_value(Option::TYPE_FLOAT, "11.22");
  ASSERT_TRUE(PyFloat_Check(float_obj));
  EXPECT_DOUBLE_EQ(PyFloat_AsDouble(float_obj), 11.22);
  Py_DECREF(float_obj);
}

TEST(PyUtil, BoolType)
{
  PyObject* bool_obj_true =
      get_python_typed_option_value(Option::TYPE_BOOL, "true");
  ASSERT_EQ(bool_obj_true, Py_True);
  Py_DECREF(bool_obj_true);

  PyObject* bool_obj_false =
      get_python_typed_option_value(Option::TYPE_BOOL, "false");
  ASSERT_EQ(bool_obj_false, Py_False);
  Py_DECREF(bool_obj_false);
}

TEST(PyUtil, StringType)
{
  PyObject* string_obj = get_python_typed_option_value(Option::TYPE_STR, "test");
  ASSERT_TRUE(PyUnicode_Check(string_obj));
  EXPECT_STREQ(PyUnicode_AsUTF8(string_obj), "test");
  Py_DECREF(string_obj);
}

/* Begin Negative Tests */

TEST(PyUtil, BoolWithInvalidString)
{
  PyObject* bool_obj_false =
      get_python_typed_option_value(Option::TYPE_BOOL, "maybe");
  ASSERT_EQ(bool_obj_false, Py_False);
  Py_DECREF(bool_obj_false);
}

TEST(PyUtil, StringTypeEmptyString)
{
  PyObject* string_obj = get_python_typed_option_value(Option::TYPE_STR, "");
  ASSERT_TRUE(PyUnicode_Check(string_obj));
  EXPECT_STREQ(PyUnicode_AsUTF8(string_obj), "");
  Py_DECREF(string_obj);
}

TEST(PyUtil, IntTypeExtremes)
{
  PyObject* int_obj_max =
      get_python_typed_option_value(Option::TYPE_INT, "2147483647");
  ASSERT_TRUE(PyLong_Check(int_obj_max));
  EXPECT_EQ(PyLong_AsLong(int_obj_max), 2147483647);
  Py_DECREF(int_obj_max);

  PyObject* int_obj_min =
      get_python_typed_option_value(Option::TYPE_INT, "-2147483648");
  ASSERT_TRUE(PyLong_Check(int_obj_min));
  EXPECT_EQ(PyLong_AsLong(int_obj_min), -2147483648);
  Py_DECREF(int_obj_min);
}

/*TODO: Also need to test/fix other type mismatches 
  Commented out until Tracker 74181 is resolved
  Causes a segfault since there is no guard against bad input
TEST(PyUtil, IntTypeMismatch)
{
  PyObject* int_obj = get_python_typed_option_value(Option::TYPE_INT, "abc");
  ASSERT_TRUE(PyLong_Check(int_obj));
  Py_DECREF(int_obj);
}
*/
/* End Negative Tests */

int
main(int argc, char* argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::AddGlobalTestEnvironment(new PythonEnv);

  return RUN_ALL_TESTS();
}
