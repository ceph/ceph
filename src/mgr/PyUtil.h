// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>

#include <Python.h>

#include "common/options.h"

PyObject *get_python_typed_option_value(
  Option::type_t type,
  const std::string& value);
