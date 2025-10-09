// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <string>

#include <Python.h>

#include "common/options.h"

PyObject *get_python_typed_option_value(
  Option::type_t type,
  const std::string& value);
