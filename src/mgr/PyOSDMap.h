// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>

#include "PythonCompat.h"



extern PyTypeObject BasePyOSDMapType;
extern PyTypeObject BasePyOSDMapIncrementalType;
extern PyTypeObject BasePyCRUSHType;

PyObject *construct_with_capsule(
    const std::string &module,
    const std::string &clsname,
    void *wrapped);

