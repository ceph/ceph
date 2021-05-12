// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "ActivePyModule.h"
#include "PyUtil.h"

class Injector {
private:
  static int64_t get_num_osds();

public:
  static PyObject *get_python(const std::string &what);
};

