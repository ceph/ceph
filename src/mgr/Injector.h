#pragma once

#include "PyUtil.h"

class Injector {
public:
  static PyObject *get_python(const std::string &what);
};

