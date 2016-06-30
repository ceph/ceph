#ifndef PYSTATE_H_
#define PYSTATE_H_

#include "Python.h"

class PyModules;

extern PyModules *global_handle;
extern PyMethodDef CephStateMethods[];

#endif

