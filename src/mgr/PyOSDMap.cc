// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Mgr.h"

#include "osd/OSDMap.h"
#include "common/errno.h"
#include "common/version.h"

#include "PyState.h"
#include "Gil.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr

static PyObject *
osdmap_get_epoch(PyObject *self, PyObject *obj)
{
  OSDMap *osdmap = static_cast<OSDMap*>(PyCapsule_GetPointer(obj, nullptr));
  return PyInt_FromLong(osdmap->get_epoch());
}

PyMethodDef OSDMapMethods[] = {
  {"get_epoch", osdmap_get_epoch, METH_O, "Get OSDMap epoch"},
  {NULL, NULL, 0, NULL}
};

