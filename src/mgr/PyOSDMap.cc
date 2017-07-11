// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Mgr.h"

#include "osd/OSDMap.h"
#include "common/errno.h"
#include "common/version.h"

#include "PyOSDMap.h"
#include "PyFormatter.h"
#include "Gil.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr

// ----------

static PyObject *osdmap_get_epoch(PyObject *self, PyObject *obj)
{
  OSDMap *osdmap = static_cast<OSDMap*>(PyCapsule_GetPointer(obj, nullptr));
  return PyInt_FromLong(osdmap->get_epoch());
}

static PyObject *osdmap_dump(PyObject *self, PyObject *obj)
{
  OSDMap *osdmap = static_cast<OSDMap*>(PyCapsule_GetPointer(obj, nullptr));
  PyFormatter f;
  osdmap->dump(&f);
  return f.get();
}


static void delete_osdmap_incremental(PyObject *object)
{
  OSDMap::Incremental *inc = static_cast<OSDMap::Incremental*>(
    PyCapsule_GetPointer(object, nullptr));
  derr << __func__ << " " << inc << dendl;
  delete inc;
}

static PyObject *osdmap_new_incremental(PyObject *self, PyObject *obj)
{
  OSDMap *osdmap = static_cast<OSDMap*>(PyCapsule_GetPointer(obj, nullptr));

  // Construct a capsule containing an OSDMap.
  OSDMap::Incremental *inc = new OSDMap::Incremental;
  inc->fsid = osdmap->get_fsid();
  inc->epoch = osdmap->get_epoch() + 1;
  return PyCapsule_New(inc, nullptr, &delete_osdmap_incremental);
}

static PyObject *osdmap_calc_pg_upmaps(PyObject *self, PyObject *args)
{
  PyObject *mapobj, *incobj, *pool_list;
  double max_deviation = 0;
  int max_iterations = 0;
  if (!PyArg_ParseTuple(args, "OOdiO:calc_pg_upmaps",
			&mapobj, &incobj, &max_deviation,
			&max_iterations, &pool_list)) {
    return nullptr;
  }

  OSDMap *osdmap = static_cast<OSDMap*>(PyCapsule_GetPointer(mapobj, nullptr));
  OSDMap::Incremental *inc = static_cast<OSDMap::Incremental*>(
    PyCapsule_GetPointer(incobj, nullptr));

  dout(10) << __func__ << " osdmap " << osdmap << " inc " << inc
	   << " max_deviation " << max_deviation
	   << " max_iterations " << max_iterations
	   << dendl;
  set<int64_t> pools;
  // FIXME: unpack pool_list and translate to pools set
  int r = osdmap->calc_pg_upmaps(g_ceph_context,
				 max_deviation,
				 max_iterations,
				 pools,
				 inc);
  dout(10) << __func__ << " r = " << r << dendl;
  return PyInt_FromLong(r);
}

PyMethodDef OSDMapMethods[] = {
  {"get_epoch", osdmap_get_epoch, METH_O, "Get OSDMap epoch"},
  {"dump", osdmap_dump, METH_O, "Dump OSDMap::Incremental"},
  {"new_incremental", osdmap_new_incremental, METH_O,
   "Create OSDMap::Incremental"},
  {"calc_pg_upmaps", osdmap_calc_pg_upmaps, METH_VARARGS,
   "Calculate new pg-upmap values"},
  {NULL, NULL, 0, NULL}
};

// ----------

static PyObject *osdmap_inc_get_epoch(PyObject *self, PyObject *obj)
{
  OSDMap::Incremental *inc = static_cast<OSDMap::Incremental*>(
    PyCapsule_GetPointer(obj, nullptr));
  return PyInt_FromLong(inc->epoch);
}

static PyObject *osdmap_inc_dump(PyObject *self, PyObject *obj)
{
  OSDMap::Incremental *inc = static_cast<OSDMap::Incremental*>(
    PyCapsule_GetPointer(obj, nullptr));
  PyFormatter f;
  inc->dump(&f);
  return f.get();
}

PyMethodDef OSDMapIncrementalMethods[] = {
  {"get_epoch", osdmap_inc_get_epoch, METH_O, "Get OSDMap::Incremental epoch"},
  {"dump", osdmap_inc_dump, METH_O, "Dump OSDMap::Incremental"},
  {NULL, NULL, 0, NULL}
};


// ----------



PyMethodDef CRUSHMapMethods[] = {
//  {"get_epoch", osdmap_get_epoch, METH_O, "Get OSDMap epoch"},
  {NULL, NULL, 0, NULL}
};
