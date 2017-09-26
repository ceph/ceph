// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Mgr.h"

#include "osd/OSDMap.h"
#include "common/errno.h"
#include "common/version.h"
#include "include/stringify.h"

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

static PyObject *osdmap_get_crush_version(PyObject *self, PyObject *obj)
{
  OSDMap *osdmap = static_cast<OSDMap*>(PyCapsule_GetPointer(obj, nullptr));
  return PyInt_FromLong(osdmap->get_crush_version());
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
  dout(10) << __func__ << " " << inc << dendl;
  delete inc;
}

static PyObject *osdmap_new_incremental(PyObject *self, PyObject *obj)
{
  OSDMap *osdmap = static_cast<OSDMap*>(PyCapsule_GetPointer(obj, nullptr));
  OSDMap::Incremental *inc = new OSDMap::Incremental;
  inc->fsid = osdmap->get_fsid();
  inc->epoch = osdmap->get_epoch() + 1;
  // always include latest crush map here... this is okay since we never
  // actually use this map in the real world (and even if we did it would
  // be a no-op).
  osdmap->crush->encode(inc->crush, CEPH_FEATURES_ALL);
  dout(10) << __func__ << " " << inc << dendl;
  return PyCapsule_New(inc, nullptr, &delete_osdmap_incremental);
}

static void delete_osdmap(PyObject *object)
{
  OSDMap *osdmap = static_cast<OSDMap*>(PyCapsule_GetPointer(object, nullptr));
  assert(osdmap);
  dout(10) << __func__ << " " << osdmap << dendl;
  delete osdmap;
}

static PyObject *osdmap_apply_incremental(PyObject *self, PyObject *args)
{
  PyObject *mapobj, *incobj;
  if (!PyArg_ParseTuple(args, "OO:apply_incremental",
			&mapobj, &incobj)) {
    return nullptr;
  }
  OSDMap *osdmap = static_cast<OSDMap*>(PyCapsule_GetPointer(mapobj, nullptr));
  OSDMap::Incremental *inc = static_cast<OSDMap::Incremental*>(
    PyCapsule_GetPointer(incobj, nullptr));
  if (!osdmap || !inc) {
    return nullptr;
  }

  bufferlist bl;
  osdmap->encode(bl, CEPH_FEATURES_ALL|CEPH_FEATURE_RESERVED);
  OSDMap *next = new OSDMap;
  next->decode(bl);
  next->apply_incremental(*inc);
  dout(10) << __func__ << " map " << osdmap << " inc " << inc
	   << " next " << next << dendl;
  return PyCapsule_New(next, nullptr, &delete_osdmap);
}

static PyObject *osdmap_get_crush(PyObject *self, PyObject *obj)
{
  OSDMap *osdmap = static_cast<OSDMap*>(PyCapsule_GetPointer(obj, nullptr));

  // Construct a capsule containing a the CrushWrapper.
  return PyCapsule_New(osdmap->crush.get(), nullptr, nullptr);
}

static PyObject *osdmap_get_pools_by_take(PyObject *self, PyObject *args)
{
  PyObject *mapobj;
  int take;
  if (!PyArg_ParseTuple(args, "Oi:get_pools_by_take",
			&mapobj, &take)) {
    return nullptr;
  }
  OSDMap *osdmap = static_cast<OSDMap*>(PyCapsule_GetPointer(mapobj, nullptr));
  PyFormatter f;
  f.open_array_section("pools");
  for (auto& p : osdmap->get_pools()) {
    if (osdmap->crush->rule_has_take(p.second.crush_rule, take)) {
      f.dump_int("pool", p.first);
    }
  }
  f.close_section();
  return f.get();
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

static PyObject *osdmap_map_pool_pgs_up(PyObject *self, PyObject *args)
{
  PyObject *mapobj;
  int poolid;
  if (!PyArg_ParseTuple(args, "Oi:map_pool_pgs_up",
			&mapobj, &poolid)) {
    return nullptr;
  }
  OSDMap *osdmap = static_cast<OSDMap*>(PyCapsule_GetPointer(mapobj, nullptr));
  if (!osdmap)
    return nullptr;
  auto pi = osdmap->get_pg_pool(poolid);
  if (!pi)
    return nullptr;
  map<pg_t,vector<int>> pm;
  for (unsigned ps = 0; ps < pi->get_pg_num(); ++ps) {
    pg_t pgid(ps, poolid);
    osdmap->pg_to_up_acting_osds(pgid, &pm[pgid], nullptr, nullptr, nullptr);
  }
  PyFormatter f;
  for (auto p : pm) {
    string pg = stringify(p.first);
    f.open_array_section(pg.c_str());
    for (auto o : p.second) {
      f.dump_int("osd", o);
    }
    f.close_section();
  }
  return f.get();
}

PyMethodDef OSDMapMethods[] = {
  {"get_epoch", osdmap_get_epoch, METH_O, "Get OSDMap epoch"},
  {"get_crush_version", osdmap_get_crush_version, METH_O, "Get CRUSH version"},
  {"dump", osdmap_dump, METH_O, "Dump OSDMap::Incremental"},
  {"new_incremental", osdmap_new_incremental, METH_O,
   "Create OSDMap::Incremental"},
  {"apply_incremental", osdmap_apply_incremental, METH_VARARGS,
   "Apply OSDMap::Incremental and return the resulting OSDMap"},
  {"get_crush", osdmap_get_crush, METH_O, "Get CrushWrapper"},
  {"get_pools_by_take", osdmap_get_pools_by_take, METH_VARARGS,
   "Get pools that have CRUSH rules that TAKE the given root"},
  {"calc_pg_upmaps", osdmap_calc_pg_upmaps, METH_VARARGS,
   "Calculate new pg-upmap values"},
  {"map_pool_pgs_up", osdmap_map_pool_pgs_up, METH_VARARGS,
   "Calculate up set mappings for all PGs in a pool"},
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

static int get_int_float_map(PyObject *obj, map<int,double> *out)
{
  PyObject *ls = PyDict_Items(obj);
  for (int j = 0; j < PyList_Size(ls); ++j) {
    PyObject *pair = PyList_GET_ITEM(ls, j);
    if (!PyTuple_Check(pair)) {
      derr << __func__ << " item " << j << " not a tuple" << dendl;
      return -1;
    }
    int k;
    double v;
    if (!PyArg_ParseTuple(pair, "id:pair", &k, &v)) {
      derr << __func__ << " item " << j << " not a size 2 tuple" << dendl;
      return -1;
    }
    (*out)[k] = v;
  }
  return 0;
}

static PyObject *osdmap_inc_set_osd_reweights(PyObject *self, PyObject *args)
{
  PyObject *incobj, *weightobj;
  if (!PyArg_ParseTuple(args, "OO:set_osd_reweights",
			&incobj, &weightobj)) {
    return nullptr;
  }
  OSDMap::Incremental *inc = static_cast<OSDMap::Incremental*>(
    PyCapsule_GetPointer(incobj, nullptr));
  map<int,double> wm;
  if (get_int_float_map(weightobj, &wm) < 0) {
    return nullptr;
  }

  for (auto i : wm) {
    inc->new_weight[i.first] = std::max(0.0, std::min(1.0, i.second)) * 0x10000;
  }
  Py_RETURN_NONE;
}

static PyObject *osdmap_inc_set_compat_weight_set_weights(
  PyObject *self, PyObject *args)
{
  PyObject *incobj, *weightobj;
  if (!PyArg_ParseTuple(args, "OO:set_compat_weight_set_weights",
			&incobj, &weightobj)) {
    return nullptr;
  }
  OSDMap::Incremental *inc = static_cast<OSDMap::Incremental*>(
    PyCapsule_GetPointer(incobj, nullptr));
  map<int,double> wm;
  if (get_int_float_map(weightobj, &wm) < 0) {
    return nullptr;
  }

  CrushWrapper crush;
  assert(inc->crush.length());  // see new_incremental
  auto p = inc->crush.begin();
  ::decode(crush, p);
  crush.create_choose_args(CrushWrapper::DEFAULT_CHOOSE_ARGS, 1);
  for (auto i : wm) {
    crush.choose_args_adjust_item_weightf(
      g_ceph_context,
      crush.choose_args_get(CrushWrapper::DEFAULT_CHOOSE_ARGS),
      i.first,
      { i.second },
      nullptr);
  }
  inc->crush.clear();
  crush.encode(inc->crush, CEPH_FEATURES_ALL);
  Py_RETURN_NONE;
}



PyMethodDef OSDMapIncrementalMethods[] = {
  {"get_epoch", osdmap_inc_get_epoch, METH_O, "Get OSDMap::Incremental epoch"},
  {"dump", osdmap_inc_dump, METH_O, "Dump OSDMap::Incremental"},
  {"set_osd_reweights", osdmap_inc_set_osd_reweights, METH_VARARGS,
   "Set osd reweight values"},
  {"set_crush_compat_weight_set_weights",
   osdmap_inc_set_compat_weight_set_weights, METH_VARARGS,
   "Set weight values in the pending CRUSH compat weight-set"},
  {NULL, NULL, 0, NULL}
};


// ----------

static PyObject *crush_dump(PyObject *self, PyObject *obj)
{
  CrushWrapper *crush = static_cast<CrushWrapper*>(
    PyCapsule_GetPointer(obj, nullptr));
  PyFormatter f;
  crush->dump(&f);
  return f.get();
}

static PyObject *crush_get_item_name(PyObject *self, PyObject *args)
{
  PyObject *obj;
  int item;
  if (!PyArg_ParseTuple(args, "Oi:get_item_name",
			&obj, &item)) {
    return nullptr;
  }
  CrushWrapper *crush = static_cast<CrushWrapper*>(
    PyCapsule_GetPointer(obj, nullptr));
  if (!crush->item_exists(item)) {
    Py_RETURN_NONE;
  }
  return PyString_FromString(crush->get_item_name(item));
}

static PyObject *crush_get_item_weight(PyObject *self, PyObject *args)
{
  PyObject *obj;
  int item;
  if (!PyArg_ParseTuple(args, "Oi:get_item_weight",
			&obj, &item)) {
    return nullptr;
  }
  CrushWrapper *crush = static_cast<CrushWrapper*>(
    PyCapsule_GetPointer(obj, nullptr));
  if (!crush->item_exists(item)) {
    Py_RETURN_NONE;
  }
  return PyFloat_FromDouble(crush->get_item_weightf(item));
}

static PyObject *crush_find_takes(PyObject *self, PyObject *obj)
{
  CrushWrapper *crush = static_cast<CrushWrapper*>(
    PyCapsule_GetPointer(obj, nullptr));
  set<int> takes;
  crush->find_takes(&takes);
  PyFormatter f;
  f.open_array_section("takes");
  for (auto root : takes) {
    f.dump_int("root", root);
  }
  f.close_section();
  return f.get();
}

static PyObject *crush_get_take_weight_osd_map(PyObject *self, PyObject *args)
{
  PyObject *obj;
  int root;
  if (!PyArg_ParseTuple(args, "Oi:get_take_weight_osd_map",
			&obj, &root)) {
    return nullptr;
  }
  CrushWrapper *crush = static_cast<CrushWrapper*>(
    PyCapsule_GetPointer(obj, nullptr));

  map<int,float> wmap;
  crush->get_take_weight_osd_map(root, &wmap);
  PyFormatter f;
  f.open_object_section("weights");
  for (auto& p : wmap) {
    string n = stringify(p.first);     // ick
    f.dump_float(n.c_str(), p.second);
  }
  f.close_section();
  return f.get();
}

PyMethodDef CRUSHMapMethods[] = {
  {"dump", crush_dump, METH_O, "Dump map"},
  {"get_item_name", crush_get_item_name, METH_VARARGS, "Get item name"},
  {"get_item_weight", crush_get_item_weight, METH_VARARGS, "Get item weight"},
  {"find_takes", crush_find_takes, METH_O, "Find distinct TAKE roots"},
  {"get_take_weight_osd_map", crush_get_take_weight_osd_map, METH_VARARGS,
   "Get OSD weight map for a given TAKE root node"},
  {NULL, NULL, 0, NULL}
};
