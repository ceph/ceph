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

using std::map;
using std::set;
using std::string;
using std::vector;

typedef struct {
  PyObject_HEAD
  OSDMap *osdmap;
} BasePyOSDMap;

typedef struct {
  PyObject_HEAD
  OSDMap::Incremental *inc;
} BasePyOSDMapIncremental;

typedef struct {
  PyObject_HEAD
  std::shared_ptr<CrushWrapper> crush;
} BasePyCRUSH;

// ----------

static PyObject *osdmap_get_epoch(BasePyOSDMap *self, PyObject *obj)
{
  return PyLong_FromLong(self->osdmap->get_epoch());
}

static PyObject *osdmap_get_crush_version(BasePyOSDMap* self, PyObject *obj)
{
  return PyLong_FromLong(self->osdmap->get_crush_version());
}

static PyObject *osdmap_dump(BasePyOSDMap* self, PyObject *obj)
{
  PyFormatter f;
  self->osdmap->dump(&f, g_ceph_context);
  return f.get();
}

static PyObject *osdmap_new_incremental(BasePyOSDMap *self, PyObject *obj)
{
  OSDMap::Incremental *inc = new OSDMap::Incremental;

  inc->fsid = self->osdmap->get_fsid();
  inc->epoch = self->osdmap->get_epoch() + 1;
  // always include latest crush map here... this is okay since we never
  // actually use this map in the real world (and even if we did it would
  // be a no-op).
  self->osdmap->crush->encode(inc->crush, CEPH_FEATURES_ALL);
  dout(10) << __func__ << " " << inc << dendl;

  return construct_with_capsule("mgr_module", "OSDMapIncremental",
                                (void*)(inc));
}

static PyObject *osdmap_apply_incremental(BasePyOSDMap *self,
    BasePyOSDMapIncremental *incobj)
{
  if (!PyObject_TypeCheck(incobj, &BasePyOSDMapIncrementalType)) {
    derr << "Wrong type in osdmap_apply_incremental!" << dendl;
    return nullptr;
  }

  bufferlist bl;
  self->osdmap->encode(bl, CEPH_FEATURES_ALL|CEPH_FEATURE_RESERVED);
  OSDMap *next = new OSDMap;
  next->decode(bl);
  next->apply_incremental(*(incobj->inc));
  dout(10) << __func__ << " map " << self->osdmap << " inc " << incobj->inc
	   << " next " << next << dendl;

  return construct_with_capsule("mgr_module", "OSDMap", (void*)next);
}

static PyObject *osdmap_get_crush(BasePyOSDMap* self, PyObject *obj)
{
  return construct_with_capsule("mgr_module", "CRUSHMap",
      (void*)(&(self->osdmap->crush)));
}

static PyObject *osdmap_get_pools_by_take(BasePyOSDMap* self, PyObject *args)
{
  int take;
  if (!PyArg_ParseTuple(args, "i:get_pools_by_take",
			&take)) {
    return nullptr;
  }

  PyFormatter f;
  f.open_array_section("pools");
  for (auto& p : self->osdmap->get_pools()) {
    if (self->osdmap->crush->rule_has_take(p.second.crush_rule, take)) {
      f.dump_int("pool", p.first);
    }
  }
  f.close_section();
  return f.get();
}

static PyObject *osdmap_calc_pg_upmaps(BasePyOSDMap* self, PyObject *args)
{
  PyObject *pool_list;
  BasePyOSDMapIncremental *incobj;
  int max_deviation = 0;
  int max_iterations = 0;
  if (!PyArg_ParseTuple(args, "OiiO:calc_pg_upmaps",
			&incobj, &max_deviation,
			&max_iterations, &pool_list)) {
    return nullptr;
  }
  if (!PyList_CheckExact(pool_list)) {
    derr << __func__ << " pool_list not a list" << dendl;
    return nullptr;
  }
  set<int64_t> pools;
  for (auto i = 0; i < PyList_Size(pool_list); ++i) {
    PyObject *pool_name = PyList_GET_ITEM(pool_list, i);
    if (!PyUnicode_Check(pool_name)) {
      derr << __func__ << " " << pool_name << " not a string" << dendl;
      return nullptr;
    }
    auto pool_id = self->osdmap->lookup_pg_pool_name(
      PyUnicode_AsUTF8(pool_name));
    if (pool_id < 0) {
      derr << __func__ << " pool '" << PyUnicode_AsUTF8(pool_name)
           << "' does not exist" << dendl;
      return nullptr;
    }
    pools.insert(pool_id);
  }

  dout(10) << __func__ << " osdmap " << self->osdmap << " inc " << incobj->inc
	   << " max_deviation " << max_deviation
	   << " max_iterations " << max_iterations
	   << " pools " << pools
	   << dendl;
  PyThreadState *tstate = PyEval_SaveThread();
  int r = self->osdmap->calc_pg_upmaps(g_ceph_context,
				 max_deviation,
				 max_iterations,
				 pools,
				 incobj->inc);
  PyEval_RestoreThread(tstate);
  dout(10) << __func__ << " r = " << r << dendl;
  return PyLong_FromLong(r);
}

static PyObject *osdmap_balance_primaries(BasePyOSDMap* self, PyObject *args)
{
  int pool_id;
  BasePyOSDMapIncremental *incobj;
  if (!PyArg_ParseTuple(args, "iO:balance_primaries",
                        &pool_id, &incobj)) {
    return nullptr;
  }
  auto check_pool = self->osdmap->get_pg_pool(pool_id);
  if (!check_pool) {
    derr << __func__ << " pool '" << pool_id
         << "' does not exist" << dendl;
    return nullptr;
  }
  dout(10) << __func__ << " osdmap " << self->osdmap
           << " pool_id " << pool_id
           << " inc " << incobj->inc
           << dendl;
  PyThreadState *tstate = PyEval_SaveThread();
  OSDMap tmp_osd_map;
  tmp_osd_map.deepish_copy_from(*(self->osdmap));
  int r = self->osdmap->balance_primaries(g_ceph_context,
                                 pool_id,
                                 incobj->inc,
				 tmp_osd_map);
  PyEval_RestoreThread(tstate);
  dout(10) << __func__ << " r = " << r << dendl;
  return PyLong_FromLong(r);
}

static PyObject *osdmap_map_pool_pgs_up(BasePyOSDMap* self, PyObject *args)
{
  int poolid;
  if (!PyArg_ParseTuple(args, "i:map_pool_pgs_up",
			&poolid)) {
    return nullptr;
  }
  auto pi = self->osdmap->get_pg_pool(poolid);
  if (!pi)
    return nullptr;
  map<pg_t,vector<int>> pm;
  for (unsigned ps = 0; ps < pi->get_pg_num(); ++ps) {
    pg_t pgid(ps, poolid);
    self->osdmap->pg_to_up_acting_osds(pgid, &pm[pgid], nullptr, nullptr, nullptr);
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

static int
BasePyOSDMap_init(BasePyOSDMap *self, PyObject *args, PyObject *kwds)
{
    PyObject *osdmap_capsule = nullptr;
    static const char *kwlist[] = {"osdmap_capsule", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O",
				     const_cast<char**>(kwlist),
				     &osdmap_capsule)) {
      return -1;
    }
    if (!PyObject_TypeCheck(osdmap_capsule, &PyCapsule_Type)) {
      PyErr_Format(PyExc_TypeError,
		   "Expected a PyCapsule_Type, not %s",
		   Py_TYPE(osdmap_capsule)->tp_name);
      return -1;
    }

    self->osdmap = (OSDMap*)PyCapsule_GetPointer(
        osdmap_capsule, nullptr);
    ceph_assert(self->osdmap);

    return 0;
}


static void
BasePyOSDMap_dealloc(BasePyOSDMap *self)
{
  if (self->osdmap) {
    delete self->osdmap;
    self->osdmap = nullptr;
  } else {
    derr << "Destroying improperly initialized BasePyOSDMap " << self << dendl;
  }
  Py_TYPE(self)->tp_free(self);
}

static PyObject *osdmap_pg_to_up_acting_osds(BasePyOSDMap *self, PyObject *args)
{
  int pool_id = 0;
  int ps = 0;
  if (!PyArg_ParseTuple(args, "ii:pg_to_up_acting_osds",
			&pool_id, &ps)) {
    return nullptr;
  }

  std::vector<int> up;
  int up_primary;
  std::vector<int> acting;
  int acting_primary;
  pg_t pg_id(ps, pool_id);
  self->osdmap->pg_to_up_acting_osds(pg_id,
      &up, &up_primary,
      &acting, &acting_primary);

  // (Ab)use PyFormatter as a convenient way to generate a dict
  PyFormatter f;
  f.dump_int("up_primary", up_primary);
  f.dump_int("acting_primary", acting_primary);
  f.open_array_section("up");
  for (const auto &i : up) {
    f.dump_int("osd", i);
  }
  f.close_section();
  f.open_array_section("acting");
  for (const auto &i : acting) {
    f.dump_int("osd", i);
  }
  f.close_section();

  return f.get();
}

static PyObject *osdmap_pool_raw_used_rate(BasePyOSDMap *self, PyObject *args)
{
  int pool_id = 0;
  if (!PyArg_ParseTuple(args, "i:pool_raw_used_rate",
			&pool_id)) {
    return nullptr;
  }

  if (!self->osdmap->have_pg_pool(pool_id)) {
    return nullptr;
  }

  float rate = self->osdmap->pool_raw_used_rate(pool_id);

  return PyFloat_FromDouble(rate);
}

static PyObject *osdmap_build_simple(PyObject *cls, PyObject *args, PyObject *kwargs)
{
  static const char *kwlist[] = {"epoch", "uuid", "num_osd", nullptr};
  int epoch = 1;
  char* uuid_str = nullptr;
  int num_osd = -1;
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "izi",
				   const_cast<char**>(kwlist),
				   &epoch, &uuid_str, &num_osd)) {
    Py_RETURN_NONE;
  }
  uuid_d uuid;
  if (uuid_str) {
    if (!uuid.parse(uuid_str)) {
      PyErr_Format(PyExc_ValueError, "bad uuid %s", uuid_str);
      Py_RETURN_NONE;
    }
  } else {
    uuid.generate_random();
  }

  auto osdmap = without_gil([&] {
    OSDMap* osdmap = new OSDMap();
    // negative osd is allowed, in that case i just count all osds in ceph.conf
    osdmap->build_simple(g_ceph_context, epoch, uuid, num_osd);
    return osdmap;
  });
  return construct_with_capsule("mgr_module", "OSDMap", reinterpret_cast<void*>(osdmap));
}

PyMethodDef BasePyOSDMap_methods[] = {
  {"_get_epoch", (PyCFunction)osdmap_get_epoch, METH_NOARGS, "Get OSDMap epoch"},
  {"_get_crush_version", (PyCFunction)osdmap_get_crush_version, METH_NOARGS,
    "Get CRUSH version"},
  {"_dump", (PyCFunction)osdmap_dump, METH_NOARGS, "Dump OSDMap::Incremental"},
  {"_new_incremental", (PyCFunction)osdmap_new_incremental, METH_NOARGS,
   "Create OSDMap::Incremental"},
  {"_apply_incremental", (PyCFunction)osdmap_apply_incremental, METH_O,
   "Apply OSDMap::Incremental and return the resulting OSDMap"},
  {"_get_crush", (PyCFunction)osdmap_get_crush, METH_NOARGS, "Get CrushWrapper"},
  {"_get_pools_by_take", (PyCFunction)osdmap_get_pools_by_take, METH_VARARGS,
   "Get pools that have CRUSH rules that TAKE the given root"},
  {"_calc_pg_upmaps", (PyCFunction)osdmap_calc_pg_upmaps, METH_VARARGS,
   "Calculate new pg-upmap values"},
  {"_balance_primaries", (PyCFunction)osdmap_balance_primaries, METH_VARARGS,
   "Calculate new pg-upmap-primary values"},
  {"_map_pool_pgs_up", (PyCFunction)osdmap_map_pool_pgs_up, METH_VARARGS,
   "Calculate up set mappings for all PGs in a pool"},
  {"_pg_to_up_acting_osds", (PyCFunction)osdmap_pg_to_up_acting_osds, METH_VARARGS,
    "Calculate up+acting OSDs for a PG ID"},
  {"_pool_raw_used_rate", (PyCFunction)osdmap_pool_raw_used_rate, METH_VARARGS,
   "Get raw space to logical space ratio"},
  {"_build_simple", (PyCFunction)osdmap_build_simple, METH_VARARGS | METH_CLASS,
   "Create a simple OSDMap"},
  {NULL, NULL, 0, NULL}
};

PyTypeObject BasePyOSDMapType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  "ceph_module.BasePyOSDMap", /* tp_name */
  sizeof(BasePyOSDMap),     /* tp_basicsize */
  0,                         /* tp_itemsize */
  (destructor)BasePyOSDMap_dealloc,      /* tp_dealloc */
  0,                         /* tp_print */
  0,                         /* tp_getattr */
  0,                         /* tp_setattr */
  0,                         /* tp_compare */
  0,                         /* tp_repr */
  0,                         /* tp_as_number */
  0,                         /* tp_as_sequence */
  0,                         /* tp_as_mapping */
  0,                         /* tp_hash */
  0,                         /* tp_call */
  0,                         /* tp_str */
  0,                         /* tp_getattro */
  0,                         /* tp_setattro */
  0,                         /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,        /* tp_flags */
  "Ceph OSDMap",             /* tp_doc */
  0,                         /* tp_traverse */
  0,                         /* tp_clear */
  0,                         /* tp_richcompare */
  0,                         /* tp_weaklistoffset */
  0,                         /* tp_iter */
  0,                         /* tp_iternext */
  BasePyOSDMap_methods,     /* tp_methods */
  0,                         /* tp_members */
  0,                         /* tp_getset */
  0,                         /* tp_base */
  0,                         /* tp_dict */
  0,                         /* tp_descr_get */
  0,                         /* tp_descr_set */
  0,                         /* tp_dictoffset */
  (initproc)BasePyOSDMap_init,                         /* tp_init */
  0,                         /* tp_alloc */
  0,     /* tp_new */
};

// ----------


static int
BasePyOSDMapIncremental_init(BasePyOSDMapIncremental *self,
    PyObject *args, PyObject *kwds)
{
    PyObject *inc_capsule = nullptr;
    static const char *kwlist[] = {"inc_capsule", NULL};

    if (! PyArg_ParseTupleAndKeywords(args, kwds, "O",
                                      const_cast<char**>(kwlist),
                                      &inc_capsule)) {
      ceph_abort();
      return -1;
    }
    ceph_assert(PyObject_TypeCheck(inc_capsule, &PyCapsule_Type));

    self->inc = (OSDMap::Incremental*)PyCapsule_GetPointer(
        inc_capsule, nullptr);
    ceph_assert(self->inc);

    return 0;
}

static void
BasePyOSDMapIncremental_dealloc(BasePyOSDMapIncremental *self)
{
  if (self->inc) {
    delete self->inc;
    self->inc = nullptr;
  } else {
    derr << "Destroying improperly initialized BasePyOSDMap " << self << dendl;
  }
  Py_TYPE(self)->tp_free(self);
}

static PyObject *osdmap_inc_get_epoch(BasePyOSDMapIncremental *self,
    PyObject *obj)
{
  return PyLong_FromLong(self->inc->epoch);
}

static PyObject *osdmap_inc_dump(BasePyOSDMapIncremental *self,
    PyObject *obj)
{
  PyFormatter f;
  self->inc->dump(&f);
  return f.get();
}

static int get_int_float_map(PyObject *obj, map<int,double> *out)
{
  PyObject *ls = PyDict_Items(obj);
  for (int j = 0; j < PyList_Size(ls); ++j) {
    PyObject *pair = PyList_GET_ITEM(ls, j);
    if (!PyTuple_Check(pair)) {
      derr << __func__ << " item " << j << " not a tuple" << dendl;
      Py_DECREF(ls);
      return -1;
    }
    int k;
    double v;
    if (!PyArg_ParseTuple(pair, "id:pair", &k, &v)) {
      derr << __func__ << " item " << j << " not a size 2 tuple" << dendl;
      Py_DECREF(ls);
      return -1;
    }
    (*out)[k] = v;
  }

  Py_DECREF(ls);
  return 0;
}

static PyObject *osdmap_inc_set_osd_reweights(BasePyOSDMapIncremental *self,
    PyObject *weightobj)
{
  map<int,double> wm;
  if (get_int_float_map(weightobj, &wm) < 0) {
    return nullptr;
  }

  for (auto i : wm) {
    self->inc->new_weight[i.first] = std::max(0.0, std::min(1.0, i.second)) * 0x10000;
  }
  Py_RETURN_NONE;
}

static PyObject *osdmap_inc_set_compat_weight_set_weights(
  BasePyOSDMapIncremental *self, PyObject *weightobj)
{
  map<int,double> wm;
  if (get_int_float_map(weightobj, &wm) < 0) {
    return nullptr;
  }

  CrushWrapper crush;
  ceph_assert(self->inc->crush.length());  // see new_incremental
  auto p = self->inc->crush.cbegin();
  decode(crush, p);
  crush.create_choose_args(CrushWrapper::DEFAULT_CHOOSE_ARGS, 1);
  for (auto i : wm) {
    crush.choose_args_adjust_item_weightf(
      g_ceph_context,
      crush.choose_args_get(CrushWrapper::DEFAULT_CHOOSE_ARGS),
      i.first,
      { i.second },
      nullptr);
  }
  self->inc->crush.clear();
  crush.encode(self->inc->crush, CEPH_FEATURES_ALL);
  Py_RETURN_NONE;
}

PyMethodDef BasePyOSDMapIncremental_methods[] = {
  {"_get_epoch", (PyCFunction)osdmap_inc_get_epoch, METH_NOARGS,
    "Get OSDMap::Incremental epoch"},
  {"_dump", (PyCFunction)osdmap_inc_dump, METH_NOARGS,
    "Dump OSDMap::Incremental"},
  {"_set_osd_reweights", (PyCFunction)osdmap_inc_set_osd_reweights,
    METH_O, "Set osd reweight values"},
  {"_set_crush_compat_weight_set_weights",
   (PyCFunction)osdmap_inc_set_compat_weight_set_weights, METH_O,
   "Set weight values in the pending CRUSH compat weight-set"},
  {NULL, NULL, 0, NULL}
};

PyTypeObject BasePyOSDMapIncrementalType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  "ceph_module.BasePyOSDMapIncremental", /* tp_name */
  sizeof(BasePyOSDMapIncremental),     /* tp_basicsize */
  0,                         /* tp_itemsize */
  (destructor)BasePyOSDMapIncremental_dealloc,      /* tp_dealloc */
  0,                         /* tp_print */
  0,                         /* tp_getattr */
  0,                         /* tp_setattr */
  0,                         /* tp_compare */
  0,                         /* tp_repr */
  0,                         /* tp_as_number */
  0,                         /* tp_as_sequence */
  0,                         /* tp_as_mapping */
  0,                         /* tp_hash */
  0,                         /* tp_call */
  0,                         /* tp_str */
  0,                         /* tp_getattro */
  0,                         /* tp_setattro */
  0,                         /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,        /* tp_flags */
  "Ceph OSDMapIncremental",  /* tp_doc */
  0,                         /* tp_traverse */
  0,                         /* tp_clear */
  0,                         /* tp_richcompare */
  0,                         /* tp_weaklistoffset */
  0,                         /* tp_iter */
  0,                         /* tp_iternext */
  BasePyOSDMapIncremental_methods,     /* tp_methods */
  0,                         /* tp_members */
  0,                         /* tp_getset */
  0,                         /* tp_base */
  0,                         /* tp_dict */
  0,                         /* tp_descr_get */
  0,                         /* tp_descr_set */
  0,                         /* tp_dictoffset */
  (initproc)BasePyOSDMapIncremental_init,                         /* tp_init */
  0,                         /* tp_alloc */
  0,                         /* tp_new */
};


// ----------

static int
BasePyCRUSH_init(BasePyCRUSH *self,
    PyObject *args, PyObject *kwds)
{
    PyObject *crush_capsule = nullptr;
    static const char *kwlist[] = {"crush_capsule", NULL};

    if (! PyArg_ParseTupleAndKeywords(args, kwds, "O",
                                      const_cast<char**>(kwlist),
                                      &crush_capsule)) {
      ceph_abort();
      return -1;
    }
    ceph_assert(PyObject_TypeCheck(crush_capsule, &PyCapsule_Type));

    auto ptr_ref = (std::shared_ptr<CrushWrapper>*)(
        PyCapsule_GetPointer(crush_capsule, nullptr));

    // We passed a pointer to a shared pointer, which is weird, but
    // just enough to get it into the constructor: this is a real shared
    // pointer construction now, and then we throw away that pointer to
    // the shared pointer.
    self->crush = *ptr_ref;
    ceph_assert(self->crush);

    return 0;
}

static void
BasePyCRUSH_dealloc(BasePyCRUSH *self)
{
  self->crush.reset();
  Py_TYPE(self)->tp_free(self);
}

static PyObject *crush_dump(BasePyCRUSH *self, PyObject *obj)
{
  PyFormatter f;
  self->crush->dump(&f);
  return f.get();
}

static PyObject *crush_get_item_name(BasePyCRUSH *self, PyObject *args)
{
  int item;
  if (!PyArg_ParseTuple(args, "i:get_item_name", &item)) {
    return nullptr;
  }
  if (!self->crush->item_exists(item)) {
    Py_RETURN_NONE;
  }
  return PyUnicode_FromString(self->crush->get_item_name(item));
}

static PyObject *crush_get_item_weight(BasePyCRUSH *self, PyObject *args)
{
  int item;
  if (!PyArg_ParseTuple(args, "i:get_item_weight", &item)) {
    return nullptr;
  }
  if (!self->crush->item_exists(item)) {
    Py_RETURN_NONE;
  }
  return PyFloat_FromDouble(self->crush->get_item_weightf(item));
}

static PyObject *crush_find_roots(BasePyCRUSH *self)
{
  set<int> roots;
  self->crush->find_roots(&roots);
  PyFormatter f;
  f.open_array_section("roots");
  for (auto root : roots) {
    f.dump_int("root", root);
  }
  f.close_section();
  return f.get();
}

static PyObject *crush_find_takes(BasePyCRUSH *self, PyObject *obj)
{
  set<int> takes;
  self->crush->find_takes(&takes);
  PyFormatter f;
  f.open_array_section("takes");
  for (auto root : takes) {
    f.dump_int("root", root);
  }
  f.close_section();
  return f.get();
}

static PyObject *crush_get_take_weight_osd_map(BasePyCRUSH *self, PyObject *args)
{
  int root;
  if (!PyArg_ParseTuple(args, "i:get_take_weight_osd_map",
			&root)) {
    return nullptr;
  }
  map<int,float> wmap;

  if (!self->crush->item_exists(root)) {
    return nullptr;
  }

  self->crush->get_take_weight_osd_map(root, &wmap);
  PyFormatter f;
  f.open_object_section("weights");
  for (auto& p : wmap) {
    string n = stringify(p.first);     // ick
    f.dump_float(n.c_str(), p.second);
  }
  f.close_section();
  return f.get();
}

PyMethodDef BasePyCRUSH_methods[] = {
  {"_dump", (PyCFunction)crush_dump, METH_NOARGS, "Dump map"},
  {"_get_item_name", (PyCFunction)crush_get_item_name, METH_VARARGS,
    "Get item name"},
  {"_get_item_weight", (PyCFunction)crush_get_item_weight, METH_VARARGS,
    "Get item weight"},
  {"_find_roots", (PyCFunction)crush_find_roots, METH_NOARGS,
   "Find all tree roots"},
  {"_find_takes", (PyCFunction)crush_find_takes, METH_NOARGS,
    "Find distinct TAKE roots"},
  {"_get_take_weight_osd_map", (PyCFunction)crush_get_take_weight_osd_map,
    METH_VARARGS, "Get OSD weight map for a given TAKE root node"},
  {NULL, NULL, 0, NULL}
};

PyTypeObject BasePyCRUSHType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  "ceph_module.BasePyCRUSH", /* tp_name */
  sizeof(BasePyCRUSH),     /* tp_basicsize */
  0,                         /* tp_itemsize */
  (destructor)BasePyCRUSH_dealloc,      /* tp_dealloc */
  0,                         /* tp_print */
  0,                         /* tp_getattr */
  0,                         /* tp_setattr */
  0,                         /* tp_compare */
  0,                         /* tp_repr */
  0,                         /* tp_as_number */
  0,                         /* tp_as_sequence */
  0,                         /* tp_as_mapping */
  0,                         /* tp_hash */
  0,                         /* tp_call */
  0,                         /* tp_str */
  0,                         /* tp_getattro */
  0,                         /* tp_setattro */
  0,                         /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,        /* tp_flags */
  "Ceph OSDMapIncremental",  /* tp_doc */
  0,                         /* tp_traverse */
  0,                         /* tp_clear */
  0,                         /* tp_richcompare */
  0,                         /* tp_weaklistoffset */
  0,                         /* tp_iter */
  0,                         /* tp_iternext */
  BasePyCRUSH_methods,     /* tp_methods */
  0,                         /* tp_members */
  0,                         /* tp_getset */
  0,                         /* tp_base */
  0,                         /* tp_dict */
  0,                         /* tp_descr_get */
  0,                         /* tp_descr_set */
  0,                         /* tp_dictoffset */
  (initproc)BasePyCRUSH_init,                         /* tp_init */
  0,                         /* tp_alloc */
  0,                         /* tp_new */
};
