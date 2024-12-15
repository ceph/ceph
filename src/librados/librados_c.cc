// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <limits.h>

#include "acconfig.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/ceph_argparse.h"
#include "common/ceph_json.h"
#include "common/common_init.h"
#include "common/TracepointProvider.h"
#include "common/hobject.h"
#include "common/async/waiter.h"
#include "include/rados/librados.h"
#include "include/types.h"
#include <include/stringify.h>

#include "librados/librados_c.h"
#include "librados/AioCompletionImpl.h"
#include "librados/IoCtxImpl.h"
#include "librados/ObjectOperationImpl.h"
#include "librados/PoolAsyncCompletionImpl.h"
#include "librados/RadosClient.h"
#include "librados/RadosXattrIter.h"
#include "librados/ListObjectImpl.h"
#include "librados/librados_util.h"
#include <cls/lock/cls_lock_client.h>

#include <string>
#include <map>
#include <set>
#include <vector>
#include <list>
#include <stdexcept>

#ifdef WITH_LTTNG
#define TRACEPOINT_DEFINE
#define TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#include "tracing/librados.h"
#undef TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#undef TRACEPOINT_DEFINE
#else
#define tracepoint(...)
#endif

#if defined(HAVE_ASM_SYMVER) || defined(HAVE_ATTR_SYMVER)
// prefer __attribute__() over global asm(".symver"). because the latter
// is not parsed by the compiler and is partitioned away by GCC if
// lto-partitions is enabled, in other words, these asm() statements
// are dropped by the -flto option by default. the way to address it is
// to use __attribute__. so this information can be processed by the
// C compiler, and be preserved after LTO partitions the code
#ifdef HAVE_ATTR_SYMVER
#define LIBRADOS_C_API_BASE(fn)               \
  extern __typeof (_##fn##_base) _##fn##_base __attribute__((__symver__ (#fn "@")))
#define LIBRADOS_C_API_BASE_DEFAULT(fn)       \
  extern __typeof (_##fn) _##fn __attribute__((__symver__ (#fn "@@")))
#define LIBRADOS_C_API_DEFAULT(fn, ver)       \
  extern __typeof (_##fn) _##fn __attribute__((__symver__ (#fn "@@LIBRADOS_" #ver)))
#else
#define LIBRADOS_C_API_BASE(fn)               \
  asm(".symver _" #fn "_base, " #fn "@")
#define LIBRADOS_C_API_BASE_DEFAULT(fn)       \
  asm(".symver _" #fn ", " #fn "@@")
#define LIBRADOS_C_API_DEFAULT(fn, ver)       \
  asm(".symver _" #fn ", " #fn "@@LIBRADOS_" #ver)
#endif

#define LIBRADOS_C_API_BASE_F(fn) _ ## fn ## _base
#define LIBRADOS_C_API_DEFAULT_F(fn) _ ## fn

#else
#define LIBRADOS_C_API_BASE(fn)
#define LIBRADOS_C_API_BASE_DEFAULT(fn)
#define LIBRADOS_C_API_DEFAULT(fn, ver)

#define LIBRADOS_C_API_BASE_F(fn) _ ## fn ## _base
// There shouldn't be multiple default versions of the same
// function.
#define LIBRADOS_C_API_DEFAULT_F(fn) fn
#endif

using std::ostringstream;
using std::pair;
using std::string;
using std::map;
using std::set;
using std::vector;
using std::list;

#define dout_subsys ceph_subsys_rados
#undef dout_prefix
#define dout_prefix *_dout << "librados: "

#define RADOS_LIST_MAX_ENTRIES 1024

static TracepointProvider::Traits tracepoint_traits("librados_tp.so", "rados_tracing");

/*
 * Structure of this file
 *
 * RadosClient and the related classes are the internal implementation of librados.
 * Above that layer sits the C API, found in include/rados/librados.h, and
 * the C++ API, found in include/rados/librados.hpp
 *
 * The C++ API sometimes implements things in terms of the C API.
 * Both the C++ and C API rely on RadosClient.
 *
 * Visually:
 * +--------------------------------------+
 * |             C++ API                  |
 * +--------------------+                 |
 * |       C API        |                 |
 * +--------------------+-----------------+
 * |          RadosClient                 |
 * +--------------------------------------+
 */

///////////////////////////// C API //////////////////////////////

static CephContext *rados_create_cct(
  const char * const clustername,
  CephInitParameters *iparams)
{
  // missing things compared to global_init:
  // g_ceph_context, g_conf, g_lockdep, signal handlers
  CephContext *cct = common_preinit(*iparams, CODE_ENVIRONMENT_LIBRARY, 0);
  if (clustername)
    cct->_conf->cluster = clustername;
  cct->_conf.parse_env(cct->get_module_type()); // environment variables override
  cct->_conf.apply_changes(nullptr);

  TracepointProvider::initialize<tracepoint_traits>(cct);
  return cct;
}

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_create)(
  rados_t *pcluster,
  const char * const id)
{
  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
  if (id) {
    iparams.name.set(CEPH_ENTITY_TYPE_CLIENT, id);
  }
  CephContext *cct = rados_create_cct("", &iparams);

  tracepoint(librados, rados_create_enter, id);
  *pcluster = reinterpret_cast<rados_t>(new librados::RadosClient(cct));
  tracepoint(librados, rados_create_exit, 0, *pcluster);

  cct->put();
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_create);

// as above, but
// 1) don't assume 'client.'; name is a full type.id namestr
// 2) allow setting clustername
// 3) flags is for future expansion (maybe some of the global_init()
//    behavior is appropriate for some consumers of librados, for instance)

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_create2)(
  rados_t *pcluster,
  const char *const clustername,
  const char * const name,
  uint64_t flags)
{
  // client is assumed, but from_str will override
  int retval = 0;
  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
  if (!name || !iparams.name.from_str(name)) {
    retval = -EINVAL;
  }

  CephContext *cct = rados_create_cct(clustername, &iparams);
  tracepoint(librados, rados_create2_enter, clustername, name, flags);
  if (retval == 0) {
    *pcluster = reinterpret_cast<rados_t>(new librados::RadosClient(cct));
  }
  tracepoint(librados, rados_create2_exit, retval, *pcluster);

  cct->put();
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_create2);

/* This function is intended for use by Ceph daemons. These daemons have
 * already called global_init and want to use that particular configuration for
 * their cluster.
 */
extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_create_with_context)(
  rados_t *pcluster,
  rados_config_t cct_)
{
  CephContext *cct = (CephContext *)cct_;
  TracepointProvider::initialize<tracepoint_traits>(cct);

  tracepoint(librados, rados_create_with_context_enter, cct_);
  librados::RadosClient *radosp = new librados::RadosClient(cct);
  *pcluster = (void *)radosp;
  tracepoint(librados, rados_create_with_context_exit, 0, *pcluster);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_create_with_context);

extern "C" rados_config_t LIBRADOS_C_API_DEFAULT_F(rados_cct)(rados_t cluster)
{
  tracepoint(librados, rados_cct_enter, cluster);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  rados_config_t retval = (rados_config_t)client->cct;
  tracepoint(librados, rados_cct_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_cct);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_connect)(rados_t cluster)
{
  tracepoint(librados, rados_connect_enter, cluster);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  int retval = client->connect();
  tracepoint(librados, rados_connect_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_connect);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_shutdown)(rados_t cluster)
{
  tracepoint(librados, rados_shutdown_enter, cluster);
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  radosp->shutdown();
  delete radosp;
  tracepoint(librados, rados_shutdown_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_shutdown);

extern "C" uint64_t LIBRADOS_C_API_DEFAULT_F(rados_get_instance_id)(
  rados_t cluster)
{
  tracepoint(librados, rados_get_instance_id_enter, cluster);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  uint64_t retval = client->get_instance_id();
  tracepoint(librados, rados_get_instance_id_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_get_instance_id);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_get_min_compatible_osd)(
  rados_t cluster,
  int8_t* require_osd_release)
{
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  return client->get_min_compatible_osd(require_osd_release);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_get_min_compatible_osd);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_get_min_compatible_client)(
  rados_t cluster,
  int8_t* min_compat_client,
  int8_t* require_min_compat_client)
{
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  return client->get_min_compatible_client(min_compat_client,
                                           require_min_compat_client);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_get_min_compatible_client);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_version)(
  int *major, int *minor, int *extra)
{
  tracepoint(librados, rados_version_enter, major, minor, extra);
  if (major)
    *major = LIBRADOS_VER_MAJOR;
  if (minor)
    *minor = LIBRADOS_VER_MINOR;
  if (extra)
    *extra = LIBRADOS_VER_EXTRA;
  tracepoint(librados, rados_version_exit, LIBRADOS_VER_MAJOR, LIBRADOS_VER_MINOR, LIBRADOS_VER_EXTRA);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_version);


// -- config --
extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_conf_read_file)(
  rados_t cluster,
  const char *path_list)
{
  tracepoint(librados, rados_conf_read_file_enter, cluster, path_list);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  auto& conf = client->cct->_conf;
  ostringstream warnings;
  int ret = conf.parse_config_files(path_list, &warnings, 0);
  if (ret) {
    if (warnings.tellp() > 0)
      lderr(client->cct) << warnings.str() << dendl;
    client->cct->_conf.complain_about_parse_error(client->cct);
    tracepoint(librados, rados_conf_read_file_exit, ret);
    return ret;
  }
  conf.parse_env(client->cct->get_module_type()); // environment variables override

  conf.apply_changes(nullptr);
  client->cct->_conf.complain_about_parse_error(client->cct);
  tracepoint(librados, rados_conf_read_file_exit, 0);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_conf_read_file);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_conf_parse_argv)(
  rados_t cluster,
  int argc,
  const char **argv)
{
  tracepoint(librados, rados_conf_parse_argv_enter, cluster, argc);
  int i;
  for(i = 0; i < argc; i++) {
    tracepoint(librados, rados_conf_parse_argv_arg, argv[i]);
  }
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  auto& conf = client->cct->_conf;
  auto args = argv_to_vec(argc, argv);
  int ret = conf.parse_argv(args);
  if (ret) {
    tracepoint(librados, rados_conf_parse_argv_exit, ret);
    return ret;
  }
  conf.apply_changes(nullptr);
  tracepoint(librados, rados_conf_parse_argv_exit, 0);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_conf_parse_argv);

// like above, but return the remainder of argv to contain remaining
// unparsed args.  Must be allocated to at least argc by caller.
// remargv will contain n <= argc pointers to original argv[], the end
// of which may be NULL

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_conf_parse_argv_remainder)(
  rados_t cluster, int argc,
  const char **argv,
  const char **remargv)
{
  tracepoint(librados, rados_conf_parse_argv_remainder_enter, cluster, argc);
  unsigned int i;
  for(i = 0; i < (unsigned int) argc; i++) {
    tracepoint(librados, rados_conf_parse_argv_remainder_arg, argv[i]);
  }
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  auto& conf = client->cct->_conf;
  vector<const char*> args;
  for (int i=0; i<argc; i++)
    args.push_back(argv[i]);
  int ret = conf.parse_argv(args);
  if (ret) {
    tracepoint(librados, rados_conf_parse_argv_remainder_exit, ret);
    return ret;
  }
  conf.apply_changes(NULL);
  ceph_assert(args.size() <= (unsigned int)argc);
  for (i = 0; i < (unsigned int)argc; ++i) {
    if (i < args.size())
      remargv[i] = args[i];
    else
      remargv[i] = (const char *)NULL;
    tracepoint(librados, rados_conf_parse_argv_remainder_remarg, remargv[i]);
  }
  tracepoint(librados, rados_conf_parse_argv_remainder_exit, 0);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_conf_parse_argv_remainder);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_conf_parse_env)(
  rados_t cluster, const char *env)
{
  tracepoint(librados, rados_conf_parse_env_enter, cluster, env);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  auto& conf = client->cct->_conf;
  conf.parse_env(client->cct->get_module_type(), env);
  conf.apply_changes(nullptr);
  tracepoint(librados, rados_conf_parse_env_exit, 0);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_conf_parse_env);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_conf_set)(
  rados_t cluster,
  const char *option,
  const char *value)
{
  tracepoint(librados, rados_conf_set_enter, cluster, option, value);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  auto& conf = client->cct->_conf;
  int ret = conf.set_val(option, value);
  if (ret) {
    tracepoint(librados, rados_conf_set_exit, ret);
    return ret;
  }
  conf.apply_changes(nullptr);
  tracepoint(librados, rados_conf_set_exit, 0);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_conf_set);

/* cluster info */
extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_cluster_stat)(
  rados_t cluster,
  rados_cluster_stat_t *result)
{
  tracepoint(librados, rados_cluster_stat_enter, cluster);
  librados::RadosClient *client = (librados::RadosClient *)cluster;

  ceph_statfs stats;
  int r = client->get_fs_stats(stats);
  result->kb = stats.kb;
  result->kb_used = stats.kb_used;
  result->kb_avail = stats.kb_avail;
  result->num_objects = stats.num_objects;
  tracepoint(librados, rados_cluster_stat_exit, r, result->kb, result->kb_used, result->kb_avail, result->num_objects);
  return r;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_cluster_stat);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_conf_get)(
  rados_t cluster,
  const char *option,
  char *buf, size_t len)
{
  tracepoint(librados, rados_conf_get_enter, cluster, option, len);
  char *tmp = buf;
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  const auto& conf = client->cct->_conf;
  int retval = conf.get_val(option, &tmp, len);
  tracepoint(librados, rados_conf_get_exit, retval, retval ? "" : option);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_conf_get);

extern "C" int64_t LIBRADOS_C_API_DEFAULT_F(rados_pool_lookup)(
  rados_t cluster,
  const char *name)
{
  tracepoint(librados, rados_pool_lookup_enter, cluster, name);
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  int64_t retval = radosp->lookup_pool(name);
  tracepoint(librados, rados_pool_lookup_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_pool_lookup);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_pool_reverse_lookup)(
  rados_t cluster,
  int64_t id,
  char *buf,
  size_t maxlen)
{
  tracepoint(librados, rados_pool_reverse_lookup_enter, cluster, id, maxlen);
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  std::string name;
  int r = radosp->pool_get_name(id, &name, true);
  if (r < 0) {
    tracepoint(librados, rados_pool_reverse_lookup_exit, r, "");
    return r;
  }
  if (name.length() >= maxlen) {
    tracepoint(librados, rados_pool_reverse_lookup_exit, -ERANGE, "");
    return -ERANGE;
  }
  strcpy(buf, name.c_str());
  int retval = name.length();
  tracepoint(librados, rados_pool_reverse_lookup_exit, retval, buf);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_pool_reverse_lookup);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_cluster_fsid)(
  rados_t cluster,
  char *buf,
  size_t maxlen)
{
  tracepoint(librados, rados_cluster_fsid_enter, cluster, maxlen);
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  std::string fsid;
  radosp->get_fsid(&fsid);
  if (fsid.length() >= maxlen) {
    tracepoint(librados, rados_cluster_fsid_exit, -ERANGE, "");
    return -ERANGE;
  }
  strcpy(buf, fsid.c_str());
  int retval = fsid.length();
  tracepoint(librados, rados_cluster_fsid_exit, retval, buf);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_cluster_fsid);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_wait_for_latest_osdmap)(
  rados_t cluster)
{
  tracepoint(librados, rados_wait_for_latest_osdmap_enter, cluster);
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  int retval = radosp->wait_for_latest_osdmap();
  tracepoint(librados, rados_wait_for_latest_osdmap_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_wait_for_latest_osdmap);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_blocklist_add)(
  rados_t cluster,
  char *client_address,
  uint32_t expire_seconds)
{
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  return radosp->blocklist_add(client_address, expire_seconds);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_blocklist_add);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_blacklist_add)(
  rados_t cluster,
  char *client_address,
  uint32_t expire_seconds)
{
  return LIBRADOS_C_API_DEFAULT_F(rados_blocklist_add)(
    cluster, client_address, expire_seconds);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_blacklist_add);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_getaddrs)(
  rados_t cluster,
  char** addrs)
{
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  auto s = radosp->get_addrs();
  *addrs = strdup(s.c_str());
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_getaddrs);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_set_osdmap_full_try)(
  rados_ioctx_t io)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  ctx->extra_op_flags |= CEPH_OSD_FLAG_FULL_TRY;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_set_osdmap_full_try);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_unset_osdmap_full_try)(
  rados_ioctx_t io)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  ctx->extra_op_flags &= ~CEPH_OSD_FLAG_FULL_TRY;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_unset_osdmap_full_try);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_set_pool_full_try)(
  rados_ioctx_t io)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  ctx->extra_op_flags |= CEPH_OSD_FLAG_FULL_TRY;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_set_pool_full_try);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_unset_pool_full_try)(
  rados_ioctx_t io)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  ctx->extra_op_flags &= ~CEPH_OSD_FLAG_FULL_TRY;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_unset_pool_full_try);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_application_enable)(
  rados_ioctx_t io,
  const char *app_name,
  int force)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  return ctx->application_enable(app_name, force != 0);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_application_enable);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_application_list)(
  rados_ioctx_t io,
  char *values,
  size_t *values_len)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  std::set<std::string> app_names;
  int r = ctx->application_list(&app_names);
  if (r < 0) {
    return r;
  }

  size_t total_len = 0;
  for (auto app_name : app_names) {
    total_len += app_name.size() + 1;
  }

  if (*values_len < total_len) {
    *values_len = total_len;
    return -ERANGE;
  }

  char *values_p = values;
  for (auto app_name : app_names) {
    size_t len = app_name.size() + 1;
    strncpy(values_p, app_name.c_str(), len);
    values_p += len;
  }
  *values_p = '\0';
  *values_len = total_len;
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_application_list);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_application_metadata_get)(
  rados_ioctx_t io,
  const char *app_name,
  const char *key,
  char *value,
  size_t *value_len)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  std::string value_str;
  int r = ctx->application_metadata_get(app_name, key, &value_str);
  if (r < 0) {
    return r;
  }

  size_t len = value_str.size() + 1;
  if (*value_len < len) {
    *value_len = len;
    return -ERANGE;
  }

  strncpy(value, value_str.c_str(), len);
  *value_len = len;
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_application_metadata_get);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_application_metadata_set)(
  rados_ioctx_t io,
  const char *app_name,
  const char *key,
  const char *value)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  return ctx->application_metadata_set(app_name, key, value);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_application_metadata_set);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_application_metadata_remove)(
  rados_ioctx_t io,
  const char *app_name,
  const char *key)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  return ctx->application_metadata_remove(app_name, key);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_application_metadata_remove);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_application_metadata_list)(
  rados_ioctx_t io,
  const char *app_name,
  char *keys, size_t *keys_len,
  char *values, size_t *vals_len)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  std::map<std::string, std::string> metadata;
  int r = ctx->application_metadata_list(app_name, &metadata);
  if (r < 0) {
    return r;
  }

  size_t total_key_len = 0;
  size_t total_val_len = 0;
  for (auto pair : metadata) {
    total_key_len += pair.first.size() + 1;
    total_val_len += pair.second.size() + 1;
  }

  if (*keys_len < total_key_len || *vals_len < total_val_len) {
    *keys_len = total_key_len;
    *vals_len = total_val_len;
    return -ERANGE;
  }

  char *keys_p = keys;
  char *vals_p = values;
  for (auto pair : metadata) {
    size_t key_len = pair.first.size() + 1;
    strncpy(keys_p, pair.first.c_str(), key_len);
    keys_p += key_len;

    size_t val_len = pair.second.size() + 1;
    strncpy(vals_p, pair.second.c_str(), val_len);
    vals_p += val_len;
  }
  *keys_p = '\0';
  *keys_len = total_key_len;

  *vals_p = '\0';
  *vals_len = total_val_len;
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_application_metadata_list);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_pool_list)(
  rados_t cluster,
  char *buf,
  size_t len)
{
  tracepoint(librados, rados_pool_list_enter, cluster, len);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  std::list<std::pair<int64_t, std::string> > pools;
  int r = client->pool_list(pools);
  if (r < 0) {
    tracepoint(librados, rados_pool_list_exit, r);
    return r;
  }

  if (len > 0 && !buf) {
    tracepoint(librados, rados_pool_list_exit, -EINVAL);
    return -EINVAL;
  }

  char *b = buf;
  if (b) {
    // FIPS zeroization audit 20191116: this memset is not security related.
    memset(b, 0, len);
  }
  int needed = 0;
  std::list<std::pair<int64_t, std::string> >::const_iterator i = pools.begin();
  std::list<std::pair<int64_t, std::string> >::const_iterator p_end =
    pools.end();
  for (; i != p_end; ++i) {
    int rl = i->second.length() + 1;
    if (len < (unsigned)rl)
      break;
    const char* pool = i->second.c_str();
    tracepoint(librados, rados_pool_list_pool, pool);
    if (b) {
      strncat(b, pool, rl);
      b += rl;
    }
    needed += rl;
    len -= rl;
  }
  for (; i != p_end; ++i) {
    int rl = i->second.length() + 1;
    needed += rl;
  }
  int retval = needed + 1;
  tracepoint(librados, rados_pool_list_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_pool_list);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_inconsistent_pg_list)(
  rados_t cluster,
  int64_t pool_id,
  char *buf,
  size_t len)
{
  tracepoint(librados, rados_inconsistent_pg_list_enter, cluster, pool_id, len);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  std::vector<std::string> pgs;
  if (int r = client->get_inconsistent_pgs(pool_id, &pgs); r < 0) {
    tracepoint(librados, rados_inconsistent_pg_list_exit, r);
    return r;
  }

  if (len > 0 && !buf) {
    tracepoint(librados, rados_inconsistent_pg_list_exit, -EINVAL);
    return -EINVAL;
  }

  char *b = buf;
  if (b) {
    // FIPS zeroization audit 20191116: this memset is not security related.
    memset(b, 0, len);
  }
  int needed = 0;
  for (const auto& s : pgs) {
    unsigned rl = s.length() + 1;
    if (b && len >= rl) {
      tracepoint(librados, rados_inconsistent_pg_list_pg, s.c_str());
      strncat(b, s.c_str(), rl);
      b += rl;
      len -= rl;
    }
    needed += rl;
  }
  int retval = needed + 1;
  tracepoint(librados, rados_inconsistent_pg_list_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_inconsistent_pg_list);


static void dict_to_map(const char *dict,
                        std::map<std::string, std::string>* dict_map)
{
  while (*dict != '\0') {
    const char* key = dict;
    dict += strlen(key) + 1;
    const char* value = dict;
    dict += strlen(value) + 1;
    (*dict_map)[key] = value;
  }
}

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_service_register)(
  rados_t cluster,
  const char *service,
  const char *daemon,
  const char *metadata_dict)
{
  librados::RadosClient *client = (librados::RadosClient *)cluster;

  std::map<std::string, std::string> metadata;
  dict_to_map(metadata_dict, &metadata);

  return client->service_daemon_register(service, daemon, metadata);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_service_register);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_service_update_status)(
  rados_t cluster,
  const char *status_dict)
{
  librados::RadosClient *client = (librados::RadosClient *)cluster;

  std::map<std::string, std::string> status;
  dict_to_map(status_dict, &status);

  return client->service_daemon_update_status(std::move(status));
}
LIBRADOS_C_API_BASE_DEFAULT(rados_service_update_status);

static void do_out_buffer(bufferlist& outbl, char **outbuf, size_t *outbuflen)
{
  if (outbuf) {
    if (outbl.length() > 0) {
      *outbuf = (char *)malloc(outbl.length());
      memcpy(*outbuf, outbl.c_str(), outbl.length());
    } else {
      *outbuf = NULL;
    }
  }
  if (outbuflen)
    *outbuflen = outbl.length();
}

static void do_out_buffer(string& outbl, char **outbuf, size_t *outbuflen)
{
  if (outbuf) {
    if (outbl.length() > 0) {
      *outbuf = (char *)malloc(outbl.length());
      memcpy(*outbuf, outbl.c_str(), outbl.length());
    } else {
      *outbuf = NULL;
    }
  }
  if (outbuflen)
    *outbuflen = outbl.length();
}

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ping_monitor)(
  rados_t cluster,
  const char *mon_id,
  char **outstr,
  size_t *outstrlen)
{
  tracepoint(librados, rados_ping_monitor_enter, cluster, mon_id);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  string str;

  if (!mon_id) {
    tracepoint(librados, rados_ping_monitor_exit, -EINVAL, NULL, NULL);
    return -EINVAL;
  }

  int ret = client->ping_monitor(mon_id, &str);
  if (ret == 0) {
    do_out_buffer(str, outstr, outstrlen);
  }
  tracepoint(librados, rados_ping_monitor_exit, ret, ret < 0 ? NULL : outstr, ret < 0 ? NULL : outstrlen);
  return ret;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ping_monitor);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_mon_command)(
  rados_t cluster,
  const char **cmd, size_t cmdlen,
  const char *inbuf, size_t inbuflen,
  char **outbuf, size_t *outbuflen,
  char **outs, size_t *outslen)
{
  tracepoint(librados, rados_mon_command_enter, cluster, cmdlen, inbuf, inbuflen);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  bufferlist inbl;
  bufferlist outbl;
  string outstring;
  vector<string> cmdvec;

  for (size_t i = 0; i < cmdlen; i++) {
    tracepoint(librados, rados_mon_command_cmd, cmd[i]);
    cmdvec.push_back(cmd[i]);
  }

  inbl.append(inbuf, inbuflen);
  int ret = client->mon_command(cmdvec, inbl, &outbl, &outstring);

  do_out_buffer(outbl, outbuf, outbuflen);
  do_out_buffer(outstring, outs, outslen);
  tracepoint(librados, rados_mon_command_exit, ret, outbuf, outbuflen, outs, outslen);
  return ret;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_mon_command);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_mon_command_target)(
  rados_t cluster,
  const char *name,
  const char **cmd, size_t cmdlen,
  const char *inbuf, size_t inbuflen,
  char **outbuf, size_t *outbuflen,
  char **outs, size_t *outslen)
{
  tracepoint(librados, rados_mon_command_target_enter, cluster, name, cmdlen, inbuf, inbuflen);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  bufferlist inbl;
  bufferlist outbl;
  string outstring;
  vector<string> cmdvec;

  // is this a numeric id?
  char *endptr;
  errno = 0;
  long rank = strtol(name, &endptr, 10);
  if ((errno == ERANGE && (rank == LONG_MAX || rank == LONG_MIN)) ||
      (errno != 0 && rank == 0) ||
      endptr == name ||    // no digits
      *endptr != '\0') {   // extra characters
    rank = -1;
  }

  for (size_t i = 0; i < cmdlen; i++) {
    tracepoint(librados, rados_mon_command_target_cmd, cmd[i]);
    cmdvec.push_back(cmd[i]);
  }

  inbl.append(inbuf, inbuflen);
  int ret;
  if (rank >= 0)
    ret = client->mon_command(rank, cmdvec, inbl, &outbl, &outstring);
  else
    ret = client->mon_command(name, cmdvec, inbl, &outbl, &outstring);

  do_out_buffer(outbl, outbuf, outbuflen);
  do_out_buffer(outstring, outs, outslen);
  tracepoint(librados, rados_mon_command_target_exit, ret, outbuf, outbuflen, outs, outslen);
  return ret;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_mon_command_target);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_osd_command)(
  rados_t cluster, int osdid, const char **cmd,
  size_t cmdlen,
  const char *inbuf, size_t inbuflen,
  char **outbuf, size_t *outbuflen,
  char **outs, size_t *outslen)
{
  tracepoint(librados, rados_osd_command_enter, cluster, osdid, cmdlen, inbuf, inbuflen);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  bufferlist inbl;
  bufferlist outbl;
  string outstring;
  vector<string> cmdvec;

  for (size_t i = 0; i < cmdlen; i++) {
    tracepoint(librados, rados_osd_command_cmd, cmd[i]);
    cmdvec.push_back(cmd[i]);
  }

  inbl.append(inbuf, inbuflen);
  int ret = client->osd_command(osdid, cmdvec, inbl, &outbl, &outstring);

  do_out_buffer(outbl, outbuf, outbuflen);
  do_out_buffer(outstring, outs, outslen);
  tracepoint(librados, rados_osd_command_exit, ret, outbuf, outbuflen, outs, outslen);
  return ret;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_osd_command);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_mgr_command)(
  rados_t cluster, const char **cmd,
  size_t cmdlen,
  const char *inbuf, size_t inbuflen,
  char **outbuf, size_t *outbuflen,
  char **outs, size_t *outslen)
{
  tracepoint(librados, rados_mgr_command_enter, cluster, cmdlen, inbuf,
      inbuflen);

  librados::RadosClient *client = (librados::RadosClient *)cluster;
  bufferlist inbl;
  bufferlist outbl;
  string outstring;
  vector<string> cmdvec;

  for (size_t i = 0; i < cmdlen; i++) {
    tracepoint(librados, rados_mgr_command_cmd, cmd[i]);
    cmdvec.push_back(cmd[i]);
  }

  inbl.append(inbuf, inbuflen);
  int ret = client->mgr_command(cmdvec, inbl, &outbl, &outstring);

  do_out_buffer(outbl, outbuf, outbuflen);
  do_out_buffer(outstring, outs, outslen);
  tracepoint(librados, rados_mgr_command_exit, ret, outbuf, outbuflen, outs,
      outslen);
  return ret;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_mgr_command);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_mgr_command_target)(
  rados_t cluster,
  const char *name,
  const char **cmd,
  size_t cmdlen,
  const char *inbuf, size_t inbuflen,
  char **outbuf, size_t *outbuflen,
  char **outs, size_t *outslen)
{
  tracepoint(librados, rados_mgr_command_target_enter, cluster, name, cmdlen,
	     inbuf, inbuflen);

  librados::RadosClient *client = (librados::RadosClient *)cluster;
  bufferlist inbl;
  bufferlist outbl;
  string outstring;
  vector<string> cmdvec;

  for (size_t i = 0; i < cmdlen; i++) {
    tracepoint(librados, rados_mgr_command_target_cmd, cmd[i]);
    cmdvec.push_back(cmd[i]);
  }

  inbl.append(inbuf, inbuflen);
  int ret = client->mgr_command(name, cmdvec, inbl, &outbl, &outstring);

  do_out_buffer(outbl, outbuf, outbuflen);
  do_out_buffer(outstring, outs, outslen);
  tracepoint(librados, rados_mgr_command_target_exit, ret, outbuf, outbuflen,
	     outs, outslen);
  return ret;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_mgr_command_target);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_pg_command)(
  rados_t cluster, const char *pgstr,
  const char **cmd, size_t cmdlen,
  const char *inbuf, size_t inbuflen,
  char **outbuf, size_t *outbuflen,
  char **outs, size_t *outslen)
{
  tracepoint(librados, rados_pg_command_enter, cluster, pgstr, cmdlen, inbuf, inbuflen);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  bufferlist inbl;
  bufferlist outbl;
  string outstring;
  pg_t pgid;
  vector<string> cmdvec;

  for (size_t i = 0; i < cmdlen; i++) {
    tracepoint(librados, rados_pg_command_cmd, cmd[i]);
    cmdvec.push_back(cmd[i]);
  }

  inbl.append(inbuf, inbuflen);
  if (!pgid.parse(pgstr))
    return -EINVAL;

  int ret = client->pg_command(pgid, cmdvec, inbl, &outbl, &outstring);

  do_out_buffer(outbl, outbuf, outbuflen);
  do_out_buffer(outstring, outs, outslen);
  tracepoint(librados, rados_pg_command_exit, ret, outbuf, outbuflen, outs, outslen);
  return ret;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_pg_command);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_buffer_free)(char *buf)
{
  tracepoint(librados, rados_buffer_free_enter, buf);
  if (buf)
    free(buf);
  tracepoint(librados, rados_buffer_free_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_buffer_free);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_monitor_log)(
  rados_t cluster,
  const char *level,
  rados_log_callback_t cb,
  void *arg)
{
  tracepoint(librados, rados_monitor_log_enter, cluster, level, cb, arg);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  int retval = client->monitor_log(level, cb, nullptr, arg);
  tracepoint(librados, rados_monitor_log_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_monitor_log);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_monitor_log2)(
  rados_t cluster,
  const char *level,
	rados_log_callback2_t cb,
  void *arg)
{
  tracepoint(librados, rados_monitor_log2_enter, cluster, level, cb, arg);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  int retval = client->monitor_log(level, nullptr, cb, arg);
  tracepoint(librados, rados_monitor_log2_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_monitor_log2);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_create)(
  rados_t cluster,
  const char *name,
  rados_ioctx_t *io)
{
  tracepoint(librados, rados_ioctx_create_enter, cluster, name);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  librados::IoCtxImpl *ctx;

  int r = client->create_ioctx(name, &ctx);
  if (r < 0) {
    tracepoint(librados, rados_ioctx_create_exit, r, NULL);
    return r;
  }

  *io = ctx;
  ctx->get();
  tracepoint(librados, rados_ioctx_create_exit, 0, ctx);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_create);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_create2)(
  rados_t cluster,
  int64_t pool_id,
  rados_ioctx_t *io)
{
  tracepoint(librados, rados_ioctx_create2_enter, cluster, pool_id);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  librados::IoCtxImpl *ctx;

  int r = client->create_ioctx(pool_id, &ctx);
  if (r < 0) {
    tracepoint(librados, rados_ioctx_create2_exit, r, NULL);
    return r;
  }

  *io = ctx;
  ctx->get();
  tracepoint(librados, rados_ioctx_create2_exit, 0, ctx);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_create2);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_ioctx_destroy)(rados_ioctx_t io)
{
  tracepoint(librados, rados_ioctx_destroy_enter, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  if (ctx) {
    ctx->put();
  }
  tracepoint(librados, rados_ioctx_destroy_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_destroy);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_pool_stat)(
  rados_ioctx_t io,
  struct rados_pool_stat_t *stats)
{
  tracepoint(librados, rados_ioctx_pool_stat_enter, io);
  librados::IoCtxImpl *io_ctx_impl = (librados::IoCtxImpl *)io;
  list<string> ls;
  std::string pool_name;

  int err = io_ctx_impl->client->pool_get_name(io_ctx_impl->get_id(), &pool_name);
  if (err) {
    tracepoint(librados, rados_ioctx_pool_stat_exit, err, stats);
    return err;
  }
  ls.push_back(pool_name);

  map<string, ::pool_stat_t> rawresult;
  bool per_pool = false;
  err = io_ctx_impl->client->get_pool_stats(ls, &rawresult, &per_pool);
  if (err) {
    tracepoint(librados, rados_ioctx_pool_stat_exit, err, stats);
    return err;
  }

  ::pool_stat_t& r = rawresult[pool_name];
  uint64_t allocated_bytes = r.get_allocated_data_bytes(per_pool) +
    r.get_allocated_omap_bytes(per_pool);
  // FIXME: raw_used_rate is unknown hence use 1.0 here
  // meaning we keep net amount aggregated over all replicas
  // Not a big deal so far since this field isn't exposed
  uint64_t user_bytes = r.get_user_data_bytes(1.0, per_pool) +
    r.get_user_omap_bytes(1.0, per_pool);

  stats->num_kb = shift_round_up(allocated_bytes, 10);
  stats->num_bytes = allocated_bytes;
  stats->num_objects = r.stats.sum.num_objects;
  stats->num_object_clones = r.stats.sum.num_object_clones;
  stats->num_object_copies = r.stats.sum.num_object_copies;
  stats->num_objects_missing_on_primary = r.stats.sum.num_objects_missing_on_primary;
  stats->num_objects_unfound = r.stats.sum.num_objects_unfound;
  stats->num_objects_degraded =
    r.stats.sum.num_objects_degraded +
    r.stats.sum.num_objects_misplaced; // FIXME: this is imprecise
  stats->num_rd = r.stats.sum.num_rd;
  stats->num_rd_kb = r.stats.sum.num_rd_kb;
  stats->num_wr = r.stats.sum.num_wr;
  stats->num_wr_kb = r.stats.sum.num_wr_kb;
  stats->num_user_bytes = user_bytes;
  stats->compressed_bytes_orig = r.store_stats.data_compressed_original;
  stats->compressed_bytes = r.store_stats.data_compressed;
  stats->compressed_bytes_alloc = r.store_stats.data_compressed_allocated;

  tracepoint(librados, rados_ioctx_pool_stat_exit, 0, stats);
  return 0;
}
LIBRADOS_C_API_DEFAULT(rados_ioctx_pool_stat, 14.2.0);

extern "C" int LIBRADOS_C_API_BASE_F(rados_ioctx_pool_stat)(
    rados_ioctx_t io, struct __librados_base::rados_pool_stat_t *stats)
{
  struct rados_pool_stat_t new_stats;
  int r = LIBRADOS_C_API_DEFAULT_F(rados_ioctx_pool_stat)(io, &new_stats);
  if (r < 0) {
    return r;
  }

  stats->num_bytes = new_stats.num_bytes;
  stats->num_kb = new_stats.num_kb;
  stats->num_objects = new_stats.num_objects;
  stats->num_object_clones = new_stats.num_object_clones;
  stats->num_object_copies = new_stats.num_object_copies;
  stats->num_objects_missing_on_primary = new_stats.num_objects_missing_on_primary;
  stats->num_objects_unfound = new_stats.num_objects_unfound;
  stats->num_objects_degraded = new_stats.num_objects_degraded;
  stats->num_rd = new_stats.num_rd;
  stats->num_rd_kb = new_stats.num_rd_kb;
  stats->num_wr = new_stats.num_wr;
  stats->num_wr_kb = new_stats.num_wr_kb;
  return 0;
}
LIBRADOS_C_API_BASE(rados_ioctx_pool_stat);

extern "C" rados_config_t LIBRADOS_C_API_DEFAULT_F(rados_ioctx_cct)(
  rados_ioctx_t io)
{
  tracepoint(librados, rados_ioctx_cct_enter, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  rados_config_t retval = (rados_config_t)ctx->client->cct;
  tracepoint(librados, rados_ioctx_cct_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_cct);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_ioctx_snap_set_read)(
  rados_ioctx_t io,
  rados_snap_t seq)
{
  tracepoint(librados, rados_ioctx_snap_set_read_enter, io, seq);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  ctx->set_snap_read((snapid_t)seq);
  tracepoint(librados, rados_ioctx_snap_set_read_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_snap_set_read);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_selfmanaged_snap_set_write_ctx)(
  rados_ioctx_t io,
  rados_snap_t seq,
  rados_snap_t *snaps,
  int num_snaps)
{
  tracepoint(librados, rados_ioctx_selfmanaged_snap_set_write_ctx_enter, io, seq, snaps, num_snaps);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  vector<snapid_t> snv;
  snv.resize(num_snaps);
  for (int i=0; i<num_snaps; i++) {
    snv[i] = (snapid_t)snaps[i];
  }
  int retval = ctx->set_snap_write_context((snapid_t)seq, snv);
  tracepoint(librados, rados_ioctx_selfmanaged_snap_set_write_ctx_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_selfmanaged_snap_set_write_ctx);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_write)(
  rados_ioctx_t io,
  const char *o,
  const char *buf,
  size_t len,
  uint64_t off)
{
  tracepoint(librados, rados_write_enter, io, o, buf, len, off);
  if (len > UINT_MAX/2)
    return -E2BIG;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  int retval = ctx->write(oid, bl, len, off);
  tracepoint(librados, rados_write_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_append)(
  rados_ioctx_t io,
  const char *o,
  const char *buf,
  size_t len)
{
  tracepoint(librados, rados_append_enter, io, o, buf, len);
  if (len > UINT_MAX/2)
    return -E2BIG;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  int retval = ctx->append(oid, bl, len);
  tracepoint(librados, rados_append_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_append);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_write_full)(
  rados_ioctx_t io,
  const char *o,
  const char *buf,
  size_t len)
{
  tracepoint(librados, rados_write_full_enter, io, o, buf, len);
  if (len > UINT_MAX/2)
    return -E2BIG;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  int retval = ctx->write_full(oid, bl);
  tracepoint(librados, rados_write_full_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_full);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_writesame)(
  rados_ioctx_t io,
  const char *o,
  const char *buf,
  size_t data_len,
  size_t write_len,
  uint64_t off)
{
  tracepoint(librados, rados_writesame_enter, io, o, buf, data_len, write_len, off);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, data_len);
  int retval = ctx->writesame(oid, bl, write_len, off);
  tracepoint(librados, rados_writesame_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_writesame);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_trunc)(
  rados_ioctx_t io,
  const char *o,
  uint64_t size)
{
  tracepoint(librados, rados_trunc_enter, io, o, size);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->trunc(oid, size);
  tracepoint(librados, rados_trunc_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_trunc);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_remove)(
  rados_ioctx_t io,
  const char *o)
{
  tracepoint(librados, rados_remove_enter, io, o);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->remove(oid);
  tracepoint(librados, rados_remove_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_remove);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_read)(
  rados_ioctx_t io,
  const char *o,
  char *buf,
  size_t len,
  uint64_t off)
{
  tracepoint(librados, rados_read_enter, io, o, buf, len, off);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int ret;
  object_t oid(o);

  bufferlist bl;
  bufferptr bp = buffer::create_static(len, buf);
  bl.push_back(bp);

  ret = ctx->read(oid, bl, len, off);
  if (ret >= 0) {
    if (bl.length() > len) {
      tracepoint(librados, rados_read_exit, -ERANGE, NULL);
      return -ERANGE;
    }
    if (!bl.is_provided_buffer(buf))
      bl.begin().copy(bl.length(), buf);
    ret = bl.length();    // hrm :/
  }

  tracepoint(librados, rados_read_exit, ret, buf);
  return ret;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_read);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_checksum)(
  rados_ioctx_t io, const char *o,
  rados_checksum_type_t type,
  const char *init_value, size_t init_value_len,
  size_t len, uint64_t off, size_t chunk_size,
  char *pchecksum, size_t checksum_len)
{
  tracepoint(librados, rados_checksum_enter, io, o, type, init_value,
	     init_value_len, len, off, chunk_size);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);

  bufferlist init_value_bl;
  init_value_bl.append(init_value, init_value_len);

  bufferlist checksum_bl;

  int retval = ctx->checksum(oid, get_checksum_op_type(type), init_value_bl,
			     len, off, chunk_size, &checksum_bl);
  if (retval >= 0) {
    if (checksum_bl.length() > checksum_len) {
      tracepoint(librados, rados_checksum_exit, -ERANGE, NULL, 0);
      return -ERANGE;
    }

    checksum_bl.begin().copy(checksum_bl.length(), pchecksum);
  }
  tracepoint(librados, rados_checksum_exit, retval, pchecksum, checksum_len);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_checksum);

extern "C" uint64_t LIBRADOS_C_API_DEFAULT_F(rados_get_last_version)(
  rados_ioctx_t io)
{
  tracepoint(librados, rados_get_last_version_enter, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  uint64_t retval = ctx->last_version();
  tracepoint(librados, rados_get_last_version_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_get_last_version);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_pool_create)(
  rados_t cluster,
  const char *name)
{
  tracepoint(librados, rados_pool_create_enter, cluster, name);
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  string sname(name);
  int retval = radosp->pool_create(sname);
  tracepoint(librados, rados_pool_create_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_pool_create);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_pool_create_with_auid)(
  rados_t cluster,
  const char *name,
  uint64_t auid)
{
  tracepoint(librados, rados_pool_create_with_auid_enter, cluster, name, auid);
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  string sname(name);
  int retval = 0;
  if (auid != CEPH_AUTH_UID_DEFAULT) {
    retval = -EINVAL;
  } else {
    retval = radosp->pool_create(sname);
  }
  tracepoint(librados, rados_pool_create_with_auid_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_pool_create_with_auid);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_pool_create_with_crush_rule)(
  rados_t cluster,
  const char *name,
  __u8 crush_rule_num)
{
  tracepoint(librados, rados_pool_create_with_crush_rule_enter, cluster, name, crush_rule_num);
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  string sname(name);
  int retval = radosp->pool_create(sname, crush_rule_num);
  tracepoint(librados, rados_pool_create_with_crush_rule_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_pool_create_with_crush_rule);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_pool_create_with_all)(
  rados_t cluster,
  const char *name,
  uint64_t auid,
  __u8 crush_rule_num)
{
  tracepoint(librados, rados_pool_create_with_all_enter, cluster, name, auid, crush_rule_num);
  librados::RadosClient *radosp = (librados::RadosClient *)cluster;
  string sname(name);
  int retval = 0;
  if (auid != CEPH_AUTH_UID_DEFAULT) {
    retval = -EINVAL;
  } else {
    retval = radosp->pool_create(sname, crush_rule_num);
  }
  tracepoint(librados, rados_pool_create_with_all_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_pool_create_with_all);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_pool_get_base_tier)(
  rados_t cluster,
  int64_t pool_id,
  int64_t* base_tier)
{
  tracepoint(librados, rados_pool_get_base_tier_enter, cluster, pool_id);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  int retval = client->pool_get_base_tier(pool_id, base_tier);
  tracepoint(librados, rados_pool_get_base_tier_exit, retval, *base_tier);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_pool_get_base_tier);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_pool_delete)(
  rados_t cluster,
  const char *pool_name)
{
  tracepoint(librados, rados_pool_delete_enter, cluster, pool_name);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  int retval = client->pool_delete(pool_name);
  tracepoint(librados, rados_pool_delete_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_pool_delete);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_pool_set_auid)(
  rados_ioctx_t io,
  uint64_t auid)
{
  tracepoint(librados, rados_ioctx_pool_set_auid_enter, io, auid);
  int retval = -EOPNOTSUPP;
  tracepoint(librados, rados_ioctx_pool_set_auid_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_pool_set_auid);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_pool_get_auid)(
  rados_ioctx_t io,
  uint64_t *auid)
{
  tracepoint(librados, rados_ioctx_pool_get_auid_enter, io);
  int retval = -EOPNOTSUPP;
  tracepoint(librados, rados_ioctx_pool_get_auid_exit, retval, *auid);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_pool_get_auid);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_pool_requires_alignment)(
  rados_ioctx_t io)
{
  tracepoint(librados, rados_ioctx_pool_requires_alignment_enter, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->client->pool_requires_alignment(ctx->get_id());
  tracepoint(librados, rados_ioctx_pool_requires_alignment_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_pool_requires_alignment);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_pool_requires_alignment2)(
  rados_ioctx_t io,
  int *req)
{
  tracepoint(librados, rados_ioctx_pool_requires_alignment_enter2, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  bool requires_alignment;
  int retval = ctx->client->pool_requires_alignment2(ctx->get_id(), 
  	&requires_alignment);
  tracepoint(librados, rados_ioctx_pool_requires_alignment_exit2, retval, 
  	requires_alignment);
  if (req)
    *req = requires_alignment;
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_pool_requires_alignment2);

extern "C" uint64_t LIBRADOS_C_API_DEFAULT_F(rados_ioctx_pool_required_alignment)(
  rados_ioctx_t io)
{
  tracepoint(librados, rados_ioctx_pool_required_alignment_enter, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  uint64_t retval = ctx->client->pool_required_alignment(ctx->get_id());
  tracepoint(librados, rados_ioctx_pool_required_alignment_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_pool_required_alignment);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_pool_required_alignment2)(
  rados_ioctx_t io,
  uint64_t *alignment)
{
  tracepoint(librados, rados_ioctx_pool_required_alignment_enter2, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->client->pool_required_alignment2(ctx->get_id(),
  	alignment);
  tracepoint(librados, rados_ioctx_pool_required_alignment_exit2, retval, 
  	*alignment);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_pool_required_alignment2);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_ioctx_locator_set_key)(
  rados_ioctx_t io,
  const char *key)
{
  tracepoint(librados, rados_ioctx_locator_set_key_enter, io, key);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  if (key)
    ctx->oloc.key = key;
  else
    ctx->oloc.key = "";
  tracepoint(librados, rados_ioctx_locator_set_key_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_locator_set_key);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_ioctx_set_namespace)(
  rados_ioctx_t io,
  const char *nspace)
{
  tracepoint(librados, rados_ioctx_set_namespace_enter, io, nspace);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  if (nspace)
    ctx->oloc.nspace = nspace;
  else
    ctx->oloc.nspace = "";
  tracepoint(librados, rados_ioctx_set_namespace_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_set_namespace);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_get_namespace)(
  rados_ioctx_t io,
  char *s,
  unsigned maxlen)
{
  tracepoint(librados, rados_ioctx_get_namespace_enter, io, maxlen);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  auto length = ctx->oloc.nspace.length();
  if (length >= maxlen) {
    tracepoint(librados, rados_ioctx_get_namespace_exit, -ERANGE, "");
    return -ERANGE;
  }
  strcpy(s, ctx->oloc.nspace.c_str());
  int retval = (int)length;
  tracepoint(librados, rados_ioctx_get_namespace_exit, retval, s);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_get_namespace);

extern "C" rados_t LIBRADOS_C_API_DEFAULT_F(rados_ioctx_get_cluster)(
  rados_ioctx_t io)
{
  tracepoint(librados, rados_ioctx_get_cluster_enter, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  rados_t retval = (rados_t)ctx->client;
  tracepoint(librados, rados_ioctx_get_cluster_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_get_cluster);

extern "C" int64_t LIBRADOS_C_API_DEFAULT_F(rados_ioctx_get_id)(
  rados_ioctx_t io)
{
  tracepoint(librados, rados_ioctx_get_id_enter, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int64_t retval = ctx->get_id();
  tracepoint(librados, rados_ioctx_get_id_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_get_id);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_get_pool_name)(
  rados_ioctx_t io,
  char *s,
  unsigned maxlen)
{
  tracepoint(librados, rados_ioctx_get_pool_name_enter, io, maxlen);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  std::string pool_name;

  int err = ctx->client->pool_get_name(ctx->get_id(), &pool_name);
  if (err) {
    tracepoint(librados, rados_ioctx_get_pool_name_exit, err, "");
    return err;
  }
  if (pool_name.length() >= maxlen) {
    tracepoint(librados, rados_ioctx_get_pool_name_exit, -ERANGE, "");
    return -ERANGE;
  }
  strcpy(s, pool_name.c_str());
  int retval = pool_name.length();
  tracepoint(librados, rados_ioctx_get_pool_name_exit, retval, s);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_get_pool_name);

// snaps

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_snap_create)(
  rados_ioctx_t io,
  const char *snapname)
{
  tracepoint(librados, rados_ioctx_snap_create_enter, io, snapname);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->snap_create(snapname);
  tracepoint(librados, rados_ioctx_snap_create_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_snap_create);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_snap_remove)(
  rados_ioctx_t io,
  const char *snapname)
{
  tracepoint(librados, rados_ioctx_snap_remove_enter, io, snapname);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->snap_remove(snapname);
  tracepoint(librados, rados_ioctx_snap_remove_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_snap_remove);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_snap_rollback)(
  rados_ioctx_t io,
  const char *oid,
  const char *snapname)
{
  tracepoint(librados, rados_ioctx_snap_rollback_enter, io, oid, snapname);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->rollback(oid, snapname);
  tracepoint(librados, rados_ioctx_snap_rollback_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_snap_rollback);

// Deprecated name kept for backward compatibility
extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_rollback)(
  rados_ioctx_t io,
  const char *oid,
  const char *snapname)
{
  return LIBRADOS_C_API_DEFAULT_F(rados_ioctx_snap_rollback)(io, oid, snapname);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_rollback);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_selfmanaged_snap_create)(
  rados_ioctx_t io,
  uint64_t *snapid)
{
  tracepoint(librados, rados_ioctx_selfmanaged_snap_create_enter, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->selfmanaged_snap_create(snapid);
  tracepoint(librados, rados_ioctx_selfmanaged_snap_create_exit, retval, *snapid);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_selfmanaged_snap_create);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_aio_ioctx_selfmanaged_snap_create)(
  rados_ioctx_t io,
  rados_snap_t *snapid,
  rados_completion_t completion)
{
  tracepoint(librados, rados_ioctx_selfmanaged_snap_create_enter, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  librados::AioCompletionImpl *c = (librados::AioCompletionImpl*)completion;
  ctx->aio_selfmanaged_snap_create(snapid, c);
  tracepoint(librados, rados_ioctx_selfmanaged_snap_create_exit, 0, 0);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_ioctx_selfmanaged_snap_create);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_selfmanaged_snap_remove)(
  rados_ioctx_t io,
  uint64_t snapid)
{
  tracepoint(librados, rados_ioctx_selfmanaged_snap_remove_enter, io, snapid);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->selfmanaged_snap_remove(snapid);
  tracepoint(librados, rados_ioctx_selfmanaged_snap_remove_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_selfmanaged_snap_remove);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_aio_ioctx_selfmanaged_snap_remove)(
  rados_ioctx_t io,
  rados_snap_t snapid,
  rados_completion_t completion)
{
  tracepoint(librados, rados_ioctx_selfmanaged_snap_remove_enter, io, snapid);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  librados::AioCompletionImpl *c = (librados::AioCompletionImpl*)completion;
  ctx->aio_selfmanaged_snap_remove(snapid, c);
  tracepoint(librados, rados_ioctx_selfmanaged_snap_remove_exit, 0);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_ioctx_selfmanaged_snap_remove);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_selfmanaged_snap_rollback)(
  rados_ioctx_t io,
  const char *oid,
  uint64_t snapid)
{
  tracepoint(librados, rados_ioctx_selfmanaged_snap_rollback_enter, io, oid, snapid);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->selfmanaged_snap_rollback_object(oid, ctx->snapc, snapid);
  tracepoint(librados, rados_ioctx_selfmanaged_snap_rollback_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_selfmanaged_snap_rollback);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_snap_list)(
  rados_ioctx_t io,
  rados_snap_t *snaps,
  int maxlen)
{
  tracepoint(librados, rados_ioctx_snap_list_enter, io, maxlen);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  vector<uint64_t> snapvec;
  int r = ctx->snap_list(&snapvec);
  if (r < 0) {
    tracepoint(librados, rados_ioctx_snap_list_exit, r, snaps, 0);
    return r;
  }
  if ((int)snapvec.size() <= maxlen) {
    for (unsigned i=0; i<snapvec.size(); i++) {
      snaps[i] = snapvec[i];
    }
    int retval = snapvec.size();
    tracepoint(librados, rados_ioctx_snap_list_exit, retval, snaps, retval);
    return retval;
  }
  int retval = -ERANGE;
  tracepoint(librados, rados_ioctx_snap_list_exit, retval, snaps, 0);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_snap_list);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_snap_lookup)(
  rados_ioctx_t io,
  const char *name,
  rados_snap_t *id)
{
  tracepoint(librados, rados_ioctx_snap_lookup_enter, io, name);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->snap_lookup(name, (uint64_t *)id);
  tracepoint(librados, rados_ioctx_snap_lookup_exit, retval, *id);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_snap_lookup);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_snap_get_name)(
  rados_ioctx_t io,
  rados_snap_t id,
  char *name,
  int maxlen)
{
  tracepoint(librados, rados_ioctx_snap_get_name_enter, io, id, maxlen);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  std::string sname;
  int r = ctx->snap_get_name(id, &sname);
  if (r < 0) {
    tracepoint(librados, rados_ioctx_snap_get_name_exit, r, "");
    return r;
  }
  if ((int)sname.length() >= maxlen) {
    int retval = -ERANGE;
    tracepoint(librados, rados_ioctx_snap_get_name_exit, retval, "");
    return retval;
  }
  strncpy(name, sname.c_str(), maxlen);
  tracepoint(librados, rados_ioctx_snap_get_name_exit, 0, name);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_snap_get_name);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_snap_get_stamp)(
  rados_ioctx_t io,
  rados_snap_t id,
  time_t *t)
{
  tracepoint(librados, rados_ioctx_snap_get_stamp_enter, io, id);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->snap_get_stamp(id, t);
  tracepoint(librados, rados_ioctx_snap_get_stamp_exit, retval, *t);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_snap_get_stamp);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_cmpext)(
  rados_ioctx_t io,
  const char *o,
  const char *cmp_buf,
  size_t cmp_len,
  uint64_t off)
{
  tracepoint(librados, rados_cmpext_enter, io, o, cmp_buf, cmp_len, off);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int ret;
  object_t oid(o);

  bufferlist cmp_bl;
  cmp_bl.append(cmp_buf, cmp_len);

  ret = ctx->cmpext(oid, off, cmp_bl);
  tracepoint(librados, rados_cmpext_exit, ret);

  return ret;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_cmpext);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_getxattr)(
  rados_ioctx_t io,
  const char *o,
  const char *name,
  char *buf,
  size_t len)
{
  tracepoint(librados, rados_getxattr_enter, io, o, name, len);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int ret;
  object_t oid(o);
  bufferlist bl;
  bl.push_back(buffer::create_static(len, buf));
  ret = ctx->getxattr(oid, name, bl);
  if (ret >= 0) {
    if (bl.length() > len) {
      tracepoint(librados, rados_getxattr_exit, -ERANGE, buf, 0);
      return -ERANGE;
    }
    if (!bl.is_provided_buffer(buf))
      bl.begin().copy(bl.length(), buf);
    ret = bl.length();
  }

  tracepoint(librados, rados_getxattr_exit, ret, buf, ret);
  return ret;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_getxattr);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_getxattrs)(
  rados_ioctx_t io,
  const char *oid,
  rados_xattrs_iter_t *iter)
{
  tracepoint(librados, rados_getxattrs_enter, io, oid);
  librados::RadosXattrsIter *it = new librados::RadosXattrsIter();
  if (!it) {
    tracepoint(librados, rados_getxattrs_exit, -ENOMEM, NULL);
    return -ENOMEM;
  }
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t obj(oid);
  int ret = ctx->getxattrs(obj, it->attrset);
  if (ret) {
    delete it;
    tracepoint(librados, rados_getxattrs_exit, ret, NULL);
    return ret;
  }
  it->i = it->attrset.begin();

  *iter = it;
  tracepoint(librados, rados_getxattrs_exit, 0, *iter);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_getxattrs);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_getxattrs_next)(
  rados_xattrs_iter_t iter,
  const char **name,
  const char **val,
  size_t *len)
{
  tracepoint(librados, rados_getxattrs_next_enter, iter);
  librados::RadosXattrsIter *it = static_cast<librados::RadosXattrsIter*>(iter);
  if (it->val) {
    free(it->val);
    it->val = NULL;
  }
  if (it->i == it->attrset.end()) {
    *name = NULL;
    *val = NULL;
    *len = 0;
    tracepoint(librados, rados_getxattrs_next_exit, 0, NULL, NULL, 0);
    return 0;
  }
  const std::string &s(it->i->first);
  *name = s.c_str();
  bufferlist &bl(it->i->second);
  size_t bl_len = bl.length();
  if (!bl_len) {
    // malloc(0) is not guaranteed to return a valid pointer
    *val = (char *)NULL;
  } else {
    it->val = (char*)malloc(bl_len);
    if (!it->val) {
      tracepoint(librados, rados_getxattrs_next_exit, -ENOMEM, *name, NULL, 0);
      return -ENOMEM;
    }
    memcpy(it->val, bl.c_str(), bl_len);
    *val = it->val;
  }
  *len = bl_len;
  ++it->i;
  tracepoint(librados, rados_getxattrs_next_exit, 0, *name, *val, *len);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_getxattrs_next);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_getxattrs_end)(
  rados_xattrs_iter_t iter)
{
  tracepoint(librados, rados_getxattrs_end_enter, iter);
  librados::RadosXattrsIter *it = static_cast<librados::RadosXattrsIter*>(iter);
  delete it;
  tracepoint(librados, rados_getxattrs_end_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_getxattrs_end);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_setxattr)(
  rados_ioctx_t io,
  const char *o,
  const char *name,
  const char *buf,
  size_t len)
{
  tracepoint(librados, rados_setxattr_enter, io, o, name, buf, len);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  int retval = ctx->setxattr(oid, name, bl);
  tracepoint(librados, rados_setxattr_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_setxattr);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_rmxattr)(
  rados_ioctx_t io,
  const char *o,
  const char *name)
{
  tracepoint(librados, rados_rmxattr_enter, io, o, name);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->rmxattr(oid, name);
  tracepoint(librados, rados_rmxattr_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_rmxattr);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_stat)(
  rados_ioctx_t io,
  const char *o,
  uint64_t *psize,
  time_t *pmtime)
{
  tracepoint(librados, rados_stat_enter, io, o);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->stat(oid, psize, pmtime);
  tracepoint(librados, rados_stat_exit, retval, psize, pmtime);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_stat);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_stat2)(
  rados_ioctx_t io,
  const char *o,
  uint64_t *psize,
  struct timespec *pmtime)
{
  tracepoint(librados, rados_stat2_enter, io, o);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->stat2(oid, psize, pmtime);
  tracepoint(librados, rados_stat2_exit, retval, psize, pmtime);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_stat2);

extern "C" int LIBRADOS_C_API_BASE_F(rados_tmap_update)(
  rados_ioctx_t io,
  const char *o,
  const char *cmdbuf,
  size_t cmdbuflen)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist cmdbl;
  cmdbl.append(cmdbuf, cmdbuflen);
  return ctx->tmap_update(oid, cmdbl);
}
LIBRADOS_C_API_BASE(rados_tmap_update);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_tmap_update)(
  rados_ioctx_t io,
  const char *o,
  const char *cmdbuf,
  size_t cmdbuflen)
{
  return -ENOTSUP;
}
LIBRADOS_C_API_DEFAULT(rados_tmap_update, 14.2.0);

extern "C" int LIBRADOS_C_API_BASE_F(rados_tmap_put)(
  rados_ioctx_t io,
  const char *o,
  const char *buf,
  size_t buflen)
{
  bufferlist bl;
  bl.append(buf, buflen);

  bufferlist header;
  std::map<std::string, bufferlist> m;
  bufferlist::const_iterator bl_it = bl.begin();
  decode(header, bl_it);
  decode(m, bl_it);

  bufferlist out_bl;
  encode(header, out_bl);
  encode(m, out_bl);

  return LIBRADOS_C_API_DEFAULT_F(rados_write_full)(
    io, o, out_bl.c_str(), out_bl.length());
}
LIBRADOS_C_API_BASE(rados_tmap_put);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_tmap_put)(
  rados_ioctx_t io,
  const char *o,
  const char *buf,
  size_t buflen)
{
  return -EOPNOTSUPP;
}
LIBRADOS_C_API_DEFAULT(rados_tmap_put, 14.2.0);

extern "C" int LIBRADOS_C_API_BASE_F(rados_tmap_get)(
  rados_ioctx_t io,
  const char *o,
  char *buf,
  size_t buflen)
{
  return LIBRADOS_C_API_DEFAULT_F(rados_read)(io, o, buf, buflen, 0);
}
LIBRADOS_C_API_BASE(rados_tmap_get);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_tmap_get)(
  rados_ioctx_t io,
  const char *o,
  char *buf,
  size_t buflen)
{
  return -EOPNOTSUPP;
}
LIBRADOS_C_API_DEFAULT(rados_tmap_get, 14.2.0);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_exec)(
  rados_ioctx_t io,
  const char *o,
  const char *cls,
  const char *method,
  const char *inbuf,
  size_t in_len,
  char *buf,
  size_t out_len)
{
  tracepoint(librados, rados_exec_enter, io, o, cls, method, inbuf, in_len, out_len);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist inbl, outbl;
  int ret;
  inbl.append(inbuf, in_len);
  ret = ctx->exec(oid, cls, method, inbl, outbl);
  if (ret >= 0) {
    if (outbl.length()) {
      if (outbl.length() > out_len) {
	tracepoint(librados, rados_exec_exit, -ERANGE, buf, 0);
	return -ERANGE;
      }
      outbl.begin().copy(outbl.length(), buf);
      ret = outbl.length();   // hrm :/
    }
  }
  tracepoint(librados, rados_exec_exit, ret, buf, ret);
  return ret;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_exec);

extern "C" rados_object_list_cursor LIBRADOS_C_API_DEFAULT_F(rados_object_list_begin)(
  rados_ioctx_t io)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;

  hobject_t *result = new hobject_t(ctx->objecter->enumerate_objects_begin());
  return (rados_object_list_cursor)result;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_object_list_begin);

extern "C" rados_object_list_cursor LIBRADOS_C_API_DEFAULT_F(rados_object_list_end)(
  rados_ioctx_t io)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;

  hobject_t *result = new hobject_t(ctx->objecter->enumerate_objects_end());
  return (rados_object_list_cursor)result;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_object_list_end);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_object_list_is_end)(
  rados_ioctx_t io,
  rados_object_list_cursor cur)
{
  hobject_t *hobj = (hobject_t*)cur;
  return hobj->is_max();
}
LIBRADOS_C_API_BASE_DEFAULT(rados_object_list_is_end);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_object_list_cursor_free)(
  rados_ioctx_t io,
  rados_object_list_cursor cur)
{
  hobject_t *hobj = (hobject_t*)cur;
  delete hobj;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_object_list_cursor_free);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_object_list_cursor_cmp)(
  rados_ioctx_t io,
  rados_object_list_cursor lhs_cur,
  rados_object_list_cursor rhs_cur)
{
  hobject_t *lhs = (hobject_t*)lhs_cur;
  hobject_t *rhs = (hobject_t*)rhs_cur;
  return cmp(*lhs, *rhs);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_object_list_cursor_cmp);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_object_list)(rados_ioctx_t io,
  const rados_object_list_cursor start,
  const rados_object_list_cursor finish,
  const size_t result_item_count,
  const char *filter_buf,
  const size_t filter_buf_len,
  rados_object_list_item *result_items,
  rados_object_list_cursor *next)
{
  ceph_assert(next);

  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;

  // Zero out items so that they will be safe to free later
  // FIPS zeroization audit 20191116: this memset is not security related.
  memset(result_items, 0, sizeof(rados_object_list_item) * result_item_count);

  bufferlist filter_bl;
  if (filter_buf != nullptr) {
    filter_bl.append(filter_buf, filter_buf_len);
  }

  ceph::async::waiter<boost::system::error_code,
		      std::vector<librados::ListObjectImpl>,
		      hobject_t> w;
  ctx->objecter->enumerate_objects<librados::ListObjectImpl>(
      ctx->poolid,
      ctx->oloc.nspace,
      *((hobject_t*)start),
      *((hobject_t*)finish),
      result_item_count,
      filter_bl,
      w);

  hobject_t *next_hobj = (hobject_t*)(*next);
  ceph_assert(next_hobj);

  auto [ec, result, next_hash] = w.wait();

  if (ec) {
    *next_hobj = hobject_t::get_max();
    return ceph::from_error_code(ec);
  }

  ceph_assert(result.size() <= result_item_count);  // Don't overflow!

  int k = 0;
  for (auto i = result.begin(); i != result.end(); ++i) {
    rados_object_list_item &item = result_items[k++];
    do_out_buffer(i->oid, &item.oid, &item.oid_length);
    do_out_buffer(i->nspace, &item.nspace, &item.nspace_length);
    do_out_buffer(i->locator, &item.locator, &item.locator_length);
  }

  *next_hobj = next_hash;

  return result.size();
}
LIBRADOS_C_API_BASE_DEFAULT(rados_object_list);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_object_list_free)(
  const size_t result_size,
  rados_object_list_item *results)
{
  ceph_assert(results);

  for (unsigned int i = 0; i < result_size; ++i) {
    LIBRADOS_C_API_DEFAULT_F(rados_buffer_free)(results[i].oid);
    LIBRADOS_C_API_DEFAULT_F(rados_buffer_free)(results[i].locator);
    LIBRADOS_C_API_DEFAULT_F(rados_buffer_free)(results[i].nspace);
  }
}
LIBRADOS_C_API_BASE_DEFAULT(rados_object_list_free);

/* list objects */

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_nobjects_list_open)(
  rados_ioctx_t io,
  rados_list_ctx_t *listh)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;

  tracepoint(librados, rados_nobjects_list_open_enter, io);

  Objecter::NListContext *h = new Objecter::NListContext;
  h->pool_id = ctx->poolid;
  h->pool_snap_seq = ctx->snap_seq;
  h->nspace = ctx->oloc.nspace;	// After dropping compatibility need nspace
  *listh = (void *)new librados::ObjListCtx(ctx, h);
  tracepoint(librados, rados_nobjects_list_open_exit, 0, *listh);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_nobjects_list_open);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_nobjects_list_close)(
  rados_list_ctx_t h)
{
  tracepoint(librados, rados_nobjects_list_close_enter, h);
  librados::ObjListCtx *lh = (librados::ObjListCtx *)h;
  delete lh;
  tracepoint(librados, rados_nobjects_list_close_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_nobjects_list_close);

extern "C" uint32_t LIBRADOS_C_API_DEFAULT_F(rados_nobjects_list_seek)(
  rados_list_ctx_t listctx,
  uint32_t pos)
{
  librados::ObjListCtx *lh = (librados::ObjListCtx *)listctx;
  tracepoint(librados, rados_nobjects_list_seek_enter, listctx, pos);
  uint32_t r = lh->ctx->nlist_seek(lh->nlc, pos);
  tracepoint(librados, rados_nobjects_list_seek_exit, r);
  return r;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_nobjects_list_seek);

extern "C" uint32_t LIBRADOS_C_API_DEFAULT_F(rados_nobjects_list_seek_cursor)(
  rados_list_ctx_t listctx,
  rados_object_list_cursor cursor)
{
  librados::ObjListCtx *lh = (librados::ObjListCtx *)listctx;

  tracepoint(librados, rados_nobjects_list_seek_cursor_enter, listctx);
  uint32_t r = lh->ctx->nlist_seek(lh->nlc, cursor);
  tracepoint(librados, rados_nobjects_list_seek_cursor_exit, r);
  return r;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_nobjects_list_seek_cursor);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_nobjects_list_get_cursor)(
  rados_list_ctx_t listctx,
  rados_object_list_cursor *cursor)
{
  librados::ObjListCtx *lh = (librados::ObjListCtx *)listctx;

  tracepoint(librados, rados_nobjects_list_get_cursor_enter, listctx);
  *cursor = lh->ctx->nlist_get_cursor(lh->nlc);
  tracepoint(librados, rados_nobjects_list_get_cursor_exit, 0);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_nobjects_list_get_cursor);

extern "C" uint32_t LIBRADOS_C_API_DEFAULT_F(rados_nobjects_list_get_pg_hash_position)(
  rados_list_ctx_t listctx)
{
  librados::ObjListCtx *lh = (librados::ObjListCtx *)listctx;
  tracepoint(librados, rados_nobjects_list_get_pg_hash_position_enter, listctx);
  uint32_t retval = lh->nlc->get_pg_hash_position();
  tracepoint(librados, rados_nobjects_list_get_pg_hash_position_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_nobjects_list_get_pg_hash_position);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_nobjects_list_next)(
  rados_list_ctx_t listctx,
  const char **entry,
  const char **key,
  const char **nspace)
{
  tracepoint(librados, rados_nobjects_list_next_enter, listctx);
  uint32_t retval = rados_nobjects_list_next2(listctx, entry, key, nspace, NULL, NULL, NULL);
  tracepoint(librados, rados_nobjects_list_next_exit, 0, *entry, key, nspace);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_nobjects_list_next);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_nobjects_list_next2)(
  rados_list_ctx_t listctx,
  const char **entry,
  const char **key,
  const char **nspace,
  size_t *entry_size,
  size_t *key_size,
  size_t *nspace_size)
{
  tracepoint(librados, rados_nobjects_list_next2_enter, listctx);
  librados::ObjListCtx *lh = (librados::ObjListCtx *)listctx;
  Objecter::NListContext *h = lh->nlc;

  // if the list is non-empty, this method has been called before
  if (!h->list.empty())
    // so let's kill the previously-returned object
    h->list.pop_front();

  if (h->list.empty()) {
    int ret = lh->ctx->nlist(lh->nlc, RADOS_LIST_MAX_ENTRIES);
    if (ret < 0) {
      tracepoint(librados, rados_nobjects_list_next2_exit, ret, NULL, NULL, NULL, NULL, NULL, NULL);
      return ret;
    }
    if (h->list.empty()) {
      tracepoint(librados, rados_nobjects_list_next2_exit, -ENOENT, NULL, NULL, NULL, NULL, NULL, NULL);
      return -ENOENT;
    }
  }

  *entry = h->list.front().oid.c_str();

  if (key) {
    if (h->list.front().locator.size())
      *key = h->list.front().locator.c_str();
    else
      *key = NULL;
  }
  if (nspace)
    *nspace = h->list.front().nspace.c_str();

  if (entry_size)
    *entry_size = h->list.front().oid.size();
  if (key_size)
    *key_size = h->list.front().locator.size();
  if (nspace_size)
    *nspace_size = h->list.front().nspace.size();

  tracepoint(librados, rados_nobjects_list_next2_exit, 0, entry, key, nspace,
             entry_size, key_size, nspace_size);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_nobjects_list_next2);


/*
 * removed legacy v2 list objects stubs
 *
 * thse return -ENOTSUP where possible.
 */
extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_objects_list_open)(
  rados_ioctx_t io,
  rados_list_ctx_t *ctx)
{
  return -ENOTSUP;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_objects_list_open);

extern "C" uint32_t LIBRADOS_C_API_DEFAULT_F(rados_objects_list_get_pg_hash_position)(
  rados_list_ctx_t ctx)
{
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_objects_list_get_pg_hash_position);

extern "C" uint32_t LIBRADOS_C_API_DEFAULT_F(rados_objects_list_seek)(
  rados_list_ctx_t ctx,
  uint32_t pos)
{
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_objects_list_seek);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_objects_list_next)(
  rados_list_ctx_t ctx,
  const char **entry,
  const char **key)
{
  return -ENOTSUP;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_objects_list_next);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_objects_list_close)(
  rados_list_ctx_t ctx)
{
}
LIBRADOS_C_API_BASE_DEFAULT(rados_objects_list_close);


// -------------------------
// aio

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_create_completion)(
  void *cb_arg,
  rados_callback_t cb_complete,
  rados_callback_t cb_safe,
  rados_completion_t *pc)
{
  tracepoint(librados, rados_aio_create_completion_enter, cb_arg, cb_complete, cb_safe);
  librados::AioCompletionImpl *c = new librados::AioCompletionImpl;
  if (cb_complete)
    c->set_complete_callback(cb_arg, cb_complete);
  if (cb_safe)
    c->set_safe_callback(cb_arg, cb_safe);
  *pc = c;
  tracepoint(librados, rados_aio_create_completion_exit, 0, *pc);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_create_completion);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_create_completion2)(
  void *cb_arg,
  rados_callback_t cb_complete,
  rados_completion_t *pc)
{
  tracepoint(librados, rados_aio_create_completion2_enter, cb_arg, cb_complete);
  librados::AioCompletionImpl *c = new librados::AioCompletionImpl;
  if (cb_complete)
    c->set_complete_callback(cb_arg, cb_complete);
  *pc = c;
  tracepoint(librados, rados_aio_create_completion2_exit, 0, *pc);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_create_completion2);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_wait_for_complete)(
  rados_completion_t c)
{
  tracepoint(librados, rados_aio_wait_for_complete_enter, c);
  int retval = ((librados::AioCompletionImpl*)c)->wait_for_complete();
  tracepoint(librados, rados_aio_wait_for_complete_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_wait_for_complete);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_wait_for_safe)(
  rados_completion_t c)
{
  tracepoint(librados, rados_aio_wait_for_safe_enter, c);
  int retval = ((librados::AioCompletionImpl*)c)->wait_for_complete();
  tracepoint(librados, rados_aio_wait_for_safe_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_wait_for_safe);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_is_complete)(
  rados_completion_t c)
{
  tracepoint(librados, rados_aio_is_complete_enter, c);
  int retval = ((librados::AioCompletionImpl*)c)->is_complete();
  tracepoint(librados, rados_aio_is_complete_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_is_complete);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_is_safe)(
  rados_completion_t c)
{
  tracepoint(librados, rados_aio_is_safe_enter, c);
  int retval = ((librados::AioCompletionImpl*)c)->is_safe();
  tracepoint(librados, rados_aio_is_safe_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_is_safe);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_wait_for_complete_and_cb)(
  rados_completion_t c)
{
  tracepoint(librados, rados_aio_wait_for_complete_and_cb_enter, c);
  int retval = ((librados::AioCompletionImpl*)c)->wait_for_complete_and_cb();
  tracepoint(librados, rados_aio_wait_for_complete_and_cb_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_wait_for_complete_and_cb);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_wait_for_safe_and_cb)(
  rados_completion_t c)
{
  tracepoint(librados, rados_aio_wait_for_safe_and_cb_enter, c);
  int retval = ((librados::AioCompletionImpl*)c)->wait_for_safe_and_cb();
  tracepoint(librados, rados_aio_wait_for_safe_and_cb_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_wait_for_safe_and_cb);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_is_complete_and_cb)(
  rados_completion_t c)
{
  tracepoint(librados, rados_aio_is_complete_and_cb_enter, c);
  int retval = ((librados::AioCompletionImpl*)c)->is_complete_and_cb();
  tracepoint(librados, rados_aio_is_complete_and_cb_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_is_complete_and_cb);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_is_safe_and_cb)(
  rados_completion_t c)
{
  tracepoint(librados, rados_aio_is_safe_and_cb_enter, c);
  int retval = ((librados::AioCompletionImpl*)c)->is_safe_and_cb();
  tracepoint(librados, rados_aio_is_safe_and_cb_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_is_safe_and_cb);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_get_return_value)(
  rados_completion_t c)
{
  tracepoint(librados, rados_aio_get_return_value_enter, c);
  int retval = ((librados::AioCompletionImpl*)c)->get_return_value();
  tracepoint(librados, rados_aio_get_return_value_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_get_return_value);

extern "C" uint64_t LIBRADOS_C_API_DEFAULT_F(rados_aio_get_version)(
  rados_completion_t c)
{
  tracepoint(librados, rados_aio_get_version_enter, c);
  uint64_t retval = ((librados::AioCompletionImpl*)c)->get_version();
  tracepoint(librados, rados_aio_get_version_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_get_version);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_aio_release)(
  rados_completion_t c)
{
  tracepoint(librados, rados_aio_release_enter, c);
  ((librados::AioCompletionImpl*)c)->put();
  tracepoint(librados, rados_aio_release_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_release);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_read)(
  rados_ioctx_t io, const char *o,
  rados_completion_t completion,
  char *buf, size_t len, uint64_t off)
{
  tracepoint(librados, rados_aio_read_enter, io, o, completion, len, off);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->aio_read(oid, (librados::AioCompletionImpl*)completion,
		       buf, len, off, ctx->snap_seq);
  tracepoint(librados, rados_aio_read_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_read);

#ifdef WITH_BLKIN
extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_read_traced)(
  rados_ioctx_t io, const char *o,
  rados_completion_t completion,
  char *buf, size_t len, uint64_t off,
  struct blkin_trace_info *info)
{
  tracepoint(librados, rados_aio_read_enter, io, o, completion, len, off);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->aio_read(oid, (librados::AioCompletionImpl*)completion,
                             buf, len, off, ctx->snap_seq, info);
  tracepoint(librados, rados_aio_read_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_read_traced);
#endif

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_write)(
  rados_ioctx_t io, const char *o,
  rados_completion_t completion,
  const char *buf, size_t len, uint64_t off)
{
  tracepoint(librados, rados_aio_write_enter, io, o, completion, buf, len, off);
  if (len > UINT_MAX/2)
    return -E2BIG;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  int retval = ctx->aio_write(oid, (librados::AioCompletionImpl*)completion,
			bl, len, off);
  tracepoint(librados, rados_aio_write_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_write);

#ifdef WITH_BLKIN
extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_write_traced)(
  rados_ioctx_t io, const char *o,
  rados_completion_t completion,
  const char *buf, size_t len, uint64_t off,
  struct blkin_trace_info *info)
{
  tracepoint(librados, rados_aio_write_enter, io, o, completion, buf, len, off);
  if (len > UINT_MAX/2)
    return -E2BIG;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  int retval = ctx->aio_write(oid, (librados::AioCompletionImpl*)completion,
                              bl, len, off, info);
  tracepoint(librados, rados_aio_write_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_write_traced);
#endif

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_append)(
  rados_ioctx_t io, const char *o,
  rados_completion_t completion,
  const char *buf, size_t len)
{
  tracepoint(librados, rados_aio_append_enter, io, o, completion, buf, len);
  if (len > UINT_MAX/2)
    return -E2BIG;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  int retval = ctx->aio_append(oid, (librados::AioCompletionImpl*)completion,
			 bl, len);
  tracepoint(librados, rados_aio_append_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_append);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_write_full)(
  rados_ioctx_t io, const char *o,
  rados_completion_t completion,
  const char *buf, size_t len)
{
  tracepoint(librados, rados_aio_write_full_enter, io, o, completion, buf, len);
  if (len > UINT_MAX/2)
    return -E2BIG;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  int retval = ctx->aio_write_full(oid, (librados::AioCompletionImpl*)completion, bl);
  tracepoint(librados, rados_aio_write_full_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_write_full);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_writesame)(
  rados_ioctx_t io, const char *o,
  rados_completion_t completion,
  const char *buf, size_t data_len,
  size_t write_len, uint64_t off)
{
  tracepoint(librados, rados_aio_writesame_enter, io, o, completion, buf,
						data_len, write_len, off);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, data_len);
  int retval = ctx->aio_writesame(o, (librados::AioCompletionImpl*)completion,
				  bl, write_len, off);
  tracepoint(librados, rados_aio_writesame_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_writesame);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_remove)(
  rados_ioctx_t io, const char *o,
  rados_completion_t completion)
{
  tracepoint(librados, rados_aio_remove_enter, io, o, completion);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->aio_remove(oid, (librados::AioCompletionImpl*)completion);
  tracepoint(librados, rados_aio_remove_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_remove);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_flush_async)(
  rados_ioctx_t io,
  rados_completion_t completion)
{
  tracepoint(librados, rados_aio_flush_async_enter, io, completion);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  ctx->flush_aio_writes_async((librados::AioCompletionImpl*)completion);
  tracepoint(librados, rados_aio_flush_async_exit, 0);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_flush_async);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_flush)(rados_ioctx_t io)
{
  tracepoint(librados, rados_aio_flush_enter, io);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  ctx->flush_aio_writes();
  tracepoint(librados, rados_aio_flush_exit, 0);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_flush);

struct AioGetxattrData {
  AioGetxattrData(char* buf, rados_completion_t c, size_t l) :
    user_buf(buf), len(l), user_completion((librados::AioCompletionImpl*)c) {}
  bufferlist bl;
  char* user_buf;
  size_t len;
  struct librados::CB_AioCompleteAndSafe user_completion;
};

static void rados_aio_getxattr_complete(rados_completion_t c, void *arg) {
  AioGetxattrData *cdata = reinterpret_cast<AioGetxattrData*>(arg);
  int rc = LIBRADOS_C_API_DEFAULT_F(rados_aio_get_return_value)(c);
  if (rc >= 0) {
    if (cdata->bl.length() > cdata->len) {
      rc = -ERANGE;
    } else {
      if (!cdata->bl.is_provided_buffer(cdata->user_buf))
	cdata->bl.begin().copy(cdata->bl.length(), cdata->user_buf);
      rc = cdata->bl.length();
    }
  }
  cdata->user_completion(rc);
  reinterpret_cast<librados::AioCompletionImpl*>(c)->put();
  delete cdata;
}

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_getxattr)(
  rados_ioctx_t io, const char *o,
  rados_completion_t completion,
  const char *name, char *buf, size_t len)
{
  tracepoint(librados, rados_aio_getxattr_enter, io, o, completion, name, len);
  // create data object to be passed to async callback
  AioGetxattrData *cdata = new AioGetxattrData(buf, completion, len);
  if (!cdata) {
    tracepoint(librados, rados_aio_getxattr_exit, -ENOMEM, NULL, 0);
    return -ENOMEM;
  }
  cdata->bl.push_back(buffer::create_static(len, buf));
  // create completion callback
  librados::AioCompletionImpl *c = new librados::AioCompletionImpl;
  c->set_complete_callback(cdata, rados_aio_getxattr_complete);
  // call async getxattr of IoCtx
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int ret = ctx->aio_getxattr(oid, c, name, cdata->bl);
  tracepoint(librados, rados_aio_getxattr_exit, ret, buf, ret);
  return ret;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_getxattr);

namespace {
struct AioGetxattrsData {
  AioGetxattrsData(rados_completion_t c, rados_xattrs_iter_t *_iter) :
    iter(_iter), user_completion((librados::AioCompletionImpl*)c) {
    it = new librados::RadosXattrsIter();
  }
  ~AioGetxattrsData() {
    if (it) delete it;
  }
  librados::RadosXattrsIter *it;
  rados_xattrs_iter_t *iter;
  struct librados::CB_AioCompleteAndSafe user_completion;
};
}

static void rados_aio_getxattrs_complete(rados_completion_t c, void *arg) {
  AioGetxattrsData *cdata = reinterpret_cast<AioGetxattrsData*>(arg);
  int rc = LIBRADOS_C_API_DEFAULT_F(rados_aio_get_return_value)(c);
  if (rc) {
    cdata->user_completion(rc);
  } else {
    cdata->it->i = cdata->it->attrset.begin();
    *cdata->iter = cdata->it;
    cdata->it = 0;
    cdata->user_completion(0);
  }
  reinterpret_cast<librados::AioCompletionImpl*>(c)->put();
  delete cdata;
}

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_getxattrs)(
  rados_ioctx_t io, const char *oid,
  rados_completion_t completion,
  rados_xattrs_iter_t *iter)
{
  tracepoint(librados, rados_aio_getxattrs_enter, io, oid, completion);
  // create data object to be passed to async callback
  AioGetxattrsData *cdata = new AioGetxattrsData(completion, iter);
  if (!cdata) {
    tracepoint(librados, rados_getxattrs_exit, -ENOMEM, NULL);
    return -ENOMEM;
  }
  // create completion callback
  librados::AioCompletionImpl *c = new librados::AioCompletionImpl;
  c->set_complete_callback(cdata, rados_aio_getxattrs_complete);
  // call async getxattrs of IoCtx
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t obj(oid);
  int ret = ctx->aio_getxattrs(obj, c, cdata->it->attrset);
  tracepoint(librados, rados_aio_getxattrs_exit, ret, cdata->it);
  return ret;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_getxattrs);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_setxattr)(
  rados_ioctx_t io, const char *o,
  rados_completion_t completion,
  const char *name, const char *buf, size_t len)
{
  tracepoint(librados, rados_aio_setxattr_enter, io, o, completion, name, buf, len);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  bl.append(buf, len);
  int retval = ctx->aio_setxattr(oid, (librados::AioCompletionImpl*)completion, name, bl);
  tracepoint(librados, rados_aio_setxattr_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_setxattr);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_rmxattr)(
  rados_ioctx_t io, const char *o,
  rados_completion_t completion,
  const char *name)
{
  tracepoint(librados, rados_aio_rmxattr_enter, io, o, completion, name);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->aio_rmxattr(oid, (librados::AioCompletionImpl*)completion, name);
  tracepoint(librados, rados_aio_rmxattr_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_rmxattr);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_stat)(
  rados_ioctx_t io, const char *o,
  rados_completion_t completion,
  uint64_t *psize, time_t *pmtime)
{
  tracepoint(librados, rados_aio_stat_enter, io, o, completion);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->aio_stat(oid, (librados::AioCompletionImpl*)completion,
		       psize, pmtime);
  tracepoint(librados, rados_aio_stat_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_stat);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_stat2)(
  rados_ioctx_t io, const char *o,
  rados_completion_t completion,
  uint64_t *psize, struct timespec *pmtime)
{
  tracepoint(librados, rados_aio_stat2_enter, io, o, completion);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->aio_stat2(oid, (librados::AioCompletionImpl*)completion,
		       psize, pmtime);
  tracepoint(librados, rados_aio_stat2_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_stat2);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_cmpext)(
  rados_ioctx_t io, const char *o,
  rados_completion_t completion, const char *cmp_buf,
  size_t cmp_len, uint64_t off)
{
  tracepoint(librados, rados_aio_cmpext_enter, io, o, completion, cmp_buf,
	     cmp_len, off);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->aio_cmpext(oid, (librados::AioCompletionImpl*)completion,
			       cmp_buf, cmp_len, off);
  tracepoint(librados, rados_aio_cmpext_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_cmpext);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_cancel)(
  rados_ioctx_t io,
  rados_completion_t completion)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  return ctx->aio_cancel((librados::AioCompletionImpl*)completion);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_cancel);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_exec)(
  rados_ioctx_t io, const char *o,
  rados_completion_t completion,
  const char *cls, const char *method,
  const char *inbuf, size_t in_len,
  char *buf, size_t out_len)
{
  tracepoint(librados, rados_aio_exec_enter, io, o, completion);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist inbl;
  inbl.append(inbuf, in_len);
  int retval = ctx->aio_exec(oid, (librados::AioCompletionImpl*)completion,
		       cls, method, inbl, buf, out_len);
  tracepoint(librados, rados_aio_exec_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_exec);

struct C_WatchCB : public librados::WatchCtx {
  rados_watchcb_t wcb;
  void *arg;
  C_WatchCB(rados_watchcb_t _wcb, void *_arg) : wcb(_wcb), arg(_arg) {}
  void notify(uint8_t opcode, uint64_t ver, bufferlist& bl) override {
    wcb(opcode, ver, arg);
  }
};

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_watch)(
  rados_ioctx_t io, const char *o, uint64_t ver,
  uint64_t *handle,
  rados_watchcb_t watchcb, void *arg)
{
  tracepoint(librados, rados_watch_enter, io, o, ver, watchcb, arg);
  uint64_t *cookie = handle;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  C_WatchCB *wc = new C_WatchCB(watchcb, arg);
  int retval = ctx->watch(oid, cookie, wc, NULL, true);
  tracepoint(librados, rados_watch_exit, retval, *handle);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_watch);

struct C_WatchCB2 : public librados::WatchCtx2 {
  rados_watchcb2_t wcb;
  rados_watcherrcb_t errcb;
  void *arg;
  C_WatchCB2(rados_watchcb2_t _wcb,
	     rados_watcherrcb_t _errcb,
	     void *_arg) : wcb(_wcb), errcb(_errcb), arg(_arg) {}
  void handle_notify(uint64_t notify_id,
		     uint64_t cookie,
		     uint64_t notifier_gid,
		     bufferlist& bl) override {
    wcb(arg, notify_id, cookie, notifier_gid, bl.c_str(), bl.length());
  }
  void handle_error(uint64_t cookie, int err) override {
    if (errcb)
      errcb(arg, cookie, err);
  }
};

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_watch3)(
  rados_ioctx_t io, const char *o, uint64_t *handle,
  rados_watchcb2_t watchcb,
  rados_watcherrcb_t watcherrcb,
  uint32_t timeout,
  void *arg)
{
  tracepoint(librados, rados_watch3_enter, io, o, handle, watchcb, timeout, arg);
  int ret;
  if (!watchcb || !o || !handle) {
    ret = -EINVAL;
  } else {
    uint64_t *cookie = handle;
    librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
    object_t oid(o);
    C_WatchCB2 *wc = new C_WatchCB2(watchcb, watcherrcb, arg);
    ret = ctx->watch(oid, cookie, NULL, wc, timeout, true);
  }
  tracepoint(librados, rados_watch3_exit, ret, handle ? *handle : 0);
  return ret;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_watch3);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_watch2)(
  rados_ioctx_t io, const char *o, uint64_t *handle,
  rados_watchcb2_t watchcb,
  rados_watcherrcb_t watcherrcb,
  void *arg)
{
  return LIBRADOS_C_API_DEFAULT_F(rados_watch3)(
    io, o, handle, watchcb, watcherrcb, 0, arg);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_watch2);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_watch2)(
  rados_ioctx_t io, const char *o,
  rados_completion_t completion,
  uint64_t *handle,
  rados_watchcb2_t watchcb,
  rados_watcherrcb_t watcherrcb,
  uint32_t timeout, void *arg)
{
  tracepoint(librados, rados_aio_watch2_enter, io, o, completion, handle, watchcb, timeout, arg);
  int ret;
  if (!completion || !watchcb || !o || !handle) {
    ret = -EINVAL;
  } else {
    uint64_t *cookie = handle;
    librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
    object_t oid(o);
    librados::AioCompletionImpl *c =
      reinterpret_cast<librados::AioCompletionImpl*>(completion);
    C_WatchCB2 *wc = new C_WatchCB2(watchcb, watcherrcb, arg);
    ret = ctx->aio_watch(oid, c, cookie, NULL, wc, timeout, true);
  }
  tracepoint(librados, rados_aio_watch2_exit, ret, handle ? *handle : 0);
  return ret;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_watch2);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_watch)(
  rados_ioctx_t io, const char *o,
  rados_completion_t completion,
  uint64_t *handle,
  rados_watchcb2_t watchcb,
  rados_watcherrcb_t watcherrcb, void *arg)
{
  return LIBRADOS_C_API_DEFAULT_F(rados_aio_watch2)(
    io, o, completion, handle, watchcb, watcherrcb, 0, arg);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_watch);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_unwatch)(
  rados_ioctx_t io,
  const char *o,
  uint64_t handle)
{
  tracepoint(librados, rados_unwatch_enter, io, o, handle);
  uint64_t cookie = handle;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->unwatch(cookie);
  tracepoint(librados, rados_unwatch_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_unwatch);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_unwatch2)(
  rados_ioctx_t io,
  uint64_t handle)
{
  tracepoint(librados, rados_unwatch2_enter, io, handle);
  uint64_t cookie = handle;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->unwatch(cookie);
  tracepoint(librados, rados_unwatch2_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_unwatch2);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_unwatch)(
  rados_ioctx_t io, uint64_t handle,
  rados_completion_t completion)
{
  tracepoint(librados, rados_aio_unwatch_enter, io, handle, completion);
  uint64_t cookie = handle;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  librados::AioCompletionImpl *c =
    reinterpret_cast<librados::AioCompletionImpl*>(completion);
  int retval = ctx->aio_unwatch(cookie, c);
  tracepoint(librados, rados_aio_unwatch_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_unwatch);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_watch_check)(
  rados_ioctx_t io,
  uint64_t handle)
{
  tracepoint(librados, rados_watch_check_enter, io, handle);
  uint64_t cookie = handle;
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->watch_check(cookie);
  tracepoint(librados, rados_watch_check_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_watch_check);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_notify)(
  rados_ioctx_t io, const char *o,
  uint64_t ver, const char *buf, int buf_len)
{
  tracepoint(librados, rados_notify_enter, io, o, ver, buf, buf_len);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  if (buf) {
    bufferptr p = buffer::create(buf_len);
    memcpy(p.c_str(), buf, buf_len);
    bl.push_back(p);
  }
  int retval = ctx->notify(oid, bl, 0, NULL, NULL, NULL);
  tracepoint(librados, rados_notify_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_notify);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_notify2)(
  rados_ioctx_t io, const char *o,
  const char *buf, int buf_len,
  uint64_t timeout_ms,
  char **reply_buffer,
  size_t *reply_buffer_len)
{
  tracepoint(librados, rados_notify2_enter, io, o, buf, buf_len, timeout_ms);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  if (buf) {
    bufferptr p = buffer::create(buf_len);
    memcpy(p.c_str(), buf, buf_len);
    bl.push_back(p);
  }
  int ret = ctx->notify(oid, bl, timeout_ms, NULL, reply_buffer, reply_buffer_len);
  tracepoint(librados, rados_notify2_exit, ret);
  return ret;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_notify2);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_decode_notify_response)(
  char *reply_buffer, size_t reply_buffer_len,
  struct notify_ack_t **acks, size_t *nr_acks,
  struct notify_timeout_t **timeouts, size_t *nr_timeouts)
{
  if (!reply_buffer || !reply_buffer_len) {
    return -EINVAL;
  }

  bufferlist bl;
  bl.append(reply_buffer, reply_buffer_len);

  map<pair<uint64_t,uint64_t>,bufferlist> acked;
  set<pair<uint64_t,uint64_t>> missed;
  auto iter = bl.cbegin();
  decode(acked, iter);
  decode(missed, iter);

  *acks = nullptr;
  *nr_acks = acked.size();
  if (*nr_acks) {
    *acks = new notify_ack_t[*nr_acks];
    struct notify_ack_t *ack = *acks;
    for (auto &[who, payload] : acked) {
      ack->notifier_id = who.first;
      ack->cookie = who.second;
      ack->payload = nullptr;
      ack->payload_len = payload.length();
      if (ack->payload_len) {
        ack->payload = (char *)malloc(ack->payload_len);
        memcpy(ack->payload, payload.c_str(), ack->payload_len);
      }

      ack++;
    }
  }

  *timeouts = nullptr;
  *nr_timeouts = missed.size();
  if (*nr_timeouts) {
    *timeouts = new notify_timeout_t[*nr_timeouts];
    struct notify_timeout_t *timeout = *timeouts;
    for (auto &[notifier_id, cookie] : missed) {
      timeout->notifier_id = notifier_id;
      timeout->cookie = cookie;
      timeout++;
    }
  }

  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_decode_notify_response);


extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_free_notify_response)(
  struct notify_ack_t *acks, size_t nr_acks,
  struct notify_timeout_t *timeouts)
{
  for (uint64_t n = 0; n < nr_acks; ++n) {
    assert(acks);
    if (acks[n].payload) {
      free(acks[n].payload);
    }
  }
  if (acks) {
    delete[] acks;
  }
  if (timeouts) {
    delete[] timeouts;
  }
}
LIBRADOS_C_API_BASE_DEFAULT(rados_free_notify_response);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_notify)(
  rados_ioctx_t io, const char *o,
  rados_completion_t completion,
  const char *buf, int buf_len,
  uint64_t timeout_ms, char **reply_buffer,
  size_t *reply_buffer_len)
{
  tracepoint(librados, rados_aio_notify_enter, io, o, completion, buf, buf_len,
             timeout_ms);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  if (buf) {
    bl.push_back(buffer::copy(buf, buf_len));
  }
  librados::AioCompletionImpl *c =
    reinterpret_cast<librados::AioCompletionImpl*>(completion);
  int ret = ctx->aio_notify(oid, c, bl, timeout_ms, NULL, reply_buffer,
                            reply_buffer_len);
  tracepoint(librados, rados_aio_notify_exit, ret);
  return ret;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_notify);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_notify_ack)(
  rados_ioctx_t io, const char *o,
  uint64_t notify_id, uint64_t handle,
  const char *buf, int buf_len)
{
  tracepoint(librados, rados_notify_ack_enter, io, o, notify_id, handle, buf, buf_len);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  bufferlist bl;
  if (buf) {
    bufferptr p = buffer::create(buf_len);
    memcpy(p.c_str(), buf, buf_len);
    bl.push_back(p);
  }
  ctx->notify_ack(oid, notify_id, handle, bl);
  tracepoint(librados, rados_notify_ack_exit, 0);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_notify_ack);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_watch_flush)(rados_t cluster)
{
  tracepoint(librados, rados_watch_flush_enter, cluster);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  int retval = client->watch_flush();
  tracepoint(librados, rados_watch_flush_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_watch_flush);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_watch_flush)(
  rados_t cluster,
  rados_completion_t completion)
{
  tracepoint(librados, rados_aio_watch_flush_enter, cluster, completion);
  librados::RadosClient *client = (librados::RadosClient *)cluster;
  librados::AioCompletionImpl *c = (librados::AioCompletionImpl*)completion;
  int retval = client->async_watch_flush(c);
  tracepoint(librados, rados_aio_watch_flush_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_watch_flush);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_set_alloc_hint)(
  rados_ioctx_t io, const char *o,
  uint64_t expected_object_size,
  uint64_t expected_write_size)
{
  tracepoint(librados, rados_set_alloc_hint_enter, io, o, expected_object_size, expected_write_size);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->set_alloc_hint(oid, expected_object_size,
				   expected_write_size, 0);
  tracepoint(librados, rados_set_alloc_hint_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_set_alloc_hint);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_set_alloc_hint2)(
  rados_ioctx_t io, const char *o,
  uint64_t expected_object_size,
  uint64_t expected_write_size,
  uint32_t flags)
{
  tracepoint(librados, rados_set_alloc_hint2_enter, io, o, expected_object_size, expected_write_size, flags);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->set_alloc_hint(oid, expected_object_size,
				   expected_write_size, flags);
  tracepoint(librados, rados_set_alloc_hint2_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_set_alloc_hint2);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_lock_exclusive)(
  rados_ioctx_t io, const char * o,
  const char * name, const char * cookie,
  const char * desc,
  struct timeval * duration, uint8_t flags)
{
  tracepoint(librados, rados_lock_exclusive_enter, io, o, name, cookie, desc, duration, flags);
  librados::IoCtx ctx;
  librados::IoCtx::from_rados_ioctx_t(io, ctx);

  int retval = ctx.lock_exclusive(o, name, cookie, desc, duration, flags);
  tracepoint(librados, rados_lock_exclusive_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_lock_exclusive);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_lock_shared)(
  rados_ioctx_t io, const char * o,
  const char * name, const char * cookie,
  const char * tag, const char * desc,
  struct timeval * duration, uint8_t flags)
{
  tracepoint(librados, rados_lock_shared_enter, io, o, name, cookie, tag, desc, duration, flags);
  librados::IoCtx ctx;
  librados::IoCtx::from_rados_ioctx_t(io, ctx);

  int retval = ctx.lock_shared(o, name, cookie, tag, desc, duration, flags);
  tracepoint(librados, rados_lock_shared_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_lock_shared);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_unlock)(
  rados_ioctx_t io, const char *o, const char *name,
  const char *cookie)
{
  tracepoint(librados, rados_unlock_enter, io, o, name, cookie);
  librados::IoCtx ctx;
  librados::IoCtx::from_rados_ioctx_t(io, ctx);

  int retval = ctx.unlock(o, name, cookie);
  tracepoint(librados, rados_unlock_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_unlock);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_unlock)(
  rados_ioctx_t io, const char *o, const char *name,
  const char *cookie, rados_completion_t completion)
{
  tracepoint(librados, rados_aio_unlock_enter, io, o, name, cookie, completion);
  librados::IoCtx ctx;
  librados::IoCtx::from_rados_ioctx_t(io, ctx);
  librados::AioCompletionImpl *comp = (librados::AioCompletionImpl*)completion;
  comp->get();
  librados::AioCompletion c(comp);
  int retval = ctx.aio_unlock(o, name, cookie, &c);
  tracepoint(librados, rados_aio_unlock_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_unlock);

extern "C" ssize_t LIBRADOS_C_API_DEFAULT_F(rados_list_lockers)(
  rados_ioctx_t io, const char *o,
  const char *name, int *exclusive,
  char *tag, size_t *tag_len,
  char *clients, size_t *clients_len,
  char *cookies, size_t *cookies_len,
  char *addrs, size_t *addrs_len)
{
  tracepoint(librados, rados_list_lockers_enter, io, o, name, *tag_len, *clients_len, *cookies_len, *addrs_len);
  librados::IoCtx ctx;
  librados::IoCtx::from_rados_ioctx_t(io, ctx);
  std::string name_str = name;
  std::string oid = o;
  std::string tag_str;
  int tmp_exclusive;
  std::list<librados::locker_t> lockers;
  int r = ctx.list_lockers(oid, name_str, &tmp_exclusive, &tag_str, &lockers);
  if (r < 0) {
    tracepoint(librados, rados_list_lockers_exit, r, *exclusive, "", *tag_len, *clients_len, *cookies_len, *addrs_len);
	  return r;
  }

  size_t clients_total = 0;
  size_t cookies_total = 0;
  size_t addrs_total = 0;
  list<librados::locker_t>::const_iterator it;
  for (it = lockers.begin(); it != lockers.end(); ++it) {
    clients_total += it->client.length() + 1;
    cookies_total += it->cookie.length() + 1;
    addrs_total += it->address.length() + 1;
  }

  bool too_short = ((clients_total > *clients_len) ||
                    (cookies_total > *cookies_len) ||
                    (addrs_total > *addrs_len) ||
                    (tag_str.length() + 1 > *tag_len));
  *clients_len = clients_total;
  *cookies_len = cookies_total;
  *addrs_len = addrs_total;
  *tag_len = tag_str.length() + 1;
  if (too_short) {
    tracepoint(librados, rados_list_lockers_exit, -ERANGE, *exclusive, "", *tag_len, *clients_len, *cookies_len, *addrs_len);
    return -ERANGE;
  }

  strcpy(tag, tag_str.c_str());
  char *clients_p = clients;
  char *cookies_p = cookies;
  char *addrs_p = addrs;
  for (it = lockers.begin(); it != lockers.end(); ++it) {
    strcpy(clients_p, it->client.c_str());
    strcpy(cookies_p, it->cookie.c_str());
    strcpy(addrs_p, it->address.c_str());
    tracepoint(librados, rados_list_lockers_locker, clients_p, cookies_p, addrs_p);
    clients_p += it->client.length() + 1;
    cookies_p += it->cookie.length() + 1;
    addrs_p += it->address.length() + 1;
  }
  if (tmp_exclusive)
    *exclusive = 1;
  else
    *exclusive = 0;

  int retval = lockers.size();
  tracepoint(librados, rados_list_lockers_exit, retval, *exclusive, tag, *tag_len, *clients_len, *cookies_len, *addrs_len);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_list_lockers);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_break_lock)(
  rados_ioctx_t io, const char *o,
  const char *name, const char *client,
  const char *cookie)
{
  tracepoint(librados, rados_break_lock_enter, io, o, name, client, cookie);
  librados::IoCtx ctx;
  librados::IoCtx::from_rados_ioctx_t(io, ctx);

  int retval = ctx.break_lock(o, name, client, cookie);
  tracepoint(librados, rados_break_lock_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_break_lock);

extern "C" rados_write_op_t LIBRADOS_C_API_DEFAULT_F(rados_create_write_op)()
{
  tracepoint(librados, rados_create_write_op_enter);
  rados_write_op_t retval = new (std::nothrow) librados::ObjectOperationImpl;
  tracepoint(librados, rados_create_write_op_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_create_write_op);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_release_write_op)(
  rados_write_op_t write_op)
{
  tracepoint(librados, rados_release_write_op_enter, write_op);
  delete static_cast<librados::ObjectOperationImpl*>(write_op);
  tracepoint(librados, rados_release_write_op_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_release_write_op);

static ::ObjectOperation* to_object_operation(rados_write_op_t write_op)
{
  return &static_cast<librados::ObjectOperationImpl*>(write_op)->o;
}

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_set_flags)(
  rados_write_op_t write_op,
  int flags)
{
  tracepoint(librados, rados_write_op_set_flags_enter, write_op, flags);
  to_object_operation(write_op)->set_last_op_flags(get_op_flags(flags));
  tracepoint(librados, rados_write_op_set_flags_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_set_flags);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_assert_version)(
  rados_write_op_t write_op,
  uint64_t ver)
{
  tracepoint(librados, rados_write_op_assert_version_enter, write_op, ver);
  to_object_operation(write_op)->assert_version(ver);
  tracepoint(librados, rados_write_op_assert_version_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_assert_version);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_assert_exists)(
  rados_write_op_t write_op)
{
  tracepoint(librados, rados_write_op_assert_exists_enter, write_op);
  to_object_operation(write_op)->stat(nullptr, nullptr, nullptr);
  tracepoint(librados, rados_write_op_assert_exists_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_assert_exists);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_cmpext)(
  rados_write_op_t write_op,
  const char *cmp_buf,
  size_t cmp_len,
  uint64_t off,
  int *prval)
{
  tracepoint(librados, rados_write_op_cmpext_enter, write_op, cmp_buf,
	     cmp_len, off, prval);
  to_object_operation(write_op)->cmpext(off, cmp_len, cmp_buf, prval);
  tracepoint(librados, rados_write_op_cmpext_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_cmpext);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_cmpxattr)(
  rados_write_op_t write_op,
  const char *name,
  uint8_t comparison_operator,
  const char *value,
  size_t value_len)
{
  tracepoint(librados, rados_write_op_cmpxattr_enter, write_op, name, comparison_operator, value, value_len);
  bufferlist bl;
  bl.append(value, value_len);
  to_object_operation(write_op)->cmpxattr(name,
					  comparison_operator,
					  CEPH_OSD_CMPXATTR_MODE_STRING,
					  bl);
  tracepoint(librados, rados_write_op_cmpxattr_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_cmpxattr);

static void rados_c_omap_cmp(ObjectOperation *op,
			     const char *key,
			     uint8_t comparison_operator,
			     const char *val,
                             size_t key_len,
			     size_t val_len,
			     int *prval)
{
  bufferlist bl;
  bl.append(val, val_len);
  std::map<std::string, pair<bufferlist, int> > assertions;
  string lkey = string(key, key_len);

  assertions[lkey] = std::make_pair(bl, comparison_operator);
  op->omap_cmp(assertions, prval);
}

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_omap_cmp)(
  rados_write_op_t write_op,
  const char *key,
  uint8_t comparison_operator,
  const char *val,
  size_t val_len,
  int *prval)
{
  tracepoint(librados, rados_write_op_omap_cmp_enter, write_op, key, comparison_operator, val, val_len, prval);
  rados_c_omap_cmp(to_object_operation(write_op), key, comparison_operator,
                   val, strlen(key), val_len, prval);
  tracepoint(librados, rados_write_op_omap_cmp_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_omap_cmp);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_omap_cmp2)(
  rados_write_op_t write_op,
  const char *key,
  uint8_t comparison_operator,
  const char *val,
  size_t key_len,
  size_t val_len,
  int *prval)
{
  tracepoint(librados, rados_write_op_omap_cmp_enter, write_op, key, comparison_operator, val, val_len, prval);
  rados_c_omap_cmp(to_object_operation(write_op), key, comparison_operator,
                   val, key_len, val_len, prval);
  tracepoint(librados, rados_write_op_omap_cmp_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_omap_cmp2);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_setxattr)(
  rados_write_op_t write_op,
  const char *name,
  const char *value,
  size_t value_len)
{
  tracepoint(librados, rados_write_op_setxattr_enter, write_op, name, value, value_len);
  bufferlist bl;
  bl.append(value, value_len);
  to_object_operation(write_op)->setxattr(name, bl);
  tracepoint(librados, rados_write_op_setxattr_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_setxattr);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_rmxattr)(
  rados_write_op_t write_op,
  const char *name)
{
  tracepoint(librados, rados_write_op_rmxattr_enter, write_op, name);
  to_object_operation(write_op)->rmxattr(name);
  tracepoint(librados, rados_write_op_rmxattr_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_rmxattr);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_create)(
  rados_write_op_t write_op,
  int exclusive,
  const char* category) // unused
{
  tracepoint(librados, rados_write_op_create_enter, write_op, exclusive);
  to_object_operation(write_op)->create(!!exclusive);
  tracepoint(librados, rados_write_op_create_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_create);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_write)(
  rados_write_op_t write_op,
  const char *buffer,
  size_t len,
  uint64_t offset)
{
  tracepoint(librados, rados_write_op_write_enter, write_op, buffer, len, offset);
  bufferlist bl;
  bl.append(buffer,len);
  to_object_operation(write_op)->write(offset, bl);
  tracepoint(librados, rados_write_op_write_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_write);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_write_full)(
  rados_write_op_t write_op,
  const char *buffer,
  size_t len)
{
  tracepoint(librados, rados_write_op_write_full_enter, write_op, buffer, len);
  bufferlist bl;
  bl.append(buffer,len);
  to_object_operation(write_op)->write_full(bl);
  tracepoint(librados, rados_write_op_write_full_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_write_full);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_writesame)(
  rados_write_op_t write_op,
  const char *buffer,
  size_t data_len,
  size_t write_len,
  uint64_t offset)
{
  tracepoint(librados, rados_write_op_writesame_enter, write_op, buffer, data_len, write_len, offset);
  bufferlist bl;
  bl.append(buffer, data_len);
  to_object_operation(write_op)->writesame(offset, write_len, bl);
  tracepoint(librados, rados_write_op_writesame_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_writesame);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_append)(
  rados_write_op_t write_op,
  const char *buffer,
  size_t len)
{
  tracepoint(librados, rados_write_op_append_enter, write_op, buffer, len);
  bufferlist bl;
  bl.append(buffer,len);
  to_object_operation(write_op)->append(bl);
  tracepoint(librados, rados_write_op_append_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_append);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_remove)(
  rados_write_op_t write_op)
{
  tracepoint(librados, rados_write_op_remove_enter, write_op);
  to_object_operation(write_op)->remove();
  tracepoint(librados, rados_write_op_remove_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_remove);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_truncate)(
  rados_write_op_t write_op,
  uint64_t offset)
{
  tracepoint(librados, rados_write_op_truncate_enter, write_op, offset);
  to_object_operation(write_op)->truncate(offset);
  tracepoint(librados, rados_write_op_truncate_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_truncate);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_zero)(
  rados_write_op_t write_op,
  uint64_t offset,
  uint64_t len)
{
  tracepoint(librados, rados_write_op_zero_enter, write_op, offset, len);
  to_object_operation(write_op)->zero(offset, len);
  tracepoint(librados, rados_write_op_zero_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_zero);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_exec)(
  rados_write_op_t write_op,
  const char *cls,
  const char *method,
  const char *in_buf,
  size_t in_len,
  int *prval)
{
  tracepoint(librados, rados_write_op_exec_enter, write_op, cls, method, in_buf, in_len, prval);
  bufferlist inbl;
  inbl.append(in_buf, in_len);
  to_object_operation(write_op)->call(cls, method, inbl, NULL, NULL, prval);
  tracepoint(librados, rados_write_op_exec_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_exec);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_omap_set)(
  rados_write_op_t write_op,
  char const* const* keys,
  char const* const* vals,
  const size_t *lens,
  size_t num)
{
  tracepoint(librados, rados_write_op_omap_set_enter, write_op, num);
  std::map<std::string, bufferlist> entries;
  for (size_t i = 0; i < num; ++i) {
    tracepoint(librados, rados_write_op_omap_set_entry, keys[i], vals[i], lens[i]);
    bufferlist bl(lens[i]);
    bl.append(vals[i], lens[i]);
    entries[keys[i]] = bl;
  }
  to_object_operation(write_op)->omap_set(entries);
  tracepoint(librados, rados_write_op_omap_set_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_omap_set);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_omap_set2)(
  rados_write_op_t write_op,
  char const* const* keys,
  char const* const* vals,
  const size_t *key_lens,
  const size_t *val_lens,
  size_t num)
{
  tracepoint(librados, rados_write_op_omap_set_enter, write_op, num);
  std::map<std::string, bufferlist> entries;
  for (size_t i = 0; i < num; ++i) {
    bufferlist bl(val_lens[i]);
    bl.append(vals[i], val_lens[i]);
    string key(keys[i], key_lens[i]);
    entries[key] = bl;
  }
  to_object_operation(write_op)->omap_set(entries);
  tracepoint(librados, rados_write_op_omap_set_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_omap_set2);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_omap_rm_keys)(
  rados_write_op_t write_op,
  char const* const* keys,
  size_t keys_len)
{
  tracepoint(librados, rados_write_op_omap_rm_keys_enter, write_op, keys_len);
  for(size_t i = 0; i < keys_len; i++) {
    tracepoint(librados, rados_write_op_omap_rm_keys_entry, keys[i]);
  }
  std::set<std::string> to_remove(keys, keys + keys_len);
  to_object_operation(write_op)->omap_rm_keys(to_remove);
  tracepoint(librados, rados_write_op_omap_rm_keys_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_omap_rm_keys);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_omap_rm_keys2)(
  rados_write_op_t write_op,
  char const* const* keys,
  const size_t* key_lens,
  size_t keys_len)
{
  tracepoint(librados, rados_write_op_omap_rm_keys_enter, write_op, keys_len);
  std::set<std::string> to_remove;
  for(size_t i = 0; i < keys_len; i++) {
    to_remove.emplace(keys[i], key_lens[i]);
  }
  to_object_operation(write_op)->omap_rm_keys(to_remove);
  tracepoint(librados, rados_write_op_omap_rm_keys_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_omap_rm_keys2);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_omap_rm_range2)(
  rados_write_op_t write_op,
  const char *key_begin,
  size_t key_begin_len,
  const char *key_end,
  size_t key_end_len)
{
  tracepoint(librados, rados_write_op_omap_rm_range_enter,
             write_op, key_begin, key_end);
  to_object_operation(write_op)->omap_rm_range({key_begin, key_begin_len},
                                               {key_end, key_end_len});
  tracepoint(librados, rados_write_op_omap_rm_range_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_omap_rm_range2);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_omap_clear)(
  rados_write_op_t write_op)
{
  tracepoint(librados, rados_write_op_omap_clear_enter, write_op);
  to_object_operation(write_op)->omap_clear();
  tracepoint(librados, rados_write_op_omap_clear_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_omap_clear);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_set_alloc_hint)(
  rados_write_op_t write_op,
  uint64_t expected_object_size,
  uint64_t expected_write_size)
{
  tracepoint(librados, rados_write_op_set_alloc_hint_enter, write_op, expected_object_size, expected_write_size);
  to_object_operation(write_op)->set_alloc_hint(expected_object_size,
                                                expected_write_size, 0);
  tracepoint(librados, rados_write_op_set_alloc_hint_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_set_alloc_hint);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_write_op_set_alloc_hint2)(
  rados_write_op_t write_op,
  uint64_t expected_object_size,
  uint64_t expected_write_size,
  uint32_t flags)
{
  tracepoint(librados, rados_write_op_set_alloc_hint2_enter, write_op, expected_object_size, expected_write_size, flags);
  to_object_operation(write_op)->set_alloc_hint(expected_object_size,
                                                expected_write_size,
						flags);
  tracepoint(librados, rados_write_op_set_alloc_hint2_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_set_alloc_hint2);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_write_op_operate)(
  rados_write_op_t write_op,
  rados_ioctx_t io,
  const char *oid,
  time_t *mtime,
  int flags)
{
  tracepoint(librados, rados_write_op_operate_enter, write_op, io, oid, mtime, flags);
  object_t obj(oid);
  auto oimpl = static_cast<librados::ObjectOperationImpl*>(write_op);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;

  if (mtime) {
    oimpl->rt = ceph::real_clock::from_time_t(*mtime);
    oimpl->prt = &oimpl->rt;
  }

  int retval = ctx->operate(obj, &oimpl->o, oimpl->prt, translate_flags(flags));
  tracepoint(librados, rados_write_op_operate_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_operate);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_write_op_operate2)(
  rados_write_op_t write_op,
  rados_ioctx_t io,
  const char *oid,
  struct timespec *ts,
  int flags)
{
  tracepoint(librados, rados_write_op_operate2_enter, write_op, io, oid, ts, flags);
  object_t obj(oid);
  auto oimpl = static_cast<librados::ObjectOperationImpl*>(write_op);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;

  if (ts) {
    oimpl->rt = ceph::real_clock::from_timespec(*ts);
    oimpl->prt = &oimpl->rt;
  }

  int retval = ctx->operate(obj, &oimpl->o, oimpl->prt, translate_flags(flags));
  tracepoint(librados, rados_write_op_operate_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_write_op_operate2);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_write_op_operate)(
  rados_write_op_t write_op,
  rados_ioctx_t io,
  rados_completion_t completion,
  const char *oid,
  time_t *mtime,
  int flags)
{
  tracepoint(librados, rados_aio_write_op_operate_enter, write_op, io, completion, oid, mtime, flags);
  object_t obj(oid);
  auto oimpl = static_cast<librados::ObjectOperationImpl*>(write_op);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  librados::AioCompletionImpl *c = (librados::AioCompletionImpl*)completion;

  if (mtime) {
    oimpl->rt = ceph::real_clock::from_time_t(*mtime);
    oimpl->prt = &oimpl->rt;
  }

  int retval = ctx->aio_operate(obj, &oimpl->o, c, ctx->snapc, oimpl->prt, translate_flags(flags));
  tracepoint(librados, rados_aio_write_op_operate_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_write_op_operate);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_write_op_operate2)(
  rados_write_op_t write_op,
  rados_ioctx_t io,
  rados_completion_t completion,
  const char *oid,
  struct timespec *mtime,
  int flags)
{
  tracepoint(librados, rados_aio_write_op_operate2_enter, write_op, io, completion, oid, mtime, flags);
  object_t obj(oid);
  auto oimpl = static_cast<librados::ObjectOperationImpl*>(write_op);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  librados::AioCompletionImpl *c = (librados::AioCompletionImpl*)completion;

  if (mtime) {
    oimpl->rt = ceph::real_clock::from_timespec(*mtime);
    oimpl->prt = &oimpl->rt;
  }

  int retval = ctx->aio_operate(obj, &oimpl->o, c, ctx->snapc, oimpl->prt, translate_flags(flags));
  tracepoint(librados, rados_aio_write_op_operate_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_write_op_operate2);

extern "C" rados_read_op_t LIBRADOS_C_API_DEFAULT_F(rados_create_read_op)()
{
  tracepoint(librados, rados_create_read_op_enter);
  rados_read_op_t retval = new (std::nothrow)::ObjectOperation;
  tracepoint(librados, rados_create_read_op_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_create_read_op);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_release_read_op)(
  rados_read_op_t read_op)
{
  tracepoint(librados, rados_release_read_op_enter, read_op);
  delete (::ObjectOperation *)read_op;
  tracepoint(librados, rados_release_read_op_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_release_read_op);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_read_op_set_flags)(
  rados_read_op_t read_op,
  int flags)
{
  tracepoint(librados, rados_read_op_set_flags_enter, read_op, flags);
  ((::ObjectOperation *)read_op)->set_last_op_flags(get_op_flags(flags));
  tracepoint(librados, rados_read_op_set_flags_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_read_op_set_flags);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_read_op_assert_version)(
  rados_read_op_t read_op,
  uint64_t ver)
{
  tracepoint(librados, rados_read_op_assert_version_enter, read_op, ver);
  ((::ObjectOperation *)read_op)->assert_version(ver);
  tracepoint(librados, rados_read_op_assert_version_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_read_op_assert_version);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_read_op_assert_exists)(
  rados_read_op_t read_op)
{
  tracepoint(librados, rados_read_op_assert_exists_enter, read_op);
  ((::ObjectOperation *)read_op)->stat(nullptr, nullptr, nullptr);
  tracepoint(librados, rados_read_op_assert_exists_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_read_op_assert_exists);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_read_op_cmpext)(
  rados_read_op_t read_op,
  const char *cmp_buf,
  size_t cmp_len,
  uint64_t off,
  int *prval)
{
  tracepoint(librados, rados_read_op_cmpext_enter, read_op, cmp_buf,
	     cmp_len, off, prval);
  ((::ObjectOperation *)read_op)->cmpext(off, cmp_len, cmp_buf, prval);
  tracepoint(librados, rados_read_op_cmpext_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_read_op_cmpext);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_read_op_cmpxattr)(
  rados_read_op_t read_op,
  const char *name,
  uint8_t comparison_operator,
  const char *value,
  size_t value_len)
{
  tracepoint(librados, rados_read_op_cmpxattr_enter, read_op, name, comparison_operator, value, value_len);
  bufferlist bl;
  bl.append(value, value_len);
  ((::ObjectOperation *)read_op)->cmpxattr(name,
					   comparison_operator,
					   CEPH_OSD_CMPXATTR_MODE_STRING,
					   bl);
  tracepoint(librados, rados_read_op_cmpxattr_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_read_op_cmpxattr);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_read_op_omap_cmp)(
  rados_read_op_t read_op,
  const char *key,
  uint8_t comparison_operator,
  const char *val,
  size_t val_len,
  int *prval)
{
  tracepoint(librados, rados_read_op_omap_cmp_enter, read_op, key, comparison_operator, val, val_len, prval);
  rados_c_omap_cmp((::ObjectOperation *)read_op, key, comparison_operator,
                   val,  strlen(key), val_len, prval);
  tracepoint(librados, rados_read_op_omap_cmp_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_read_op_omap_cmp);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_read_op_omap_cmp2)(
  rados_read_op_t read_op,
  const char *key,
  uint8_t comparison_operator,
  const char *val,
  size_t key_len,
  size_t val_len,
  int *prval)
{
  tracepoint(librados, rados_read_op_omap_cmp_enter, read_op, key, comparison_operator, val, val_len, prval);
  rados_c_omap_cmp((::ObjectOperation *)read_op, key, comparison_operator,
                   val, key_len, val_len, prval);
  tracepoint(librados, rados_read_op_omap_cmp_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_read_op_omap_cmp2);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_read_op_stat)(
  rados_read_op_t read_op,
  uint64_t *psize,
  time_t *pmtime,
  int *prval)
{
  tracepoint(librados, rados_read_op_stat_enter, read_op, psize, pmtime, prval);
  ((::ObjectOperation *)read_op)->stat(psize, pmtime, prval);
  tracepoint(librados, rados_read_op_stat_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_read_op_stat);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_read_op_stat2)(
  rados_read_op_t read_op,
  uint64_t *psize,
  struct timespec *pmtime,
  int *prval)
{
  tracepoint(librados, rados_read_op_stat2_enter, read_op, psize, pmtime, prval);
  ((::ObjectOperation *)read_op)->stat(psize, pmtime, prval);
  tracepoint(librados, rados_read_op_stat2_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_read_op_stat2);

class C_bl_to_buf : public Context {
  char *out_buf;
  size_t out_len;
  size_t *bytes_read;
  int *prval;
public:
  bufferlist out_bl;
  C_bl_to_buf(char *out_buf,
	      size_t out_len,
	      size_t *bytes_read,
	      int *prval) : out_buf(out_buf), out_len(out_len),
			    bytes_read(bytes_read), prval(prval) {}
  void finish(int r) override {
    if (out_bl.length() > out_len) {
      if (prval)
	*prval = -ERANGE;
      if (bytes_read)
	*bytes_read = 0;
      return;
    }
    if (bytes_read)
      *bytes_read = out_bl.length();
    if (out_buf && !out_bl.is_provided_buffer(out_buf))
      out_bl.begin().copy(out_bl.length(), out_buf);
  }
};

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_read_op_read)(
  rados_read_op_t read_op,
  uint64_t offset,
  size_t len,
  char *buf,
  size_t *bytes_read,
  int *prval)
{
  tracepoint(librados, rados_read_op_read_enter, read_op, offset, len, buf, bytes_read, prval);
  C_bl_to_buf *ctx = new C_bl_to_buf(buf, len, bytes_read, prval);
  ctx->out_bl.push_back(buffer::create_static(len, buf));
  ((::ObjectOperation *)read_op)->read(offset, len, &ctx->out_bl, prval, ctx);
  tracepoint(librados, rados_read_op_read_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_read_op_read);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_read_op_checksum)(
  rados_read_op_t read_op,
  rados_checksum_type_t type,
  const char *init_value,
  size_t init_value_len,
  uint64_t offset, size_t len,
  size_t chunk_size, char *pchecksum,
  size_t checksum_len, int *prval)
{
  tracepoint(librados, rados_read_op_checksum_enter, read_op, type, init_value,
	     init_value_len, offset, len, chunk_size);
  bufferlist init_value_bl;
  init_value_bl.append(init_value, init_value_len);

  C_bl_to_buf *ctx = nullptr;
  if (pchecksum != nullptr) {
    ctx = new C_bl_to_buf(pchecksum, checksum_len, nullptr, prval);
  }
  ((::ObjectOperation *)read_op)->checksum(get_checksum_op_type(type),
					   init_value_bl, offset, len,
					   chunk_size,
					   (ctx ? &ctx->out_bl : nullptr),
					   prval, ctx);
  tracepoint(librados, rados_read_op_checksum_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_read_op_checksum);

class C_out_buffer : public Context {
  char **out_buf;
  size_t *out_len;
public:
  bufferlist out_bl;
  C_out_buffer(char **out_buf, size_t *out_len) : out_buf(out_buf),
						  out_len(out_len) {}
  void finish(int r) override {
    // ignore r since we don't know the meaning of return values
    // from custom class methods
    do_out_buffer(out_bl, out_buf, out_len);
  }
};

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_read_op_exec)(
  rados_read_op_t read_op,
  const char *cls,
  const char *method,
  const char *in_buf,
  size_t in_len,
  char **out_buf,
  size_t *out_len,
  int *prval)
{
  tracepoint(librados, rados_read_op_exec_enter, read_op, cls, method, in_buf, in_len, out_buf, out_len, prval);
  bufferlist inbl;
  inbl.append(in_buf, in_len);
  C_out_buffer *ctx = new C_out_buffer(out_buf, out_len);
  ((::ObjectOperation *)read_op)->call(cls, method, inbl, &ctx->out_bl, ctx,
				       prval);
  tracepoint(librados, rados_read_op_exec_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_read_op_exec);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_read_op_exec_user_buf)(
  rados_read_op_t read_op,
  const char *cls,
  const char *method,
  const char *in_buf,
  size_t in_len,
  char *out_buf,
  size_t out_len,
  size_t *used_len,
  int *prval)
{
  tracepoint(librados, rados_read_op_exec_user_buf_enter, read_op, cls, method, in_buf, in_len, out_buf, out_len, used_len, prval);
  C_bl_to_buf *ctx = new C_bl_to_buf(out_buf, out_len, used_len, prval);
  bufferlist inbl;
  inbl.append(in_buf, in_len);
  ((::ObjectOperation *)read_op)->call(cls, method, inbl, &ctx->out_bl, ctx,
				       prval);
  tracepoint(librados, rados_read_op_exec_user_buf_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_read_op_exec_user_buf);

struct RadosOmapIter {
  std::map<std::string, bufferlist> values;
  std::map<std::string, bufferlist>::iterator i;
};

class C_OmapIter : public Context {
  RadosOmapIter *iter;
public:
  explicit C_OmapIter(RadosOmapIter *iter) : iter(iter) {}
  void finish(int r) override {
    iter->i = iter->values.begin();
  }
};

class C_XattrsIter : public Context {
  librados::RadosXattrsIter *iter;
public:
  explicit C_XattrsIter(librados::RadosXattrsIter *iter) : iter(iter) {}
  void finish(int r) override {
    iter->i = iter->attrset.begin();
  }
};

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_read_op_getxattrs)(
  rados_read_op_t read_op,
  rados_xattrs_iter_t *iter,
  int *prval)
{
  tracepoint(librados, rados_read_op_getxattrs_enter, read_op, prval);
  librados::RadosXattrsIter *xattrs_iter = new librados::RadosXattrsIter;
  ((::ObjectOperation *)read_op)->getxattrs(&xattrs_iter->attrset, prval);
  ((::ObjectOperation *)read_op)->set_handler(new C_XattrsIter(xattrs_iter));
  *iter = xattrs_iter;
  tracepoint(librados, rados_read_op_getxattrs_exit, *iter);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_read_op_getxattrs);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_read_op_omap_get_vals)(
  rados_read_op_t read_op,
  const char *start_after,
  const char *filter_prefix,
  uint64_t max_return,
  rados_omap_iter_t *iter,
  int *prval)
{
  tracepoint(librados, rados_read_op_omap_get_vals_enter, read_op, start_after, filter_prefix, max_return, prval);
  RadosOmapIter *omap_iter = new RadosOmapIter;
  const char *start = start_after ? start_after : "";
  const char *filter = filter_prefix ? filter_prefix : "";
  ((::ObjectOperation *)read_op)->omap_get_vals(
    start,
    filter,
    max_return,
    &omap_iter->values,
    nullptr,
    prval);
  ((::ObjectOperation *)read_op)->set_handler(new C_OmapIter(omap_iter));
  *iter = omap_iter;
  tracepoint(librados, rados_read_op_omap_get_vals_exit, *iter);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_read_op_omap_get_vals);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_read_op_omap_get_vals2)(
  rados_read_op_t read_op,
  const char *start_after,
  const char *filter_prefix,
  uint64_t max_return,
  rados_omap_iter_t *iter,
  unsigned char *pmore,
  int *prval)
{
  tracepoint(librados, rados_read_op_omap_get_vals_enter, read_op, start_after, filter_prefix, max_return, prval);
  RadosOmapIter *omap_iter = new RadosOmapIter;
  const char *start = start_after ? start_after : "";
  const char *filter = filter_prefix ? filter_prefix : "";
  ((::ObjectOperation *)read_op)->omap_get_vals(
    start,
    filter,
    max_return,
    &omap_iter->values,
    (bool*)pmore,
    prval);
  ((::ObjectOperation *)read_op)->set_handler(new C_OmapIter(omap_iter));
  *iter = omap_iter;
  tracepoint(librados, rados_read_op_omap_get_vals_exit, *iter);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_read_op_omap_get_vals2);

struct C_OmapKeysIter : public Context {
  RadosOmapIter *iter;
  std::set<std::string> keys;
  explicit C_OmapKeysIter(RadosOmapIter *iter) : iter(iter) {}
  void finish(int r) override {
    // map each key to an empty bl
    for (std::set<std::string>::const_iterator i = keys.begin();
	 i != keys.end(); ++i) {
      iter->values[*i];
    }
    iter->i = iter->values.begin();
  }
};

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_read_op_omap_get_keys)(
  rados_read_op_t read_op,
  const char *start_after,
  uint64_t max_return,
  rados_omap_iter_t *iter,
  int *prval)
{
  tracepoint(librados, rados_read_op_omap_get_keys_enter, read_op, start_after, max_return, prval);
  RadosOmapIter *omap_iter = new RadosOmapIter;
  C_OmapKeysIter *ctx = new C_OmapKeysIter(omap_iter);
  ((::ObjectOperation *)read_op)->omap_get_keys(
    start_after ? start_after : "",
    max_return, &ctx->keys, nullptr, prval);
  ((::ObjectOperation *)read_op)->set_handler(ctx);
  *iter = omap_iter;
  tracepoint(librados, rados_read_op_omap_get_keys_exit, *iter);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_read_op_omap_get_keys);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_read_op_omap_get_keys2)(
  rados_read_op_t read_op,
  const char *start_after,
  uint64_t max_return,
  rados_omap_iter_t *iter,
  unsigned char *pmore,
  int *prval)
{
  tracepoint(librados, rados_read_op_omap_get_keys_enter, read_op, start_after, max_return, prval);
  RadosOmapIter *omap_iter = new RadosOmapIter;
  C_OmapKeysIter *ctx = new C_OmapKeysIter(omap_iter);
  ((::ObjectOperation *)read_op)->omap_get_keys(
    start_after ? start_after : "",
    max_return, &ctx->keys,
    (bool*)pmore, prval);
  ((::ObjectOperation *)read_op)->set_handler(ctx);
  *iter = omap_iter;
  tracepoint(librados, rados_read_op_omap_get_keys_exit, *iter);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_read_op_omap_get_keys2);

static void internal_rados_read_op_omap_get_vals_by_keys(rados_read_op_t read_op,
                                                         set<string>& to_get,
                                                         rados_omap_iter_t *iter,
                                                         int *prval)
{
  RadosOmapIter *omap_iter = new RadosOmapIter;
  ((::ObjectOperation *)read_op)->omap_get_vals_by_keys(to_get,
                                                        &omap_iter->values,
                                                        prval);
  ((::ObjectOperation *)read_op)->set_handler(new C_OmapIter(omap_iter));
  *iter = omap_iter;
}

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_read_op_omap_get_vals_by_keys)(
  rados_read_op_t read_op,
  char const* const* keys,
  size_t keys_len,
  rados_omap_iter_t *iter,
  int *prval)
{
  tracepoint(librados, rados_read_op_omap_get_vals_by_keys_enter, read_op, keys, keys_len, iter, prval);
  std::set<std::string> to_get(keys, keys + keys_len);
  internal_rados_read_op_omap_get_vals_by_keys(read_op, to_get, iter, prval);
  tracepoint(librados, rados_read_op_omap_get_vals_by_keys_exit, *iter);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_read_op_omap_get_vals_by_keys);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_read_op_omap_get_vals_by_keys2)(
  rados_read_op_t read_op,
  char const* const* keys,
  size_t num_keys,
  const size_t* key_lens,
  rados_omap_iter_t *iter,
  int *prval)
{
  tracepoint(librados, rados_read_op_omap_get_vals_by_keys_enter, read_op, keys, num_keys, iter, prval);
  std::set<std::string> to_get;
  for (size_t i = 0; i < num_keys; i++) {
    to_get.emplace(keys[i], key_lens[i]);
  }
  internal_rados_read_op_omap_get_vals_by_keys(read_op, to_get, iter, prval);
  tracepoint(librados, rados_read_op_omap_get_vals_by_keys_exit, *iter);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_read_op_omap_get_vals_by_keys2);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_omap_get_next2)(
  rados_omap_iter_t iter,
  char **key,
  char **val,
  size_t *key_len,
  size_t *val_len)
{
  tracepoint(librados, rados_omap_get_next_enter, iter);
  RadosOmapIter *it = static_cast<RadosOmapIter *>(iter);
  if (it->i == it->values.end()) {
    if (key)
      *key = NULL;
    if (val)
      *val = NULL;
    if (key_len)
      *key_len = 0;
    if (val_len)
      *val_len = 0;
    tracepoint(librados, rados_omap_get_next_exit, 0, key, val, val_len);
    return 0;
  }
  if (key)
    *key = (char*)it->i->first.c_str();
  if (val)
    *val = it->i->second.c_str();
  if (key_len)
    *key_len = it->i->first.length();
  if (val_len)
    *val_len = it->i->second.length();
  ++it->i;
  tracepoint(librados, rados_omap_get_next_exit, 0, key, val, val_len);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_omap_get_next2);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_omap_get_next)(
  rados_omap_iter_t iter,
  char **key,
  char **val,
  size_t *len)
{
  return LIBRADOS_C_API_DEFAULT_F(rados_omap_get_next2)(iter, key, val, nullptr, len);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_omap_get_next);

extern "C" unsigned int LIBRADOS_C_API_DEFAULT_F(rados_omap_iter_size)(
  rados_omap_iter_t iter)
{
  RadosOmapIter *it = static_cast<RadosOmapIter *>(iter);
  return it->values.size();
}
LIBRADOS_C_API_BASE_DEFAULT(rados_omap_iter_size);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_omap_get_end)(
  rados_omap_iter_t iter)
{
  tracepoint(librados, rados_omap_get_end_enter, iter);
  RadosOmapIter *it = static_cast<RadosOmapIter *>(iter);
  delete it;
  tracepoint(librados, rados_omap_get_end_exit);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_omap_get_end);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_read_op_operate)(
  rados_read_op_t read_op,
  rados_ioctx_t io,
  const char *oid,
  int flags)
{
  tracepoint(librados, rados_read_op_operate_enter, read_op, io, oid, flags);
  object_t obj(oid);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  int retval = ctx->operate_read(obj, (::ObjectOperation *)read_op, NULL,
				 translate_flags(flags));
  tracepoint(librados, rados_read_op_operate_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_read_op_operate);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_read_op_operate)(
  rados_read_op_t read_op,
  rados_ioctx_t io,
  rados_completion_t completion,
  const char *oid,
  int flags)
{
  tracepoint(librados, rados_aio_read_op_operate_enter, read_op, io, completion, oid, flags);
  object_t obj(oid);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  librados::AioCompletionImpl *c = (librados::AioCompletionImpl*)completion;
  int retval = ctx->aio_operate_read(obj, (::ObjectOperation *)read_op,
				     c, translate_flags(flags), NULL);
  tracepoint(librados, rados_aio_read_op_operate_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_read_op_operate);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_cache_pin)(
  rados_ioctx_t io,
  const char *o)
{
  tracepoint(librados, rados_cache_pin_enter, io, o);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->cache_pin(oid);
  tracepoint(librados, rados_cache_pin_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_cache_pin);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_cache_unpin)(
  rados_ioctx_t io,
  const char *o)
{
  tracepoint(librados, rados_cache_unpin_enter, io, o);
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;
  object_t oid(o);
  int retval = ctx->cache_unpin(oid);
  tracepoint(librados, rados_cache_unpin_exit, retval);
  return retval;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_cache_unpin);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_object_list_slice)(
  rados_ioctx_t io,
  const rados_object_list_cursor start,
  const rados_object_list_cursor finish,
  const size_t n,
  const size_t m,
  rados_object_list_cursor *split_start,
  rados_object_list_cursor *split_finish)
{
  librados::IoCtxImpl *ctx = (librados::IoCtxImpl *)io;

  ceph_assert(split_start);
  ceph_assert(split_finish);
  hobject_t *split_start_hobj = (hobject_t*)(*split_start);
  hobject_t *split_finish_hobj = (hobject_t*)(*split_finish);
  ceph_assert(split_start_hobj);
  ceph_assert(split_finish_hobj);
  hobject_t *start_hobj = (hobject_t*)(start);
  hobject_t *finish_hobj = (hobject_t*)(finish);

  ctx->object_list_slice(
      *start_hobj,
      *finish_hobj,
      n,
      m,
      split_start_hobj,
      split_finish_hobj);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_object_list_slice);
