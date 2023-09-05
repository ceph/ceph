// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <cstdarg>
#include <cstring>
#include <boost/container/small_vector.hpp>
#include "common/ceph_context.h"
#include "common/ceph_releases.h"
#include "common/config.h"
#include "crimson/common/config_proxy.h"
#include "common/debug.h"

#include "crimson/osd/exceptions.h"
#include "crimson/osd/ops_executer.h"
#include "crimson/osd/pg_backend.h"

#include "objclass/objclass.h"
#include "osd/ClassHandler.h"

#include "auth/Crypto.h"
#include "common/armor.h"

using std::map;
using std::string;

#define dout_context ClassHandler::get_instance().cct

static constexpr int dout_subsys = ceph_subsys_objclass;

static inline int execute_osd_op(cls_method_context_t hctx, OSDOp& op)
{
  // we can expect the memory under `ret` will be still fine after
  // executing the osd op as we're running inside `seastar::thread`
  // created for us by `seastar::async` in `::do_op_call()`.
  int ret = 0;
  using osd_op_errorator = crimson::osd::OpsExecuter::osd_op_errorator;
  reinterpret_cast<crimson::osd::OpsExecuter*>(hctx)->execute_op(op)
  .handle_error_interruptible(
    osd_op_errorator::all_same_way([&ret] (const std::error_code& err) {
      assert(err.value() > 0);
      ret = -err.value();
      return seastar::now();
    })).get(); // we're blocking here which requires `seastar::thread`.
  return ret;
}

int cls_call(cls_method_context_t hctx, const char *cls, const char *method,
                                 char *indata, int datalen,
                                 char **outdata, int *outdatalen)
{
// FIXME, HACK: this is for testing only. Let's use dynamic linker to verify
// our depedencies
  return 0;
}

int cls_getxattr(cls_method_context_t hctx,
                 const char *name,
                 char **outdata,
                 int *outdatalen)
{
  return 0;
}

int cls_setxattr(cls_method_context_t hctx,
                 const char *name,
                 const char *value,
                 int val_len)
{
  return 0;
}

int cls_read(cls_method_context_t hctx,
             int ofs, int len,
             char **outdata,
             int *outdatalen)
{
  return 0;
}

int cls_get_request_origin(cls_method_context_t hctx, entity_inst_t *origin)
{
  assert(origin);

  try {
    const auto& message = \
      reinterpret_cast<crimson::osd::OpsExecuter*>(hctx)->get_message();
    *origin = message.get_orig_source_inst();
    return 0;
  } catch (crimson::osd::error& e) {
    return -e.code().value();
  }
}

int cls_cxx_create(cls_method_context_t hctx, const bool exclusive)
{
  OSDOp op{CEPH_OSD_OP_CREATE};
  op.op.flags = (exclusive ? CEPH_OSD_OP_FLAG_EXCL : 0);
  return execute_osd_op(hctx, op);
}

int cls_cxx_remove(cls_method_context_t hctx)
{
  OSDOp op{CEPH_OSD_OP_DELETE};
  return execute_osd_op(hctx, op);
}

int cls_cxx_stat(cls_method_context_t hctx, uint64_t *size, time_t *mtime)
{
  OSDOp op{CEPH_OSD_OP_STAT};
  if (const auto ret = execute_osd_op(hctx, op); ret < 0) {
    return ret;
  }
  utime_t ut;
  uint64_t s;
  try {
    auto iter = op.outdata.cbegin();
    decode(s, iter);
    decode(ut, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }
  if (size) {
    *size = s;
  }
  if (mtime) {
    *mtime = ut.sec();
  }
  return 0;
}

int cls_cxx_stat2(cls_method_context_t hctx,
                  uint64_t *size,
                  ceph::real_time *mtime)
{
  OSDOp op{CEPH_OSD_OP_STAT};
  if (const int ret = execute_osd_op(hctx, op); ret < 0) {
    return ret;
  }
  uint64_t dummy_size;
  real_time dummy_mtime;
  uint64_t& out_size = size ? *size : dummy_size;
  real_time& out_mtime = mtime ? *mtime : dummy_mtime;
  try {
    auto iter = op.outdata.cbegin();
    decode(out_size, iter);
    decode(out_mtime, iter);
    return 0;
  } catch (buffer::error& err) {
    return -EIO;
  }
}

int cls_cxx_read2(cls_method_context_t hctx,
                  int ofs,
                  int len,
                  bufferlist *outbl,
                  uint32_t op_flags)
{
  OSDOp op{CEPH_OSD_OP_SYNC_READ};
  op.op.extent.offset = ofs;
  op.op.extent.length = len;
  op.op.flags = op_flags;
  if (const auto ret = execute_osd_op(hctx, op); ret < 0) {
    return ret;
  }
  *outbl = std::move(op.outdata);
  return outbl->length();
}

int cls_cxx_write2(cls_method_context_t hctx,
                   int ofs,
                   int len,
                   bufferlist *inbl,
                   uint32_t op_flags)
{
  OSDOp op{CEPH_OSD_OP_WRITE};
  op.op.extent.offset = ofs;
  op.op.extent.length = len;
  op.op.flags = op_flags;
  op.indata = *inbl;
  return execute_osd_op(hctx, op);
}

int cls_cxx_write_full(cls_method_context_t hctx, bufferlist * const inbl)
{
  OSDOp op{CEPH_OSD_OP_WRITEFULL};
  op.op.extent.offset = 0;
  op.op.extent.length = inbl->length();
  op.indata = *inbl;
  return execute_osd_op(hctx, op);
}

int cls_cxx_replace(cls_method_context_t hctx,
                    int ofs,
                    int len,
                    bufferlist *inbl)
{
  {
    OSDOp top{CEPH_OSD_OP_TRUNCATE};
    top.op.extent.offset = 0;
    top.op.extent.length = 0;
    if (const auto ret = execute_osd_op(hctx, top); ret < 0) {
      return ret;
    }
  }

  {
    OSDOp wop{CEPH_OSD_OP_WRITE};
    wop.op.extent.offset = ofs;
    wop.op.extent.length = len;
    wop.indata = *inbl;
    if (const auto ret = execute_osd_op(hctx, wop); ret < 0) {
      return ret;
    }
  }
  return 0;
}

int cls_cxx_truncate(cls_method_context_t hctx, int ofs)
{
  OSDOp op{CEPH_OSD_OP_TRUNCATE};
  op.op.extent.offset = ofs;
  op.op.extent.length = 0;
  return execute_osd_op(hctx, op);
}

int cls_cxx_write_zero(cls_method_context_t hctx, int offset, int len)
{
  OSDOp op{CEPH_OSD_OP_ZERO};
  op.op.extent.offset = offset;
  op.op.extent.length = len;
  return execute_osd_op(hctx, op);
}

int cls_cxx_getxattr(cls_method_context_t hctx,
                     const char *name,
                     bufferlist *outbl)
{
  OSDOp op{CEPH_OSD_OP_GETXATTR};
  op.op.xattr.name_len = strlen(name);
  op.indata.append(name, op.op.xattr.name_len);
  if (const auto ret = execute_osd_op(hctx, op); ret < 0) {
    return ret;
  }
  *outbl = std::move(op.outdata);
  return outbl->length();
}

int cls_cxx_getxattrs(cls_method_context_t hctx,
                      map<string, bufferlist> *attrset)
{
  OSDOp op{CEPH_OSD_OP_GETXATTRS};
  if (const int ret = execute_osd_op(hctx, op); ret < 0) {
    return ret;
  }
  try {
    auto iter = op.outdata.cbegin();
    decode(*attrset, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }
  return 0;
}

int cls_cxx_setxattr(cls_method_context_t hctx,
                     const char *name,
                     bufferlist *inbl)
{
  OSDOp op{CEPH_OSD_OP_SETXATTR};
  op.op.xattr.name_len = std::strlen(name);
  op.op.xattr.value_len = inbl->length();
  op.indata.append(name, op.op.xattr.name_len);
  op.indata.append(*inbl);
  return execute_osd_op(hctx, op);
}

int cls_cxx_snap_revert(cls_method_context_t hctx, snapid_t snapid)
{
  OSDOp op{CEPH_OSD_OP_ROLLBACK};
  op.op.snap.snapid = snapid;
  return execute_osd_op(hctx, op);
}

int cls_cxx_map_get_all_vals(cls_method_context_t hctx,
                             map<string, bufferlist>* vals,
                             bool *more)
{
  return 0;
}

int cls_cxx_map_get_keys(cls_method_context_t hctx,
                         const std::string& start_obj,
                         const uint64_t max_to_get,
                         std::set<std::string>* const keys,
                         bool* const more)
{
  OSDOp op{CEPH_OSD_OP_OMAPGETKEYS};
  encode(start_obj, op.indata);
  encode(max_to_get, op.indata);
  if (const auto ret = execute_osd_op(hctx, op); ret < 0) {
    return ret;
  }
  try {
    auto iter = op.outdata.cbegin();
    decode(*keys, iter);
    decode(*more, iter);
  } catch (buffer::error&) {
    return -EIO;
  }
  return keys->size();
}

int cls_cxx_map_get_vals(cls_method_context_t hctx,
                         const std::string& start_obj,
                         const std::string& filter_prefix,
                         const uint64_t max_to_get,
                         std::map<std::string, ceph::bufferlist> *vals,
                         bool* const more)
{
  OSDOp op{CEPH_OSD_OP_OMAPGETVALS};
  encode(start_obj, op.indata);
  encode(max_to_get, op.indata);
  encode(filter_prefix, op.indata);
  if (const auto ret = execute_osd_op(hctx, op); ret < 0) {
    return ret;
  }
  try {
    auto iter = op.outdata.cbegin();
    decode(*vals, iter);
    decode(*more, iter);
  } catch (buffer::error&) {
    return -EIO;
  }
  return vals->size();
}

int cls_cxx_map_get_vals_by_keys(cls_method_context_t hctx,
				 const std::set<std::string> &keys,
				 std::map<std::string, ceph::bufferlist> *vals)
{
  OSDOp op{CEPH_OSD_OP_OMAPGETVALSBYKEYS};
  encode(keys, op.indata);
  if (const auto ret = execute_osd_op(hctx, op); ret < 0) {
    return ret;
  }
  try {
    auto iter = op.outdata.cbegin();
    decode(*vals, iter);
  } catch (buffer::error&) {
    return -EIO;
  }
  return 0;
}

int cls_cxx_map_read_header(cls_method_context_t hctx, bufferlist *outbl)
{
  OSDOp op{CEPH_OSD_OP_OMAPGETHEADER};
  if (const auto ret = execute_osd_op(hctx, op); ret < 0) {
    return ret;
  }
  *outbl = std::move(op.outdata);
  return 0;
}

int cls_cxx_map_get_val(cls_method_context_t hctx,
                        const string &key,
                        bufferlist *outbl)
{
  OSDOp op{CEPH_OSD_OP_OMAPGETVALSBYKEYS};
  {
    std::set<std::string> k{key};
    encode(k, op.indata);
  }
  if (const auto ret = execute_osd_op(hctx, op); ret < 0) {
    return ret;
  }
  std::map<std::string, ceph::bufferlist> m;
  try {
    auto iter = op.outdata.cbegin();
    decode(m, iter);
  } catch (buffer::error&) {
    return -EIO;
  }
  if (auto iter = std::begin(m); iter != std::end(m)) {
    *outbl = std::move(iter->second);
    return 0;
  } else {
    return -ENOENT;
  }
}

int cls_cxx_map_set_val(cls_method_context_t hctx,
                        const string &key,
                        bufferlist *inbl)
{
  OSDOp op{CEPH_OSD_OP_OMAPSETVALS};
  {
    std::map<std::string, ceph::bufferlist> m;
    m[key] = *inbl;
    encode(m, op.indata);
  }
  return execute_osd_op(hctx, op);
}

int cls_cxx_map_set_vals(cls_method_context_t hctx,
                         const std::map<string, ceph::bufferlist> *map)
{
  OSDOp op{CEPH_OSD_OP_OMAPSETVALS};
  encode(*map, op.indata);
  return execute_osd_op(hctx, op);
}

int cls_cxx_map_clear(cls_method_context_t hctx)
{
  OSDOp op{CEPH_OSD_OP_OMAPCLEAR};
  return execute_osd_op(hctx, op);
}

int cls_cxx_map_write_header(cls_method_context_t hctx, bufferlist *inbl)
{
  OSDOp op{CEPH_OSD_OP_OMAPSETHEADER};
  op.indata = std::move(*inbl);
  return execute_osd_op(hctx, op);
}

int cls_cxx_map_remove_range(cls_method_context_t hctx,
                             const std::string& key_begin,
                             const std::string& key_end)
{
  OSDOp op{CEPH_OSD_OP_OMAPRMKEYRANGE};
  encode(key_begin, op.indata);
  encode(key_end, op.indata);
  return execute_osd_op(hctx, op);
}

int cls_cxx_map_remove_key(cls_method_context_t hctx, const string &key)
{
  OSDOp op{CEPH_OSD_OP_OMAPRMKEYS};
  std::vector<string> to_rm{key};
  encode(to_rm, op.indata);
  return execute_osd_op(hctx, op);
}

int cls_cxx_list_watchers(cls_method_context_t hctx,
                          obj_list_watch_response_t *watchers)
{
  OSDOp op{CEPH_OSD_OP_LIST_WATCHERS};
  if (const auto ret = execute_osd_op(hctx, op); ret < 0) {
    return ret;
  }

  try {
    auto iter = op.outdata.cbegin();
    decode(*watchers, iter);
  } catch (buffer::error&) {
    return -EIO;
  }
  return 0;
}

uint64_t cls_current_version(cls_method_context_t hctx)
{
  auto* ox = reinterpret_cast<crimson::osd::OpsExecuter*>(hctx);
  return ox->get_last_user_version();
}


int cls_current_subop_num(cls_method_context_t hctx)
{
  auto* ox = reinterpret_cast<crimson::osd::OpsExecuter*>(hctx);
  // in contrast to classical OSD, crimson doesn't count OP_CALL and
  // OP_STAT which seems fine regarding how the plugins we take care
  // about use this part of API.
  return ox->get_processed_rw_ops_num();
}

uint64_t cls_get_features(cls_method_context_t hctx)
{
  return 0;
}

uint64_t cls_get_client_features(cls_method_context_t hctx)
{
  try {
    const auto& message = \
      reinterpret_cast<crimson::osd::OpsExecuter*>(hctx)->get_message();
    return message.get_features();
  } catch (crimson::osd::error& e) {
    return -e.code().value();
  }
}

uint64_t cls_get_pool_stripe_width(cls_method_context_t hctx)
{
  auto* ox = reinterpret_cast<crimson::osd::OpsExecuter*>(hctx);
  return ox->get_pool_stripe_width();
}

ceph_release_t cls_get_required_osd_release(cls_method_context_t hctx)
{
  // FIXME
  return ceph_release_t::nautilus;
}

ceph_release_t cls_get_min_compatible_client(cls_method_context_t hctx)
{
  // FIXME
  return ceph_release_t::nautilus;
}

const ConfigProxy& cls_get_config(cls_method_context_t hctx)
{
  return crimson::common::local_conf();
}

const object_info_t& cls_get_object_info(cls_method_context_t hctx)
{
  return reinterpret_cast<crimson::osd::OpsExecuter*>(hctx)->get_object_info();
}

int cls_get_snapset_seq(cls_method_context_t hctx, uint64_t *snap_seq)
{
  auto* ox = reinterpret_cast<crimson::osd::OpsExecuter*>(hctx);
  auto obc = ox->get_obc();
  if (!obc->obs.exists ||
      (obc->obs.oi.is_whiteout() &&
       obc->ssc->snapset.clones.empty())) {
    return -ENOENT;
  }
  *snap_seq = obc->ssc->snapset.seq;
  return 0;
}

int cls_cxx_chunk_write_and_set(cls_method_context_t hctx,
                                int ofs,
                                int len,
                                bufferlist *write_inbl,
                                uint32_t op_flags,
                                bufferlist *set_inbl,
                                int set_len)
{
  return 0;
}

int cls_get_manifest_ref_count(cls_method_context_t hctx, string fp_oid)
{
  return 0;
}

uint64_t cls_get_osd_min_alloc_size(cls_method_context_t hctx) {
  // FIXME
  return 4096;
}

int cls_cxx_gather(cls_method_context_t hctx, const std::set<std::string> &src_objs, const std::string& pool,
		   const char *cls, const char *method, bufferlist& inbl)
{
  return 0;
}

int cls_cxx_get_gathered_data(cls_method_context_t hctx, std::map<std::string, bufferlist> *results)
{
  return 0;
}

// although at first glance the implementation looks the same as in
// the classical OSD, it's different b/c of how the dout macro expands.
int cls_log(int level, const char *format, ...)
{
   size_t size = 256;
   va_list ap;
   while (1) {
     boost::container::small_vector<char, 256> buf(size);
     va_start(ap, format);
     int n = vsnprintf(buf.data(), size, format, ap);
     va_end(ap);
#define MAX_SIZE 8196UL
     if ((n > -1 && static_cast<size_t>(n) < size) || size > MAX_SIZE) {
       dout(ceph::dout::need_dynamic(level)) << buf.data() << dendl;
       return n;
     }
     size *= 2;
   }
}
