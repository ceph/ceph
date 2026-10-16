// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <errno.h>
#include <exception>
#include <iterator>
#include <new>
#include <type_traits>

#include "cls/rgw/cls_rgw_const.h"
#include "cls/rgw/cls_rgw_client.h"
#ifdef WITH_RGW_CLS_DATUM
 #include "cls/rgw/cls_rgw_bi_datum.h"
 #include "cls/rgw/cls_rgw_bilog_datum.h"
 #include "cls/rgw/cls_rgw_bucket_datum.h"
 #include "cls/rgw/cls_rgw_olh_datum.h"
 #include "cls/rgw/cls_rgw_write_datum.h"
#endif

#include "common/debug.h"

using std::list;
using std::map;
using std::pair;
using std::string;
using std::vector;

using ceph::real_time;

using namespace librados;
using namespace cls::rgw;

const string BucketIndexShardsManager::KEY_VALUE_SEPARATOR = "#";
const string BucketIndexShardsManager::SHARDS_SEPARATOR = ",";

/**
 * This class represents the bucket index object operation callback context.
 */
template <typename T>
class ClsBucketIndexOpCtx : public ObjectOperationCompletion {
private:
  T *data;
  int *ret_code;
public:
  ClsBucketIndexOpCtx(T* _data, int *_ret_code) : data(_data), ret_code(_ret_code) { ceph_assert(data); }
  ~ClsBucketIndexOpCtx() override {}
  void handle_completion(int r, bufferlist& outbl) override {
    // if successful, or we're asked for a retry, copy result into
    // destination (*data)
    if (r >= 0 || r == RGWBIAdvanceAndRetryError) {
      try {
        auto iter = outbl.cbegin();
        decode((*data), iter);
      } catch (ceph::buffer::error& err) {
        r = -EIO;
      }
    }
    if (ret_code) {
      *ret_code = r;
    }
  }
};

namespace {

template <typename ReplyT, typename DecodeT>
class reply_completion final : public ObjectOperationCompletion {
  ReplyT& output;
  int *status;
  [[no_unique_address]] DecodeT decode_reply;

public:
  reply_completion(ReplyT& output, int *status, DecodeT decode_reply)
    : output(output), status(status), decode_reply(std::move(decode_reply))
  {}

public:
  void handle_completion(int result, ::ceph::buffer::list& bytes) override {
    try {
      if (0 <= result || RGWBIAdvanceAndRetryError == result) {
        decode_reply(bytes, output);
      }
    } catch (const std::bad_alloc&) {
      result = -ENOMEM;
    } catch (const std::exception&) {
      result = -EIO;
    }
    if (status) {
      *status = result;
    }
  }
};

} // anonymous namespace

void cls_rgw_bucket_init_index(ObjectWriteOperation& o)
{
  bufferlist in;
  o.exec(method::bucket_init_index, in);
}

void cls_rgw_bucket_init_index2(ObjectWriteOperation& o)
{
  bufferlist in;
  o.exec(method::bucket_init_index2, in);
}

void cls_rgw_bucket_set_tag_timeout(librados::ObjectWriteOperation& op,
                                    uint64_t timeout)
{
#ifdef WITH_RGW_CLS_DATUM
  auto in = ::cls::rgw::bucket_datum::encode_timeout(timeout);
#else
  const auto call = rgw_cls_tag_timeout_op{.tag_timeout = timeout};
  bufferlist in;
  encode(call, in);
#endif
  op.exec(method::bucket_set_tag_timeout, in);
}

void cls_rgw_bucket_update_stats(librados::ObjectWriteOperation& o,
				 bool absolute,
                                 const map<RGWObjCategory, rgw_bucket_category_stats>& stats,
                                 const map<RGWObjCategory, rgw_bucket_category_stats>* dec_stats)
{
#ifdef WITH_RGW_CLS_DATUM
  auto in = ::cls::rgw::bucket_datum::encode_stats(absolute, stats, dec_stats);
#else
  rgw_cls_bucket_update_stats_op call;
  call.absolute = absolute;
  call.stats = stats;
  if (dec_stats != NULL)
    call.dec_stats = *dec_stats;
  bufferlist in;
  encode(call, in);
#endif
  o.exec(method::bucket_update_stats, in);
}

void cls_rgw_bucket_prepare_op(ObjectWriteOperation& o, RGWModifyOp op, const string& tag,
                               const cls_rgw_obj_key& key, const string& locator)
{
#ifdef WITH_RGW_CLS_DATUM
  auto in = ::cls::rgw::write_datum::encode_prepare(op, tag, key, locator);
#else
  rgw_cls_obj_prepare_op call;
  call.op = op;
  call.tag = tag;
  call.key = key;
  call.locator = locator;
  bufferlist in;
  encode(call, in);
#endif
  o.exec(method::bucket_prepare_op, in);
}

void cls_rgw_bucket_complete_op(ObjectWriteOperation& o, RGWModifyOp op, const string& tag,
                                const rgw_bucket_entry_ver& ver,
                                const cls_rgw_obj_key& key,
                                const rgw_bucket_dir_entry_meta& dir_meta,
				const list<cls_rgw_obj_key> *remove_objs, bool log_op,
                                uint16_t bilog_flags,
                                const rgw_zone_set *zones_trace,
				const std::string& obj_locator)
{
#ifdef WITH_RGW_CLS_DATUM
  auto in = ::cls::rgw::write_datum::encode_complete(op, tag, ver, key, dir_meta,
                                                  remove_objs, log_op, bilog_flags,
                                                  zones_trace, obj_locator);
#else
  bufferlist in;
  rgw_cls_obj_complete_op call;
  call.op = op;
  call.op_tag = tag;
  call.key = key;
  call.ver = ver;
  call.locator = obj_locator;
  call.meta = dir_meta;
  call.log_op = log_op;
  call.bilog_flags = bilog_flags;
  if (remove_objs)
    call.remove_objs = *remove_objs;
  if (zones_trace) {
    call.zones_trace = *zones_trace;
  }
  encode(call, in);
#endif
  o.exec(method::bucket_complete_op, in);
}

void CLSRGWCompleteModifyOpBase::complete_op(librados::ObjectWriteOperation& o,
                                             const rgw_bucket_entry_ver& ver,
                                             const rgw_bucket_dir_entry_meta& dir_meta,
                                             const std::list<cls_rgw_obj_key>* remove_objs,
                                             const std::string& locator) const {
  cls_rgw_bucket_complete_op(o, op, op_tag, ver, key, dir_meta,
                             remove_objs, log_op, bilog_flags,
                             &zones_trace, locator);
}

void CLSRGWLinkOLHBase::link_olh(librados::ObjectWriteOperation& o,
                                  const ceph::bufferlist& olh_tag,
                                  bool delete_marker,
                                  const rgw_bucket_dir_entry_meta* meta,
                                  uint64_t olh_epoch,
                                  ceph::real_time unmod_since,
                                  bool high_precision_time,
                                  ceph::bufferlist* epoch_out_bl) const {
  cls_rgw_bucket_link_olh(o, key, olh_tag, delete_marker, op_tag, meta,
                          olh_epoch, unmod_since, high_precision_time,
                          log_op, zones_trace, epoch_out_bl);
}

void CLSRGWUnlinkInstance::unlink_instance(librados::ObjectWriteOperation& o,
                                           const std::string& olh_tag,
                                           uint64_t olh_epoch,
                                           ceph::bufferlist* epoch_out_bl) const {
  cls_rgw_bucket_unlink_instance(o, key, op_tag, olh_tag, olh_epoch,
                                 log_op, bilog_flags, zones_trace,
                                 epoch_out_bl);
}

void cls_rgw_bucket_list_op(librados::ObjectReadOperation& op,
                            const cls_rgw_obj_key& start_obj,
                            const std::string& filter_prefix,
                            const std::string& delimiter,
                            uint32_t num_entries,
                            bool list_versions,
                            rgw_cls_list_ret* result)
{
#ifdef WITH_RGW_CLS_DATUM
  ceph_assert(result);
  auto in = ::cls::rgw::bucket_datum::encode_request(start_obj, filter_prefix, delimiter,
                                                   num_entries, list_versions);
  op.exec(method::bucket_list, in, new reply_completion {*result, nullptr,
    [](const ::ceph::buffer::list& bytes, rgw_cls_list_ret& output) {
      static_assert(std::is_nothrow_move_assignable_v<rgw_cls_list_ret>);
      auto candidate = ::cls::rgw::bucket_datum::decode_reply<::cls::rgw::bucket_datum::flat_entries>(bytes);
      output.dir.header = std::move(candidate.dir.header);
      output.dir.m = std::move(candidate.dir.m);
      output.marker = std::move(candidate.marker);
      output.is_truncated = candidate.is_truncated;
      output.cls_filtered = candidate.cls_filtered;
    }});
#else
  bufferlist in;
  rgw_cls_list_op call;
  call.start_obj = start_obj;
  call.filter_prefix = filter_prefix;
  call.delimiter = delimiter;
  call.num_entries = num_entries;
  call.list_versions = list_versions;
  encode(call, in);

  op.exec(method::bucket_list, in,
	  new ClsBucketIndexOpCtx<rgw_cls_list_ret>(result, NULL));
#endif
}

void cls_rgw_remove_obj(librados::ObjectWriteOperation& o, list<string>& keep_attr_prefixes)
{
  bufferlist in;
  rgw_cls_obj_remove_op call;
  call.keep_attr_prefixes = keep_attr_prefixes;
  encode(call, in);
  o.exec(method::obj_remove, in);
}

void cls_rgw_obj_store_pg_ver(librados::ObjectWriteOperation& o, const string& attr)
{
  bufferlist in;
  rgw_cls_obj_store_pg_ver_op call;
  call.attr = attr;
  encode(call, in);
  o.exec(method::obj_store_pg_ver, in);
}

void cls_rgw_obj_check_attrs_prefix(librados::ObjectOperation& o, const string& prefix, bool fail_if_exist)
{
  bufferlist in;
  rgw_cls_obj_check_attrs_prefix call;
  call.check_prefix = prefix;
  call.fail_if_exist = fail_if_exist;
  encode(call, in);
  o.exec(method::obj_check_attrs_prefix, in);
}

void cls_rgw_obj_check_mtime(librados::ObjectOperation& o, const real_time& mtime, bool high_precision_time, RGWCheckMTimeType type)
{
  bufferlist in;
  rgw_cls_obj_check_mtime call;
  call.mtime = mtime;
  call.high_precision_time = high_precision_time;
  call.type = type;
  encode(call, in);
  o.exec(method::obj_check_mtime, in);
}

int cls_rgw_bi_get(librados::IoCtx& io_ctx, const string oid,
                   BIIndexType index_type, const cls_rgw_obj_key& key,
                   rgw_cls_bi_entry *entry)
{
  bufferlist in, out;
#ifdef WITH_RGW_CLS_DATUM
  in = ::cls::rgw::bi_datum::encode_get_request(key, index_type);
#else
  rgw_cls_bi_get_op call;
  call.key = key;
  call.type = index_type;
  encode(call, in);
#endif
  int r = io_ctx.exec(oid, method::bi_get, in, out);
  if (r < 0)
    return r;

  try {
#ifdef WITH_RGW_CLS_DATUM
    auto candidate = ::cls::rgw::bi_datum::decode_get_reply(out);
#else
    rgw_cls_bi_get_ret op_ret;
    auto iter = out.cbegin();
    decode(op_ret, iter);
    auto candidate = std::move(op_ret.entry);
#endif
    *entry = std::move(candidate);
  } catch (ceph::buffer::error& err) {
    return -EIO;
  }

  return 0;
}

namespace
{
void bi_put(ObjectWriteOperation& op, const rgw_cls_bi_entry& entry)
{
#ifdef WITH_RGW_CLS_DATUM
  auto input = ::cls::rgw::bi_datum::encode_put_entry(entry);
#else
  ::ceph::buffer::list input;
  rgw_cls_bi_put_op call;
  call.entry = entry;
  encode(call, input);
#endif
  op.exec(method::bi_put, input);
}
} // anonymous namespace

int cls_rgw_bi_put(librados::IoCtx& io_ctx, const string oid, const rgw_cls_bi_entry& entry)
{
  librados::ObjectWriteOperation op;
  bi_put(op, entry);
  int r = io_ctx.operate(oid, &op);
  if (r < 0)
    return r;

  return 0;
}

void cls_rgw_bi_put(ObjectWriteOperation& op, const string oid, const rgw_cls_bi_entry& entry)
{
  bi_put(op, entry);
}

void cls_rgw_bi_put_entries(librados::ObjectWriteOperation& op,
                            std::vector<rgw_cls_bi_entry> entries,
                            bool check_existing)
{
#ifdef WITH_RGW_CLS_DATUM
  auto in = ::cls::rgw::bi_datum::encode_put_request(entries, check_existing);
#else
  const auto call = rgw_cls_bi_put_entries_op{
    .entries = std::move(entries),
    .check_existing = check_existing
  };

  bufferlist in;
  encode(call, in);
#endif

  op.exec(method::bi_put_entries, in);
}

/* nb: any entries passed in are replaced with the results of the cls
 * call, so caller does not need to clear entries between calls
 */
template <typename EntriesT>
static int bi_list(librados::IoCtx& io_ctx, const std::string& oid,
                   const std::string& name_filter, const std::string& marker, uint32_t max,
                   EntriesT *destination, bool *truncated, bool reshardlog)
try {
  bufferlist in, out;
#ifdef WITH_RGW_CLS_DATUM
  in = ::cls::rgw::bi_datum::encode_request(max, name_filter, marker, reshardlog);
#else
  rgw_cls_bi_list_op call;
  call.name_filter = name_filter;
  call.marker = marker;
  call.max = max;
  call.reshardlog = reshardlog;
  encode(call, in);
#endif
  int r = io_ctx.exec(oid, method::bi_list, in, out);
  if (r < 0)
    return r;

#ifdef WITH_RGW_CLS_DATUM
  auto [candidate, is_truncated] = ::cls::rgw::bi_datum::decode_reply<EntriesT>(out);
#else
  rgw_cls_bi_list_ret op_ret;
  auto iter = out.cbegin();
  decode(op_ret, iter);
  auto candidate = [&] {
    if constexpr (std::is_same_v<EntriesT, std::list<rgw_cls_bi_entry>>) {
      return std::move(op_ret.entries);
    }
    if constexpr (!std::is_same_v<EntriesT, std::list<rgw_cls_bi_entry>>) {
      return EntriesT(std::make_move_iterator(op_ret.entries.begin()),
                      std::make_move_iterator(op_ret.entries.end()));
    }
  }();
  const bool is_truncated = op_ret.is_truncated;
#endif
  destination->swap(candidate);
  *truncated = is_truncated;

  return 0;
} catch (const std::bad_alloc&) {
  return -ENOMEM;
} catch (const std::exception&) {
  return -EIO;
}

int cls_rgw_bi_list(librados::IoCtx& io_ctx, const std::string& oid,
		    const std::string& name_filter, const std::string& marker, uint32_t max,
		    std::list<rgw_cls_bi_entry> *entries, bool *is_truncated, bool reshardlog)
{
  return bi_list(io_ctx, oid, name_filter, marker, max, entries, is_truncated, reshardlog);
}

int cls_rgw_bi_list(librados::IoCtx& io_ctx, const std::string& oid,
                    const std::string& name_filter, const std::string& marker, uint32_t max,
                    std::vector<rgw_cls_bi_entry>& entries, bool& is_truncated, bool reshardlog)
{
  return bi_list(io_ctx, oid, name_filter, marker, max, &entries, &is_truncated, reshardlog);
}

int cls_rgw_bucket_link_olh(librados::IoCtx& io_ctx, const string& oid,
                            const cls_rgw_obj_key& key, const bufferlist& olh_tag,
                            bool delete_marker, const string& op_tag, const rgw_bucket_dir_entry_meta *meta,
                            uint64_t olh_epoch, ceph::real_time unmod_since, bool high_precision_time, bool log_op, const rgw_zone_set& zones_trace)
{
  librados::ObjectWriteOperation op;
  cls_rgw_bucket_link_olh(op, key, olh_tag, delete_marker, op_tag, meta,
                          olh_epoch, unmod_since, high_precision_time, log_op,
                          zones_trace);

  return io_ctx.operate(oid, &op);
}


void cls_rgw_bucket_link_olh(librados::ObjectWriteOperation& op, const cls_rgw_obj_key& key,
                            const bufferlist& olh_tag, bool delete_marker,
                            const string& op_tag, const rgw_bucket_dir_entry_meta *meta,
                            uint64_t olh_epoch, ceph::real_time unmod_since, bool high_precision_time, bool log_op, const rgw_zone_set& zones_trace,
                            bufferlist* epoch_out_bl)
{
#ifdef WITH_RGW_CLS_DATUM
  auto in = ::cls::rgw::write_datum::encode_link(key, olh_tag, delete_marker, op_tag, meta,
                                              olh_epoch, unmod_since, high_precision_time,
                                              log_op, zones_trace);
#else
  bufferlist in;
  rgw_cls_link_olh_op call;
  call.key = key;
  call.olh_tag = olh_tag.to_str();
  call.op_tag = op_tag;
  call.delete_marker = delete_marker;
  if (meta) {
    call.meta = *meta;
  }
  call.olh_epoch = olh_epoch;
  call.log_op = log_op;
  call.unmod_since = unmod_since;
  call.high_precision_time = high_precision_time;
  call.zones_trace = zones_trace;
  encode(call, in);
#endif
  if (epoch_out_bl) {
    op.exec(method::bucket_link_olh, in, epoch_out_bl, nullptr);
  } else {
    op.exec(method::bucket_link_olh, in);
  }
}

void cls_rgw_bucket_refresh_instance(librados::ObjectWriteOperation& op, const cls_rgw_obj_key& key)
{
#ifdef WITH_RGW_CLS_DATUM
  auto in = ::cls::rgw::write_datum::encode_refresh(key);
#else
  bufferlist in;
  rgw_cls_refresh_instance_op call;
  call.key = key;
  encode(call, in);
#endif
  op.exec(method::bucket_refresh_instance, in);
}

int cls_rgw_bucket_unlink_instance(librados::IoCtx& io_ctx, const string& oid,
                                   const cls_rgw_obj_key& key, const string& op_tag,
                                   const string& olh_tag, uint64_t olh_epoch, bool log_op,
                                   uint16_t bilog_flags, const rgw_zone_set& zones_trace)
{
  librados::ObjectWriteOperation op;
  cls_rgw_bucket_unlink_instance(op, key, op_tag, olh_tag, olh_epoch, log_op, bilog_flags, zones_trace);
  int r = io_ctx.operate(oid, &op);
  if (r < 0)
    return r;

  return 0;
}

void cls_rgw_bucket_unlink_instance(librados::ObjectWriteOperation& op,
                                   const cls_rgw_obj_key& key, const string& op_tag,
                                   const string& olh_tag, uint64_t olh_epoch, bool log_op,
                                   uint16_t bilog_flags, const rgw_zone_set& zones_trace,
                                   bufferlist* epoch_out_bl)
{
#ifdef WITH_RGW_CLS_DATUM
  auto in = ::cls::rgw::write_datum::encode_unlink(key, op_tag, olh_tag, olh_epoch,
                                                log_op, bilog_flags, zones_trace);
#else
  bufferlist in;
  rgw_cls_unlink_instance_op call;
  call.key = key;
  call.op_tag = op_tag;
  call.olh_epoch = olh_epoch;
  call.olh_tag = olh_tag;
  call.log_op = log_op;
  call.zones_trace = zones_trace;
  call.bilog_flags = bilog_flags;
  encode(call, in);
#endif
  if (epoch_out_bl) {
    op.exec(method::bucket_unlink_instance, in, epoch_out_bl, nullptr);
  } else {
    op.exec(method::bucket_unlink_instance, in);
  }
}

namespace
{
#ifndef WITH_RGW_CLS_DATUM
void decode_olh_reply(const ::ceph::buffer::list& input, ::rgw_cls_read_olh_log_ret& value)
{
  ::rgw_cls_read_olh_log_ret candidate;
  auto cursor = input.cbegin();
  ::decode(candidate, cursor);
  value = std::move(candidate);
}

void decode_olh_reply(const ::ceph::buffer::list& input, ::cls::rgw::olh::reply& value)
{
  ::rgw_cls_read_olh_log_ret native;
  decode_olh_reply(input, native);
  ::cls::rgw::olh::reply candidate;
  candidate.epochs.reserve(native.log.size());
  for (auto& [epoch, entries] : native.log) {
    auto& rows = candidate.epochs.emplace_back(epoch, std::vector<::cls::rgw::olh::event> {}).second;
    rows.reserve(entries.size());
    for (auto& entry : entries) {
      rows.push_back({{std::move(entry.key.name), std::move(entry.key.instance)}, std::move(entry.op_tag),
                     entry.epoch, static_cast<::cls::rgw::olh::action>(entry.op), entry.delete_marker});
    }
  }
  candidate.truncated = native.is_truncated;
  value = std::move(candidate);
}
#endif

template <typename ReplyT>
void get_olh_log(ObjectReadOperation& op, const cls_rgw_obj_key& olh,
                 const uint64_t marker, const std::string& tag, ReplyT& output, int& status)
{
#ifdef WITH_RGW_CLS_DATUM
  auto input = ::cls::rgw::olh_datum::encode_request(olh, marker, tag, true);
#else
  ::ceph::buffer::list input;
  rgw_cls_read_olh_log_op call;
  call.olh = olh;
  call.ver_marker = marker;
  call.olh_tag = tag;
  call.get_stales = true;
  ::encode(call, input);
#endif
  op.exec(method::bucket_read_olh_log, input, new reply_completion {output, &status,
    [](const ::ceph::buffer::list& bytes, ReplyT& output) {
#ifdef WITH_RGW_CLS_DATUM
      ::cls::rgw::olh_datum::decode_reply(bytes, output);
#else
      decode_olh_reply(bytes, output);
#endif
    }});
}

template <typename ReplyT>
int get_olh_log(IoCtx& io_ctx, const std::string& oid, const cls_rgw_obj_key& olh,
                const uint64_t marker, const std::string& tag, ReplyT& output)
{
  int status = 0;
  ObjectReadOperation op;
  get_olh_log(op, olh, marker, tag, output, status);
  const int result = io_ctx.operate(oid, &op, nullptr);

  return 0 > result ? result : (0 > status ? status : result);
}

} // anonymous namespace

void cls_rgw_get_olh_log(librados::ObjectReadOperation& op, const cls_rgw_obj_key& olh, uint64_t ver_marker, const string& olh_tag, rgw_cls_read_olh_log_ret& log_ret, int& op_ret)
{
  get_olh_log(op, olh, ver_marker, olh_tag, log_ret, op_ret);
}

void cls_rgw_get_olh_log(ObjectReadOperation& op, const cls_rgw_obj_key& olh,
                        const uint64_t marker, const std::string& tag,
                        ::cls::rgw::olh::reply& output, int& status)
{
  get_olh_log(op, olh, marker, tag, output, status);
}

int cls_rgw_get_olh_log(IoCtx& io_ctx, string& oid, const cls_rgw_obj_key& olh, uint64_t ver_marker,
                        const string& olh_tag,
                        rgw_cls_read_olh_log_ret& log_ret)
{
  return get_olh_log(io_ctx, oid, olh, ver_marker, olh_tag, log_ret);
}

int cls_rgw_get_olh_log(IoCtx& io_ctx, const std::string& oid, const cls_rgw_obj_key& olh,
                       const uint64_t marker, const std::string& tag, ::cls::rgw::olh::reply& output)
{
  return get_olh_log(io_ctx, oid, olh, marker, tag, output);
}

void cls_rgw_trim_olh_log(librados::ObjectWriteOperation& op, const cls_rgw_obj_key& olh, uint64_t ver, const string& olh_tag)
{
#ifdef WITH_RGW_CLS_DATUM
  auto in = ::cls::rgw::write_datum::encode_trim(olh, ver, olh_tag);
#else
  bufferlist in;
  rgw_cls_trim_olh_log_op call;
  call.olh = olh;
  call.ver = ver;
  call.olh_tag = olh_tag;
  encode(call, in);
#endif
  op.exec(method::bucket_trim_olh_log, in);
}

int cls_rgw_clear_olh(IoCtx& io_ctx, string& oid, const cls_rgw_obj_key& olh, const string& olh_tag)
{
  librados::ObjectWriteOperation op;
  cls_rgw_clear_olh(op, olh, olh_tag);

  return io_ctx.operate(oid, &op);
}

void cls_rgw_clear_olh(librados::ObjectWriteOperation& op, const cls_rgw_obj_key& olh, const string& olh_tag)
{
#ifdef WITH_RGW_CLS_DATUM
  auto in = ::cls::rgw::write_datum::encode_clear(olh, olh_tag);
#else
  bufferlist in;
  rgw_cls_bucket_clear_olh_op call;
  call.key = olh;
  call.olh_tag = olh_tag;
  encode(call, in);
#endif
  op.exec(method::bucket_clear_olh, in);
}

void cls_rgw_bilog_list(librados::ObjectReadOperation& op,
                        const std::string& marker, uint32_t max,
                        cls_rgw_bi_log_list_ret *pdata, int *ret)
{
#ifdef WITH_RGW_CLS_DATUM
  ceph_assert(pdata);
  auto in = ::cls::rgw::bilog_datum::encode_list(marker, max);
  op.exec(method::bi_log_list, in, new reply_completion {*pdata, ret,
    [](const ::ceph::buffer::list& bytes, cls_rgw_bi_log_list_ret& output) {
      auto [entries, truncated] =
        ::cls::rgw::bilog_datum::decode_reply<std::list<::rgw_bi_log_entry>>(bytes);
      output.entries = std::move(entries);
      output.truncated = truncated;
    }});
#else
  cls_rgw_bi_log_list_op call;
  call.marker = marker;
  call.max = max;

  bufferlist in;
  encode(call, in);
  op.exec(method::bi_log_list, in, new ClsBucketIndexOpCtx<cls_rgw_bi_log_list_ret>(pdata, ret));
#endif
}

void cls_rgw_bilog_trim(librados::ObjectWriteOperation& op,
                        const std::string& start_marker,
                        const std::string& end_marker)
{
#ifdef WITH_RGW_CLS_DATUM
  auto in = ::cls::rgw::bilog_datum::encode_trim(start_marker, end_marker);
#else
  cls_rgw_bi_log_trim_op call;
  call.start_marker = start_marker;
  call.end_marker = end_marker;

  bufferlist in;
  encode(call, in);
#endif
  op.exec(method::bi_log_trim, in);
}

void cls_rgw_bucket_reshard_log_trim(librados::ObjectWriteOperation& op)
{
  bufferlist in;
  op.exec(method::reshard_log_trim, in);
}

void cls_rgw_bucket_check_index(librados::ObjectReadOperation& op,
                                bufferlist& out)
{
  bufferlist in;
  op.exec(method::bucket_check_index, in, &out, nullptr);
}

void cls_rgw_bucket_check_index_decode(const bufferlist& out,
                                       rgw_cls_check_index_ret& result)
{
#ifdef WITH_RGW_CLS_DATUM
  static_assert(std::is_nothrow_move_assignable_v<rgw_cls_check_index_ret>);
  auto candidate = ::cls::rgw::bucket_datum::decode_check_reply(out);
  result.existing_header = std::move(candidate.existing_header);
  result.calculated_header = std::move(candidate.calculated_header);
#else
  auto p = out.cbegin();
  decode(result, p);
#endif
}

void cls_rgw_bucket_rebuild_index(librados::ObjectWriteOperation& op)
{
  bufferlist in;
  op.exec(method::bucket_rebuild_index, in);
}

void cls_rgw_encode_suggestion(char op, rgw_bucket_dir_entry& dirent, bufferlist& updates)
{
  updates.append(op);
  encode(dirent, updates);
}

void cls_rgw_suggest_changes(ObjectWriteOperation& o, bufferlist& updates)
{
  o.exec(method::dir_suggest_changes, updates);
}

void cls_rgw_bilog_start(ObjectWriteOperation& op)
{
  bufferlist in;
  op.exec(method::bi_log_resync, in);
}

void cls_rgw_bilog_stop(ObjectWriteOperation& op)
{
  bufferlist in;
  op.exec(method::bi_log_stop, in);
}

class GetDirHeaderCompletion : public ObjectOperationCompletion {
  boost::intrusive_ptr<RGWGetDirHeader_CB> cb;
public:
  explicit GetDirHeaderCompletion(boost::intrusive_ptr<RGWGetDirHeader_CB> cb)
    : cb(std::move(cb)) {}

  void handle_completion(int r, bufferlist& outbl) override {
    rgw_cls_list_ret ret;
    try {
      auto iter = outbl.cbegin();
      decode(ret, iter);
    } catch (ceph::buffer::error& err) {
      r = -EIO;
    }
    cb->handle_response(r, ret.dir.header);
  }
};

int cls_rgw_get_dir_header_async(IoCtx& io_ctx, const string& oid,
                                 boost::intrusive_ptr<RGWGetDirHeader_CB> cb)
{
  bufferlist in, out;
  rgw_cls_list_op call;
  call.num_entries = 0;
  encode(call, in);
  ObjectReadOperation op;
  op.exec(method::bucket_list, in,
          new GetDirHeaderCompletion(std::move(cb)));
  AioCompletion *c = librados::Rados::aio_create_completion(nullptr, nullptr);
  int r = io_ctx.aio_operate(oid, c, &op, NULL);
  c->release();
  if (r < 0)
    return r;

  return 0;
}

int cls_rgw_usage_log_read(IoCtx& io_ctx, const string& oid, const string& user, const string& bucket,
                           uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
                           string& read_iter, map<rgw_user_bucket, rgw_usage_log_entry>& usage,
                           bool *is_truncated)
{
  if (is_truncated)
    *is_truncated = false;

  bufferlist in, out;
  rgw_cls_usage_log_read_op call;
  call.start_epoch = start_epoch;
  call.end_epoch = end_epoch;
  call.owner = user;
  call.max_entries = max_entries;
  call.bucket = bucket;
  call.iter = read_iter;
  encode(call, in);
  int r = io_ctx.exec(oid, method::user_usage_log_read, in, out);
  if (r < 0)
    return r;

  try {
    rgw_cls_usage_log_read_ret result;
    auto iter = out.cbegin();
    decode(result, iter);
    read_iter = result.next_iter;
    if (is_truncated)
      *is_truncated = result.truncated;

    usage = result.usage;
  } catch (ceph::buffer::error& e) {
    return -EINVAL;
  }

  return 0;
}

int cls_rgw_usage_log_trim(IoCtx& io_ctx, const string& oid, const string& user, const string& bucket,
			   uint64_t start_epoch, uint64_t end_epoch)
{
  bufferlist in;
  rgw_cls_usage_log_trim_op call;
  call.start_epoch = start_epoch;
  call.end_epoch = end_epoch;
  call.user = user;
  call.bucket = bucket;
  encode(call, in);

  bool done = false;
  do {
    ObjectWriteOperation op;
    op.exec(method::user_usage_log_trim, in);
    int r = io_ctx.operate(oid, &op);
    if (r == -ENODATA)
      done = true;
    else if (r < 0)
      return r;
  } while (!done);

  return 0;
}

void cls_rgw_usage_log_trim(librados::ObjectWriteOperation& op, const string& user, const string& bucket, uint64_t start_epoch, uint64_t end_epoch)
{
  bufferlist in;
  rgw_cls_usage_log_trim_op call;
  call.start_epoch = start_epoch;
  call.end_epoch = end_epoch;
  call.user = user;
  call.bucket = bucket;
  encode(call, in);

  op.exec(method::user_usage_log_trim, in);
}

void cls_rgw_usage_log_clear(ObjectWriteOperation& op)
{
  bufferlist in;
  op.exec(method::usage_log_clear, in);
}

void cls_rgw_usage_log_add(ObjectWriteOperation& op, rgw_usage_log_info& info)
{
  bufferlist in;
  rgw_cls_usage_log_add_op call;
  call.info = info;
  encode(call, in);
  op.exec(method::user_usage_log_add, in);
}

/* garbage collection */

void cls_rgw_gc_set_entry(ObjectWriteOperation& op, uint32_t expiration_secs, cls_rgw_gc_obj_info& info)
{
  bufferlist in;
  cls_rgw_gc_set_entry_op call;
  call.expiration_secs = expiration_secs;
  call.info = info;
  encode(call, in);
  op.exec(method::gc_set_entry, in);
}

void cls_rgw_gc_defer_entry(ObjectWriteOperation& op, uint32_t expiration_secs, const string& tag)
{
  bufferlist in;
  cls_rgw_gc_defer_entry_op call;
  call.expiration_secs = expiration_secs;
  call.tag = tag;
  encode(call, in);
  op.exec(method::gc_defer_entry, in);
}

void cls_rgw_gc_list(ObjectReadOperation& op, const string& marker,
                     uint32_t max, bool expired_only, bufferlist& out)
{
  bufferlist in;
  cls_rgw_gc_list_op call;
  call.marker = marker;
  call.max = max;
  call.expired_only = expired_only;
  encode(call, in);
  op.exec(method::gc_list, in, &out, nullptr);
}

int cls_rgw_gc_list_decode(const bufferlist& out,
                           std::list<cls_rgw_gc_obj_info>& entries,
                           bool& truncated, std::string& next_marker)
{
  cls_rgw_gc_list_ret ret;
  try {
    auto iter = out.cbegin();
    decode(ret, iter);
  } catch (ceph::buffer::error& err) {
    return -EIO;
  }

  entries.swap(ret.entries);

  truncated = ret.truncated;
  next_marker = std::move(ret.next_marker);
  return 0;
}

void cls_rgw_gc_remove(librados::ObjectWriteOperation& op, const vector<string>& tags)
{
  bufferlist in;
  cls_rgw_gc_remove_op call;
  call.tags = tags;
  encode(call, in);
  op.exec(method::gc_remove, in);
}

void cls_rgw_lc_get_head(ObjectReadOperation& op, bufferlist& out)
{
  bufferlist in;
  op.exec(method::lc_get_head, in, &out, nullptr);
}

int cls_rgw_lc_get_head_decode(const bufferlist& out, cls_rgw_lc_obj_head& head)
{
  cls_rgw_lc_get_head_ret ret;
  try {
    auto iter = out.cbegin();
    decode(ret, iter);
  } catch (ceph::buffer::error& err) {
    return -EIO;
  }
  head = std::move(ret.head);

  return 0;
}

void cls_rgw_lc_put_head(ObjectWriteOperation& op, const cls_rgw_lc_obj_head& head)
{
  bufferlist in;
  cls_rgw_lc_put_head_op call;
  call.head = head;
  encode(call, in);
  op.exec(method::lc_put_head, in);
}

void cls_rgw_lc_get_next_entry(ObjectReadOperation& op, const string& marker,
                               bufferlist& out)
{
  bufferlist in;
  cls_rgw_lc_get_next_entry_op call;
  call.marker = marker;
  encode(call, in);
  op.exec(method::lc_get_next_entry, in, &out, nullptr);
}

int cls_rgw_lc_get_next_entry_decode(const bufferlist& out, cls_rgw_lc_entry& entry)
{
  cls_rgw_lc_get_next_entry_ret ret;
  try {
    auto iter = out.cbegin();
    decode(ret, iter);
  } catch (ceph::buffer::error& err) {
    return -EIO;
  }
  entry = std::move(ret.entry);

  return 0;
}

void cls_rgw_lc_rm_entry(ObjectWriteOperation& op,
                         const cls_rgw_lc_entry& entry)
{
  bufferlist in;
  cls_rgw_lc_rm_entry_op call;
  call.entry = entry;
  encode(call, in);
  op.exec(method::lc_rm_entry, in);
}

void cls_rgw_lc_set_entry(ObjectWriteOperation& op,
                          const cls_rgw_lc_entry& entry)
{
  bufferlist in, out;
  cls_rgw_lc_set_entry_op call;
  call.entry = entry;
  encode(call, in);
  op.exec(method::lc_set_entry, in);
}

void cls_rgw_lc_get_entry(ObjectReadOperation& op, const std::string& marker,
                          bufferlist& out)
{
  bufferlist in;
  cls_rgw_lc_get_entry_op call{marker};
  encode(call, in);
  op.exec(method::lc_get_entry, in, &out, nullptr);
}

int cls_rgw_lc_get_entry_decode(const bufferlist& out, cls_rgw_lc_entry& entry)
{
  cls_rgw_lc_get_entry_ret ret;
  try {
    auto iter = out.cbegin();
    decode(ret, iter);
  } catch (ceph::buffer::error& err) {
    return -EIO;
  }

  entry = std::move(ret.entry);
  return 0;
}

void cls_rgw_lc_list(ObjectReadOperation& op, const string& marker,
                     uint32_t max_entries, bufferlist& out)
{
  bufferlist in;
  cls_rgw_lc_list_entries_op call;
  call.marker = marker;
  call.max_entries = max_entries;

  encode(call, in);

  op.exec(method::lc_list_entries, in, &out, nullptr);
}

int cls_rgw_lc_list_decode(const bufferlist& out, std::vector<cls_rgw_lc_entry>& entries)
{
  cls_rgw_lc_list_entries_ret ret;
  try {
    auto iter = out.cbegin();
    decode(ret, iter);
  } catch (ceph::buffer::error& err) {
    return -EIO;
  }

  std::sort(std::begin(ret.entries), std::end(ret.entries),
	    [](const cls_rgw_lc_entry& a, const cls_rgw_lc_entry& b)
	      { return a.bucket < b.bucket; });
  entries = std::move(ret.entries);
  return 0;
}

void cls_rgw_mp_upload_part_info_update(librados::ObjectWriteOperation& op,
                                        const std::string& part_key,
                                        const RGWUploadPartInfo& info)
{
  cls_rgw_mp_upload_part_info_update_op call;
  call.part_key = part_key;
  call.info     = info;

  buffer::list in;
  encode(call, in);

  op.exec(method::mp_upload_part_info_update, in);
}

void cls_rgw_reshard_add(librados::ObjectWriteOperation& op,
			 const cls_rgw_reshard_entry& entry,
			 const bool create_only)
{
  bufferlist in;
  cls_rgw_reshard_add_op call;
  call.entry = entry;
  call.create_only = create_only;
  encode(call, in);
  op.exec(method::reshard_add, in);
}

int cls_rgw_reshard_list(librados::IoCtx& io_ctx, const string& oid, string& marker, uint32_t max,
                         list<cls_rgw_reshard_entry>& entries, bool* is_truncated)
{
  bufferlist in, out;
  cls_rgw_reshard_list_op call;
  call.marker = marker;
  call.max = max;
  encode(call, in);
  int r = io_ctx.exec(oid, method::reshard_list, in, out);
  if (r < 0)
    return r;

  cls_rgw_reshard_list_ret op_ret;
  auto iter = out.cbegin();
  try {
    decode(op_ret, iter);
  } catch (ceph::buffer::error& err) {
    return -EIO;
  }

  entries.swap(op_ret.entries);
  *is_truncated = op_ret.is_truncated;

  return 0;
}

int cls_rgw_reshard_get(librados::IoCtx& io_ctx, const string& oid, cls_rgw_reshard_entry& entry)
{
  bufferlist in, out;
  cls_rgw_reshard_get_op call;
  call.entry = entry;
  encode(call, in);
  int r = io_ctx.exec(oid, method::reshard_get, in, out);
  if (r < 0)
    return r;

  cls_rgw_reshard_get_ret op_ret;
  auto iter = out.cbegin();
  try {
    decode(op_ret, iter);
  } catch (ceph::buffer::error& err) {
    return -EIO;
  }

  entry = op_ret.entry;

  return 0;
}

void cls_rgw_reshard_remove(librados::ObjectWriteOperation& op, const cls_rgw_reshard_entry& entry)
{
  bufferlist in;
  cls_rgw_reshard_remove_op call;
  call.tenant = entry.tenant;
  call.bucket_name = entry.bucket_name;
  call.bucket_id = entry.bucket_id;
  encode(call, in);
  op.exec(method::reshard_remove, in);
}

void cls_rgw_clear_bucket_resharding(librados::ObjectWriteOperation& op)
{
  bufferlist in;
  cls_rgw_clear_bucket_resharding_op call;
  encode(call, in);
  op.exec(method::clear_bucket_resharding, in);
}

void cls_rgw_get_bucket_resharding(librados::ObjectReadOperation& op,
                                   bufferlist& out)
{
  bufferlist in;
  cls_rgw_get_bucket_resharding_op call;
  encode(call, in);
  op.exec(method::get_bucket_resharding, in, &out, nullptr);
}

void cls_rgw_get_bucket_resharding_decode(const bufferlist& out,
                                          cls_rgw_bucket_instance_entry& entry)
{
  cls_rgw_get_bucket_resharding_ret op_ret;
  auto iter = out.cbegin();
  decode(op_ret, iter);

  entry = std::move(op_ret.new_instance);
}

void cls_rgw_guard_bucket_resharding(librados::ObjectOperation& op, int ret_err)
{
  bufferlist in, out;
  cls_rgw_guard_bucket_resharding_op call;
  call.ret_err = ret_err;
  encode(call, in);
  op.exec(method::guard_bucket_resharding, in);
}

void cls_rgw_set_bucket_resharding(librados::ObjectWriteOperation& op,
                                   cls_rgw_reshard_status status)
{
  bufferlist in;
  cls_rgw_set_bucket_resharding_op call;
  call.entry.reshard_status = status;
  encode(call, in);

  op.assert_exists(); // the shard must exist; if not fail rather than recreate
  op.exec(method::set_bucket_resharding, in);
}
