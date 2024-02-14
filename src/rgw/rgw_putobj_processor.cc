// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "include/rados/librados.hpp"
#include "rgw_aio.h"
#include "rgw_putobj_processor.h"
#include "rgw_multi.h"
#include "rgw_compression.h"
#include "services/svc_sys_obj.h"
#include "services/svc_zone.h"
#include "rgw_sal_rados.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::putobj {

int HeadObjectProcessor::process(bufferlist&& data, uint64_t logical_offset)
{
  const bool flush = (data.length() == 0);

  // capture the first chunk for special handling
  if (data_offset < head_chunk_size || data_offset == 0) {
    if (flush) {
      // flush partial chunk
      return process_first_chunk(std::move(head_data), &processor);
    }

    auto remaining = head_chunk_size - data_offset;
    auto count = std::min<uint64_t>(data.length(), remaining);
    data.splice(0, count, &head_data);
    data_offset += count;

    if (data_offset == head_chunk_size) {
      // process the first complete chunk
      ceph_assert(head_data.length() == head_chunk_size);
      int r = process_first_chunk(std::move(head_data), &processor);
      if (r < 0) {
        return r;
      }
    }
    if (data.length() == 0) { // avoid flushing stripe processor
      return 0;
    }
  }
  ceph_assert(processor); // process_first_chunk() must initialize

  // send everything else through the processor
  auto write_offset = data_offset;
  data_offset += data.length();
  return processor->process(std::move(data), write_offset);
}


static int process_completed(const AioResultList& completed, RawObjSet *written)
{
  std::optional<int> error;
  for (auto& r : completed) {
    if (r.result >= 0) {
      written->insert(r.obj.get_ref().obj);
    } else if (!error) { // record first error code
      error = r.result;
    }
  }
  return error.value_or(0);
}

void RadosWriter::add_write_hint(librados::ObjectWriteOperation& op) {
  const rgw_obj obj = head_obj->get_obj();
  const RGWObjState *obj_state = obj_ctx.get_state(obj);
  const bool compressed = obj_state->compressed;
  uint32_t alloc_hint_flags = 0;
  if (compressed) {
    alloc_hint_flags |= librados::ALLOC_HINT_FLAG_INCOMPRESSIBLE;
  }

  op.set_alloc_hint2(0, 0, alloc_hint_flags);
}

void RadosWriter::set_head_obj(std::unique_ptr<rgw::sal::Object> head)
{
  head_obj = std::move(head);
}

int RadosWriter::set_stripe_obj(const rgw_raw_obj& raw_obj)
{
  stripe_obj = store->svc()->rados->obj(raw_obj);
  return stripe_obj.open(dpp);
}

int RadosWriter::process(bufferlist&& bl, uint64_t offset)
{
  bufferlist data = std::move(bl);
  const uint64_t cost = data.length();
  if (cost == 0) { // no empty writes, use aio directly for creates
    return 0;
  }
  librados::ObjectWriteOperation op;
  add_write_hint(op);
  if (offset == 0) {
    op.write_full(data);
  } else {
    op.write(offset, data);
  }
  constexpr uint64_t id = 0; // unused
  auto c = aio->get(stripe_obj, Aio::librados_op(std::move(op), y), cost, id);
  return process_completed(c, &written);
}

int RadosWriter::write_exclusive(const bufferlist& data)
{
  const uint64_t cost = data.length();

  librados::ObjectWriteOperation op;
  op.create(true); // exclusive create
  add_write_hint(op);
  op.write_full(data);

  constexpr uint64_t id = 0; // unused
  auto c = aio->get(stripe_obj, Aio::librados_op(std::move(op), y), cost, id);
  auto d = aio->drain();
  c.splice(c.end(), d);
  return process_completed(c, &written);
}

int RadosWriter::drain()
{
  return process_completed(aio->drain(), &written);
}

RadosWriter::~RadosWriter()
{
  // wait on any outstanding aio completions
  process_completed(aio->drain(), &written);

  bool need_to_remove_head = false;
  std::optional<rgw_raw_obj> raw_head;
  if (!rgw::sal::Object::empty(head_obj.get())) {
    raw_head.emplace();
    rgw::sal::RadosObject* obj = dynamic_cast<rgw::sal::RadosObject*>(head_obj.get());
    obj->get_raw_obj(&*raw_head);
  }

  /**
   * We should delete the object in the "multipart" namespace to avoid race condition.
   * Such race condition is caused by the fact that the multipart object is the gatekeeper of a multipart
   * upload, when it is deleted, a second upload would start with the same suffix("2/"), therefore, objects
   * written by the second upload may be deleted by the first upload.
   * details is describled on #11749
   *
   * The above comment still stands, but instead of searching for a specific object in the multipart
   * namespace, we just make sure that we remove the object that is marked as the head object after
   * we remove all the other raw objects. Note that we use different call to remove the head object,
   * as this one needs to go via the bucket index prepare/complete 2-phase commit scheme.
   */
  for (const auto& obj : written) {
    if (raw_head && obj == *raw_head) {
      ldpp_dout(dpp, 5) << "NOTE: we should not process the head object (" << obj << ") here" << dendl;
      need_to_remove_head = true;
      continue;
    }

    int r = store->delete_raw_obj(dpp, obj);
    if (r < 0 && r != -ENOENT) {
      ldpp_dout(dpp, 0) << "WARNING: failed to remove obj (" << obj << "), leaked" << dendl;
    }
  }

  if (need_to_remove_head) {
    std::string version_id;
    ldpp_dout(dpp, 5) << "NOTE: we are going to process the head obj (" << *raw_head << ")" << dendl;
    int r = head_obj->delete_object(dpp, &obj_ctx, null_yield);
    if (r < 0 && r != -ENOENT) {
      ldpp_dout(dpp, 0) << "WARNING: failed to remove obj (" << *raw_head << "), leaked" << dendl;
    }
  }
}


// advance to the next stripe
int ManifestObjectProcessor::next(uint64_t offset, uint64_t *pstripe_size)
{
  // advance the manifest
  int r = manifest_gen.create_next(offset);
  if (r < 0) {
    return r;
  }

  rgw_raw_obj stripe_obj = manifest_gen.get_cur_obj(store);

  uint64_t chunk_size = 0;
  r = store->get_raw_chunk_size(dpp, stripe_obj, &chunk_size);
  if (r < 0) {
    return r;
  }
  r = writer.set_stripe_obj(stripe_obj);
  if (r < 0) {
    return r;
  }

  chunk = ChunkProcessor(&writer, chunk_size);
  *pstripe_size = manifest_gen.cur_stripe_max_size();
  return 0;
}



int AtomicObjectProcessor::process_first_chunk(bufferlist&& data,
                                               DataProcessor **processor)
{
  first_chunk = std::move(data);
  *processor = &stripe;
  return 0;
}

int AtomicObjectProcessor::prepare(optional_yield y)
{
  uint64_t max_head_chunk_size;
  uint64_t head_max_size;
  uint64_t chunk_size = 0;
  uint64_t alignment;

  int r = dynamic_cast<rgw::sal::RadosObject*>(head_obj.get())->get_max_chunk_size(
				       dpp, head_obj->get_bucket()->get_placement_rule(),
				       &max_head_chunk_size, &alignment);
  if (r < 0) {
    return r;
  }

  bool same_pool = true;
  if (head_obj->get_bucket()->get_placement_rule() != tail_placement_rule) {
    if (!head_obj->placement_rules_match(head_obj->get_bucket()->get_placement_rule(), tail_placement_rule)) {
      same_pool = false;
      r = dynamic_cast<rgw::sal::RadosObject*>(head_obj.get())->get_max_chunk_size(dpp, tail_placement_rule, &chunk_size);
      if (r < 0) {
        return r;
      }
      head_max_size = 0;
    }
  }

  if (same_pool) {
    RGWZonePlacementInfo placement_info;
    if (!store->svc()->zone->get_zone_params().get_placement(head_obj->get_bucket()->get_placement_rule().name, &placement_info) || placement_info.inline_data) {
      head_max_size = max_head_chunk_size;
    } else {
      head_max_size = 0;
    }
    chunk_size = max_head_chunk_size;
  }

  uint64_t stripe_size;
  const uint64_t default_stripe_size = store->ctx()->_conf->rgw_obj_stripe_size;

  dynamic_cast<rgw::sal::RadosObject*>(head_obj.get())->get_max_aligned_size(
					default_stripe_size, alignment, &stripe_size);

  manifest.set_trivial_rule(head_max_size, stripe_size);

  rgw_obj obj = head_obj->get_obj();

  r = manifest_gen.create_begin(store->ctx(), &manifest,
                                head_obj->get_bucket()->get_placement_rule(),
                                &tail_placement_rule,
                                obj.bucket, obj);
  if (r < 0) {
    return r;
  }

  rgw_raw_obj stripe_obj = manifest_gen.get_cur_obj(store);

  r = writer.set_stripe_obj(stripe_obj);
  if (r < 0) {
    return r;
  }

  set_head_chunk_size(head_max_size);
  // initialize the processors
  chunk = ChunkProcessor(&writer, chunk_size);
  stripe = StripeProcessor(&chunk, this, head_max_size);
  return 0;
}

int AtomicObjectProcessor::complete(size_t accounted_size,
                                    const std::string& etag,
                                    ceph::real_time *mtime,
                                    ceph::real_time set_mtime,
                                    rgw::sal::Attrs& attrs,
                                    ceph::real_time delete_at,
                                    const char *if_match,
                                    const char *if_nomatch,
                                    const std::string *user_data,
                                    rgw_zone_set *zones_trace,
                                    bool *pcanceled, optional_yield y)
{
  int r = writer.drain();
  if (r < 0) {
    return r;
  }
  const uint64_t actual_size = get_actual_size();
  r = manifest_gen.create_next(actual_size);
  if (r < 0) {
    return r;
  }

  head_obj->set_atomic(&obj_ctx);

  RGWRados::Object op_target(store->getRados(),
		  head_obj->get_bucket()->get_info(),
		  obj_ctx, head_obj->get_obj());
  RGWRados::Object::Write obj_op(&op_target);

  /* some object types shouldn't be versioned, e.g., multipart parts */
  op_target.set_versioning_disabled(!head_obj->get_bucket()->versioning_enabled());
  obj_op.meta.data = &first_chunk;
  obj_op.meta.manifest = &manifest;
  obj_op.meta.ptag = &unique_tag; /* use req_id as operation tag */
  obj_op.meta.if_match = if_match;
  obj_op.meta.if_nomatch = if_nomatch;
  obj_op.meta.mtime = mtime;
  obj_op.meta.set_mtime = set_mtime;
  obj_op.meta.owner = owner;
  obj_op.meta.flags = PUT_OBJ_CREATE;
  obj_op.meta.olh_epoch = olh_epoch;
  obj_op.meta.delete_at = delete_at;
  obj_op.meta.user_data = user_data;
  obj_op.meta.zones_trace = zones_trace;
  obj_op.meta.modify_tail = true;

  r = obj_op.write_meta(dpp, actual_size, accounted_size, attrs, y);
  if (r < 0) {
    if (r == -ETIMEDOUT) {
      // The head object write may eventually succeed, clear the set of objects for deletion. if it
      // doesn't ever succeed, we'll orphan any tail objects as if we'd crashed before that write
      writer.clear_written();
    }
    return r;
  }
  if (!obj_op.meta.canceled) {
    // on success, clear the set of objects for deletion
    writer.clear_written();
  }
  if (pcanceled) {
    *pcanceled = obj_op.meta.canceled;
  }
  return 0;
}


int MultipartObjectProcessor::process_first_chunk(bufferlist&& data,
                                                  DataProcessor **processor)
{
  // write the first chunk of the head object as part of an exclusive create,
  // then drain to wait for the result in case of EEXIST
  int r = writer.write_exclusive(data);
  if (r == -EEXIST) {
    // randomize the oid prefix and reprepare the head/manifest
    std::string oid_rand = gen_rand_alphanumeric(store->ctx(), 32);

    mp.init(target_obj->get_name(), upload_id, oid_rand);
    manifest.set_prefix(target_obj->get_name() + "." + oid_rand);

    r = prepare_head();
    if (r < 0) {
      return r;
    }
    // resubmit the write op on the new head object
    r = writer.write_exclusive(data);
  }
  if (r < 0) {
    return r;
  }
  *processor = &stripe;
  return 0;
}

int MultipartObjectProcessor::prepare_head()
{
  const uint64_t default_stripe_size = store->ctx()->_conf->rgw_obj_stripe_size;
  uint64_t chunk_size;
  uint64_t stripe_size;
  uint64_t alignment;

  int r = dynamic_cast<rgw::sal::RadosObject*>(target_obj.get())->get_max_chunk_size(dpp,
					  tail_placement_rule, &chunk_size, &alignment);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: unexpected: get_max_chunk_size(): placement_rule=" << tail_placement_rule.to_str() << " obj=" << target_obj << " returned r=" << r << dendl;
    return r;
  }
  dynamic_cast<rgw::sal::RadosObject*>(target_obj.get())->get_max_aligned_size(
					default_stripe_size, alignment, &stripe_size);

  manifest.set_multipart_part_rule(stripe_size, part_num);

  r = manifest_gen.create_begin(store->ctx(), &manifest,
				head_obj->get_bucket()->get_placement_rule(),
				&tail_placement_rule,
				target_obj->get_bucket()->get_key(),
				target_obj->get_obj());
  if (r < 0) {
    return r;
  }

  rgw_raw_obj stripe_obj = manifest_gen.get_cur_obj(store);
  dynamic_cast<rgw::sal::RadosObject*>(head_obj.get())->raw_obj_to_obj(stripe_obj);
  head_obj->set_hash_source(target_obj->get_name());

  // point part uploads at the part head instead of the final multipart head
  writer.set_head_obj(head_obj->clone());

  r = writer.set_stripe_obj(stripe_obj);
  if (r < 0) {
    return r;
  }
  stripe_size = manifest_gen.cur_stripe_max_size();
  set_head_chunk_size(stripe_size);

  chunk = ChunkProcessor(&writer, chunk_size);
  stripe = StripeProcessor(&chunk, this, stripe_size);
  return 0;
}

int MultipartObjectProcessor::prepare(optional_yield y)
{
  manifest.set_prefix(target_obj->get_name() + "." + upload_id);

  return prepare_head();
}

int MultipartObjectProcessor::complete(size_t accounted_size,
                                       const std::string& etag,
                                       ceph::real_time *mtime,
                                       ceph::real_time set_mtime,
                                       std::map<std::string, bufferlist>& attrs,
                                       ceph::real_time delete_at,
                                       const char *if_match,
                                       const char *if_nomatch,
                                       const std::string *user_data,
                                       rgw_zone_set *zones_trace,
                                       bool *pcanceled, optional_yield y)
{
  int r = writer.drain();
  if (r < 0) {
    return r;
  }
  const uint64_t actual_size = get_actual_size();
  r = manifest_gen.create_next(actual_size);
  if (r < 0) {
    return r;
  }

  RGWRados::Object op_target(store->getRados(),
		  head_obj->get_bucket()->get_info(),
		  obj_ctx, head_obj->get_obj());
  RGWRados::Object::Write obj_op(&op_target);

  op_target.set_versioning_disabled(true);
  op_target.set_meta_placement_rule(&tail_placement_rule);
  obj_op.meta.set_mtime = set_mtime;
  obj_op.meta.mtime = mtime;
  obj_op.meta.owner = owner;
  obj_op.meta.delete_at = delete_at;
  obj_op.meta.zones_trace = zones_trace;
  obj_op.meta.modify_tail = true;

  r = obj_op.write_meta(dpp, actual_size, accounted_size, attrs, y);
  if (r < 0)
    return r;

  RGWUploadPartInfo info;
  string p = "part.";
  bool sorted_omap = is_v2_upload_id(upload_id);

  if (sorted_omap) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%08d", part_num);
    p.append(buf);
  } else {
    p.append(part_num_str);
  }
  info.num = part_num;
  info.etag = etag;
  info.size = actual_size;
  info.accounted_size = accounted_size;
  info.modified = real_clock::now();
  info.manifest = manifest;

  bool compressed;
  r = rgw_compression_info_from_attrset(attrs, compressed, info.cs_info);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "cannot get compression info" << dendl;
    return r;
  }

  std::unique_ptr<rgw::sal::Object> meta_obj =
    head_obj->get_bucket()->get_object(rgw_obj_key(mp.get_meta(), std::string(), RGW_OBJ_NS_MULTIPART));
  meta_obj->set_in_extra_data(true);

  rgw_raw_obj meta_raw_obj;
  store->getRados()->obj_to_raw(meta_obj->get_bucket()->get_placement_rule(), 
                                meta_obj->get_obj(),
                                &meta_raw_obj);
  rgw_rados_ref meta_obj_ref;
  r = store->getRados()->get_raw_obj_ref(dpp, meta_raw_obj, &meta_obj_ref);
  if (r < 0) {
    ldpp_dout(dpp, -1) << "ERROR: failed to get obj ref of meta obj with ret=" << r << dendl;
    return r;
  }

  librados::ObjectWriteOperation op;
  cls_rgw_mp_upload_part_info_update(op, p, info);
  r = rgw_rados_operate(dpp, meta_obj_ref.pool.ioctx(), meta_obj_ref.obj.oid, &op, y);
  ldpp_dout(dpp, 20) << "Update meta: " << meta_obj_ref.obj.oid << " part " << p << " prefix " << info.manifest.get_prefix() << " return " << r << dendl;

  if (r == -EOPNOTSUPP) {
    // New CLS call to update part info is not yet supported. Fall back to the old handling.
    bufferlist bl;
    encode(info, bl);
    r = meta_obj->omap_set_val_by_key(dpp, p, bl, true, null_yield);
  }
  if (r < 0) {
    return r == -ENOENT ? -ERR_NO_SUCH_UPLOAD : r;
  }

  if (!obj_op.meta.canceled) {
    // on success, clear the set of objects for deletion
    writer.clear_written();
  }
  if (pcanceled) {
    *pcanceled = obj_op.meta.canceled;
  }
  return 0;
}

int AppendObjectProcessor::process_first_chunk(bufferlist &&data, rgw::sal::DataProcessor **processor)
{
  int r = writer.write_exclusive(data);
  if (r < 0) {
    return r;
  }
  *processor = &stripe;
  return 0;
}

int AppendObjectProcessor::prepare(optional_yield y)
{
  RGWObjState *astate;
  int r = head_obj->get_obj_state(dpp, &obj_ctx, &astate, y);
  if (r < 0) {
    return r;
  }
  cur_size = astate->size;
  *cur_accounted_size = astate->accounted_size;
  if (!astate->exists) {
    if (position != 0) {
      ldpp_dout(dpp, 5) << "ERROR: Append position should be zero" << dendl;
      return -ERR_POSITION_NOT_EQUAL_TO_LENGTH;
    } else {
      cur_part_num = 1;
      //set the prefix
      char buf[33];
      gen_rand_alphanumeric(store->ctx(), buf, sizeof(buf) - 1);
      string oid_prefix = head_obj->get_name();
      oid_prefix.append(".");
      oid_prefix.append(buf);
      oid_prefix.append("_");
      manifest.set_prefix(oid_prefix);
    }
  } else {
    // check whether the object appendable
    map<string, bufferlist>::iterator iter = astate->attrset.find(RGW_ATTR_APPEND_PART_NUM);
    if (iter == astate->attrset.end()) {
      ldpp_dout(dpp, 5) << "ERROR: The object is not appendable" << dendl;
      return -ERR_OBJECT_NOT_APPENDABLE;
    }
    if (position != *cur_accounted_size) {
      ldpp_dout(dpp, 5) << "ERROR: Append position should be equal to the obj size" << dendl;
      return -ERR_POSITION_NOT_EQUAL_TO_LENGTH;
    }
    try {
      using ceph::decode;
      decode(cur_part_num, iter->second);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 5) << "ERROR: failed to decode part num" << dendl;
      return -EIO;
    }
    cur_part_num++;
    //get the current obj etag
    iter = astate->attrset.find(RGW_ATTR_ETAG);
    if (iter != astate->attrset.end()) {
      string s = rgw_string_unquote(iter->second.c_str());
      size_t pos = s.find("-");
      cur_etag = s.substr(0, pos);
    }

    iter = astate->attrset.find(RGW_ATTR_STORAGE_CLASS);
    if (iter != astate->attrset.end()) {
      tail_placement_rule.storage_class = iter->second.to_str();
    }
    cur_manifest = &(*astate->manifest);
    manifest.set_prefix(cur_manifest->get_prefix());
    astate->keep_tail = true;
  }
  manifest.set_multipart_part_rule(store->ctx()->_conf->rgw_obj_stripe_size, cur_part_num);

  rgw_obj obj = head_obj->get_obj();

  r = manifest_gen.create_begin(store->ctx(), &manifest, head_obj->get_bucket()->get_placement_rule(), &tail_placement_rule, obj.bucket, obj);
  if (r < 0) {
    return r;
  }
  rgw_raw_obj stripe_obj = manifest_gen.get_cur_obj(store);

  uint64_t chunk_size = 0;
  r = store->get_raw_chunk_size(dpp, stripe_obj, &chunk_size);
  if (r < 0) {
    return r;
  }
  r = writer.set_stripe_obj(std::move(stripe_obj));
  if (r < 0) {
    return r;
  }

  uint64_t stripe_size = manifest_gen.cur_stripe_max_size();

  uint64_t max_head_size = std::min(chunk_size, stripe_size);
  set_head_chunk_size(max_head_size);

  // initialize the processors
  chunk = ChunkProcessor(&writer, chunk_size);
  stripe = StripeProcessor(&chunk, this, stripe_size);

  return 0;
}

int AppendObjectProcessor::complete(size_t accounted_size, const string &etag, ceph::real_time *mtime,
                                    ceph::real_time set_mtime, rgw::sal::Attrs& attrs,
                                    ceph::real_time delete_at, const char *if_match, const char *if_nomatch,
                                    const string *user_data, rgw_zone_set *zones_trace, bool *pcanceled,
                                    optional_yield y)
{
  int r = writer.drain();
  if (r < 0)
    return r;
  const uint64_t actual_size = get_actual_size();
  r = manifest_gen.create_next(actual_size);
  if (r < 0) {
    return r;
  }
  head_obj->set_atomic(&obj_ctx);
  RGWRados::Object op_target(store->getRados(),
		  head_obj->get_bucket()->get_info(),
		  obj_ctx, head_obj->get_obj());
  RGWRados::Object::Write obj_op(&op_target);
  //For Append obj, disable versioning
  op_target.set_versioning_disabled(true);
  if (cur_manifest) {
    cur_manifest->append(dpp, manifest, store->get_zone());
    obj_op.meta.manifest = cur_manifest;
  } else {
    obj_op.meta.manifest = &manifest;
  }
  obj_op.meta.ptag = &unique_tag; /* use req_id as operation tag */
  obj_op.meta.mtime = mtime;
  obj_op.meta.set_mtime = set_mtime;
  obj_op.meta.owner = owner;
  obj_op.meta.flags = PUT_OBJ_CREATE;
  obj_op.meta.delete_at = delete_at;
  obj_op.meta.user_data = user_data;
  obj_op.meta.zones_trace = zones_trace;
  obj_op.meta.modify_tail = true;
  obj_op.meta.appendable = true;
  //Add the append part number
  bufferlist cur_part_num_bl;
  using ceph::encode;
  encode(cur_part_num, cur_part_num_bl);
  attrs[RGW_ATTR_APPEND_PART_NUM] = cur_part_num_bl;
  //calculate the etag
  if (!cur_etag.empty()) {
    MD5 hash;
    // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
    hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
    char petag[CEPH_CRYPTO_MD5_DIGESTSIZE];
    char final_etag[CEPH_CRYPTO_MD5_DIGESTSIZE];
    char final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 16];
    hex_to_buf(cur_etag.c_str(), petag, CEPH_CRYPTO_MD5_DIGESTSIZE);
    hash.Update((const unsigned char *)petag, sizeof(petag));
    hex_to_buf(etag.c_str(), petag, CEPH_CRYPTO_MD5_DIGESTSIZE);
    hash.Update((const unsigned char *)petag, sizeof(petag));
    hash.Final((unsigned char *)final_etag);
    buf_to_hex((unsigned char *)final_etag, sizeof(final_etag), final_etag_str);
    snprintf(&final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2],  sizeof(final_etag_str) - CEPH_CRYPTO_MD5_DIGESTSIZE * 2,
             "-%lld", (long long)cur_part_num);
    bufferlist etag_bl;
    etag_bl.append(final_etag_str, strlen(final_etag_str) + 1);
    attrs[RGW_ATTR_ETAG] = etag_bl;
  }
  r = obj_op.write_meta(dpp, actual_size + cur_size,
			accounted_size + *cur_accounted_size,
			attrs, y);
  if (r < 0) {
    return r;
  }
  if (!obj_op.meta.canceled) {
    // on success, clear the set of objects for deletion
    writer.clear_written();
  }
  if (pcanceled) {
    *pcanceled = obj_op.meta.canceled;
  }
  *cur_accounted_size += accounted_size;

  return 0;
}

} // namespace rgw::putobj
