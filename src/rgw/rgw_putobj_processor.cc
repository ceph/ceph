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

#include "rgw_aio.h"
#include "rgw_putobj_processor.h"
#include "rgw_multi.h"
#include "rgw_compression.h"
#include "services/svc_sys_obj.h"
#include "rgw_sal_rados.h"

#define dout_subsys ceph_subsys_rgw

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
  r = writer->set_stripe_obj(stripe_obj);
  if (r < 0) {
    return r;
  }

  chunk = ChunkProcessor(writer.get(), chunk_size);
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

  int r = head_obj->get_max_chunk_size(dpp, bucket->get_placement_rule(),
				       &max_head_chunk_size, &alignment);
  if (r < 0) {
    return r;
  }

  bool same_pool = true;
  if (bucket->get_placement_rule() != tail_placement_rule) {
    if (!head_obj->placement_rules_match(bucket->get_placement_rule(), tail_placement_rule)) {
      same_pool = false;
      r = head_obj->get_max_chunk_size(dpp, tail_placement_rule, &chunk_size);
      if (r < 0) {
        return r;
      }
      head_max_size = 0;
    }
  }

  if (same_pool) {
    head_max_size = max_head_chunk_size;
    chunk_size = max_head_chunk_size;
  }

  uint64_t stripe_size;
  const uint64_t default_stripe_size = store->ctx()->_conf->rgw_obj_stripe_size;

  head_obj->get_max_aligned_size(default_stripe_size, alignment, &stripe_size);

  manifest.set_trivial_rule(head_max_size, stripe_size);

  rgw_obj obj = head_obj->get_obj();

  r = manifest_gen.create_begin(store->ctx(), &manifest,
                                bucket->get_placement_rule(),
                                &tail_placement_rule,
                                obj.bucket, obj);
  if (r < 0) {
    return r;
  }

  rgw_raw_obj stripe_obj = manifest_gen.get_cur_obj(store);

  r = writer->set_stripe_obj(stripe_obj);
  if (r < 0) {
    return r;
  }

  set_head_chunk_size(head_max_size);
  // initialize the processors
  chunk = ChunkProcessor(writer.get(), chunk_size);
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
  int r = writer->drain();
  if (r < 0) {
    return r;
  }
  const uint64_t actual_size = get_actual_size();
  r = manifest_gen.create_next(actual_size);
  if (r < 0) {
    return r;
  }

  head_obj->set_atomic(&obj_ctx);

  std::unique_ptr<rgw::sal::Object::WriteOp> obj_op = head_obj->get_write_op(&obj_ctx);

  /* some object types shouldn't be versioned, e.g., multipart parts */
  obj_op->params.versioning_disabled = !bucket->versioning_enabled();
  obj_op->params.data = &first_chunk;
  obj_op->params.manifest = &manifest;
  obj_op->params.ptag = &unique_tag; /* use req_id as operation tag */
  obj_op->params.if_match = if_match;
  obj_op->params.if_nomatch = if_nomatch;
  obj_op->params.mtime = mtime;
  obj_op->params.set_mtime = set_mtime;
  obj_op->params.owner = ACLOwner(owner);
  obj_op->params.flags = PUT_OBJ_CREATE;
  obj_op->params.olh_epoch = olh_epoch;
  obj_op->params.delete_at = delete_at;
  obj_op->params.user_data = user_data;
  obj_op->params.zones_trace = zones_trace;
  obj_op->params.modify_tail = true;
  obj_op->params.attrs = &attrs;

  r = obj_op->prepare(y);
  if (r < 0) {
    return r;
  }

  r = obj_op->write_meta(dpp, actual_size, accounted_size, y);
  if (r < 0) {
    return r;
  }
  if (!obj_op->params.canceled) {
    // on success, clear the set of objects for deletion
    writer->clear_written();
  }
  if (pcanceled) {
    *pcanceled = obj_op->params.canceled;
  }
  return 0;
}


int MultipartObjectProcessor::process_first_chunk(bufferlist&& data,
                                                  DataProcessor **processor)
{
  // write the first chunk of the head object as part of an exclusive create,
  // then drain to wait for the result in case of EEXIST
  int r = writer->write_exclusive(data);
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
    r = writer->write_exclusive(data);
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

  int r = target_obj->get_max_chunk_size(dpp, tail_placement_rule, &chunk_size, &alignment);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: unexpected: get_max_chunk_size(): placement_rule=" << tail_placement_rule.to_str() << " obj=" << target_obj << " returned r=" << r << dendl;
    return r;
  }
  target_obj->get_max_aligned_size(default_stripe_size, alignment, &stripe_size);

  manifest.set_multipart_part_rule(stripe_size, part_num);

  r = manifest_gen.create_begin(store->ctx(), &manifest,
				bucket->get_placement_rule(),
				&tail_placement_rule,
				target_obj->get_bucket()->get_key(),
				target_obj->get_obj());
  if (r < 0) {
    return r;
  }

  rgw_raw_obj stripe_obj = manifest_gen.get_cur_obj(store);
  head_obj->raw_obj_to_obj(stripe_obj);
  head_obj->set_hash_source(target_obj->get_name());

  r = writer->set_stripe_obj(stripe_obj);
  if (r < 0) {
    return r;
  }
  stripe_size = manifest_gen.cur_stripe_max_size();
  set_head_chunk_size(stripe_size);

  chunk = ChunkProcessor(writer.get(), chunk_size);
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
  int r = writer->drain();
  if (r < 0) {
    return r;
  }
  const uint64_t actual_size = get_actual_size();
  r = manifest_gen.create_next(actual_size);
  if (r < 0) {
    return r;
  }

  std::unique_ptr<rgw::sal::Object::WriteOp> obj_op = head_obj->get_write_op(&obj_ctx);

  obj_op->params.versioning_disabled = true;
  obj_op->params.set_mtime = set_mtime;
  obj_op->params.mtime = mtime;
  obj_op->params.owner = ACLOwner(owner);
  obj_op->params.delete_at = delete_at;
  obj_op->params.zones_trace = zones_trace;
  obj_op->params.modify_tail = true;
  obj_op->params.attrs = &attrs;
  obj_op->params.pmeta_placement_rule = &tail_placement_rule;
  r = obj_op->prepare(y);
  if (r < 0) {
    return r;
  }

  r = obj_op->write_meta(dpp, actual_size, accounted_size, y);
  if (r < 0)
    return r;

  bufferlist bl;
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

  encode(info, bl);

  std::unique_ptr<rgw::sal::Object> meta_obj =
    bucket->get_object(rgw_obj_key(mp.get_meta(), std::string(), RGW_OBJ_NS_MULTIPART));
  meta_obj->set_in_extra_data(true);

  r = meta_obj->omap_set_val_by_key(dpp, p, bl, true, null_yield);
  if (r < 0) {
    return r == -ENOENT ? -ERR_NO_SUCH_UPLOAD : r;
  }

  if (!obj_op->params.canceled) {
    // on success, clear the set of objects for deletion
    writer->clear_written();
  }
  if (pcanceled) {
    *pcanceled = obj_op->params.canceled;
  }
  return 0;
}

int AppendObjectProcessor::process_first_chunk(bufferlist &&data, rgw::putobj::DataProcessor **processor)
{
  int r = writer->write_exclusive(data);
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

  r = manifest_gen.create_begin(store->ctx(), &manifest, bucket->get_placement_rule(), &tail_placement_rule, obj.bucket, obj);
  if (r < 0) {
    return r;
  }
  rgw_raw_obj stripe_obj = manifest_gen.get_cur_obj(store);

  uint64_t chunk_size = 0;
  r = store->get_raw_chunk_size(dpp, stripe_obj, &chunk_size);
  if (r < 0) {
    return r;
  }
  r = writer->set_stripe_obj(std::move(stripe_obj));
  if (r < 0) {
    return r;
  }

  uint64_t stripe_size = manifest_gen.cur_stripe_max_size();

  uint64_t max_head_size = std::min(chunk_size, stripe_size);
  set_head_chunk_size(max_head_size);

  // initialize the processors
  chunk = ChunkProcessor(writer.get(), chunk_size);
  stripe = StripeProcessor(&chunk, this, stripe_size);

  return 0;
}

int AppendObjectProcessor::complete(size_t accounted_size, const string &etag, ceph::real_time *mtime,
                                    ceph::real_time set_mtime, rgw::sal::Attrs& attrs,
                                    ceph::real_time delete_at, const char *if_match, const char *if_nomatch,
                                    const string *user_data, rgw_zone_set *zones_trace, bool *pcanceled,
                                    optional_yield y)
{
  int r = writer->drain();
  if (r < 0)
    return r;
  const uint64_t actual_size = get_actual_size();
  r = manifest_gen.create_next(actual_size);
  if (r < 0) {
    return r;
  }
  head_obj->set_atomic(&obj_ctx);
  std::unique_ptr<rgw::sal::Object::WriteOp> obj_op = head_obj->get_write_op(&obj_ctx);
  //For Append obj, disable versioning
  obj_op->params.versioning_disabled = true;
  if (cur_manifest) {
    cur_manifest->append(dpp, manifest, store->get_zone());
    obj_op->params.manifest = cur_manifest;
  } else {
    obj_op->params.manifest = &manifest;
  }
  obj_op->params.ptag = &unique_tag; /* use req_id as operation tag */
  obj_op->params.mtime = mtime;
  obj_op->params.set_mtime = set_mtime;
  obj_op->params.owner = ACLOwner(owner);
  obj_op->params.flags = PUT_OBJ_CREATE;
  obj_op->params.delete_at = delete_at;
  obj_op->params.user_data = user_data;
  obj_op->params.zones_trace = zones_trace;
  obj_op->params.modify_tail = true;
  obj_op->params.appendable = true;
  obj_op->params.attrs = &attrs;
  //Add the append part number
  bufferlist cur_part_num_bl;
  using ceph::encode;
  encode(cur_part_num, cur_part_num_bl);
  attrs[RGW_ATTR_APPEND_PART_NUM] = cur_part_num_bl;
  //calculate the etag
  if (!cur_etag.empty()) {
    MD5 hash;
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
  r = obj_op->prepare(y);
  if (r < 0) {
    return r;
  }
  r = obj_op->write_meta(dpp, actual_size + cur_size, accounted_size + *cur_accounted_size, y);
  if (r < 0) {
    return r;
  }
  if (!obj_op->params.canceled) {
    // on success, clear the set of objects for deletion
    writer->clear_written();
  }
  if (pcanceled) {
    *pcanceled = obj_op->params.canceled;
  }
  *cur_accounted_size += accounted_size;

  return 0;
}

} // namespace rgw::putobj
